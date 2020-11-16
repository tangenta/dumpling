package export

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/storage"

	"github.com/pingcap/dumpling/v4/log"
)

const lengthLimit = 1048576

// TODO make this configurable, 5 mb is a good minimum size but on low latency/high bandwidth network you can go a lot bigger
const hardcodedS3ChunkSize = 5 * 1024 * 1024

var pool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

type writerPipe struct {
	input  chan *bytes.Buffer
	closed chan struct{}
	errCh  chan error

	currentFileSize      uint64
	currentStatementSize uint64

	fileSizeLimit      uint64
	statementSizeLimit uint64

	w storage.Writer
}

func newWriterPipe(w storage.Writer, fileSizeLimit, statementSizeLimit uint64) *writerPipe {
	return &writerPipe{
		input:  make(chan *bytes.Buffer, 8),
		closed: make(chan struct{}),
		errCh:  make(chan error, 1),
		w:      w,

		currentFileSize:      0,
		currentStatementSize: 0,
		fileSizeLimit:        fileSizeLimit,
		statementSizeLimit:   statementSizeLimit,
	}
}

func (b *writerPipe) Run(ctx context.Context) {
	defer close(b.closed)
	var errOccurs bool
	for {
		select {
		case s, ok := <-b.input:
			if !ok {
				return
			}
			if errOccurs {
				continue
			}
			err := writeBytes(ctx, b.w, s.Bytes())
			s.Reset()
			pool.Put(s)
			if err != nil {
				errOccurs = true
				b.errCh <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *writerPipe) AddFileSize(fileSize uint64) {
	b.currentFileSize += fileSize
	b.currentStatementSize += fileSize
}

func (b *writerPipe) Error() error {
	select {
	case err := <-b.errCh:
		return err
	default:
		return nil
	}
}

func (b *writerPipe) ShouldSwitchFile() bool {
	return b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit
}

func (b *writerPipe) ShouldSwitchStatement() bool {
	return (b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit) ||
		(b.statementSizeLimit != UnspecifiedSize && b.currentStatementSize >= b.statementSizeLimit)
}

func WriteMeta(ctx context.Context, meta MetaIR, w storage.Writer) error {
	log.Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(ctx, w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(ctx, w, meta.MetaSQL()); err != nil {
		return err
	}

	log.Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

func WriteInsert(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, cfg.FileSize, cfg.StatementSize)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		bf.WriteString(specCmtIter.Next())
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	var (
		insertStatementPrefix string
		row                   = MakeRowReceiver(meta.ColumnTypes())
		counter               = 0
		escapeBackSlash       = cfg.EscapeBackslash
		err                   error
	)

	selectedField := meta.SelectedField()
	// if has generated column
	if selectedField != "" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s %s VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())), selectedField)
	} else {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())))
	}
	insertStatementPrefixLen := uint64(len(insertStatementPrefix))

	for fileRowIter.HasNext() {
		wp.currentStatementSize = 0
		bf.WriteString(insertStatementPrefix)
		wp.AddFileSize(insertStatementPrefixLen)

		for fileRowIter.HasNext() {
			if err = fileRowIter.Decode(row); err != nil {
				log.Error("scanning from sql.Row failed", zap.Error(err))
				return err
			}

			lastBfSize := bf.Len()
			row.WriteToBuffer(bf, escapeBackSlash)
			counter += 1
			wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"

			fileRowIter.Next()
			shouldSwitch := wp.ShouldSwitchStatement()
			if fileRowIter.HasNext() && !shouldSwitch {
				bf.WriteString(",\n")
			} else {
				bf.WriteString(";\n")
			}
			if bf.Len() >= lengthLimit {
				wp.input <- bf
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < lengthLimit {
					bf.Grow(lengthLimit - bfCap)
				}
			}

			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case err := <-wp.errCh:
				return err
			default:
			}

			if shouldSwitch {
				break
			}
		}
		if wp.ShouldSwitchFile() {
			break
		}
	}
	log.Debug("dumping table",
		zap.String("table", meta.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func WriteInsertInCsv(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	opt := &csvOption{
		nullValue: cfg.CsvNullValue,
		separator: []byte(cfg.CsvSeparator),
		delimiter: []byte(cfg.CsvDelimiter),
	}

	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, cfg.FileSize, UnspecifiedSize)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var (
		row             = MakeRowReceiver(meta.ColumnTypes())
		counter         = 0
		escapeBackSlash = cfg.EscapeBackslash
		err             error
	)

	if !cfg.NoHeader && len(meta.ColumnNames()) != 0 {
		for i, col := range meta.ColumnNames() {
			bf.Write(opt.delimiter)
			escape([]byte(col), bf, getEscapeQuotation(escapeBackSlash, opt.delimiter))
			bf.Write(opt.delimiter)
			if i != len(meta.ColumnTypes())-1 {
				bf.Write(opt.separator)
			}
		}
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	for fileRowIter.HasNext() {
		if err = fileRowIter.Decode(row); err != nil {
			log.Error("scanning from sql.Row failed", zap.Error(err))
			return err
		}

		lastBfSize := bf.Len()
		row.WriteToBufferInCsv(bf, escapeBackSlash, opt)
		counter += 1
		wp.currentFileSize += uint64(bf.Len()-lastBfSize) + 1 // 1 is for "\n"

		bf.WriteByte('\n')
		if bf.Len() >= lengthLimit {
			wp.input <- bf
			bf = pool.Get().(*bytes.Buffer)
			if bfCap := bf.Cap(); bfCap < lengthLimit {
				bf.Grow(lengthLimit - bfCap)
			}
		}

		fileRowIter.Next()
		select {
		case <-pCtx.Done():
			return pCtx.Err()
		case err := <-wp.errCh:
			return err
		default:
		}
		if wp.ShouldSwitchFile() {
			break
		}
	}

	log.Debug("dumping table",
		zap.String("table", meta.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func write(ctx context.Context, writer storage.Writer, str string) error {
	_, err := writer.Write(ctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(str)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.String("string", str[:outputLength]),
			zap.Error(err))
	}
	return err
}

func writeBytes(ctx context.Context, writer storage.Writer, p []byte) error {
	_, err := writer.Write(ctx, p)
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(p)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.ByteString("string", p[:outputLength]),
			zap.String("writer", fmt.Sprintf("%#v", writer)),
			zap.Error(err))
	}
	return err
}

func buildFileWriter(ctx context.Context, s storage.ExternalStorage, path string) (storage.Writer, func(ctx context.Context), error) {
	fullPath := s.URI() + path
	uploader, err := s.CreateUploader(ctx, path)
	if err != nil {
		log.Error("open file failed",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, err
	}
	writer := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
	log.Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) {
		err := writer.Close(ctx)
		if err == nil {
			return
		}
		log.Error("close file failed",
			zap.String("path", fullPath),
			zap.Error(err))
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(s storage.ExternalStorage, path string) (storage.Writer, func(context.Context)) {
	var writer storage.Writer
	fullPath := s.URI() + path
	fileWriter := &InterceptFileWriter{}
	initRoutine := func(ctx context.Context) error {
		uploader, err := s.CreateUploader(ctx, path)
		if err != nil {
			log.Error("open file failed",
				zap.String("path", fullPath),
				zap.Error(err))
			return err
		}
		w := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
		writer = w
		log.Debug("opened file", zap.String("path", fullPath))
		fileWriter.Writer = writer
		return err
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) {
		if writer == nil {
			return
		}
		log.Debug("tear down lazy file writer...")
		err := writer.Close(ctx)
		if err != nil {
			log.Error("close file failed", zap.String("path", fullPath))
		}
	}
	return fileWriter, tearDownRoutine
}

// InterceptFileWriter is an interceptor of os.File,
// tracking whether a StringWriter has written something.
type InterceptFileWriter struct {
	storage.Writer
	sync.Once
	initRoutine func(context.Context) error
	err         error

	SomethingIsWritten bool
}

func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine(ctx) })
	if len(p) > 0 {
		w.SomethingIsWritten = true
	}
	if w.err != nil {
		return 0, fmt.Errorf("open file error: %s", w.err.Error())
	}
	return w.Writer.Write(ctx, p)
}

func (w *InterceptFileWriter) Close(ctx context.Context) error {
	return w.Writer.Close(ctx)
}

func wrapBackTicks(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasSuffix(identifier, "`") {
		return wrapStringWith(identifier, "`")
	}
	return identifier
}

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}

// FileFormat is the format that output to file. Currently we support SQL text and CSV file format.
type FileFormat int32

const (
	FileFormatSQLText FileFormat = iota
	FileFormatCSV
)

// String implement Stringer.String method.
func (f FileFormat) String() string {
	switch f {
	case FileFormatSQLText:
		return "text"
	case FileFormatCSV:
		return "CSV"
	default:
		return "unknown"
	}
}

// Extension returns the extension for specific format.
//  text -> ".sql"
//  csv  -> ".csv"
func (f FileFormat) Extension() string {
	switch f {
	case FileFormatSQLText:
		return ".sql"
	case FileFormatCSV:
		return ".csv"
	default:
		return ".unknown_format"
	}
}

func (f FileFormat) WriteInsert(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	switch f {
	case FileFormatSQLText:
		return WriteInsert(pCtx, cfg, meta, tblIR, w)
	case FileFormatCSV:
		return WriteInsertInCsv(pCtx, cfg, meta, tblIR, w)
	default:
		log.Warn("unknown file format", zap.Int32("FileFormat", int32(f)))
		return nil
	}
}
