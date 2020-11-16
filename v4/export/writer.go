package export

import (
	"bytes"
	"context"
	"strings"
	"text/template"

	"github.com/pingcap/br/pkg/storage"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteViewMeta(ctx context.Context, db, table, createTableSQL, createViewSQL string) error
	WriteTableData(ctx context.Context, meta TableMeta, irStream <-chan TableDataIR) error
}

type FileWriter struct {
	cfg        *Config
	cntPool    *connectionsPool
	fileFmt    FileFormat
	extStorage storage.ExternalStorage
}

func NewFileWriter(config *Config, pool *connectionsPool, externalStore storage.ExternalStorage) *FileWriter {
	sw := &FileWriter{
		cfg:        config,
		cntPool:    pool,
		extStorage: externalStore,
	}
	switch strings.ToLower(config.FileType) {
	case "sql":
		sw.fileFmt = FileFormatSQLText
	case "csv":
		sw.fileFmt = FileFormatCSV
	}
	return sw
}

func (f *FileWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db}).render(f.cfg.OutputFileTemplate, outputFileTemplateSchema)
	if err != nil {
		return err
	}
	return f.writeMetaToFile(ctx, db, createSQL, fileName+".sql")
}

func (f *FileWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db, Table: table}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	return f.writeMetaToFile(ctx, db, createSQL, fileName+".sql")
}

func (f *FileWriter) WriteViewMeta(ctx context.Context, db, view, createTableSQL, createViewSQL string) error {
	fileNameTable, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	fileNameView, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateView)
	if err != nil {
		return err
	}
	err = f.writeMetaToFile(ctx, db, createTableSQL, fileNameTable+".sql")
	if err != nil {
		return err
	}
	return f.writeMetaToFile(ctx, db, createViewSQL, fileNameView+".sql")
}

func (f *FileWriter) WriteTableData(ctx context.Context, meta TableMeta, irStream <-chan TableDataIR) error {
	if irStream == nil {
		return nil
	}
	log.Debug("start dumping table...",
		zap.String("table", meta.TableName()),
		zap.Stringer("format", f.fileFmt))
	chunkIndex := 0
	channelClosed := false
	for !channelClosed {
		select {
		case <-ctx.Done():
			log.Info("context has been done",
				zap.String("table", meta.TableName()),
				zap.Stringer("format", f.fileFmt))
			return nil
		case ir, ok := <-irStream:
			if !ok {
				channelClosed = true
				break
			}
			conn := f.cntPool.getConn()
			err := ir.Start(ctx, conn)
			if err != nil {
				return err
			}
			err = f.writeTableData(ctx, meta, ir, chunkIndex)
			if err != nil {
				f.cntPool.releaseConn(conn)
				return err
			}
			chunkIndex++
			f.cntPool.releaseConn(conn)
		}
	}
	log.Debug("dumping table successfully",
		zap.String("table", meta.TableName()))
	return nil
}

func (f *FileWriter) writeTableData(ctx context.Context, meta TableMeta, ir TableDataIR, curChkIdx int) error {
	conf, format := f.cfg, f.fileFmt
	namer := newOutputFileNamer(meta, curChkIdx)
	fileName, err := namer.NextName(conf.OutputFileTemplate)
	if err != nil {
		return err
	}
	fileName += format.Extension()

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.extStorage, fileName)
		err = format.WriteInsert(ctx, conf, meta, ir, fileWriter)
		tearDown(ctx)
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		if conf.FileSize == UnspecifiedSize {
			break
		}
		fileName, err = namer.NextName(conf.OutputFileTemplate)
		if err != nil {
			return err
		}
		fileName += format.Extension()
	}

	return ir.Rows().Close()
}

func (f *FileWriter) writeMetaToFile(ctx context.Context, target, metaSQL string, path string) error {
	fileWriter, tearDown, err := buildFileWriter(ctx, f.extStorage, path)
	if err != nil {
		return err
	}
	defer tearDown(ctx)

	return WriteMeta(ctx, &metaData{
		target:  target,
		metaSQL: metaSQL,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, fileWriter)
}

type outputFileNamer struct {
	Index int
	DB    string
	Table string
}

type csvOption struct {
	nullValue string
	separator []byte
	delimiter []byte
}

func newOutputFileNamer(ir TableMeta, chunkIndex int) *outputFileNamer {
	return &outputFileNamer{
		Index: chunkIndex,
		DB:    ir.DatabaseName(),
		Table: ir.TableName(),
	}
}

func (namer *outputFileNamer) render(tmpl *template.Template, subName string) (string, error) {
	var bf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&bf, subName, namer); err != nil {
		return "", err
	}
	return bf.String(), nil
}

func (namer *outputFileNamer) NextName(tmpl *template.Template) (string, error) {
	res, err := namer.render(tmpl, outputFileTemplateData)
	namer.Index++
	return res, err
}
