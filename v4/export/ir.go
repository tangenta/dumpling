package export

import (
	"bytes"
	"context"
	"database/sql"
)

// TableDataIR is table data intermediate representation.
// A table may be split into multiple TableDataIRs.
type TableDataIR interface {
	Start(context.Context, *sql.Conn) error
	Rows() SQLRowIter
}

// TableMeta contains the meta information of a table.
type TableMeta interface {
	DatabaseName() string
	TableName() string
	ColumnCount() uint
	ColumnTypes() []string
	ColumnNames() []string
	SelectedField() string
	SpecialComments() StringIter
	ShowCreateTable() string
	ShowCreateView() string
}

// SQLRowIter is the iterator on a collection of sql.Row.
type SQLRowIter interface {
	Decode(RowReceiver) error
	Next()
	Error() error
	HasNext() bool
	// release SQLRowIter
	Close() error
}

type RowReceiverStringer interface {
	RowReceiver
	Stringer
}

type Stringer interface {
	WriteToBuffer(*bytes.Buffer, bool)
	WriteToBufferInCsv(*bytes.Buffer, bool, *csvOption)
}

type RowReceiver interface {
	BindAddress([]interface{})
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row RowReceiver) error {
	row.BindAddress(args)
	if err := rows.Scan(args...); err != nil {
		rows.Close()
		return withStack(err)
	}
	return nil
}

// StringIter is the iterator on a collection of strings.
type StringIter interface {
	Next() string
	HasNext() bool
}

type MetaIR interface {
	SpecialComments() StringIter
	TargetName() string
	MetaSQL() string
}
