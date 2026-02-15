package QE

import (
	"errors"
)

var (
	ErrNoTable      = errors.New("no such table")
	ErrNoColumn     = errors.New("no such column")
	ErrTypeMismatch = errors.New("type mismatch")
)

type QueryEngine struct {
	vm       *VM
	tables   map[string]*TableReader
	cursors  map[int]*Cursor
	cursorID int
}

type TableReader struct {
	Name   string
	btree  interface{}
	schema map[string]ColumnType
}

type ColumnType struct {
	Name string
	Type string
}

type Cursor struct {
	ID     int
	Table  *TableReader
	Page   interface{}
	Row    int
	Closed bool
}

func NewQueryEngine() *QueryEngine {
	return &QueryEngine{
		vm:       nil,
		tables:   make(map[string]*TableReader),
		cursors:  make(map[int]*Cursor),
		cursorID: 0,
	}
}

func (qe *QueryEngine) RegisterTable(name string, schema map[string]ColumnType) {
	qe.tables[name] = &TableReader{
		Name:   name,
		schema: schema,
	}
}

func (qe *QueryEngine) OpenCursor(tableName string) (int, error) {
	table, ok := qe.tables[tableName]
	if !ok {
		return -1, ErrNoTable
	}

	qe.cursorID++
	cursor := &Cursor{
		ID:     qe.cursorID,
		Table:  table,
		Row:    -1,
		Closed: false,
	}
	qe.cursors[qe.cursorID] = cursor

	return qe.cursorID, nil
}

func (qe *QueryEngine) CloseCursor(cursorID int) error {
	cursor, ok := qe.cursors[cursorID]
	if !ok {
		return nil
	}
	cursor.Closed = true
	delete(qe.cursors, cursorID)
	return nil
}

func (qe *QueryEngine) GetCursor(cursorID int) (*Cursor, error) {
	cursor, ok := qe.cursors[cursorID]
	if !ok || cursor.Closed {
		return nil, errors.New("invalid cursor")
	}
	return cursor, nil
}

func (qe *QueryEngine) NextRow(cursorID int) (map[string]interface{}, error) {
	cursor, err := qe.GetCursor(cursorID)
	if err != nil {
		return nil, err
	}

	cursor.Row++
	row := make(map[string]interface{})
	for name := range cursor.Table.schema {
		row[name] = nil
	}
	return row, nil
}

func (qe *QueryEngine) ColumnValue(cursorID int, colName string) (interface{}, error) {
	cursor, err := qe.GetCursor(cursorID)
	if err != nil {
		return nil, err
	}

	colType, ok := cursor.Table.schema[colName]
	if !ok {
		return nil, ErrNoColumn
	}

	switch colType.Type {
	case "INTEGER", "INT", "SMALLINT", "BIGINT":
		return int64(0), nil
	case "DECIMAL", "NUMERIC":
		return float64(0), nil
	case "FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION":
		return float64(0), nil
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER":
		return "", nil
	default:
		return nil, nil
	}
}

type Operator interface {
	Init() error
	Next() (map[string]interface{}, error)
	Close() error
}

type TableScan struct {
	qe       *QueryEngine
	cursorID int
	table    string
	eof      bool
}

func NewTableScan(qe *QueryEngine, table string) *TableScan {
	return &TableScan{
		qe:    qe,
		table: table,
		eof:   false,
	}
}

func (ts *TableScan) Init() error {
	cursorID, err := ts.qe.OpenCursor(ts.table)
	if err != nil {
		return err
	}
	ts.cursorID = cursorID
	return nil
}

func (ts *TableScan) Next() (map[string]interface{}, error) {
	if ts.eof {
		return nil, nil
	}
	row, err := ts.qe.NextRow(ts.cursorID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		ts.eof = true
	}
	return row, nil
}

func (ts *TableScan) Close() error {
	return ts.qe.CloseCursor(ts.cursorID)
}

type Filter struct {
	input     Operator
	predicate func(map[string]interface{}) bool
}

func NewFilter(input Operator, pred func(map[string]interface{}) bool) *Filter {
	return &Filter{
		input:     input,
		predicate: pred,
	}
}

func (f *Filter) Init() error {
	return f.input.Init()
}

func (f *Filter) Next() (map[string]interface{}, error) {
	for {
		row, err := f.input.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if f.predicate == nil || f.predicate(row) {
			return row, nil
		}
	}
}

func (f *Filter) Close() error {
	return f.input.Close()
}

type Project struct {
	input   Operator
	columns []string
}

func NewProject(input Operator, columns []string) *Project {
	return &Project{
		input:   input,
		columns: columns,
	}
}

func (p *Project) Init() error {
	return p.input.Init()
}

func (p *Project) Next() (map[string]interface{}, error) {
	row, err := p.input.Next()
	if err != nil || row == nil {
		return row, err
	}

	result := make(map[string]interface{})
	for _, col := range p.columns {
		result[col] = row[col]
	}
	return result, nil
}

func (p *Project) Close() error {
	return p.input.Close()
}

type Limit struct {
	input  Operator
	limit  int
	offset int
	count  int
}

func NewLimit(input Operator, limit, offset int) *Limit {
	return &Limit{
		input:  input,
		limit:  limit,
		offset: offset,
		count:  0,
	}
}

func (l *Limit) Init() error {
	return l.input.Init()
}

func (l *Limit) Next() (map[string]interface{}, error) {
	if l.limit >= 0 && l.count >= l.limit {
		return nil, nil
	}

	for l.count < l.offset {
		_, err := l.input.Next()
		if err != nil {
			return nil, err
		}
		l.count++
		if l.count >= l.limit {
			return nil, nil
		}
	}

	l.count++
	return l.input.Next()
}

func (l *Limit) Close() error {
	return l.input.Close()
}
