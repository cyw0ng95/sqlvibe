package QE

import (
	"errors"
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/QP"
)

var (
	ErrNoTable      = errors.New("no such table")
	ErrNoColumn     = errors.New("no such column")
	ErrTypeMismatch = errors.New("type mismatch")
)

type QueryEngine struct {
	vm       *VM
	pm       *DS.PageManager
	tables   map[string]*TableReader
	cursors  map[int]*Cursor
	cursorID int
}

type TableReader struct {
	Name   string
	btree  *DS.BTree
	schema map[string]ColumnType
}

type ColumnType struct {
	Name string
	Type string
}

type Cursor struct {
	ID     int
	Table  *TableReader
	btree  *DS.BTreeCursor
	Row    int
	Closed bool
}

func NewQueryEngine(pm *DS.PageManager) *QueryEngine {
	return &QueryEngine{
		vm:       nil,
		pm:       pm,
		tables:   make(map[string]*TableReader),
		cursors:  make(map[int]*Cursor),
		cursorID: 0,
	}
}

func (qe *QueryEngine) RegisterTable(name string, schema map[string]ColumnType) {
	btree := DS.NewBTree(qe.pm, 0, true)
	qe.tables[name] = &TableReader{
		Name:   name,
		btree:  btree,
		schema: schema,
	}
}

func (qe *QueryEngine) Insert(tableName string, rowID uint64, data []byte) error {
	table, ok := qe.tables[tableName]
	if !ok {
		return ErrNoTable
	}
	key := make([]byte, 8)
	for i := 0; i < 8; i++ {
		key[i] = byte(rowID >> (i * 8))
	}
	return table.btree.Insert(key, data)
}

func (qe *QueryEngine) GetTableBTree(tableName string) (*DS.BTree, error) {
	table, ok := qe.tables[tableName]
	if !ok {
		return nil, ErrNoTable
	}
	return table.btree, nil
}

func (qe *QueryEngine) OpenCursor(tableName string) (int, error) {
	table, ok := qe.tables[tableName]
	if !ok {
		return -1, ErrNoTable
	}

	qe.cursorID++
	btree, _ := table.btree.First()
	cursor := &Cursor{
		ID:     qe.cursorID,
		Table:  table,
		btree:  btree,
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

	if cursor.btree != nil {
		key, _, err := cursor.btree.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, nil
		}
		row := make(map[string]interface{})
		row["_rowid"] = qe.bytesToUint64(key)
		return row, nil
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

func (qe *QueryEngine) bytesToUint64(b []byte) uint64 {
	var result uint64
	for i, v := range b {
		result |= uint64(v) << (uint(i) * 8)
	}
	return result
}

func (qe *QueryEngine) BuildPredicate(where QP.Expr) func(map[string]interface{}) bool {
	if where == nil {
		return nil
	}
	return func(row map[string]interface{}) bool {
		return qe.evalExpr(row, where)
	}
}

func (qe *QueryEngine) evalExpr(row map[string]interface{}, expr QP.Expr) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case *QP.BinaryExpr:
		leftVal := qe.evalValue(row, e.Left)
		rightVal := qe.evalValue(row, e.Right)
		switch e.Op {
		case QP.TokenEq:
			return qe.valuesEqual(leftVal, rightVal)
		case QP.TokenNe:
			return !qe.valuesEqual(leftVal, rightVal)
		case QP.TokenLt:
			return qe.compareVals(leftVal, rightVal) < 0
		case QP.TokenLe:
			return qe.compareVals(leftVal, rightVal) <= 0
		case QP.TokenGt:
			return qe.compareVals(leftVal, rightVal) > 0
		case QP.TokenGe:
			return qe.compareVals(leftVal, rightVal) >= 0
		}
	}
	return true
}

func (qe *QueryEngine) evalValue(row map[string]interface{}, expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		if val, ok := row[e.Name]; ok {
			return val
		}
		return nil
	case *QP.BinaryExpr:
		leftVal := qe.evalValue(row, e.Left)
		rightVal := qe.evalValue(row, e.Right)
		exprEval := &ExprEvaluator{}
		switch e.Op {
		case QP.TokenPlus:
			result, _ := exprEval.BinaryOp(OpAdd, leftVal, rightVal)
			return result
		case QP.TokenMinus:
			result, _ := exprEval.BinaryOp(OpSubtract, leftVal, rightVal)
			return result
		case QP.TokenAsterisk:
			result, _ := exprEval.BinaryOp(OpMultiply, leftVal, rightVal)
			return result
		case QP.TokenSlash:
			result, _ := exprEval.BinaryOp(OpDivide, leftVal, rightVal)
			return result
		case QP.TokenPercent:
			result, _ := exprEval.BinaryOp(OpRemainder, leftVal, rightVal)
			return result
		}
		return nil
	case *QP.UnaryExpr:
		val := qe.evalValue(row, e.Expr)
		if e.Op == QP.TokenMinus {
			return qe.negate(val)
		}
		return val
	}
	return nil
}

func (qe *QueryEngine) EvalExpr(row map[string]interface{}, expr QP.Expr) interface{} {
	return qe.evalValue(row, expr)
}

func (qe *QueryEngine) valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	av, aok := a.(int64)
	bv, bok := b.(int64)
	if aok && bok {
		return av == bv
	}
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return af == bf
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return as == bs
	}
	if aok {
		var iv int64
		fmt.Sscanf(fmt.Sprintf("%v", bs), "%d", &iv)
		return av == iv
	}
	if bok {
		var iv int64
		fmt.Sscanf(fmt.Sprintf("%v", as), "%d", &iv)
		return iv == bv
	}
	return false
}

func (qe *QueryEngine) compareVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	av, aok := a.(int64)
	bv, bok := b.(int64)
	if aok && bok {
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
	return 0
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
	data     []map[string]interface{}
	dataPos  int
}

func NewTableScan(qe *QueryEngine, table string) *TableScan {
	return &TableScan{
		qe:    qe,
		table: table,
		eof:   false,
	}
}

func (ts *TableScan) SetData(data []map[string]interface{}) {
	ts.data = data
	ts.dataPos = 0
}

func (ts *TableScan) Init() error {
	if ts.data == nil {
		cursorID, err := ts.qe.OpenCursor(ts.table)
		if err != nil {
			return err
		}
		ts.cursorID = cursorID
	}
	return nil
}

func (ts *TableScan) Next() (map[string]interface{}, error) {
	if ts.eof {
		return nil, nil
	}
	if ts.data != nil {
		if ts.dataPos >= len(ts.data) {
			ts.eof = true
			return nil, nil
		}
		row := ts.data[ts.dataPos]
		ts.dataPos++
		return row, nil
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
	if ts.data == nil && ts.cursorID > 0 {
		return ts.qe.CloseCursor(ts.cursorID)
	}
	return nil
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
	input       Operator
	columns     []string
	expressions []QP.Expr
	qe          *QueryEngine
}

func NewProject(input Operator, columns []string) *Project {
	return &Project{
		input:       input,
		columns:     columns,
		expressions: make([]QP.Expr, len(columns)),
	}
}

func (qe *QueryEngine) NewProjectWithExpr(input Operator, columns []string, expressions []QP.Expr) *Project {
	return &Project{
		input:       input,
		columns:     columns,
		expressions: expressions,
		qe:          qe,
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
	for i, col := range p.columns {
		if p.expressions != nil && i < len(p.expressions) && p.expressions[i] != nil {
			result[col] = p.qe.EvalExpr(row, p.expressions[i])
		} else {
			result[col] = row[col]
		}
	}
	return result, nil
}

func (p *Project) Close() error {
	return p.input.Close()
}

type Limit struct {
	input         Operator
	limit         int
	offset        int
	offsetSkipped int
	returned      int
}

func NewLimit(input Operator, limit, offset int) *Limit {
	return &Limit{
		input:         input,
		limit:         limit,
		offset:        offset,
		offsetSkipped: 0,
		returned:      0,
	}
}

func (l *Limit) Init() error {
	return l.input.Init()
}

func (l *Limit) Next() (map[string]interface{}, error) {
	if l.limit > 0 && l.returned >= l.limit {
		return nil, nil
	}

	for l.offsetSkipped < l.offset {
		_, err := l.input.Next()
		if err != nil {
			return nil, err
		}
		l.offsetSkipped++
	}

	l.returned++
	return l.input.Next()
}

func (l *Limit) Close() error {
	return l.input.Close()
}

func (qe *QueryEngine) negate(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return -v
	case float64:
		return -v
	case int:
		return -v
	}
	return val
}
