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
	vm         *VM
	pm         *DS.PageManager
	tables     map[string]*TableReader
	cursors    map[int]*Cursor
	cursorID   int
	data       map[string][]map[string]interface{}
	outerAlias string // Current outer query's table alias for correlation
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

func NewQueryEngine(pm *DS.PageManager, data map[string][]map[string]interface{}) *QueryEngine {
	return &QueryEngine{
		vm:         nil,
		pm:         pm,
		tables:     make(map[string]*TableReader),
		cursors:    make(map[int]*Cursor),
		cursorID:   0,
		data:       data,
		outerAlias: "",
	}
}

func (qe *QueryEngine) SetOuterAlias(alias string) {
	qe.outerAlias = alias
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
		switch e.Op {
		case QP.TokenAnd:
			return qe.evalExpr(row, e.Left) && qe.evalExpr(row, e.Right)
		case QP.TokenOr:
			return qe.evalExpr(row, e.Left) || qe.evalExpr(row, e.Right)
		case QP.TokenEq:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			return qe.valuesEqual(leftVal, rightVal)
		case QP.TokenNe:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			return !qe.valuesEqual(leftVal, rightVal)
		case QP.TokenLt:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			return qe.compareVals(leftVal, rightVal) < 0
		case QP.TokenLe:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			return qe.compareVals(leftVal, rightVal) <= 0
		case QP.TokenGt:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			result := qe.compareVals(leftVal, rightVal) > 0
			return result
		case QP.TokenGe:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if leftVal == nil || rightVal == nil {
				return false
			}
			return qe.compareVals(leftVal, rightVal) >= 0
		case QP.TokenIs:
			leftVal := qe.evalValue(row, e.Left)
			return leftVal == nil
		case QP.TokenIsNot:
			leftVal := qe.evalValue(row, e.Left)
			return leftVal != nil
		case QP.TokenIn:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			if rightList, ok := rightVal.([]interface{}); ok {
				for _, v := range rightList {
					if qe.valuesEqual(leftVal, v) {
						return true
					}
				}
				return false
			}
			return false
		case QP.TokenLike:
			leftVal := qe.evalValue(row, e.Left)
			rightVal := qe.evalValue(row, e.Right)
			leftStr, leftOk := leftVal.(string)
			patternStr, patOk := rightVal.(string)
			if !leftOk || !patOk {
				return false
			}
			return qe.matchLike(leftStr, patternStr)
		case QP.TokenBetween:
			leftVal := qe.evalValue(row, e.Left)
			if andExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				minVal := qe.evalValue(row, andExpr.Left)
				maxVal := qe.evalValue(row, andExpr.Right)
				if leftVal == nil || minVal == nil || maxVal == nil {
					return false
				}
				return qe.compareVals(leftVal, minVal) >= 0 && qe.compareVals(leftVal, maxVal) <= 0
			}
			return false
		case QP.TokenExists:
			subq := e.Left.(*QP.SubqueryExpr)
			result := qe.evalSubquery(row, qe.outerAlias, subq.Select)
			if result.rows == nil {
				return false
			}
			return len(result.rows) > 0
		case QP.TokenInSubquery:
			leftVal := qe.evalValue(row, e.Left)
			subq := e.Right.(*QP.SubqueryExpr)
			result := qe.evalSubquery(row, qe.outerAlias, subq.Select)
			if result.rows == nil || len(result.rows) == 0 {
				return false
			}
			colName := subq.Select.Columns[0].(*QP.ColumnRef).Name
			for _, r := range result.rows {
				if qe.valuesEqual(leftVal, r[colName]) {
					return true
				}
			}
			return false
		case QP.TokenAll:
			rightExpr := e.Right.(*QP.BinaryExpr)
			subq := rightExpr.Right.(*QP.SubqueryExpr)
			result := qe.evalSubquery(row, qe.outerAlias, subq.Select)
			if result.rows == nil || len(result.rows) == 0 {
				return false
			}
			for _, r := range result.rows {
				cmpExpr := &QP.BinaryExpr{
					Op:    rightExpr.Op,
					Left:  e.Left,
					Right: &QP.ColumnRef{Name: subq.Select.Columns[0].(*QP.ColumnRef).Name},
				}
				if !qe.evalExpr(r, cmpExpr) {
					return false
				}
			}
			return true
		case QP.TokenAny:
			rightExpr := e.Right.(*QP.BinaryExpr)
			subq := rightExpr.Right.(*QP.SubqueryExpr)
			result := qe.evalSubquery(row, qe.outerAlias, subq.Select)
			if result.rows == nil || len(result.rows) == 0 {
				return false
			}
			for _, r := range result.rows {
				cmpExpr := &QP.BinaryExpr{
					Op:    rightExpr.Op,
					Left:  e.Left,
					Right: &QP.ColumnRef{Name: subq.Select.Columns[0].(*QP.ColumnRef).Name},
				}
				if qe.evalExpr(r, cmpExpr) {
					return true
				}
			}
			return false
		}
	case *QP.UnaryExpr:
		if e.Op == QP.TokenNot {
			inner := qe.evalExpr(row, e.Expr)
			if inner {
				return false
			}
			if qe.hasNullColumn(row, e.Expr) {
				return false
			}
			return true
		}
	case *QP.SubqueryExpr:
		result := qe.evalSubquery(row, qe.outerAlias, e.Select)
		if result.rows == nil {
			return false
		}
		return len(result.rows) > 0
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
		// Handle table-qualified column references (e.g., e.dept_id)
		if e.Table != "" {
			// Try table.column format first
			if val, ok := row[e.Table+"."+e.Name]; ok {
				return val
			}
		}
		// Fall back to unqualified column name
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
		case QP.TokenEq:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if qe.valuesEqual(leftVal, rightVal) {
				return int64(1)
			}
			return int64(0)
		case QP.TokenNe:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if !qe.valuesEqual(leftVal, rightVal) {
				return int64(1)
			}
			return int64(0)
		case QP.TokenLt:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if qe.compareVals(leftVal, rightVal) < 0 {
				return int64(1)
			}
			return int64(0)
		case QP.TokenLe:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if qe.compareVals(leftVal, rightVal) <= 0 {
				return int64(1)
			}
			return int64(0)
		case QP.TokenGt:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if qe.compareVals(leftVal, rightVal) > 0 {
				return int64(1)
			}
			return int64(0)
		case QP.TokenGe:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			if qe.compareVals(leftVal, rightVal) >= 0 {
				return int64(1)
			}
			return int64(0)
		}
		return nil
	case *QP.UnaryExpr:
		val := qe.evalValue(row, e.Expr)
		if e.Op == QP.TokenMinus {
			return qe.negate(val)
		}
		return val
	case *QP.FuncCall:
		return qe.evalFuncCall(row, e)
	case *QP.AliasExpr:
		return qe.evalValue(row, e.Expr)
	case *QP.SubqueryExpr:
		result := qe.evalSubquery(row, qe.outerAlias, e.Select)
		if result.rows == nil || len(result.rows) == 0 {
			return nil
		}
		if len(e.Select.Columns) == 0 {
			return nil
		}
		// Check if this is an aggregate function
		if fc, ok := e.Select.Columns[0].(*QP.FuncCall); ok {
			switch fc.Name {
			case "MAX":
				var maxVal interface{}
				for _, r := range result.rows {
					val := qe.evalValue(r, fc.Args[0])
					if val != nil {
						if maxVal == nil || qe.compareVals(val, maxVal) > 0 {
							maxVal = val
						}
					}
				}
				return maxVal
			case "MIN":
				var minVal interface{}
				for _, r := range result.rows {
					val := qe.evalValue(r, fc.Args[0])
					if val != nil {
						if minVal == nil || qe.compareVals(val, minVal) < 0 {
							minVal = val
						}
					}
				}
				return minVal
			case "SUM":
				var sumVal float64
				var count int
				for _, r := range result.rows {
					val := qe.evalValue(r, fc.Args[0])
					if val != nil {
						count++
						switch v := val.(type) {
						case int64:
							sumVal += float64(v)
						case float64:
							sumVal += v
						}
					}
				}
				if count > 0 {
					return sumVal
				}
				return nil
			case "AVG":
				var sumVal float64
				var count int
				for _, r := range result.rows {
					val := qe.evalValue(r, fc.Args[0])
					if val != nil {
						count++
						switch v := val.(type) {
						case int64:
							sumVal += float64(v)
						case float64:
							sumVal += v
						}
					}
				}
				if count > 0 {
					avg := sumVal / float64(count)
					return avg
				}
				return nil
			case "COUNT":
				count := 0
				for _, r := range result.rows {
					// COUNT(*) - check if Args is empty
					if len(fc.Args) == 0 {
						count++
					} else {
						val := qe.evalValue(r, fc.Args[0])
						if val != nil {
							count++
						}
					}
				}
				return int64(count)
			}
			// Fall through for non-aggregate: return first column of first row
		}
		// For non-aggregate subqueries, return first column of first row
		if len(result.rows) > 0 && len(e.Select.Columns) > 0 {
			return qe.evalValue(result.rows[0], e.Select.Columns[0])
		}
		return nil
	}
	return nil
}

func (qe *QueryEngine) EvalExpr(row map[string]interface{}, expr QP.Expr) interface{} {
	return qe.evalValue(row, expr)
}

type subqueryResult struct {
	rows []map[string]interface{}
}

func (qe *QueryEngine) evalSubquery(outerRow map[string]interface{}, outerAlias string, sel *QP.SelectStmt) *subqueryResult {
	if sel == nil || sel.From == nil {
		return &subqueryResult{rows: nil}
	}

	tableName := sel.From.Name
	tableData, ok := qe.data[tableName]
	if !ok || tableData == nil {
		return &subqueryResult{rows: nil}
	}

	innerAlias := sel.From.Alias

	rows := []map[string]interface{}{}
	for _, row := range tableData {
		if sel.Where != nil {
			merged := make(map[string]interface{})
			// Add outer row with outer alias prefix
			if outerAlias != "" {
				for k, v := range outerRow {
					merged[outerAlias+"."+k] = v
					merged[k] = v
				}
			} else {
				for k, v := range outerRow {
					merged[k] = v
				}
			}
			// Add inner row values (overwrites for same columns, but alias-qualified takes precedence)
			for k, v := range row {
				merged[k] = v
				if innerAlias != "" {
					merged[innerAlias+"."+k] = v
				}
			}
			if !qe.evalExpr(merged, sel.Where) {
				continue
			}
		}
		rows = append(rows, row)
	}

	return &subqueryResult{rows: rows}
}

func (qe *QueryEngine) decodeRow(data []byte, schema map[string]ColumnType) map[string]interface{} {
	row := make(map[string]interface{})
	if data == nil || len(data) == 0 {
		for name := range schema {
			row[name] = nil
		}
		return row
	}

	pos := 0
	for name, colType := range schema {
		if pos >= len(data) {
			row[name] = nil
			continue
		}

		switch colType.Type {
		case "INTEGER":
			if pos+8 <= len(data) {
				val := int64(data[pos])
				for i := 1; i < 8; i++ {
					val |= int64(data[pos+i]) << (i * 8)
				}
				row[name] = val
			} else {
				row[name] = nil
			}
			pos += 8
		case "REAL":
			if pos+8 <= len(data) {
				var f float64
				fmt.Sscanf(string(data[pos:pos+8]), "%f", &f)
				row[name] = f
			} else {
				row[name] = nil
			}
			pos += 8
		case "TEXT":
			if pos < len(data) {
				row[name] = string(data[pos:])
				pos = len(data)
			} else {
				row[name] = nil
			}
		default:
			row[name] = nil
		}
	}
	return row
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
	// Handle mixed int64 and float64
	avInt, aok := a.(int64)
	if aok {
		if bvFloat, bok := b.(float64); bok {
			aFloat := float64(avInt)
			if aFloat < bvFloat {
				return -1
			}
			if aFloat > bvFloat {
				return 1
			}
			return 0
		}
	}
	bvInt, bok := b.(int64)
	if bok {
		if afFloat, aok := a.(float64); aok {
			bFloat := float64(bvInt)
			if afFloat < bFloat {
				return -1
			}
			if afFloat > bFloat {
				return 1
			}
			return 0
		}
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

func (qe *QueryEngine) matchLike(value, pattern string) bool {
	if pattern == "" {
		return value == ""
	}
	if pattern == "%" {
		return true
	}
	simple := true
	for _, c := range pattern {
		if c == '%' || c == '_' {
			simple = false
			break
		}
	}
	if simple {
		return value == pattern
	}
	result := matchLikeRecursive(value, pattern, 0, 0)
	return result
}

func matchLikeRecursive(value, pattern string, vi, pi int) bool {
	if pi >= len(pattern) {
		return vi >= len(value)
	}
	if vi >= len(value) {
		for ; pi < len(pattern); pi++ {
			if pattern[pi] != '%' {
				return false
			}
		}
		return true
	}
	pchar := pattern[pi]
	if pchar == '%' {
		for i := vi; i <= len(value); i++ {
			if matchLikeRecursive(value, pattern, i, pi+1) {
				return true
			}
		}
		return false
	}
	if pchar == '_' || pchar == value[vi] {
		return matchLikeRecursive(value, pattern, vi+1, pi+1)
	}
	return false
}

func (qe *QueryEngine) evalFuncCall(row map[string]interface{}, fc *QP.FuncCall) interface{} {
	switch fc.Name {
	case "COALESCE", "IFNULL":
		for _, arg := range fc.Args {
			val := qe.evalValue(row, arg)
			if val != nil {
				return val
			}
		}
		return nil
	case "MAX":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		return val
	case "MIN":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		return val
	case "COUNT":
		return int64(1)
	case "SUM":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		return val
	case "AVG":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		return val
	}
	return nil
}

func (qe *QueryEngine) hasNullColumn(row map[string]interface{}, expr QP.Expr) bool {
	switch e := expr.(type) {
	case *QP.BinaryExpr:
		if qe.hasNullColumn(row, e.Left) {
			return true
		}
		return qe.hasNullColumn(row, e.Right)
	case *QP.UnaryExpr:
		return qe.hasNullColumn(row, e.Expr)
	case *QP.ColumnRef:
		if val, ok := row[e.Name]; ok {
			return val == nil
		}
	}
	return false
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
