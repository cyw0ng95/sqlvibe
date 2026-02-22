package VM

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
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
	cursors    map[int]*QueryCursor
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

type QueryCursor struct {
	ID     int
	Table  *TableReader
	btree  *DS.BTreeCursor
	Row    int
	Closed bool
}

func NewQueryEngine(pm *DS.PageManager, data map[string][]map[string]interface{}) *QueryEngine {
	util.AssertNotNil(pm, "PageManager")
	util.AssertNotNil(data, "data")

	return &QueryEngine{
		vm:         nil,
		pm:         pm,
		tables:     make(map[string]*TableReader),
		cursors:    make(map[int]*QueryCursor),
		cursorID:   0,
		data:       data,
		outerAlias: "",
	}
}

func (qe *QueryEngine) SetOuterAlias(alias string) {
	qe.outerAlias = alias
}

func (qe *QueryEngine) RegisterTable(name string, schema map[string]ColumnType) {
	util.Assert(name != "", "table name cannot be empty")
	util.AssertNotNil(schema, "schema")

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
	btreeCursor := table.btree.NewCursor()
	if err := btreeCursor.First(); err != nil {
		return -1, err
	}
	cursor := &QueryCursor{
		ID:     qe.cursorID,
		Table:  table,
		btree:  btreeCursor,
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

func (qe *QueryEngine) GetCursor(cursorID int) (*QueryCursor, error) {
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
		if !cursor.btree.Valid() {
			return nil, nil
		}

		key, err := cursor.btree.Key()
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
		case QP.TokenConcat:
			result, _ := exprEval.BinaryOp(OpConcat, leftVal, rightVal)
			return result
		case QP.TokenAnd:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			leftBool := qe.toBool(leftVal)
			rightBool := qe.toBool(rightVal)
			if leftBool && rightBool {
				return int64(1)
			}
			return int64(0)
		case QP.TokenOr:
			if leftVal == nil || rightVal == nil {
				return nil
			}
			leftBool := qe.toBool(leftVal)
			rightBool := qe.toBool(rightVal)
			if leftBool || rightBool {
				return int64(1)
			}
			return int64(0)
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
		case QP.TokenIs:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return int64(1) // NULL IS NULL -> 1
			}
			return int64(0)
		case QP.TokenIsNot:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return int64(0)
			}
			return int64(1)
		case QP.TokenIn:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			rightVal := qe.evalValue(row, e.Right)
			if rightList, ok := rightVal.([]interface{}); ok {
				for _, v := range rightList {
					if qe.valuesEqual(leftVal, v) {
						return int64(1)
					}
				}
				return int64(0)
			}
			return int64(0)
		case QP.TokenBetween:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			if andExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				minVal := qe.evalValue(row, andExpr.Left)
				maxVal := qe.evalValue(row, andExpr.Right)
				if minVal == nil || maxVal == nil {
					return nil
				}
				if qe.compareVals(leftVal, minVal) >= 0 && qe.compareVals(leftVal, maxVal) <= 0 {
					return int64(1)
				}
				return int64(0)
			}
			return int64(0)
		case QP.TokenLike:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			rightVal := qe.evalValue(row, e.Right)
			leftStr, leftOk := leftVal.(string)
			patternStr, patOk := rightVal.(string)
			if !leftOk || !patOk {
				return int64(0)
			}
			if qe.matchLike(leftStr, patternStr) {
				return int64(1)
			}
			return int64(0)
		case QP.TokenNotLike:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			rightVal := qe.evalValue(row, e.Right)
			leftStr, leftOk := leftVal.(string)
			patternStr, patOk := rightVal.(string)
			if !leftOk || !patOk {
				return int64(0)
			}
			if qe.matchLike(leftStr, patternStr) {
				return int64(0)
			}
			return int64(1)
		case QP.TokenGlob:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			rightVal := qe.evalValue(row, e.Right)
			leftStr, leftOk := leftVal.(string)
			patternStr, patOk := rightVal.(string)
			if !leftOk || !patOk {
				return int64(0)
			}
			if qe.matchGlob(leftStr, patternStr) {
				return int64(1)
			}
			return int64(0)
		case QP.TokenNotIn:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			rightVal := qe.evalValue(row, e.Right)
			if rightList, ok := rightVal.([]interface{}); ok {
				for _, v := range rightList {
					if qe.valuesEqual(leftVal, v) {
						return int64(0)
					}
				}
				return int64(1)
			}
			return int64(1)
		case QP.TokenNotBetween:
			leftVal := qe.evalValue(row, e.Left)
			if leftVal == nil {
				return nil
			}
			if andExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				minVal := qe.evalValue(row, andExpr.Left)
				maxVal := qe.evalValue(row, andExpr.Right)
				if minVal == nil || maxVal == nil {
					return nil
				}
				if qe.compareVals(leftVal, minVal) >= 0 && qe.compareVals(leftVal, maxVal) <= 0 {
					return int64(0)
				}
				return int64(1)
			}
			return int64(1)
		}
		return nil
	case *QP.UnaryExpr:
		val := qe.evalValue(row, e.Expr)
		if e.Op == QP.TokenMinus {
			return qe.Negate(val)
		}
		if e.Op == QP.TokenNot {
			if val == nil {
				return int64(1)
			}
			if b, ok := val.(int64); ok {
				if b == 0 {
					return int64(1)
				}
				return int64(0)
			}
			return int64(0)
		}
		return val
	case *QP.FuncCall:
		return qe.evalFuncCall(row, e)
	case *QP.AliasExpr:
		return qe.evalValue(row, e.Expr)
	case *QP.CaseExpr:
		return qe.evalCaseExpr(row, e)
	case *QP.CastExpr:
		return qe.evalCastExpr(row, e)
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
			switch strings.ToUpper(fc.Name) {
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

// EvalBool evaluates a boolean expression on a row.
// Returns true if the expression evaluates to a truthy value.
func (qe *QueryEngine) EvalBool(row map[string]interface{}, expr QP.Expr) bool {
	return qe.evalExpr(row, expr)
}

func (qe *QueryEngine) evalCaseExpr(row map[string]interface{}, ce *QP.CaseExpr) interface{} {
	if ce.Operand != nil {
		operandVal := qe.evalValue(row, ce.Operand)
		for _, when := range ce.Whens {
			condVal := qe.evalValue(row, when.Condition)
			if operandVal == nil && condVal == nil {
				return qe.evalValue(row, when.Result)
			}
			if operandVal != nil && condVal != nil && qe.valuesEqual(operandVal, condVal) {
				return qe.evalValue(row, when.Result)
			}
		}
		if ce.Else != nil {
			return qe.evalValue(row, ce.Else)
		}
		return nil
	}

	for _, when := range ce.Whens {
		condVal := qe.evalValue(row, when.Condition)
		if condVal != nil {
			if intVal, ok := condVal.(int64); ok && intVal != 0 {
				return qe.evalValue(row, when.Result)
			}
			if floatVal, ok := condVal.(float64); ok && floatVal != 0 {
				return qe.evalValue(row, when.Result)
			}
		}
	}
	if ce.Else != nil {
		return qe.evalValue(row, ce.Else)
	}
	return nil
}

func (qe *QueryEngine) evalCastExpr(row map[string]interface{}, ce *QP.CastExpr) interface{} {
	val := qe.evalValue(row, ce.Expr)
	if val == nil {
		return nil
	}
	switch ce.TypeSpec.Name {
	case "INTEGER", "INT":
		if s, ok := val.(string); ok {
			if iv, err := strconv.ParseInt(s, 10, 64); err == nil {
				return iv
			}
			if fv, err := strconv.ParseFloat(s, 64); err == nil {
				return int64(fv)
			}
			return nil
		}
		if fv, ok := val.(float64); ok {
			return int64(fv)
		}
		return val
	case "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL":
		if s, ok := val.(string); ok {
			if fv, err := strconv.ParseFloat(s, 64); err == nil {
				return fv
			}
			return nil
		}
		if iv, ok := val.(int64); ok {
			return float64(iv)
		}
		return val
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER":
		if s, ok := val.(string); ok {
			return s
		}
		if bv, ok := val.([]byte); ok {
			return string(bv)
		}
		if iv, ok := val.(int64); ok {
			return strconv.FormatInt(iv, 10)
		}
		if fv, ok := val.(float64); ok {
			return strconv.FormatFloat(fv, 'f', -1, 64)
		}
		return fmt.Sprintf("%v", val)
	case "BLOB":
		return val
	case "DATE", "TIME", "TIMESTAMP", "DATETIME", "YEAR":
		// SQLite treats DATE/TIME/TIMESTAMP as NUMERIC affinity (leading-integer parsing)
		if s, ok := val.(string); ok {
			s = strings.TrimSpace(s)
			// Extract leading integer (SQLite's sqlite3Atoi64 behavior)
			end := 0
			for end < len(s) && (s[end] >= '0' && s[end] <= '9' || (end == 0 && (s[end] == '-' || s[end] == '+'))) {
				end++
			}
			if end > 0 {
				if iv, err := strconv.ParseInt(s[:end], 10, 64); err == nil {
					return iv
				}
			}
			return int64(0)
		}
		if fv, ok := val.(float64); ok {
			return int64(fv)
		}
		return val
	default:
		return val
	}
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

	av, aIsInt := a.(int64)
	bv, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		return av == bv
	}

	af, aIsFloat := a.(float64)
	bf, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return af == bf
	}

	as, aIsString := a.(string)
	bs, bIsString := b.(string)
	if aIsString && bIsString {
		return as == bs
	}

	if aIsInt && bIsFloat {
		return float64(av) == bf
	}
	if aIsFloat && bIsInt {
		return af == float64(bv)
	}

	return false
}

func (qe *QueryEngine) toBool(val interface{}) bool {
	if val == nil {
		return false
	}
	if b, ok := val.(int64); ok {
		return b != 0
	}
	if b, ok := val.(bool); ok {
		return b
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

func (qe *QueryEngine) matchGlob(value, pattern string) bool {
	if pattern == "" {
		return value == ""
	}
	if pattern == "*" {
		return true
	}
	simple := true
	for _, c := range pattern {
		if c == '*' || c == '?' {
			simple = false
			break
		}
	}
	if simple {
		return value == pattern
	}
	result := matchGlobRecursive(value, pattern, 0, 0)
	return result
}

func matchGlobRecursive(value, pattern string, vi, pi int) bool {
	if pi >= len(pattern) {
		return vi >= len(value)
	}
	if vi >= len(value) {
		for ; pi < len(pattern); pi++ {
			if pattern[pi] != '*' {
				return false
			}
		}
		return true
	}
	pchar := pattern[pi]
	if pchar == '*' {
		for i := vi; i <= len(value); i++ {
			if matchGlobRecursive(value, pattern, i, pi+1) {
				return true
			}
		}
		return false
	}
	if pchar == '?' || pchar == value[vi] {
		return matchGlobRecursive(value, pattern, vi+1, pi+1)
	}
	return false
}

// evalFuncCall evaluates function calls like COALESCE, IFNULL, MAX, MIN, etc.
// It handles built-in SQL functions that operate on row data.
func (qe *QueryEngine) evalFuncCall(row map[string]interface{}, fc *QP.FuncCall) interface{} {
	switch strings.ToUpper(fc.Name) {
	case "COALESCE", "IFNULL":
		// COALESCE returns the first non-NULL argument.
		// Per SQL spec: COALESCE(a, b, ...) is equivalent to:
		// CASE WHEN a IS NOT NULL THEN a ELSE b END
		// This also matches SQLite's IFNULL(a, b) semantics.
		for _, arg := range fc.Args {
			val := qe.evalValue(row, arg)
			if val != nil {
				return val
			}
		}
		return nil
	case "NULLIF":
		// NULLIF(a, b) returns NULL if a = b, otherwise returns a
		if len(fc.Args) != 2 {
			return nil
		}
		a := qe.evalValue(row, fc.Args[0])
		b := qe.evalValue(row, fc.Args[1])
		if qe.valuesEqual(a, b) {
			return nil
		}
		return a
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
	case "ABS":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		switch v := val.(type) {
		case int64:
			if v < 0 {
				return -v
			}
			return v
		case float64:
			return math.Abs(v)
		}
		return val
	case "CEIL", "CEILING":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return math.Ceil(v)
		}
		return val
	case "FLOOR":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return math.Floor(v)
		}
		return val
	case "ROUND":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		// Default to 0 decimal places (return int64)
		decimals := 0
		if len(fc.Args) >= 2 {
			if decVal := qe.evalValue(row, fc.Args[1]); decVal != nil {
				switch d := decVal.(type) {
				case int64:
					decimals = int(d)
				case float64:
					decimals = int(d)
				}
			}
		}
		switch v := val.(type) {
		case int64:
			if decimals == 0 {
				return v
			}
			if decimals < 0 {
				// Negative precision: round to left of decimal
				divisor := math.Pow10(-decimals)
				return int64(math.Round(float64(v)/divisor) * divisor)
			}
			// With decimal places, convert to float64
			divisor := math.Pow10(decimals)
			return math.Round(float64(v)*divisor) / divisor
		case float64:
			if decimals == 0 {
				return math.Round(v)
			}
			if decimals < 0 {
				// Negative precision: round to left of decimal
				divisor := math.Pow10(-decimals)
				return math.Round(v/divisor) * divisor
			}
			divisor := math.Pow10(decimals)
			return math.Round(v*divisor) / divisor
		}
		return val
	case "UPPER":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			return strings.ToUpper(s)
		}
		return val
	case "LOWER":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			return strings.ToLower(s)
		}
		return val
	case "LENGTH", "CHARACTER_LENGTH", "CHAR_LENGTH":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			return int64(utf8.RuneCountInString(s))
		}
		return int64(0)
	case "OCTET_LENGTH":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			return int64(len(s))
		}
		return int64(0)
	case "SUBSTRING", "SUBSTR":
		if len(fc.Args) < 2 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		s, ok := val.(string)
		if !ok {
			return nil
		}
		runes := []rune(s)
		length := len(runes)
		startVal := qe.evalValue(row, fc.Args[1])
		start := 1
		origStart := 0
		if startInt, ok := startVal.(int64); ok {
			origStart = int(startInt)
			start = origStart
		}
		if start == 0 {
			start = 1
		}
		if len(fc.Args) >= 3 {
			lenVal := qe.evalValue(row, fc.Args[2])
			if lenInt, ok := lenVal.(int64); ok {
				length = int(lenInt)
				if origStart == 0 && length > 0 {
					length = length - 1
				}
			}
		}
		if start < 0 {
			start = len(runes) + start + 1
			if start < 1 {
				start = 1
			}
		}
		if start > len(runes) {
			return ""
		}
		end := start - 1 + length
		if end > len(runes) {
			end = len(runes)
		}
		return string(runes[start-1 : end])
	case "TRIM":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if len(fc.Args) >= 2 {
				chars := qe.evalValue(row, fc.Args[1])
				if charsStr, ok := chars.(string); ok {
					return strings.Trim(s, charsStr)
				}
			}
			return strings.TrimSpace(s)
		}
		return val
	case "LTRIM":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if len(fc.Args) >= 2 {
				chars := qe.evalValue(row, fc.Args[1])
				if charsStr, ok := chars.(string); ok {
					return strings.TrimLeft(s, charsStr)
				}
			}
			return strings.TrimLeft(s, " \t\n\r")
		}
		return val
	case "RTRIM":
		if len(fc.Args) == 0 {
			return nil
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if len(fc.Args) >= 2 {
				chars := qe.evalValue(row, fc.Args[1])
				if charsStr, ok := chars.(string); ok {
					return strings.TrimRight(s, charsStr)
				}
			}
			return strings.TrimRight(s, " \t\n\r")
		}
		return val
		return val
	case "POSITION", "INSTR":
		if len(fc.Args) < 2 {
			return nil
		}
		haystack := qe.evalValue(row, fc.Args[0])
		needle := qe.evalValue(row, fc.Args[1])
		if haystack == nil || needle == nil {
			return nil
		}
		haystackStr, ok1 := haystack.(string)
		needleStr, ok2 := needle.(string)
		if !ok1 || !ok2 {
			return int64(0)
		}
		idx := strings.Index(haystackStr, needleStr)
		if idx < 0 {
			return int64(0)
		}
		return int64(utf8.RuneCountInString(haystackStr[:idx]) + 1)
	case "REPLACE":
		if len(fc.Args) < 3 {
			return nil
		}
		str := qe.evalValue(row, fc.Args[0])
		search := qe.evalValue(row, fc.Args[1])
		replace := qe.evalValue(row, fc.Args[2])
		if str == nil {
			return nil
		}
		strStr, ok := str.(string)
		if !ok {
			return str
		}
		searchStr, _ := search.(string)
		replaceStr, _ := replace.(string)
		return strings.ReplaceAll(strStr, searchStr, replaceStr)
	case "DATE":
		if len(fc.Args) == 0 {
			return time.Now().UTC().Format("2006-01-02")
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if strings.ToLower(s) == "now" {
				return time.Now().UTC().Format("2006-01-02")
			}
			for _, layout := range []string{"2006-01-02", "2006-01-02 15:04:05", time.RFC3339} {
				if t, err := time.Parse(layout, s); err == nil {
					return t.Format("2006-01-02")
				}
			}
			return s
		}
		return nil
	case "TIME":
		if len(fc.Args) == 0 {
			return time.Now().UTC().Format("15:04:05")
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if strings.ToLower(s) == "now" {
				return time.Now().UTC().Format("15:04:05")
			}
			for _, layout := range []string{"15:04:05", "2006-01-02 15:04:05", time.RFC3339} {
				if t, err := time.Parse(layout, s); err == nil {
					return t.Format("15:04:05")
				}
			}
			return s
		}
		return nil
	case "DATETIME", "TIMESTAMP":
		if len(fc.Args) == 0 {
			return time.Now().UTC().Format("2006-01-02 15:04:05")
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return nil
		}
		if s, ok := val.(string); ok {
			if strings.ToLower(s) == "now" {
				return time.Now().UTC().Format("2006-01-02 15:04:05")
			}
			for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
				if t, err := time.Parse(layout, s); err == nil {
					return t.Format("2006-01-02 15:04:05")
				}
			}
			return s
		}
		return nil
	case "CURRENT_DATE":
		return time.Now().Format("2006-01-02")
	case "CURRENT_TIME":
		return time.Now().Format("15:04:05")
	case "CURRENT_TIMESTAMP":
		return time.Now().Format("2006-01-02 15:04:05")
	case "LOCALTIME":
		return time.Now().Local().Format("15:04:05")
	case "LOCALTIMESTAMP":
		return time.Now().Local().Format("2006-01-02 15:04:05")
	case "STRFTIME", "strftime":
		if len(fc.Args) < 2 {
			return nil
		}
		format := qe.evalValue(row, fc.Args[0])
		timestamp := qe.evalValue(row, fc.Args[1])
		if format == nil || timestamp == nil {
			return nil
		}
		formatStr, _ := format.(string)
		tsStr, _ := timestamp.(string)
		var t time.Time
		if strings.ToLower(tsStr) == "now" {
			t = time.Now().UTC()
		} else {
			var err error
			for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
				t, err = time.Parse(layout, tsStr)
				if err == nil {
					break
				}
			}
			if err != nil {
				return nil
			}
		}
		sqliteFormat := strings.NewReplacer(
			"%Y", "2006", "%m", "01", "%d", "02",
			"%H", "15", "%M", "04", "%S", "05",
			"%j", "002", "%f", "05.000000",
		).Replace(formatStr)
		return t.Format(sqliteFormat)
	case "NOW":
		return time.Now().Format("2006-01-02 15:04:05")
	case "YEAR", "YEAROF":
		if len(fc.Args) == 0 {
			return time.Now().Year()
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				return int64(t.Year())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Year())
			}
		}
		return nil
	case "MONTH", "MONTHOF":
		if len(fc.Args) == 0 {
			return int64(time.Now().Month())
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				return int64(t.Month())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Month())
			}
		}
		return nil
	case "DAY", "DAYOF":
		if len(fc.Args) == 0 {
			return int64(time.Now().Day())
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				return int64(t.Day())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Day())
			}
		}
		return nil
	case "HOUR", "HOUROF":
		if len(fc.Args) == 0 {
			return int64(time.Now().Hour())
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("15:04:05", s); err == nil {
				return int64(t.Hour())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Hour())
			}
		}
		return nil
	case "MINUTE", "MINUTEOF":
		if len(fc.Args) == 0 {
			return int64(time.Now().Minute())
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("15:04:05", s); err == nil {
				return int64(t.Minute())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Minute())
			}
		}
		return nil
	case "SECOND", "SECONDOF":
		if len(fc.Args) == 0 {
			return int64(time.Now().Second())
		}
		val := qe.evalValue(row, fc.Args[0])
		if s, ok := val.(string); ok {
			if t, err := time.Parse("15:04:05", s); err == nil {
				return int64(t.Second())
			}
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				return int64(t.Second())
			}
		}
		return nil
	case "TYPEOF", "typeof":
		if len(fc.Args) == 0 {
			return "null"
		}
		val := qe.evalValue(row, fc.Args[0])
		if val == nil {
			return "null"
		}
		switch val.(type) {
		case int64:
			return "integer"
		case float64:
			return "real"
		case string:
			return "text"
		case []byte:
			return "blob"
		default:
			return "unknown"
		}
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

// Negate negates a numeric value
func (qe *QueryEngine) Negate(val interface{}) interface{} {
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

// ValuesEqual is a public wrapper for valuesEqual
func (qe *QueryEngine) ValuesEqual(a, b interface{}) bool {
	return qe.valuesEqual(a, b)
}

// CompareVals is a public wrapper for compareVals
func (qe *QueryEngine) CompareVals(a, b interface{}) int {
	return qe.compareVals(a, b)
}

// MatchLike is a public wrapper for matchLike
func (qe *QueryEngine) MatchLike(value, pattern string) bool {
	return qe.matchLike(value, pattern)
}

// ExtractValue extracts a value from an expression
func (qe *QueryEngine) ExtractValue(expr QP.Expr) interface{} {
	return qe.ExtractValueTyped(expr, "")
}

// ExtractValueTyped extracts and converts a value from an expression based on column type
func (qe *QueryEngine) ExtractValueTyped(expr QP.Expr, colType string) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		val := e.Value
		if strVal, ok := val.(string); ok {
			converted := qe.ConvertStringToType(strVal, colType)
			return converted
		}
		return val
	case *QP.ColumnRef:
		return e.Name
	case *QP.UnaryExpr:
		val := qe.ExtractValueTyped(e.Expr, colType)
		if e.Op == QP.TokenMinus {
			return qe.Negate(val)
		}
		return val
	default:
		return nil
	}
}

// ConvertStringToType converts a string value to the specified column type
func (qe *QueryEngine) ConvertStringToType(val string, colType string) interface{} {
	switch colType {
	case "INTEGER", "INT", "BIGINT", "SMALLINT":
		var intVal int64
		fmt.Sscanf(val, "%d", &intVal)
		return intVal
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "NUMERIC", "DECIMAL":
		var floatVal float64
		fmt.Sscanf(val, "%f", &floatVal)
		return floatVal
	default:
		return val
	}
}

// IndexInfo represents an index structure
type IndexInfo struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

// TryUseIndex attempts to use an index for a WHERE clause
func (qe *QueryEngine) TryUseIndex(tableName string, where QP.Expr, indexes map[string]*IndexInfo) []map[string]interface{} {
	if where == nil {
		return nil
	}

	binExpr, ok := where.(*QP.BinaryExpr)
	if !ok {
		return nil
	}

	if binExpr.Op != QP.TokenEq {
		return nil
	}

	var colName string
	var colValue interface{}

	if colRef, ok := binExpr.Left.(*QP.ColumnRef); ok {
		if lit, ok := binExpr.Right.(*QP.Literal); ok {
			colName = colRef.Name
			colValue = lit.Value
		}
	} else if colRef, ok := binExpr.Right.(*QP.ColumnRef); ok {
		if lit, ok := binExpr.Left.(*QP.Literal); ok {
			colName = colRef.Name
			colValue = lit.Value
		}
	}

	if colName == "" {
		return nil
	}

	for _, idx := range indexes {
		if idx.Table == tableName && len(idx.Columns) > 0 && idx.Columns[0] == colName {
			return qe.ScanByIndexValue(tableName, colName, colValue, idx.Unique)
		}
	}

	return nil
}

// ScanByIndexValue scans table data using an index
func (qe *QueryEngine) ScanByIndexValue(tableName, colName string, value interface{}, unique bool) []map[string]interface{} {
	tableData := qe.data[tableName]
	if tableData == nil {
		return nil
	}

	result := make([]map[string]interface{}, 0)
	for _, row := range tableData {
		if rowVal, ok := row[colName]; ok {
			if qe.ValuesEqual(rowVal, value) {
				result = append(result, row)
				if unique {
					return result
				}
			}
		}
	}
	return result
}
