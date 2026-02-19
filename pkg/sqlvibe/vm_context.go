package sqlvibe

import (
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/QP"
)

type dbVmContext struct {
	db *Database
}

func (ctx *dbVmContext) GetTableData(tableName string) ([]map[string]interface{}, error) {
	if ctx.db.data == nil {
		return nil, nil
	}
	return ctx.db.data[tableName], nil
}

func (ctx *dbVmContext) GetTableColumns(tableName string) ([]string, error) {
	if ctx.db.columnOrder == nil {
		return nil, nil
	}
	return ctx.db.columnOrder[tableName], nil
}

func (ctx *dbVmContext) InsertRow(tableName string, row map[string]interface{}) error {
	if ctx.db.data[tableName] == nil {
		ctx.db.data[tableName] = make([]map[string]interface{}, 0)
	}

	// Apply defaults for missing columns and NULL values
	tableDefaults := ctx.db.columnDefaults[tableName]
	for colName, defaultVal := range tableDefaults {
		if val, exists := row[colName]; exists {
			// Column exists but is NULL - apply default
			if val == nil {
				// Extract value from Literal if needed
				if lit, ok := defaultVal.(*QP.Literal); ok {
					row[colName] = lit.Value
				} else {
					row[colName] = defaultVal
				}
			}
		} else {
			// Column missing entirely - add it with default value
			if lit, ok := defaultVal.(*QP.Literal); ok {
				row[colName] = lit.Value
			} else {
				row[colName] = defaultVal
			}
		}
	}

	// Check NOT NULL constraints
	tableNotNull := ctx.db.columnNotNull[tableName]
	for colName, isNotNull := range tableNotNull {
		if isNotNull {
			if val, exists := row[colName]; !exists || val == nil {
				return fmt.Errorf("NOT NULL constraint failed: %s.%s", tableName, colName)
			}
		}
	}

	// Check CHECK constraints
	tableChecks := ctx.db.columnChecks[tableName]
	for colName, checkExpr := range tableChecks {
		if checkExpr != nil {
			// Evaluate the CHECK expression with the row values
			// Create a simple evaluator that can resolve column references to row values
			result := ctx.evaluateCheckConstraint(checkExpr, row)
			
			// CHECK constraint must evaluate to true (non-zero, non-null, non-false)
			isValid := false
			if result != nil {
				switch v := result.(type) {
				case bool:
					isValid = v
				case int64:
					isValid = v != 0
				case float64:
					isValid = v != 0.0
				case string:
					isValid = len(v) > 0
				default:
					isValid = true
				}
			}
			
			if !isValid {
				return fmt.Errorf("CHECK constraint failed: %s.%s", tableName, colName)
			}
		}
	}

	// Check primary key constraints
	pkCols := ctx.db.primaryKeys[tableName]
	if len(pkCols) > 0 {
		for _, pkCol := range pkCols {
			pkVal := row[pkCol]
			for _, existingRow := range ctx.db.data[tableName] {
				if existingRow[pkCol] == pkVal {
					return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, pkCol)
				}
			}
		}
	}

	ctx.db.data[tableName] = append(ctx.db.data[tableName], row)

	// Update storage engine
	rowID := int64(len(ctx.db.data[tableName]))
	serialized := ctx.db.serializeRow(row)
	ctx.db.engine.Insert(tableName, uint64(rowID), serialized)

	return nil
}

// evaluateCheckConstraint evaluates a CHECK constraint expression with row values
func (ctx *dbVmContext) evaluateCheckConstraint(expr QP.Expr, row map[string]interface{}) interface{} {
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	
	case *QP.ColumnRef:
		// Look up column value in the row
		if val, ok := row[e.Name]; ok {
			return val
		}
		return nil
	
	case *QP.BinaryExpr:
		// Evaluate left and right operands
		left := ctx.evaluateCheckConstraint(e.Left, row)
		right := ctx.evaluateCheckConstraint(e.Right, row)
		
		// Handle NULL propagation
		if left == nil || right == nil {
			// In SQL, NULL comparisons return NULL (which is falsy for CHECK)
			return nil
		}
		
		// Perform the operation
		switch e.Op {
		case QP.TokenEq:
			return ctx.db.engine.CompareVals(left, right) == 0
		case QP.TokenNe:
			return ctx.db.engine.CompareVals(left, right) != 0
		case QP.TokenLt:
			return ctx.db.engine.CompareVals(left, right) < 0
		case QP.TokenLe:
			return ctx.db.engine.CompareVals(left, right) <= 0
		case QP.TokenGt:
			return ctx.db.engine.CompareVals(left, right) > 0
		case QP.TokenGe:
			return ctx.db.engine.CompareVals(left, right) >= 0
		case QP.TokenAnd:
			// AND: both must be truthy
			return isTruthy(left) && isTruthy(right)
		case QP.TokenOr:
			// OR: at least one must be truthy
			return isTruthy(left) || isTruthy(right)
		case QP.TokenPlus:
			return addValues(left, right)
		case QP.TokenMinus:
			return subtractValues(left, right)
		case QP.TokenAsterisk:
			return multiplyValues(left, right)
		case QP.TokenSlash:
			return divideValues(left, right)
		}
	
	case *QP.UnaryExpr:
		val := ctx.evaluateCheckConstraint(e.Expr, row)
		if e.Op == QP.TokenMinus {
			if iv, ok := val.(int64); ok {
				return -iv
			}
			if fv, ok := val.(float64); ok {
				return -fv
			}
		}
		return val
	}
	
	return nil
}

// Helper functions for CHECK constraint evaluation
func isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case float64:
		return v != 0.0
	case string:
		return len(v) > 0
	default:
		return true
	}
}

// Arithmetic helper functions for CHECK constraint evaluation
func addValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l + r
		}
		if r, ok := right.(float64); ok {
			return float64(l) + r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l + float64(r)
		}
		if r, ok := right.(float64); ok {
			return l + r
		}
	}
	return nil
}

func subtractValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l - r
		}
		if r, ok := right.(float64); ok {
			return float64(l) - r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l - float64(r)
		}
		if r, ok := right.(float64); ok {
			return l - r
		}
	}
	return nil
}

func multiplyValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l * r
		}
		if r, ok := right.(float64); ok {
			return float64(l) * r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l * float64(r)
		}
		if r, ok := right.(float64); ok {
			return l * r
		}
	}
	return nil
}

func divideValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			if r != 0 {
				return l / r
			}
		}
		if r, ok := right.(float64); ok {
			if r != 0.0 {
				return float64(l) / r
			}
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			if r != 0 {
				return l / float64(r)
			}
		}
		if r, ok := right.(float64); ok {
			if r != 0.0 {
				return l / r
			}
		}
	}
	return nil
}

func (ctx *dbVmContext) UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error {
	if ctx.db.data[tableName] == nil || rowIndex < 0 || rowIndex >= len(ctx.db.data[tableName]) {
		return fmt.Errorf("invalid row index for table %s", tableName)
	}
	ctx.db.data[tableName][rowIndex] = row
	return nil
}

func (ctx *dbVmContext) DeleteRow(tableName string, rowIndex int) error {
	if ctx.db.data[tableName] == nil || rowIndex < 0 || rowIndex >= len(ctx.db.data[tableName]) {
		return fmt.Errorf("invalid row index for table %s", tableName)
	}
	// Remove the row at the given index
	ctx.db.data[tableName] = append(ctx.db.data[tableName][:rowIndex], ctx.db.data[tableName][rowIndex+1:]...)
	return nil
}

// ExecuteSubquery executes a scalar subquery and returns a single value
func (ctx *dbVmContext) ExecuteSubquery(subquery interface{}) (interface{}, error) {
	// Type assert to *QP.SelectStmt
	selectStmt, ok := subquery.(*QP.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("subquery is not a SelectStmt")
	}

	// Execute the subquery using execSelectStmt
	rows, err := ctx.db.execSelectStmt(selectStmt)
	if err != nil {
		return nil, err
	}

	// For a scalar subquery, return the first column of the first row
	if len(rows.Data) > 0 && len(rows.Data[0]) > 0 {
		return rows.Data[0][0], nil
	}

	// If no rows, return nil
	return nil, nil
}

// ExecuteSubqueryRows executes a subquery and returns all rows
func (ctx *dbVmContext) ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error) {
	// Type assert to *QP.SelectStmt
	selectStmt, ok := subquery.(*QP.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("subquery is not a SelectStmt")
	}

	// Execute the subquery using execSelectStmt
	rows, err := ctx.db.execSelectStmt(selectStmt)
	if err != nil {
		return nil, err
	}

	// Return all rows
	return rows.Data, nil
}

// ExecuteSubqueryWithContext executes a scalar subquery with outer row context
func (ctx *dbVmContext) ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error) {
	// Type assert to *QP.SelectStmt
	selectStmt, ok := subquery.(*QP.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("subquery is not a SelectStmt")
	}

	// Execute the subquery with outer row context
	rows, err := ctx.db.execSelectStmtWithContext(selectStmt, outerRow)
	if err != nil {
		return nil, err
	}

	// For a scalar subquery, return the first column of the first row
	if len(rows.Data) > 0 && len(rows.Data[0]) > 0 {
		return rows.Data[0][0], nil
	}

	// If no rows, return nil
	return nil, nil
}

// ExecuteSubqueryRowsWithContext executes a subquery with outer row context and returns all rows
func (ctx *dbVmContext) ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error) {
	// Type assert to *QP.SelectStmt
	selectStmt, ok := subquery.(*QP.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("subquery is not a SelectStmt")
	}

	// Execute the subquery with outer row context
	rows, err := ctx.db.execSelectStmtWithContext(selectStmt, outerRow)
	if err != nil {
		return nil, err
	}

	// Return all rows
	return rows.Data, nil
}

type dbVmContextWithOuter struct {
	db       *Database
	outerRow map[string]interface{}
}

func (ctx *dbVmContextWithOuter) GetTableData(tableName string) ([]map[string]interface{}, error) {
	if ctx.db.data == nil {
		return nil, nil
	}
	return ctx.db.data[tableName], nil
}

func (ctx *dbVmContextWithOuter) GetTableColumns(tableName string) ([]string, error) {
	if ctx.db.columnOrder == nil {
		return nil, nil
	}
	return ctx.db.columnOrder[tableName], nil
}

// GetOuterRowValue retrieves a value from the outer row context
func (ctx *dbVmContextWithOuter) GetOuterRowValue(columnName string) (interface{}, bool) {
	if ctx.outerRow == nil {
		return nil, false
	}
	val, ok := ctx.outerRow[columnName]
	return val, ok
}

func (ctx *dbVmContextWithOuter) InsertRow(tableName string, row map[string]interface{}) error {
	// Delegate to dbVmContext
	return (&dbVmContext{db: ctx.db}).InsertRow(tableName, row)
}

func (ctx *dbVmContextWithOuter) UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error {
	// Delegate to dbVmContext
	return (&dbVmContext{db: ctx.db}).UpdateRow(tableName, rowIndex, row)
}

func (ctx *dbVmContextWithOuter) DeleteRow(tableName string, rowIndex int) error {
	// Delegate to dbVmContext
	return (&dbVmContext{db: ctx.db}).DeleteRow(tableName, rowIndex)
}

func (ctx *dbVmContextWithOuter) ExecuteSubquery(subquery interface{}) (interface{}, error) {
	// Delegate to db's dbVmContext
	return (&dbVmContext{db: ctx.db}).ExecuteSubquery(subquery)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error) {
	// Delegate to db's dbVmContext
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRows(subquery)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error) {
	// Delegate to db's dbVmContext
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryWithContext(subquery, outerRow)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error) {
	// Delegate to db's dbVmContext
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRowsWithContext(subquery, outerRow)
}
