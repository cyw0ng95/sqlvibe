package sqlvibe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/SF/util"
)

// applyTypeAffinity coerces row values to match declared column type affinities.
// This mirrors SQLite's type affinity rules.
func (db *Database) applyTypeAffinity(tableName string, row map[string]interface{}) {
	colTypes, ok := db.tables[tableName]
	if !ok {
		return
	}
	for colName, declaredType := range colTypes {
		val, exists := row[colName]
		if !exists || val == nil {
			continue
		}
		upper := strings.ToUpper(declaredType)
		switch {
		case strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOAT") || strings.Contains(upper, "DOUBLE"):
			// REAL affinity: coerce integers and numeric strings to float64
			switch v := val.(type) {
			case int64:
				row[colName] = float64(v)
			case int:
				row[colName] = float64(v)
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					row[colName] = f
				}
			}
		case strings.Contains(upper, "INT"):
			// INTEGER affinity: coerce real values that are integers to int64
			switch v := val.(type) {
			case float64:
				if v == float64(int64(v)) {
					row[colName] = int64(v)
				}
			case string:
				if i, err := strconv.ParseInt(v, 10, 64); err == nil {
					row[colName] = i
				}
			}
		case strings.Contains(upper, "TEXT") || strings.Contains(upper, "CHAR") || strings.Contains(upper, "CLOB"):
			// TEXT affinity: coerce non-strings to their string representation
			switch v := val.(type) {
			case int64:
				row[colName] = strconv.FormatInt(v, 10)
			case float64:
				row[colName] = strconv.FormatFloat(v, 'g', -1, 64)
			}
		}
	}
}

// autoAssignPK assigns an auto-increment value to a single INTEGER PRIMARY KEY column
// when its value is nil, mimicking SQLite's rowid alias behavior.
func (db *Database) autoAssignPK(tableName string, row map[string]interface{}) {
	pkCols := db.primaryKeys[tableName]
	if len(pkCols) != 1 {
		return
	}
	pkCol := pkCols[0]
	if row[pkCol] != nil {
		return
	}
	// Check that it's an INTEGER column
	colTypes := db.tables[tableName]
	if colTypes == nil {
		return
	}
	colType := strings.ToUpper(colTypes[pkCol])
	if !strings.Contains(colType, "INT") {
		return
	}
	// Find max existing value
	var maxID int64
	for _, r := range db.data[tableName] {
		switch v := r[pkCol].(type) {
		case int64:
			if v > maxID {
				maxID = v
			}
		case int:
			if int64(v) > maxID {
				maxID = int64(v)
			}
		case float64:
			if int64(v) > maxID {
				maxID = int64(v)
			}
		}
	}
	row[pkCol] = maxID + 1
}

type dsVmContext struct {
	db         *Database
	pm         *DS.PageManager
	tableTrees map[string]*DS.BTree
}

func newDsVmContext(db *Database) *dsVmContext {
	util.AssertNotNil(db, "Database")
	return &dsVmContext{
		db:         db,
		pm:         db.pm,
		tableTrees: db.tableBTrees,
	}
}

func (ctx *dsVmContext) GetTableData(tableName string) ([]map[string]interface{}, error) {
	util.Assert(tableName != "", "tableName cannot be empty")
	// Resolve case-insensitive table name
	tableName = ctx.db.resolveTableName(tableName)
	// First check if there's in-memory data (fallback from previous writes)
	if ctx.db.data != nil && ctx.db.data[tableName] != nil && len(ctx.db.data[tableName]) > 0 {
		return ctx.db.data[tableName], nil
	}

	bt, ok := ctx.tableTrees[tableName]
	if !ok || bt == nil {
		// No BTree and no in-memory fallback
		return make([]map[string]interface{}, 0), nil
	}

	// Handle empty BTree (rootPage == 0)
	if bt.RootPage() == 0 {
		return make([]map[string]interface{}, 0), nil
	}

	// Scan BTree and convert to map format
	columns, err := ctx.GetTableColumns(tableName)
	if err != nil {
		return nil, err
	}

	cursor := bt.NewCursor()
	if err := cursor.First(); err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	rowIndex := 0
	for cursor.Valid() {
		value, err := cursor.Value()
		if err != nil {
			return nil, err
		}

		// Decode the record
		values, _, err := DS.DecodeRecord(value)
		if err != nil {
			return nil, err
		}

		// Convert to map
		row := make(map[string]interface{})
		for i, col := range columns {
			if i < len(values) {
				row[col] = values[i]
			} else {
				row[col] = nil
			}
		}
		row["_rowid_"] = rowIndex
		result = append(result, row)

		if err := cursor.Next(); err != nil {
			return nil, err
		}
		rowIndex++
	}

	return result, nil
}

func (ctx *dsVmContext) GetTableColumns(tableName string) ([]string, error) {
	util.Assert(tableName != "", "tableName cannot be empty")
	if ctx.db.columnOrder == nil {
		return nil, nil
	}
	tableName = ctx.db.resolveTableName(tableName)
	return ctx.db.columnOrder[tableName], nil
}

func (ctx *dsVmContext) InsertRow(tableName string, row map[string]interface{}) error {
	util.Assert(tableName != "", "tableName cannot be empty")
	util.AssertNotNil(row, "row")
	tableName = ctx.db.resolveTableName(tableName)
	bt, ok := ctx.tableTrees[tableName]
	if !ok || bt == nil || bt.RootPage() == 0 {
		// Fall back to in-memory
		if ctx.db.data == nil {
			ctx.db.data = make(map[string][]map[string]interface{})
		}
		if ctx.db.data[tableName] == nil {
			ctx.db.data[tableName] = make([]map[string]interface{}, 0)
		}

		// Apply type affinity coercion based on declared column types
		ctx.db.applyTypeAffinity(tableName, row)

		// Auto-assign integer primary key if nil
		ctx.db.autoAssignPK(tableName, row)

		// Check NOT NULL constraints
		tableNotNull := ctx.db.columnNotNull[tableName]
		for colName, isNotNull := range tableNotNull {
			if isNotNull {
				if val, exists := row[colName]; !exists || val == nil {
					return fmt.Errorf("NOT NULL constraint failed: %s.%s", tableName, colName)
				}
			}
		}

		// Check CHECK constraints using dbVmContext evaluator
		tableChecks := ctx.db.columnChecks[tableName]
		for colName, checkExpr := range tableChecks {
			if checkExpr != nil {
				dbCtx := &dbVmContext{db: ctx.db}
				result := dbCtx.evaluateCheckConstraint(checkExpr, row)
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

		// Check primary key uniqueness — O(1) via hash set when available.
		pkCols := ctx.db.primaryKeys[tableName]
		if len(pkCols) > 0 {
			if ctx.db.pkHashContains(tableName, row) {
				return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, pkCols[0])
			}
		}

		ctx.db.data[tableName] = append(ctx.db.data[tableName], row)
		newIdx := len(ctx.db.data[tableName]) - 1
		ctx.db.addToIndexes(tableName, row, newIdx)
		return nil
	}

	// Get column order
	columns, err := ctx.GetTableColumns(tableName)
	if err != nil {
		return err
	}

	// Extract values in column order
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		values[i] = row[col]
	}

	// Encode record
	encoded := DS.EncodeRecord(values)

	// Get rowid (either from row or auto-generate)
	rowid := int64(len(ctx.db.data[tableName]))
	if rid, ok := row["_rowid_"]; ok {
		switch v := rid.(type) {
		case int64:
			rowid = v
		case int:
			rowid = int64(v)
		}
	}

	// Create key from rowid
	key := make([]byte, 9)
	DS.PutVarint(key, rowid)

	// Insert into BTree
	if err := bt.Insert(key, encoded); err != nil {
		return fmt.Errorf("failed to insert into BTree: %w", err)
	}

	// Also keep in-memory for fallback
	if ctx.db.data[tableName] == nil {
		ctx.db.data[tableName] = make([]map[string]interface{}, 0)
	}
	ctx.db.data[tableName] = append(ctx.db.data[tableName], row)

	return nil
}

func (ctx *dsVmContext) UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error {
	util.Assert(tableName != "", "tableName cannot be empty")
	util.Assert(rowIndex >= 0, "rowIndex cannot be negative: %d", rowIndex)
	util.AssertNotNil(row, "row")
	tableName = ctx.db.resolveTableName(tableName)
	// Fall back to in-memory
	if ctx.db.data[tableName] == nil {
		return fmt.Errorf("table not found")
	}
	if rowIndex < 0 || rowIndex >= len(ctx.db.data[tableName]) {
		return fmt.Errorf("row index out of bounds")
	}
	oldRow := ctx.db.data[tableName][rowIndex]
	ctx.db.data[tableName][rowIndex] = row
	ctx.db.updateIndexes(tableName, oldRow, row, rowIndex)
	return nil
}

func (ctx *dsVmContext) DeleteRow(tableName string, rowIndex int) error {
	util.Assert(tableName != "", "tableName cannot be empty")
	util.Assert(rowIndex >= 0, "rowIndex cannot be negative: %d", rowIndex)
	tableName = ctx.db.resolveTableName(tableName)
	// Fall back to in-memory
	if ctx.db.data[tableName] == nil {
		return fmt.Errorf("table not found")
	}
	if rowIndex < 0 || rowIndex >= len(ctx.db.data[tableName]) {
		return fmt.Errorf("row index out of bounds")
	}
	row := ctx.db.data[tableName][rowIndex]
	ctx.db.removeFromIndexes(tableName, row, rowIndex)
	ctx.db.data[tableName] = append(ctx.db.data[tableName][:rowIndex], ctx.db.data[tableName][rowIndex+1:]...)
	return nil
}

// Subquery execution: delegate to dbVmContext for full support
func (ctx *dsVmContext) ExecuteSubquery(subquery interface{}) (interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubquery(subquery)
}

func (ctx *dsVmContext) ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRows(subquery)
}

func (ctx *dsVmContext) ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryWithContext(subquery, outerRow)
}

func (ctx *dsVmContext) ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRowsWithContext(subquery, outerRow)
}

func (ctx *dsVmContext) ExecuteExistsSubquery(subquery interface{}, outerRow map[string]interface{}) (bool, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteExistsSubquery(subquery, outerRow)
}

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

	// Auto-assign integer primary key if nil
	ctx.db.autoAssignPK(tableName, row)

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

	// Check primary key constraints — O(1) via hash set when available.
	pkCols := ctx.db.primaryKeys[tableName]
	if len(pkCols) > 0 {
		if ctx.db.pkHashContains(tableName, row) {
			return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, pkCols[0])
		}
	}

	ctx.db.data[tableName] = append(ctx.db.data[tableName], row)
	newIdx := len(ctx.db.data[tableName]) - 1
	ctx.db.addToIndexes(tableName, row, newIdx)

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
	oldRow := ctx.db.data[tableName][rowIndex]
	ctx.db.data[tableName][rowIndex] = row
	ctx.db.updateIndexes(tableName, oldRow, row, rowIndex)
	return nil
}

func (ctx *dbVmContext) DeleteRow(tableName string, rowIndex int) error {
	if ctx.db.data[tableName] == nil || rowIndex < 0 || rowIndex >= len(ctx.db.data[tableName]) {
		return fmt.Errorf("invalid row index for table %s", tableName)
	}
	row := ctx.db.data[tableName][rowIndex]
	ctx.db.removeFromIndexes(tableName, row, rowIndex)
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

// ExecuteExistsSubquery checks whether the subquery returns any rows,
// stopping after the first match (LIMIT 1 short-circuit).
func (ctx *dbVmContext) ExecuteExistsSubquery(subquery interface{}, outerRow map[string]interface{}) (bool, error) {
	selectStmt, ok := subquery.(*QP.SelectStmt)
	if !ok {
		return false, fmt.Errorf("subquery is not a SelectStmt")
	}
	return ctx.db.execExistsSubquery(selectStmt, outerRow)
}

type dbVmContextWithOuter struct {
	db       *Database
	outerRow map[string]interface{}
}

func (ctx *dbVmContextWithOuter) GetTableData(tableName string) ([]map[string]interface{}, error) {
	return newDsVmContext(ctx.db).GetTableData(tableName)
}

func (ctx *dbVmContextWithOuter) GetTableColumns(tableName string) ([]string, error) {
	return newDsVmContext(ctx.db).GetTableColumns(tableName)
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
	return newDsVmContext(ctx.db).InsertRow(tableName, row)
}

func (ctx *dbVmContextWithOuter) UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error {
	return newDsVmContext(ctx.db).UpdateRow(tableName, rowIndex, row)
}

func (ctx *dbVmContextWithOuter) DeleteRow(tableName string, rowIndex int) error {
	return newDsVmContext(ctx.db).DeleteRow(tableName, rowIndex)
}

func (ctx *dbVmContextWithOuter) ExecuteSubquery(subquery interface{}) (interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubquery(subquery)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRows(subquery)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryWithContext(subquery, outerRow)
}

func (ctx *dbVmContextWithOuter) ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteSubqueryRowsWithContext(subquery, outerRow)
}

func (ctx *dbVmContextWithOuter) ExecuteExistsSubquery(subquery interface{}, outerRow map[string]interface{}) (bool, error) {
	return (&dbVmContext{db: ctx.db}).ExecuteExistsSubquery(subquery, outerRow)
}
