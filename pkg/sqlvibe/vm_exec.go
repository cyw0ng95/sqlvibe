package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/CG"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/VM"
)

func (db *Database) ExecVM(sql string) (*Rows, error) {
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("VM compile error: %v", err)
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("VM compile error: %v", err)
	}
	if ast == nil {
		return nil, nil
	}

	var tableName string
	switch stmt := ast.(type) {
	case *QP.SelectStmt:
		if stmt.From != nil {
			tableName = stmt.From.Name
		}
	case *QP.InsertStmt:
		tableName = stmt.Table
	case *QP.UpdateStmt:
		tableName = stmt.Table
	case *QP.DeleteStmt:
		tableName = stmt.Table
	}

	program, err := CG.Compile(sql)
	if err != nil {
		return nil, fmt.Errorf("VM compile error: %v", err)
	}

	ctx := newDsVmContext(db)
	vm := VM.NewVMWithContext(program, ctx)

	// Get table data from DS context
	if tableName != "" {
		if tableData, err := ctx.GetTableData(tableName); err == nil && tableData != nil {
			vm.Cursors().OpenTable(tableName, tableData, db.columnOrder[tableName])
		}
	}

	err = vm.Run(nil)
	if err != nil {
		return nil, fmt.Errorf("VM execution error: %v", err)
	}

	cols := make([]string, 0)
	rows := make([][]interface{}, 0)

	for i := 0; i < program.NumRegs; i++ {
		cols = append(cols, fmt.Sprintf("col%d", i))
	}

	for i := 0; i < program.NumRegs; i++ {
		row := make([]interface{}, program.NumRegs)
		for j := 0; j < program.NumRegs; j++ {
			row[j] = vm.GetRegister(j)
		}
		if i == 0 {
			rows = append(rows, row)
		}
	}

	return &Rows{Columns: cols, Data: rows}, nil
}

// execSelectStmt executes a SelectStmt directly using VM compilation
func (db *Database) execSelectStmt(stmt *QP.SelectStmt) (*Rows, error) {
	if stmt.From == nil {
		// SELECT without FROM - compile and execute directly
		compiler := CG.NewCompiler()
		program := compiler.CompileSelect(stmt)

		vm := VM.NewVMWithContext(program, &dbVmContext{db: db})
		err := vm.Run(nil)
		if err != nil {
			return nil, err
		}

		results := vm.Results()
		cols := make([]string, len(stmt.Columns))
		for i := range stmt.Columns {
			cols[i] = fmt.Sprintf("col%d", i)
		}

		return &Rows{Columns: cols, Data: results}, nil
	}

	// SELECT with FROM - use existing VM query execution
	tableName := stmt.From.Name

	// Handle information_schema virtual tables
	if strings.ToLower(stmt.From.Schema) == "information_schema" {
		fullName := stmt.From.Schema + "." + tableName
		return db.queryInformationSchema(stmt, fullName)
	}

	if tableName != "" {
		// Check if table exists via DS context
		ctx := newDsVmContext(db)
		if tableData, err := ctx.GetTableData(tableName); err != nil || tableData == nil {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
	}

	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	compiler := CG.NewCompiler()
	// Build column index map
	colIndices := make(map[string]int)
	for i, colName := range tableCols {
		colIndices[colName] = i
	}
	compiler.SetTableSchema(colIndices, tableCols)

	program := compiler.CompileSelect(stmt)
	ctx := newDsVmContext(db)
	vm := VM.NewVMWithContext(program, ctx)

	// Reset VM state before opening cursor manually
	vm.Reset()
	vm.SetPC(0)

	// Open table cursor (use alias if present, otherwise table name)
	cursorName := tableName
	if stmt.From.Alias != "" {
		cursorName = stmt.From.Alias
	}

	// Get table data from DS context
	if tableData, err := ctx.GetTableData(tableName); err == nil && tableData != nil {
		vm.Cursors().OpenTableAtID(0, cursorName, tableData, tableCols)
	}

	// Execute without calling Reset again (use Exec instead of Run)
	err := vm.Exec(nil)
	if err == VM.ErrHalt {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Apply DISTINCT deduplication if requested
	if stmt.Distinct {
		results = deduplicateRows(results)
	}

	// Get column names from SELECT
	cols := make([]string, 0)
	for i, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			// Handle SELECT * - expand to table columns
			if colRef.Name == "*" {
				cols = append(cols, tableCols...)
			} else {
				cols = append(cols, colRef.Name)
			}
		} else {
			cols = append(cols, fmt.Sprintf("col%d", i))
		}
	}

	return &Rows{Columns: cols, Data: results}, nil
}

// execSelectStmtWithContext executes a SelectStmt with outer row context for correlated subqueries
func (db *Database) execSelectStmtWithContext(stmt *QP.SelectStmt, outerRow map[string]interface{}) (*Rows, error) {
	if stmt.From == nil {
		// SELECT without FROM - compile and execute directly (no correlation possible)
		return db.execSelectStmt(stmt)
	}

	// SELECT with FROM - use existing VM query execution with context
	tableName := stmt.From.Name

	// Handle information_schema virtual tables
	if strings.ToLower(stmt.From.Schema) == "information_schema" {
		fullName := stmt.From.Schema + "." + tableName
		return db.queryInformationSchema(stmt, fullName)
	}

	if tableName != "" {
		// Check if table exists via DS context
		ctx := newDsVmContext(db)
		if tableData, err := ctx.GetTableData(tableName); err != nil || tableData == nil {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
	}

	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	compiler := CG.NewCompiler()
	// Build column index map
	colIndices := make(map[string]int)
	for i, colName := range tableCols {
		colIndices[colName] = i
	}
	compiler.SetTableSchema(colIndices, tableCols)

	program := compiler.CompileSelect(stmt)

	// Create context with outer row
	ctx := &dbVmContextWithOuter{
		db:       db,
		outerRow: outerRow,
	}
	vm := VM.NewVMWithContext(program, ctx)

	// Reset VM state before opening cursor manually
	vm.Reset()
	vm.SetPC(0)

	// Open table cursor (use alias if present, otherwise table name)
	cursorName := tableName
	if stmt.From.Alias != "" {
		cursorName = stmt.From.Alias
	}
	// Get table data from DS context
	tableData, _ := ctx.GetTableData(tableName)
	vm.Cursors().OpenTableAtID(0, cursorName, tableData, tableCols)
	// fmt.Printf("DEBUG execSelectStmtWithContext: Cursor 0 opened, about to Exec\n")

	// Execute without calling Reset again (use Exec instead of Run)
	err := vm.Exec(nil)
	if err == VM.ErrHalt {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Apply DISTINCT deduplication if requested
	if stmt.Distinct {
		results = deduplicateRows(results)
	}

	// Get column names from SELECT
	cols := make([]string, 0)
	for i, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			// Handle SELECT * - expand to table columns
			if colRef.Name == "*" {
				cols = append(cols, tableCols...)
			} else {
				cols = append(cols, colRef.Name)
			}
		} else {
			cols = append(cols, fmt.Sprintf("col%d", i))
		}
	}

	return &Rows{Columns: cols, Data: results}, nil
}

func (db *Database) execVMQuery(sql string, stmt *QP.SelectStmt) (*Rows, error) {
	tableName := stmt.From.Name

	// Handle information_schema virtual tables
	if strings.ToLower(stmt.From.Schema) == "information_schema" {
		fullName := stmt.From.Schema + "." + tableName
		return db.queryInformationSchema(stmt, fullName)
	}

	if tableName != "" {
		// Check if table exists via DS context
		ctx := newDsVmContext(db)
		if tableData, err := ctx.GetTableData(tableName); err != nil || tableData == nil {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
	}

	// Get table column order for proper column mapping
	// For JOINs, combine columns from both tables
	var tableCols []string
	var multiTableSchemas map[string]map[string]int
	if stmt.From.Join != nil && stmt.From.Join.Right != nil {
		leftCols := db.columnOrder[tableName]
		rightCols := db.columnOrder[stmt.From.Join.Right.Name]
		if leftCols != nil && rightCols != nil {
			tableCols = append(leftCols, rightCols...)

			// Build multi-table schemas for JOIN
			multiTableSchemas = make(map[string]map[string]int)
			leftSchema := make(map[string]int)
			for i, col := range leftCols {
				leftSchema[col] = i
			}
			rightSchema := make(map[string]int)
			for i, col := range rightCols {
				rightSchema[col] = i
			}
			multiTableSchemas[tableName] = leftSchema
			multiTableSchemas[stmt.From.Join.Right.Name] = rightSchema
			// Handle aliases
			if stmt.From.Alias != "" {
				multiTableSchemas[stmt.From.Alias] = leftSchema
			}
			if stmt.From.Join.Right.Alias != "" {
				multiTableSchemas[stmt.From.Join.Right.Alias] = rightSchema
			}
		} else if leftCols != nil {
			tableCols = leftCols
		} else if rightCols != nil {
			tableCols = rightCols
		}
	} else {
		tableCols = db.columnOrder[tableName]
	}

	// Compile with schema information
	cg := CG.NewCompiler()
	// For JOINs, use TableSchemas (multi-table), NOT combined TableColIndices
	// TableColIndices is only for single-table queries
	if multiTableSchemas != nil {
		// Don't set TableColIndices for JOINs - use TableSchemas instead
		cg.SetMultiTableSchema(multiTableSchemas, tableCols)
	} else {
		// Single table query - set TableColIndices normally
		cg.SetTableSchema(make(map[string]int), tableCols)
		for i, col := range tableCols {
			cg.TableColIndices[col] = i
		}
	}

	program := cg.CompileSelect(stmt)

	ctx := newDsVmContext(db)
	vm := VM.NewVMWithContext(program, ctx)

	err := vm.Run(nil)
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Apply DISTINCT deduplication if requested
	if stmt.Distinct {
		results = deduplicateRows(results)
	}

	// Get column names from the SELECT statement
	cols := make([]string, 0)
	for i, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			// Handle SELECT * - expand to table columns
			if colRef.Name == "*" {
				if colRef.Table != "" {
					// Qualified star (e.g., t1.*) - use only that table's columns
					if multiTableSchemas != nil {
						if schema, ok := multiTableSchemas[colRef.Table]; ok {
							// Collect columns from this table in order
							type colInfo struct {
								name string
								idx  int
							}
							tableColList := make([]colInfo, 0, len(schema))
							for colName, idx := range schema {
								tableColList = append(tableColList, colInfo{name: colName, idx: idx})
							}
							// Sort by index
							for i := 0; i < len(tableColList); i++ {
								for j := i + 1; j < len(tableColList); j++ {
									if tableColList[i].idx > tableColList[j].idx {
										tableColList[i], tableColList[j] = tableColList[j], tableColList[i]
									}
								}
							}
							for _, c := range tableColList {
								cols = append(cols, c.name)
							}
							continue
						}
					}
					// Fallback for single table with alias
					cols = append(cols, tableCols...)
					break
				} else {
					// Unqualified star - use all table columns
					cols = append(cols, tableCols...)
					continue
				}
			}
			cols = append(cols, colRef.Name)
		} else if alias, ok := col.(*QP.AliasExpr); ok {
			cols = append(cols, alias.Alias)
		} else {
			// For expressions without aliases, generate a column name
			cols = append(cols, fmt.Sprintf("col_%d", i))
		}
	}

	// If still no columns, use table order
	if len(cols) == 0 {
		cols = db.columnOrder[tableName]
	}
	if cols == nil {
		cols = []string{}
	}

	return &Rows{Columns: cols, Data: results}, nil
}

func (db *Database) applyDefaults(sql string, tableName string, tableCols []string) string {
	upperSQL := strings.ToUpper(sql)
	if !strings.HasPrefix(upperSQL, "INSERT") {
		return sql
	}

	tableDefaults := db.columnDefaults[tableName]
	if len(tableDefaults) == 0 {
		return sql
	}

	tokens, err := QP.NewTokenizer(sql).Tokenize()
	if err != nil {
		return sql
	}
	parser := QP.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return sql
	}

	insertStmt, ok := stmt.(*QP.InsertStmt)
	if !ok {
		return sql
	}

	if len(insertStmt.Columns) == 0 {
		if strings.Contains(upperSQL, "DEFAULT VALUES") {
			var vals []string
			for _, col := range tableCols {
				if _, hasDef := tableDefaults[col]; hasDef {
					vals = append(vals, "NULL")
				} else {
					vals = append(vals, "NULL")
				}
			}
			return fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(vals, ", "))
		}
		return sql
	}

	colSet := make(map[string]bool)
	for _, col := range insertStmt.Columns {
		colSet[col] = true
	}

	var missingWithDefaults []string
	for _, col := range tableCols {
		if !colSet[col] {
			if _, hasDef := tableDefaults[col]; hasDef {
				missingWithDefaults = append(missingWithDefaults, col)
			}
		}
	}

	if len(missingWithDefaults) == 0 {
		return sql
	}

	newCols := append([]string{}, insertStmt.Columns...)
	newCols = append(newCols, missingWithDefaults...)

	var newVals []string
	for _, row := range insertStmt.Values {
		var rowVals []string
		for _, val := range row {
			rowVals = append(rowVals, literalToString(val))
		}
		for range missingWithDefaults {
			rowVals = append(rowVals, "NULL")
		}
		newVals = append(newVals, "("+strings.Join(rowVals, ", ")+")")
	}

	result := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(newCols, ", "),
		strings.Join(newVals, ", "))
	return result
}

func literalToString(val interface{}) string {
	// Handle QP.Literal wrapper
	if lit, ok := val.(*QP.Literal); ok {
		val = lit.Value
	}
	switch v := val.(type) {
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return "NULL"
	}
}

func (db *Database) execVMDML(sql string, tableName string) (Result, error) {
	// Ensure table exists via DS context
	ctx := newDsVmContext(db)
	tableData, err := ctx.GetTableData(tableName)
	if err != nil || tableData == nil {
		// Create empty data if not exists (for new tables)
		tableData = make([]map[string]interface{}, 0)
	}

	// Get table column order
	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	// Pre-process INSERT to add defaults for missing columns
	processedSQL := db.applyDefaults(sql, tableName, tableCols)

	// Compile the DML statement
	program, err := CG.CompileWithSchema(processedSQL, tableCols)
	if err != nil {
		return Result{}, err
	}

	// Create VM context
	vm := VM.NewVMWithContext(program, ctx)

	// Open table cursor
	vm.Cursors().OpenTableAtID(0, tableName, tableData, tableCols)

	// Execute the VM program
	err = vm.Run(nil)
	if err != nil {
		return Result{}, err
	}

	// Get rows affected from VM
	return Result{RowsAffected: vm.RowsAffected()}, nil
}

// deduplicateRows removes duplicate rows, preserving the first occurrence of each unique row.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool)
	result := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := fmt.Sprintf("%v", row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}
