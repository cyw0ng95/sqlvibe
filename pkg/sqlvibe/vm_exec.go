package sqlvibe

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/CG"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/SF/util"
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

	// Handle derived table in FROM clause (nested subqueries)
	if stmt.From.Subquery != nil {
		return db.execDerivedTableQuery(stmt)
	}

	// Delegate to execVMQuery which handles ORDER BY + LIMIT correctly
	// (including ORDER BY columns not in SELECT via extraOrderByCols mechanism).
	result, err := db.execVMQuery("", stmt)
	fmt.Printf("DEBUG execSelectStmt: execVMQuery returned %d rows (err=%v)\n", func() int {
		if result == nil {
			return -1
		}
		return len(result.Data)
	}(), err)
	return result, err
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
		// Resolve case-insensitive table name
		tableName = db.resolveTableName(tableName)
		stmt.From.Name = tableName
		// Check if table exists via table registry
		if _, exists := db.tables[tableName]; !exists {
			return nil, fmt.Errorf("no such table: %s", tableName)
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

	// For ORDER BY with columns not in SELECT, temporarily add them to the projection
	// so the VM returns them; WHERE is applied first, then sort+limit+strip.
	// Save original Columns to restore after compilation (avoid mutating shared AST).
	origColumns := stmt.Columns
	var extraOrderByCols []string
	if len(stmt.OrderBy) > 0 {
		selectColSet := make(map[string]bool)
		for _, col := range stmt.Columns {
			if cr, ok := col.(*QP.ColumnRef); ok {
				selectColSet[cr.Name] = true
				if cr.Name == "*" {
					for _, tc := range tableCols {
						selectColSet[tc] = true
					}
				}
			} else if alias, ok := col.(*QP.AliasExpr); ok {
				selectColSet[alias.Alias] = true
			}
		}
		for _, ob := range stmt.OrderBy {
			for _, ref := range collectColumnRefs(ob.Expr) {
				if !selectColSet[ref] {
					selectColSet[ref] = true
					extraOrderByCols = append(extraOrderByCols, ref)
					stmt.Columns = append(stmt.Columns, &QP.ColumnRef{Name: ref})
				}
			}
		}
	}

	program := compiler.CompileSelect(stmt)
	stmt.Columns = origColumns // restore immediately after compilation

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

	// Secondary index pre-filter: if WHERE is a simple col=val on an indexed column,
	// pass only matching rows to the VM instead of the full table.
	if stmt.Where != nil {
		if filtered := db.tryIndexLookup(tableName, stmt.Where); filtered != nil {
			tableData = filtered
		}
	}

	if tableData != nil {
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

	// Sort+limit+strip using the extended column set (WHERE already applied by VM)
	if len(extraOrderByCols) > 0 {
		numSelectCols := len(stmt.Columns) // after restore, this is the projected count
		// Build allCols = projected cols + extra order-by cols
		projCols := make([]string, numSelectCols)
		for i, col := range stmt.Columns {
			if cr, ok := col.(*QP.ColumnRef); ok {
				projCols[i] = cr.Name
			} else if alias, ok := col.(*QP.AliasExpr); ok {
				projCols[i] = alias.Alias
			}
		}
		allCols := append(projCols, extraOrderByCols...)
		topK := extractLimitInt(stmt.Limit, stmt.Offset)
		results = db.engine.SortRowsTopK(results, stmt.OrderBy, allCols, topK)
		if stmt.Limit != nil {
			if limited, err2 := db.applyLimit(&Rows{Data: results}, stmt.Limit, stmt.Offset); err2 == nil {
				results = limited.Data
			}
		}
		// Strip extra columns
		for i, row := range results {
			if len(row) > numSelectCols {
				results[i] = row[:numSelectCols]
			}
		}
	} else if stmt.Limit != nil {
		if limited, err2 := db.applyLimit(&Rows{Data: results}, stmt.Limit, stmt.Offset); err2 == nil {
			results = limited.Data
		}
	}

	// Get column names from SELECT
	cols := make([]string, 0)
	for i, col := range stmt.Columns {
		switch e := col.(type) {
		case *QP.ColumnRef:
			if e.Name == "*" {
				cols = append(cols, tableCols...)
			} else {
				cols = append(cols, e.Name)
			}
		case *QP.AliasExpr:
			cols = append(cols, e.Alias)
		default:
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
		// Resolve case-insensitive table name
		resolved := db.resolveTableName(tableName)
		if resolved != tableName {
			tableName = resolved
			stmt.From.Name = tableName
		}
		// Check if table exists via table registry
		if _, exists := db.tables[tableName]; !exists {
			return nil, fmt.Errorf("no such table: %s", tableName)
		}
	}

	// Fast path: use Go-level hash join for simple 2-table equi-joins.
	// This avoids the O(N×M) nested-loop VM bytecode for INNER JOINs.
	if stmt.From.Join != nil && stmt.From.Subquery == nil &&
		stmt.GroupBy == nil && len(stmt.OrderBy) == 0 && !stmt.Distinct {
		if hashRows, hashCols, ok := db.execHashJoin(stmt); ok {
			return &Rows{Columns: hashCols, Data: hashRows}, nil
		}
	}

	// Get table column order for proper column mapping
	// For JOINs, combine columns from both tables
	var tableCols []string
	var multiTableSchemas map[string]map[string]int
	var isRightJoin bool
	var origLeftCols, origRightCols []string
	if stmt.From.Join != nil && stmt.From.Join.Right != nil {
		// Resolve right table name too
		rightName := db.resolveTableName(stmt.From.Join.Right.Name)
		stmt.From.Join.Right.Name = rightName

		// Convert RIGHT JOIN to LEFT JOIN by swapping tables
		isRightJoin = stmt.From.Join.Type == "RIGHT"
		var origLeftName, origRightName string
		var origLeftAlias, origRightAlias string
		if isRightJoin {
			origLeftName = tableName
			origRightName = rightName
			origLeftAlias = stmt.From.Alias
			origRightAlias = stmt.From.Join.Right.Alias
			origLeftCols = db.columnOrder[origLeftName]
			origRightCols = db.columnOrder[origRightName]
			// Swap: right becomes left, left becomes right, change type to LEFT
			tableName = rightName
			stmt.From.Name = rightName
			stmt.From.Alias = origRightAlias
			stmt.From.Join.Right.Name = origLeftName
			stmt.From.Join.Right.Alias = origLeftAlias
			rightName = origLeftName
			stmt.From.Join.Type = "LEFT"
			// Swap join condition sides if exists
			if stmt.From.Join.Cond != nil {
				if bin, ok := stmt.From.Join.Cond.(*QP.BinaryExpr); ok && bin.Op == QP.TokenEq {
					bin.Left, bin.Right = bin.Right, bin.Left
				}
			}
		}

		leftCols := db.columnOrder[tableName]
		rightCols := db.columnOrder[rightName]
		if leftCols != nil && rightCols != nil {
			// For RIGHT JOIN (now swapped to LEFT), collect right-then-left cols for output order
			if isRightJoin {
				// tableCols order needs to match original left/right table order for output
				tableCols = append(origRightCols, origLeftCols...)
			} else {
				tableCols = append(leftCols, rightCols...)
			}

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
			multiTableSchemas[rightName] = rightSchema
			// Handle aliases
			if stmt.From.Alias != "" {
				multiTableSchemas[stmt.From.Alias] = leftSchema
			}
			if stmt.From.Join.Right.Alias != "" {
				multiTableSchemas[stmt.From.Join.Right.Alias] = rightSchema
			}

			// For chained 3-table JOINs, include 3rd table columns
			if stmt.From.Join.Right.Join != nil {
				thirdJoin := stmt.From.Join.Right.Join
				thirdName := db.resolveTableName(thirdJoin.Right.Name)
				thirdJoin.Right.Name = thirdName
				thirdCols := db.columnOrder[thirdName]
				if thirdCols != nil {
					tableCols = append(tableCols, thirdCols...)
					thirdSchema := make(map[string]int)
					for i, col := range thirdCols {
						thirdSchema[col] = i
					}
					multiTableSchemas[thirdName] = thirdSchema
					if thirdJoin.Right.Alias != "" {
						multiTableSchemas[thirdJoin.Right.Alias] = thirdSchema
					}
				}
			}

			// Resolve NATURAL JOIN / USING JOIN conditions now that we have schemas
			db.resolveNaturalUsing(stmt.From.Join, leftCols, rightCols, tableName, rightName)
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
		// Build TableColSources for deterministic column-to-table assignment
		// Use alias when available to correctly handle self-joins (e.g. t1 t1a JOIN t1 t1b)
		leftRef := stmt.From.Name
		if stmt.From.Alias != "" {
			leftRef = stmt.From.Alias
		}
		rightRef := stmt.From.Join.Right.Name
		if stmt.From.Join.Right.Alias != "" {
			rightRef = stmt.From.Join.Right.Alias
		}
		leftColsLen := len(db.columnOrder[stmt.From.Name])
		rightColsLen := len(db.columnOrder[stmt.From.Join.Right.Name])
		sources := make([]string, len(tableCols))
		for i := range tableCols {
			if i < leftColsLen {
				sources[i] = leftRef
			} else if i < leftColsLen+rightColsLen {
				sources[i] = rightRef
			} else if stmt.From.Join.Right.Join != nil {
				thirdRef := stmt.From.Join.Right.Join.Right.Name
				if stmt.From.Join.Right.Join.Right.Alias != "" {
					thirdRef = stmt.From.Join.Right.Join.Right.Alias
				}
				sources[i] = thirdRef
			}
		}
		cg.TableColSources = sources
	} else {
		// Single table query - set TableColIndices normally
		cg.SetTableSchema(make(map[string]int), tableCols)
		for i, col := range tableCols {
			cg.TableColIndices[col] = i
		}
	}

	// Add extra columns for ORDER BY expressions that reference non-SELECT columns
	// This allows ORDER BY col that isn't in the SELECT list to still sort correctly
	var extraOrderByCols []string
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		selectColNames := make(map[string]bool)
		for _, col := range stmt.Columns {
			if cr, ok := col.(*QP.ColumnRef); ok {
				selectColNames[cr.Name] = true
				if cr.Name == "*" {
					// SELECT * includes all columns - no need for extras
					for _, tc := range tableCols {
						selectColNames[tc] = true
					}
				}
			} else if alias, ok := col.(*QP.AliasExpr); ok {
				selectColNames[alias.Alias] = true
			}
		}
		for _, ob := range stmt.OrderBy {
			refs := collectColumnRefs(ob.Expr)
			for _, ref := range refs {
				if !selectColNames[ref] {
					selectColNames[ref] = true
					extraOrderByCols = append(extraOrderByCols, ref)
					stmt.Columns = append(stmt.Columns, &QP.ColumnRef{Name: ref})
				}
			}
		}
	}

	program := cg.CompileSelect(stmt)

	// Remove extra ORDER BY columns from stmt after compilation
	if len(extraOrderByCols) > 0 {
		stmt.Columns = stmt.Columns[:len(stmt.Columns)-len(extraOrderByCols)]
	}

	ctx := newDsVmContext(db)
	vm := VM.NewVMWithContext(program, ctx)

	// Pre-allocate result slice based on estimated table size to reduce reallocations.
	if stmt.From.Join == nil && tableName != "" {
		if tableData, ok := db.data[tableName]; ok {
			vm.PreallocResults(len(tableData))
		}
	}

	err := vm.Run(nil)
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Apply DISTINCT deduplication if requested
	if stmt.Distinct {
		results = deduplicateRows(results)
	}

	// For NATURAL/USING JOINs with SELECT *, deduplicate result columns
	// The VM generates rows with all columns from both tables; remove duplicate shared columns
	if stmt.From.Join != nil && (stmt.From.Join.Natural || len(stmt.From.Join.UsingColumns) > 0) {
		isSelectStar := len(stmt.Columns) == 1
		if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" && cr.Table == "" {
			isSelectStar = true
		}
		if isSelectStar && tableCols != nil {
			// Build keep indices: first occurrence of each column name
			seen := make(map[string]bool)
			keepIndices := make([]int, 0, len(tableCols))
			for i, c := range tableCols {
				if !seen[c] {
					seen[c] = true
					keepIndices = append(keepIndices, i)
				}
			}
			if len(keepIndices) < len(tableCols) {
				// Rebuild tableCols and filter result rows
				newCols := make([]string, len(keepIndices))
				for i, idx := range keepIndices {
					newCols[i] = tableCols[idx]
				}
				tableCols = newCols
				filteredResults := make([][]interface{}, len(results))
				for ri, row := range results {
					newRow := make([]interface{}, len(keepIndices))
					for i, idx := range keepIndices {
						if idx < len(row) {
							newRow[i] = row[idx]
						}
					}
					filteredResults[ri] = newRow
				}
				results = filteredResults
			}
		}
	}

	// For RIGHT JOIN: reorder columns to match original left/right table order
	// After the swap+dedup, columns are in [origRight-cols, origLeft-unique-cols] order
	// We need [origLeft-cols-deduped, origRight-unique-cols] order
	if isRightJoin && tableCols != nil && origLeftCols != nil && origRightCols != nil {
		isSelectStar := len(stmt.Columns) == 1
		if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" && cr.Table == "" {
			isSelectStar = true
		}
		if isSelectStar {
			// Build target column order: [origLeft deduped, origRight unique]
			targetCols := make([]string, 0, len(tableCols))
			seenT := make(map[string]bool)
			for _, c := range origLeftCols {
				if !seenT[c] {
					seenT[c] = true
					targetCols = append(targetCols, c)
				}
			}
			for _, c := range origRightCols {
				if !seenT[c] {
					seenT[c] = true
					targetCols = append(targetCols, c)
				}
			}
			// Build permutation from current tableCols to targetCols
			if len(targetCols) == len(tableCols) {
				curPos := make(map[string]int)
				for i, c := range tableCols {
					if _, already := curPos[c]; !already {
						curPos[c] = i
					}
				}
				perm := make([]int, len(targetCols))
				for i, c := range targetCols {
					perm[i] = curPos[c]
				}
				reorderedResults := make([][]interface{}, len(results))
				for ri, row := range results {
					newRow := make([]interface{}, len(perm))
					for i, p := range perm {
						if p < len(row) {
							newRow[i] = row[p]
						}
					}
					reorderedResults[ri] = newRow
				}
				tableCols = targetCols
				results = reorderedResults
			}
		}
	}

	// Get column names from the SELECT statement
	// For NATURAL/USING JOINs, build deduplicated column set (shared columns appear once)
	sharedCols := make(map[string]bool)
	if stmt.From.Join != nil && (stmt.From.Join.Natural || len(stmt.From.Join.UsingColumns) > 0) {
		for _, c := range stmt.From.Join.UsingColumns {
			sharedCols[c] = true
		}
	}

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
					// For NATURAL/USING JOIN: deduplicate shared columns (appear only once from left table)
					if len(sharedCols) > 0 {
						seenShared := make(map[string]bool)
						for _, c := range tableCols {
							if sharedCols[c] {
								if !seenShared[c] {
									seenShared[c] = true
									cols = append(cols, c)
								}
							} else {
								cols = append(cols, c)
							}
						}
					} else {
						cols = append(cols, tableCols...)
					}
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

	// If extra ORDER BY columns were added, sort now with the full columns, then strip extras
	if len(extraOrderByCols) > 0 {
		// Build full columns list (SELECT columns + extra ORDER BY cols)
		fullCols := append(cols, extraOrderByCols...)
		// Sort using full column set (use top-K heap when limit is known)
		topK := extractLimitInt(stmt.Limit, stmt.Offset)
		fmt.Printf("DEBUG execVMQuery: extraOrderByCols=%v, len(results)=%d, topK=%d, stmt.Limit=%v, fullCols=%v\n", extraOrderByCols, len(results), topK, stmt.Limit, fullCols)
		if len(results) > 0 {
			fmt.Printf("DEBUG execVMQuery: first row has %d values: %v\n", len(results[0]), results[0])
		}
		results = db.engine.SortRowsTopK(results, stmt.OrderBy, fullCols, topK)
		fmt.Printf("DEBUG execVMQuery: after SortRowsTopK, len(results)=%d\n", len(results))
		// Apply LIMIT/OFFSET if present (to avoid database.go re-applying with wrong cols)
		if stmt.Limit != nil {
			rows := &Rows{Columns: fullCols, Data: results}
			if lr, err2 := db.applyLimit(rows, stmt.Limit, stmt.Offset); err2 == nil {
				results = lr.Data
			}
			// Mark limit as consumed
			stmt.Limit = nil
			stmt.Offset = nil
		}
		// Strip extra ORDER BY columns from results
		numSelectCols := len(cols)
		stripped := make([][]interface{}, len(results))
		for i, row := range results {
			if len(row) > numSelectCols {
				stripped[i] = row[:numSelectCols]
			} else {
				stripped[i] = row
			}
		}
		results = stripped
		fmt.Printf("DEBUG execVMQuery: after strip, len(results)=%d\n", len(results))
		// Also mark ORDER BY as consumed so database.go doesn't re-sort
		stmt.OrderBy = nil
	}

	fmt.Printf("DEBUG execVMQuery: returning &Rows with %d rows\n", len(results))
	return &Rows{Columns: cols, Data: results}, nil
}

// extractLimitInt returns the integer value of a LIMIT expression, or 0 if not a constant integer.
func extractLimitInt(limitExpr, offsetExpr QP.Expr) int {
	if limitExpr == nil {
		return 0
	}
	lim := 0
	off := 0
	if lit, ok := limitExpr.(*QP.Literal); ok {
		if n, ok := lit.Value.(int64); ok {
			lim = int(n)
		}
	}
	if offsetExpr != nil {
		if lit, ok := offsetExpr.(*QP.Literal); ok {
			if n, ok := lit.Value.(int64); ok {
				off = int(n)
			}
		}
	}
	if lim <= 0 {
		return 0
	}
	return off + lim
}

// validateInsertColumnCount checks that INSERT column count matches value count.
func (db *Database) validateInsertColumnCount(sql string, tableName string, tableCols []string) error {
	upperSQL := strings.ToUpper(sql)
	if !strings.HasPrefix(upperSQL, "INSERT") {
		return nil
	}
	tokens, err := QP.NewTokenizer(sql).Tokenize()
	if err != nil {
		return nil
	}
	parser := QP.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil
	}
	insertStmt, ok := stmt.(*QP.InsertStmt)
	if !ok || insertStmt.UseDefaults || len(insertStmt.Values) == 0 {
		return nil
	}

	// Build a set of valid column names from the target table
	validCols := make(map[string]bool)
	for _, col := range tableCols {
		validCols[strings.ToLower(col)] = true
	}

	// Check that all explicitly specified column names exist in the table
	if len(insertStmt.Columns) > 0 {
		for _, col := range insertStmt.Columns {
			if !validCols[strings.ToLower(col)] {
				return fmt.Errorf("table %s has no column named %s", tableName, col)
			}
		}
	}

	// Check for invalid column references in VALUES (unquoted identifiers that aren't table columns)
	// Also include explicit INSERT column names if specified
	for _, col := range insertStmt.Columns {
		validCols[strings.ToLower(col)] = true
	}
	for _, row := range insertStmt.Values {
		for _, val := range row {
			if cr, ok := val.(*QP.ColumnRef); ok && cr.Table == "" && cr.Name != "*" {
				colLower := strings.ToLower(cr.Name)
				if !validCols[colLower] {
					return fmt.Errorf("no such column: %s", cr.Name)
				}
			}
		}
	}

	if len(insertStmt.Columns) > 0 {
		// Explicit columns: each row must have exactly that many values
		for _, row := range insertStmt.Values {
			if len(row) != len(insertStmt.Columns) {
				return fmt.Errorf("%d values for %d columns", len(row), len(insertStmt.Columns))
			}
		}
	} else {
		// No columns specified: each row must have exactly table column count
		numTableCols := len(tableCols)
		for _, row := range insertStmt.Values {
			if numTableCols > 0 && len(row) != numTableCols {
				return fmt.Errorf("table %s has %d columns but %d values were supplied", tableName, numTableCols, len(row))
			}
		}
	}
	return nil
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
				if defVal, hasDef := tableDefaults[col]; hasDef {
					vals = append(vals, literalToString(defVal))
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
		for _, col := range missingWithDefaults {
			rowVals = append(rowVals, literalToString(tableDefaults[col]))
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
	// Handle QP.Expr (e.g., DEFAULT (1+1)) - serialize back to SQL
	if expr, ok := val.(QP.Expr); ok {
		return exprToSQL(expr)
	}
	switch v := val.(type) {
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "X'" + hex.EncodeToString(v) + "'"
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return "NULL"
	}
}

// exprToSQL converts a QP expression to a SQL string representation.
func exprToSQL(expr QP.Expr) string {
	if expr == nil {
		return "NULL"
	}
	switch e := expr.(type) {
	case *QP.Literal:
		if e.Value == nil {
			return "NULL"
		}
		return literalToString(e.Value)
	case *QP.ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *QP.BinaryExpr:
		var op string
		switch e.Op {
		case QP.TokenPlus:
			op = "+"
		case QP.TokenMinus:
			op = "-"
		case QP.TokenAsterisk:
			op = "*"
		case QP.TokenSlash:
			op = "/"
		case QP.TokenPercent:
			op = "%"
		default:
			op = "+"
		}
		return "(" + exprToSQL(e.Left) + " " + op + " " + exprToSQL(e.Right) + ")"
	case *QP.UnaryExpr:
		if e.Op == QP.TokenMinus {
			return "-" + exprToSQL(e.Expr)
		}
		return exprToSQL(e.Expr)
	case *QP.FuncCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToSQL(arg)
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	default:
		return "NULL"
	}
}

func (db *Database) execVMDML(sql string, tableName string) (Result, error) {
	util.Assert(sql != "", "sql cannot be empty")
	util.Assert(tableName != "", "tableName cannot be empty")
	// Check if table exists (strip schema prefix if present)
	checkName := tableName
	if idx := strings.Index(tableName, "."); idx >= 0 {
		checkName = tableName[idx+1:]
	}
	// Resolve case-insensitive table name
	checkName = db.resolveTableName(checkName)
	tableName = checkName
	if _, exists := db.tables[checkName]; !exists {
		return Result{}, fmt.Errorf("no such table: %s", tableName)
	}

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

	// Validate INSERT column/value count before preprocessing
	if err := db.validateInsertColumnCount(sql, tableName, tableCols); err != nil {
		return Result{}, err
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
// Uses strings.Builder + type switch to avoid fmt.Sprintf overhead.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]interface{}, 0, len(rows))
	var b strings.Builder
	for _, row := range rows {
		b.Reset()
		for i, v := range row {
			if i > 0 {
				b.WriteByte(',')
			}
			switch val := v.(type) {
			case int64:
				b.WriteString(strconv.FormatInt(val, 10))
			case float64:
				b.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			case string:
				b.WriteString(val)
			case bool:
				if val {
					b.WriteString("true")
				} else {
					b.WriteString("false")
				}
			case []byte:
				b.WriteString(string(val))
			case nil:
				b.WriteString("<nil>")
			default:
				fmt.Fprintf(&b, "%v", val)
			}
		}
		key := b.String()
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// collectColumnRefs collects all unqualified column names referenced in an expression.
func collectColumnRefs(expr QP.Expr) []string {
	if expr == nil {
		return nil
	}
	var refs []string
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Name != "" && e.Name != "*" {
			refs = append(refs, e.Name)
		}
	case *QP.BinaryExpr:
		refs = append(refs, collectColumnRefs(e.Left)...)
		refs = append(refs, collectColumnRefs(e.Right)...)
	case *QP.UnaryExpr:
		refs = append(refs, collectColumnRefs(e.Expr)...)
	case *QP.FuncCall:
		for _, arg := range e.Args {
			refs = append(refs, collectColumnRefs(arg)...)
		}
	case *QP.CaseExpr:
		refs = append(refs, collectColumnRefs(e.Operand)...)
		for _, when := range e.Whens {
			refs = append(refs, collectColumnRefs(when.Condition)...)
			refs = append(refs, collectColumnRefs(when.Result)...)
		}
		refs = append(refs, collectColumnRefs(e.Else)...)
	case *QP.AliasExpr:
		refs = append(refs, collectColumnRefs(e.Expr)...)
	}
	return refs
}

// resolveNaturalUsing synthesizes a JOIN ON condition for NATURAL JOIN and JOIN ... USING (cols).
// This is called after schema info is available so we can find shared column names.
func (db *Database) resolveNaturalUsing(join *QP.Join, leftCols, rightCols []string, leftName, rightName string) {
	if join == nil {
		return
	}
	// Determine which columns to join on
	usingCols := join.UsingColumns
	if join.Natural && len(usingCols) == 0 {
		// Find columns that appear in both tables
		rightSet := make(map[string]bool)
		for _, c := range rightCols {
			rightSet[c] = true
		}
		for _, c := range leftCols {
			if rightSet[c] {
				usingCols = append(usingCols, c)
			}
		}
	}
	if len(usingCols) == 0 || join.Cond != nil {
		return
	}
	// Store the using columns back so dedup code can use them
	if join.Natural && len(join.UsingColumns) == 0 {
		join.UsingColumns = usingCols
	}
	// Build left.col = right.col AND ... condition
	var cond QP.Expr
	for _, col := range usingCols {
		eq := &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Table: leftName, Name: col},
			Right: &QP.ColumnRef{Table: rightName, Name: col},
		}
		if cond == nil {
			cond = eq
		} else {
			cond = &QP.BinaryExpr{Op: QP.TokenAnd, Left: cond, Right: eq}
		}
	}
	join.Cond = cond
}
