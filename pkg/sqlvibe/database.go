package sqlvibe

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/IS"
	"github.com/sqlvibe/sqlvibe/internal/PB"
	"github.com/sqlvibe/sqlvibe/internal/QE"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/TM"
	"github.com/sqlvibe/sqlvibe/internal/util"
)

type Database struct {
	pm             *DS.PageManager
	engine         *QE.QueryEngine
	tx             *Transaction
	txMgr          *TM.TransactionManager
	activeTx       *TM.Transaction
	dbPath         string
	tables         map[string]map[string]string        // table name -> column name -> type
	primaryKeys    map[string][]string                 // table name -> primary key column names
	columnOrder    map[string][]string                 // table name -> ordered column names
	columnDefaults map[string]map[string]interface{}   // table name -> column name -> default value
	columnNotNull  map[string]map[string]bool          // table name -> column name -> NOT NULL
	columnChecks   map[string]map[string]QP.Expr       // table name -> column name -> CHECK expression
	data           map[string][]map[string]interface{} // table name -> rows -> column name -> value
	indexes        map[string]*IndexInfo               // index name -> index info
	views          map[string]string                   // view name -> SELECT SQL
	isRegistry     *IS.Registry                        // information_schema registry
	txSnapshot     *dbSnapshot                         // snapshot for transaction rollback
	tableBTrees    map[string]*DS.BTree                // table name -> B-Tree for storage
}

type dbSnapshot struct {
	data           map[string][]map[string]interface{}
	tables         map[string]map[string]string
	primaryKeys    map[string][]string
	columnOrder    map[string][]string
	columnDefaults map[string]map[string]interface{}
	columnNotNull  map[string]map[string]bool
	columnChecks   map[string]map[string]QP.Expr
	indexes        map[string]*IndexInfo
}

func (db *Database) captureSnapshot() *dbSnapshot {
	snap := &dbSnapshot{
		data:           make(map[string][]map[string]interface{}),
		tables:         make(map[string]map[string]string),
		primaryKeys:    make(map[string][]string),
		columnOrder:    make(map[string][]string),
		columnDefaults: make(map[string]map[string]interface{}),
		columnNotNull:  make(map[string]map[string]bool),
		columnChecks:   make(map[string]map[string]QP.Expr),
		indexes:        make(map[string]*IndexInfo),
	}
	for tbl, rows := range db.data {
		rowsCopy := make([]map[string]interface{}, len(rows))
		for i, row := range rows {
			rowCopy := make(map[string]interface{}, len(row))
			for k, v := range row {
				// Deep copy byte slices to prevent shared state between snapshot and live data
				if b, ok := v.([]byte); ok {
					bCopy := make([]byte, len(b))
					copy(bCopy, b)
					rowCopy[k] = bCopy
				} else {
					rowCopy[k] = v
				}
			}
			rowsCopy[i] = rowCopy
		}
		snap.data[tbl] = rowsCopy
	}
	for k, v := range db.tables {
		colsCopy := make(map[string]string, len(v))
		for ck, cv := range v {
			colsCopy[ck] = cv
		}
		snap.tables[k] = colsCopy
	}
	for k, v := range db.primaryKeys {
		pkCopy := make([]string, len(v))
		copy(pkCopy, v)
		snap.primaryKeys[k] = pkCopy
	}
	for k, v := range db.columnOrder {
		colOrdCopy := make([]string, len(v))
		copy(colOrdCopy, v)
		snap.columnOrder[k] = colOrdCopy
	}
	for k, v := range db.columnDefaults {
		defCopy := make(map[string]interface{}, len(v))
		for dk, dv := range v {
			defCopy[dk] = dv
		}
		snap.columnDefaults[k] = defCopy
	}
	for k, v := range db.columnNotNull {
		nnCopy := make(map[string]bool, len(v))
		for nk, nv := range v {
			nnCopy[nk] = nv
		}
		snap.columnNotNull[k] = nnCopy
	}
	for k, v := range db.columnChecks {
		chkCopy := make(map[string]QP.Expr, len(v))
		for ck, cv := range v {
			chkCopy[ck] = cv
		}
		snap.columnChecks[k] = chkCopy
	}
	for k, v := range db.indexes {
		idxCopy := &IndexInfo{
			Name:    v.Name,
			Table:   v.Table,
			Unique:  v.Unique,
			Columns: make([]string, len(v.Columns)),
		}
		copy(idxCopy.Columns, v.Columns)
		snap.indexes[k] = idxCopy
	}
	return snap
}

func (db *Database) restoreSnapshot(snap *dbSnapshot) {
	db.data = snap.data
	db.tables = snap.tables
	db.primaryKeys = snap.primaryKeys
	db.columnOrder = snap.columnOrder
	db.columnDefaults = snap.columnDefaults
	db.columnNotNull = snap.columnNotNull
	db.columnChecks = snap.columnChecks
	db.indexes = snap.indexes
}

type IndexInfo struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

type Conn interface {
	Exec(sql string) (Result, error)
	Query(sql string) (*Rows, error)
	Close() error
}

type Result struct {
	LastInsertRowID int64
	RowsAffected    int64
}

type Rows struct {
	Columns []string
	Data    [][]interface{}
	pos     int  // Current position, starts at 0
	started bool // Whether Next() has been called
}

func (r *Rows) Next() bool {
	if r.Data == nil {
		return false
	}
	// On first call, don't advance pos (it's already at 0)
	if !r.started {
		r.started = true
		return len(r.Data) > 0
	}
	// On subsequent calls, advance pos
	r.pos++
	return r.pos < len(r.Data)
}

func (r *Rows) Scan(dest ...interface{}) error {
	util.Assert(r.pos >= 0, "row position cannot be negative: %d", r.pos)

	if r.Data == nil || r.pos < 0 || r.pos >= len(r.Data) {
		return fmt.Errorf("no rows available")
	}
	row := r.Data[r.pos]
	for i, val := range dest {
		if i >= len(row) {
			break
		}
		switch d := val.(type) {
		case *int:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = v
				case int64:
					*d = int(v)
				case float64:
					*d = int(v)
				}
			}
		case *int64:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = int64(v)
				case int64:
					*d = v
				case float64:
					*d = int64(v)
				}
			}
		case *float64:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = float64(v)
				case int64:
					*d = float64(v)
				case float64:
					*d = v
				}
			}
		case *string:
			if row[i] != nil {
				switch v := row[i].(type) {
				case string:
					*d = v
				default:
					*d = fmt.Sprintf("%v", v)
				}
			}
		case *interface{}:
			*d = row[i]
		}
	}
	return nil
}

type Statement struct {
	db     *Database
	sql    string
	parsed QP.ASTNode
}

type Transaction struct {
	db        *Database
	committed bool
	rollback  bool
}

func Open(path string) (*Database, error) {
	file, err := PB.OpenFile(path, PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		return nil, err
	}

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		file.Close()
		return nil, err
	}

	data := make(map[string][]map[string]interface{})
	engine := QE.NewQueryEngine(pm, data)
	txMgr := TM.NewTransactionManager(pm)

	return &Database{
		pm:             pm,
		engine:         engine,
		txMgr:          txMgr,
		activeTx:       nil,
		dbPath:         path,
		tables:         make(map[string]map[string]string),
		primaryKeys:    make(map[string][]string),
		columnOrder:    make(map[string][]string),
		columnDefaults: make(map[string]map[string]interface{}),
		columnNotNull:  make(map[string]map[string]bool),
		columnChecks:   make(map[string]map[string]QP.Expr),
		data:           data,
		indexes:        make(map[string]*IndexInfo),
		views:          make(map[string]string),
		tableBTrees:    make(map[string]*DS.BTree),
	}, nil
}

func (db *Database) getOrderedColumns(tableName string) []string {
	if cols, ok := db.columnOrder[tableName]; ok {
		return cols
	}
	if schema, ok := db.tables[tableName]; ok {
		var cols []string
		for col := range schema {
			cols = append(cols, col)
		}
		return cols
	}
	return nil
}

// resolveTableName returns the canonical table name (case-insensitive lookup).
// Returns the original key if found with exact match, otherwise finds a case-insensitive match.
func (db *Database) resolveTableName(name string) string {
	if _, ok := db.tables[name]; ok {
		return name
	}
	lower := strings.ToLower(name)
	for k := range db.tables {
		if strings.ToLower(k) == lower {
			return k
		}
	}
	return name
}

func (db *Database) evalConstantExpression(stmt *QP.SelectStmt) (*Rows, error) {
	if len(stmt.Columns) == 0 {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	var cols []string
	var row []interface{}
	emptyRow := make(map[string]interface{})

	for _, col := range stmt.Columns {
		var colName string
		var colValue interface{}

		switch c := col.(type) {
		case *QP.AliasExpr:
			colName = c.Alias
			if c.Expr != nil {
				colValue = db.engine.EvalExpr(emptyRow, c.Expr)
			}
		case *QP.BinaryExpr:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		case *QP.UnaryExpr:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		case *QP.Literal:
			colName = "expr"
			colValue = c.Value
		case *QP.FuncCall:
			colName = c.Name
			colValue = db.engine.EvalExpr(emptyRow, c)
		default:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		}

		cols = append(cols, colName)
		row = append(row, colValue)
	}

	return &Rows{Columns: cols, Data: [][]interface{}{row}}, nil
}

func (db *Database) Exec(sql string) (Result, error) {
	// Handle multi-statement SQL (separated by semicolons)
	stmts := splitStatements(sql)
	if len(stmts) > 1 {
		var lastResult Result
		for _, s := range stmts {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			result, err := db.Exec(s)
			if err != nil {
				return Result{}, err
			}
			lastResult = result
		}
		return lastResult, nil
	}

	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return Result{}, err
	}
	if len(tokens) == 0 {
		return Result{}, nil
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return Result{}, err
	}
	if ast == nil {
		return Result{}, nil
	}

	switch ast.NodeType() {
	case "CreateTableStmt":
		stmt := ast.(*QP.CreateTableStmt)
		// Case-insensitive existence check
		resolved := db.resolveTableName(stmt.Name)
		if _, exists := db.tables[resolved]; exists {
			if stmt.IfNotExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("table %s already exists", stmt.Name)
		}

		// Handle CREATE TABLE ... AS SELECT
		if stmt.AsSelect != nil {
			return db.execCreateTableAsSelect(stmt)
		}

		schema := make(map[string]QE.ColumnType)
		colTypes := make(map[string]string)
		var pkCols []string
		db.columnDefaults[stmt.Name] = make(map[string]interface{})
		db.columnNotNull[stmt.Name] = make(map[string]bool)
		db.columnChecks[stmt.Name] = make(map[string]QP.Expr)
		seenCols := make(map[string]bool)
		if len(stmt.Columns) == 0 {
			return Result{}, fmt.Errorf("table must have at least one column")
		}
		for _, col := range stmt.Columns {
			if seenCols[col.Name] {
				return Result{}, fmt.Errorf("duplicate column name: %s", col.Name)
			}
			seenCols[col.Name] = true
			schema[col.Name] = QE.ColumnType{Name: col.Name, Type: col.Type}
			colTypes[col.Name] = col.Type
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
			if col.Default != nil {
				db.columnDefaults[stmt.Name][col.Name] = col.Default
			}
			if col.NotNull {
				db.columnNotNull[stmt.Name][col.Name] = true
			}
			if col.Check != nil {
				db.columnChecks[stmt.Name][col.Name] = col.Check
			}
		}
		// Store table-level CHECK constraints under special keys
		for i, tableCheck := range stmt.TableChecks {
			db.columnChecks[stmt.Name][fmt.Sprintf("__table_check_%d__", i)] = tableCheck
		}
		db.engine.RegisterTable(stmt.Name, schema)
		db.tables[stmt.Name] = colTypes
		var colOrder []string
		for _, col := range stmt.Columns {
			colOrder = append(colOrder, col.Name)
		}
		db.columnOrder[stmt.Name] = colOrder
		if len(pkCols) > 0 {
			db.primaryKeys[stmt.Name] = pkCols
		}

		// Create BTree for table storage
		bt := DS.NewBTree(db.pm, 0, true)
		db.tableBTrees[stmt.Name] = bt

		return Result{}, nil
	case "InsertStmt":
		stmt := ast.(*QP.InsertStmt)
		if stmt.SelectQuery != nil {
			return db.execInsertSelect(stmt)
		}
		return db.execVMDML(sql, stmt.Table)
	case "UpdateStmt":
		stmt := ast.(*QP.UpdateStmt)
		return db.execVMDML(sql, stmt.Table)
	case "DeleteStmt":
		stmt := ast.(*QP.DeleteStmt)
		return db.execVMDML(sql, stmt.Table)
	case "DropTableStmt":
		stmt := ast.(*QP.DropTableStmt)
		if _, exists := db.tables[stmt.Name]; !exists {
			if stmt.IfExists {
				return Result{}, nil
			}
		}
		delete(db.tables, stmt.Name)
		delete(db.data, stmt.Name)
		delete(db.primaryKeys, stmt.Name)
		delete(db.columnDefaults, stmt.Name)
		delete(db.columnOrder, stmt.Name)
		delete(db.columnChecks, stmt.Name)
		delete(db.columnNotNull, stmt.Name)
		delete(db.tableBTrees, stmt.Name)
		return Result{}, nil
	case "CreateViewStmt":
		return db.execCreateView(ast.(*QP.CreateViewStmt), sql)
	case "DropViewStmt":
		stmt := ast.(*QP.DropViewStmt)
		if _, exists := db.views[stmt.Name]; !exists {
			if stmt.IfExists {
				return Result{}, nil
			}
		}
		delete(db.views, stmt.Name)
		return Result{}, nil
	case "AlterTableStmt":
		return db.execAlterTable(ast.(*QP.AlterTableStmt))
	case "CreateIndexStmt":
		stmt := ast.(*QP.CreateIndexStmt)
		if _, exists := db.indexes[stmt.Name]; exists {
			if stmt.IfNotExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("index %s already exists", stmt.Name)
		}
		if _, exists := db.tables[stmt.Table]; !exists {
			return Result{}, fmt.Errorf("table %s does not exist", stmt.Table)
		}
		db.indexes[stmt.Name] = &IndexInfo{
			Name:    stmt.Name,
			Table:   stmt.Table,
			Columns: stmt.Columns,
			Unique:  stmt.Unique,
		}
		return Result{}, nil
	case "DropIndexStmt":
		stmt := ast.(*QP.DropIndexStmt)
		delete(db.indexes, stmt.Name)
		return Result{}, nil
	case "BeginStmt":
		stmt := ast.(*QP.BeginStmt)
		if db.activeTx != nil {
			return Result{}, fmt.Errorf("transaction already active")
		}
		txType := TM.TransactionDeferred
		if stmt.Type == "IMMEDIATE" {
			txType = TM.TransactionImmediate
		} else if stmt.Type == "EXCLUSIVE" {
			txType = TM.TransactionExclusive
		}
		tx, err := db.txMgr.Begin(db.dbPath, txType)
		if err != nil {
			return Result{}, fmt.Errorf("failed to begin transaction: %w", err)
		}
		db.activeTx = tx
		db.txSnapshot = db.captureSnapshot()
		return Result{}, nil
	case "CommitStmt":
		if db.activeTx == nil {
			return Result{}, fmt.Errorf("no transaction active")
		}
		err := db.txMgr.CommitTransaction(db.activeTx.ID)
		if err != nil {
			return Result{}, fmt.Errorf("failed to commit transaction: %w", err)
		}
		db.activeTx = nil
		db.txSnapshot = nil
		return Result{}, nil
	case "RollbackStmt":
		if db.activeTx == nil {
			return Result{}, fmt.Errorf("no transaction active")
		}
		err := db.txMgr.RollbackTransaction(db.activeTx.ID)
		if err != nil {
			return Result{}, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		if db.txSnapshot != nil {
			db.restoreSnapshot(db.txSnapshot)
			db.txSnapshot = nil
		}
		// activeTx is cleared after snapshot restore so the engine sees clean state
		db.activeTx = nil
		return Result{}, nil
	}

	return Result{}, nil
}

func (db *Database) Query(sql string) (*Rows, error) {
	// Handle multi-statement SQL (separated by semicolons)
	// Execute all but the last as Exec, return results of last SELECT
	stmts := splitStatements(sql)
	if len(stmts) > 1 {
		var lastRows *Rows
		for i, s := range stmts {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if i == len(stmts)-1 {
				// Execute last statement as query
				rows, err := db.Query(s)
				if err != nil {
					return nil, err
				}
				lastRows = rows
			} else {
				// Execute intermediate statements as exec
				if _, err := db.Exec(s); err != nil {
					return nil, err
				}
			}
		}
		return lastRows, nil
	}

	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, nil
	}

	if ast.NodeType() == "PragmaStmt" {
		return db.handlePragma(ast.(*QP.PragmaStmt))
	}

	if ast.NodeType() == "ExplainStmt" {
		return db.handleExplain(ast.(*QP.ExplainStmt), sql)
	}

	if ast.NodeType() == "SelectStmt" {
		stmt := ast.(*QP.SelectStmt)

		// Handle CTE (WITH ... AS (...) SELECT ...) by materializing temp tables
		if len(stmt.CTEs) > 0 {
			var cteNames []string
			for _, cte := range stmt.CTEs {
				cteRows, err := db.execSelectStmt(cte.Select)
				if err != nil {
					return nil, err
				}
				if cteRows == nil {
					cteRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
				}
				colTypes := make(map[string]string)
				for _, col := range cteRows.Columns {
					colTypes[col] = "TEXT"
				}
				db.tables[cte.Name] = colTypes
				db.columnOrder[cte.Name] = cteRows.Columns
				rowMaps := make([]map[string]interface{}, len(cteRows.Data))
				for i, row := range cteRows.Data {
					rm := make(map[string]interface{})
					for j, col := range cteRows.Columns {
						if j < len(row) {
							rm[col] = row[j]
						}
					}
					rowMaps[i] = rm
				}
				db.data[cte.Name] = rowMaps
				cteNames = append(cteNames, cte.Name)
			}
			stmt.CTEs = nil
			rows, err := db.execSelectStmt(stmt)
			for _, name := range cteNames {
				delete(db.tables, name)
				delete(db.columnOrder, name)
				delete(db.data, name)
			}
			return rows, err
		}

		if stmt.From == nil {
			return db.evalConstantExpression(stmt)
		}

		// Resolve column aliases in WHERE, GROUP BY, HAVING, ORDER BY
		// This allows SQLite-style alias references like: SELECT a AS x ... WHERE x > 0
		resolveSelectAliases(stmt)

		// Handle derived table in FROM clause: SELECT ... FROM (SELECT ...) AS alias
		if stmt.From.Subquery != nil {
			return db.execDerivedTableQuery(stmt)
		}

		var tableName string
		var schemaName string
		if stmt.From != nil {
			tableName = stmt.From.Name
			schemaName = stmt.From.Schema
		}
		if tableName == "" {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		// Resolve case-insensitive table name (unquoted identifiers are case-insensitive)
		tableName = db.resolveTableName(tableName)
		stmt.From.Name = tableName

		// Handle sqlite_master virtual table
		if tableName == "sqlite_master" {
			return db.querySqliteMaster(stmt)
		}

		// Handle views by substituting the view SQL (case-insensitive)
		if viewSQL, isView := db.views[tableName]; isView {
			return db.queryView(viewSQL, stmt, sql)
		}
		// Also try case-insensitive view lookup
		lowerTbl := strings.ToLower(tableName)
		for vn, vs := range db.views {
			if strings.ToLower(vn) == lowerTbl {
				return db.queryView(vs, stmt, sql)
			}
		}

		// Handle information_schema virtual tables
		if strings.ToLower(schemaName) == "information_schema" {
			fullName := schemaName + "." + tableName
			return db.queryInformationSchema(stmt, fullName)
		}

		// SetOp (UNION, EXCEPT, INTERSECT) - hybrid implementation
		// Execute left and right queries separately using VM, then combine
		if stmt.SetOp != "" && stmt.SetOpRight != nil {
			// Compile and execute left query
			leftStmt := *stmt
			leftStmt.SetOp = ""
			leftStmt.SetOpAll = false
			leftStmt.SetOpRight = nil

			leftRows, err := db.execSelectStmt(&leftStmt)
			if err != nil {
				return nil, fmt.Errorf("SetOp left query failed: %w", err)
			}

			// Compile and execute right query
			rightRows, err := db.execSelectStmt(stmt.SetOpRight)
			if err != nil {
				return nil, fmt.Errorf("SetOp right query failed: %w", err)
			}

			// Combine results using SetOp logic
			// Validate column count match (SQLite requires same number of columns)
			if len(leftRows.Columns) != len(rightRows.Columns) {
				return nil, fmt.Errorf("SELECTs to the left and right of %s do not have the same number of result columns", stmt.SetOp)
			}
			combinedData := db.applySetOp(leftRows.Data, rightRows.Data, stmt.SetOp, stmt.SetOpAll)

			// Apply ORDER BY and LIMIT if present
			rows := &Rows{Columns: leftRows.Columns, Data: combinedData}
			if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
				rows, err = db.sortResults(rows, stmt.OrderBy)
				if err != nil {
					return nil, err
				}
			}
			if stmt.Limit != nil {
				rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
				if err != nil {
					return nil, err
				}
			}

			return rows, nil
		}

		// VM execution for all SELECT queries
		rows, err := db.execVMQuery(sql, stmt)
		if err != nil {
			return nil, err
		}

		// VM can return empty results validly - don't treat as error
		if rows == nil {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		// Handle ORDER BY - sort results
		if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
			rows, err = db.sortResults(rows, stmt.OrderBy)
			if err != nil {
				return nil, err
			}
		}

		// Handle LIMIT
		if stmt.Limit != nil {
			rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
			if err != nil {
				return nil, err
			}
		}

		return rows, nil
	}

	return nil, nil
}

func (db *Database) Close() error {
	return db.pm.Close()
}

func (db *Database) Prepare(sql string) (*Statement, error) {
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty SQL")
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, fmt.Errorf("failed to parse SQL")
	}

	return &Statement{db: db, sql: sql, parsed: ast}, nil
}

func (s *Statement) Exec(params ...interface{}) (Result, error) {
	return s.db.ExecWithParams(s.sql, params)
}

func (s *Statement) Query(params ...interface{}) (*Rows, error) {
	return s.db.QueryWithParams(s.sql, params)
}

func (s *Statement) Close() error {
	return nil
}

func ordinalSuffix(n int) string {
	switch n {
	case 1:
		return "1st"
	case 2:
		return "2nd"
	case 3:
		return "3rd"
	default:
		return fmt.Sprintf("%dth", n)
	}
}

func (db *Database) sortResults(rows *Rows, orderBy []QP.OrderBy) (*Rows, error) {
	if len(orderBy) == 0 || rows == nil || len(rows.Data) == 0 {
		return rows, nil
	}

	// Validate integer ORDER BY column references
	numCols := len(rows.Columns)
	for i, ob := range orderBy {
		if lit, ok := ob.Expr.(*QP.Literal); ok {
			if n, ok := lit.Value.(int64); ok {
				// SQLite uses 1-based integer ORDER BY column references
				if n < 1 || int(n) > numCols {
					return nil, fmt.Errorf("%s ORDER BY term out of range - should be between 1 and %d", ordinalSuffix(i+1), numCols)
				}
				// Replace literal with column reference (1-based → 0-based)
				orderBy[i].Expr = &QP.ColumnRef{Name: rows.Columns[n-1]}
			}
		}
	}

	sorted := db.engine.SortRows(rows.Data, orderBy, rows.Columns)
	return &Rows{Columns: rows.Columns, Data: sorted}, nil
}

func (db *Database) ExecWithParams(sql string, params []interface{}) (Result, error) {
	return db.Exec(sql)
}

func (db *Database) QueryWithParams(sql string, params []interface{}) (*Rows, error) {
	return db.Query(sql)
}

func (db *Database) Begin() (*Transaction, error) {
	if db.tx != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}
	db.tx = &Transaction{db: db, committed: false, rollback: false}
	return db.tx, nil
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rollback {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.committed = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	tx.rollback = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Exec(sql string) (Result, error) {
	return tx.db.Exec(sql)
}

func (tx *Transaction) Query(sql string) (*Rows, error) {
	return tx.db.Query(sql)
}

func (db *Database) extractValue(expr QP.Expr) interface{} {
	return db.engine.ExtractValue(expr)
}

func (db *Database) extractValueTyped(expr QP.Expr, colType string) interface{} {
	return db.engine.ExtractValueTyped(expr, colType)
}

func (db *Database) negateValue(val interface{}) interface{} {
	return db.engine.Negate(val)
}

func (db *Database) convertStringToType(val string, colType string) interface{} {
	return db.engine.ConvertStringToType(val, colType)
}

func (db *Database) MustExec(sql string, params ...interface{}) Result {
	res, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	return res
}

func (db *Database) tryUseIndex(tableName string, where QP.Expr) []map[string]interface{} {
	// Convert IndexInfo to QE.IndexInfo
	qeIndexes := make(map[string]*QE.IndexInfo)
	for name, idx := range db.indexes {
		qeIndexes[name] = &QE.IndexInfo{
			Name:    idx.Name,
			Table:   idx.Table,
			Columns: idx.Columns,
			Unique:  idx.Unique,
		}
	}
	return db.engine.TryUseIndex(tableName, where, qeIndexes)
}

func (db *Database) scanByIndexValue(tableName, colName string, value interface{}, unique bool) []map[string]interface{} {
	return db.engine.ScanByIndexValue(tableName, colName, value, unique)
}

func (db *Database) applyOrderBy(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	if len(orderBy) == 0 || len(data) == 0 {
		return data
	}
	return db.engine.SortRows(data, orderBy, cols)
}

func (db *Database) applyLimit(rows *Rows, limitExpr QP.Expr, offsetExpr QP.Expr) (*Rows, error) {
	if rows == nil || len(rows.Data) == 0 {
		return rows, nil
	}

	limit := len(rows.Data)
	offset := 0

	if limitExpr != nil {
		if lit, ok := limitExpr.(*QP.Literal); ok {
			if num, ok := lit.Value.(int64); ok {
				limit = int(num)
			}
		}
	}

	if offsetExpr != nil {
		if lit, ok := offsetExpr.(*QP.Literal); ok {
			if num, ok := lit.Value.(int64); ok {
				offset = int(num)
			}
		}
	}

	// Use QE's ApplyLimit
	rows.Data = db.engine.ApplyLimit(rows.Data, limit, offset)
	return rows, nil
}

func (db *Database) serializeRow(row map[string]interface{}) []byte {
	result := make([]byte, 0)
	for key, val := range row {
		result = append(result, []byte(key)...)
		result = append(result, '=')
		switch v := val.(type) {
		case int64:
			result = append(result, []byte(fmt.Sprintf("%d", v))...)
		case float64:
			result = append(result, []byte(fmt.Sprintf("%f", v))...)
		case string:
			result = append(result, []byte(v)...)
		case []byte:
			result = append(result, '[')
			for i, b := range v {
				if i > 0 {
					result = append(result, ',')
				}
				result = append(result, []byte(fmt.Sprintf("%d", b))...)
			}
			result = append(result, ']')
		case nil:
		default:
			result = append(result, []byte(fmt.Sprintf("%v", v))...)
		}
		result = append(result, ';')
	}

	return result
}

func (db *Database) querySqliteMaster(stmt *QP.SelectStmt) (*Rows, error) {
	allResults := make([]map[string]interface{}, 0)
	for tableName := range db.tables {
		sql := fmt.Sprintf("CREATE TABLE %s ()", tableName)
		allResults = append(allResults, map[string]interface{}{
			"type":     "table",
			"name":     tableName,
			"tbl_name": tableName,
			"rootpage": int64(0),
			"sql":      sql,
		})
	}
	// Include views
	for viewName, viewSQL := range db.views {
		allResults = append(allResults, map[string]interface{}{
			"type":     "view",
			"name":     viewName,
			"tbl_name": viewName,
			"rootpage": int64(0),
			"sql":      "CREATE VIEW " + viewName + " AS " + viewSQL,
		})
	}
	// Include indexes
	for idxName, idx := range db.indexes {
		allResults = append(allResults, map[string]interface{}{
			"type":     "index",
			"name":     idxName,
			"tbl_name": idx.Table,
			"rootpage": int64(0),
			"sql":      fmt.Sprintf("CREATE INDEX %s ON %s", idxName, idx.Table),
		})
	}

	filtered := make([]map[string]interface{}, 0)
	for _, row := range allResults {
		if db.evalWhereOnMap(stmt.Where, row) {
			filtered = append(filtered, row)
		}
	}

	sqliteMasterCols := []string{"type", "name", "tbl_name", "rootpage", "sql"}

	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			if cr.Name == "*" {
				cols = append(cols, sqliteMasterCols...)
			} else {
				cols = append(cols, cr.Name)
			}
		}
	}
	if len(cols) == 0 {
		cols = sqliteMasterCols
	}

	resultData := make([][]interface{}, 0)
	for _, row := range filtered {
		resultRow := make([]interface{}, 0)
		for _, colName := range cols {
			resultRow = append(resultRow, row[colName])
		}
		resultData = append(resultData, resultRow)
	}

	// Sort by name if ORDER BY name is requested
	if stmt.OrderBy != nil {
		for _, ob := range stmt.OrderBy {
			if cr, ok := ob.Expr.(*QP.ColumnRef); ok && cr.Name == "name" {
				sort.Slice(resultData, func(i, j int) bool {
					nameIdx := -1
					for k, c := range cols {
						if c == "name" {
							nameIdx = k
							break
						}
					}
					if nameIdx < 0 {
						return false
					}
					ni := fmt.Sprintf("%v", resultData[i][nameIdx])
					nj := fmt.Sprintf("%v", resultData[j][nameIdx])
					if ob.Desc {
						return ni > nj
					}
					return ni < nj
				})
			}
		}
	} else {
		// Default sort by name for deterministic output
		nameIdx := -1
		for k, c := range cols {
			if c == "name" {
				nameIdx = k
				break
			}
		}
		if nameIdx >= 0 {
			sort.Slice(resultData, func(i, j int) bool {
				ni := fmt.Sprintf("%v", resultData[i][nameIdx])
				nj := fmt.Sprintf("%v", resultData[j][nameIdx])
				return ni < nj
			})
		}
	}

	return &Rows{Columns: cols, Data: resultData}, nil
}

// queryInformationSchema handles queries to information_schema virtual tables
func (db *Database) queryInformationSchema(stmt *QP.SelectStmt, tableName string) (*Rows, error) {
	// Extract view name from "information_schema.viewname"
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid information_schema table name: %s", tableName)
	}
	viewName := strings.ToLower(parts[1])

	// Generate data based on view type
	var allResults [][]interface{}
	var columnNames []string

	switch viewName {
	case "columns":
		columnNames = []string{"column_name", "table_name", "table_schema", "data_type", "is_nullable", "column_default"}
		// Extract columns from in-memory schema
		for tblName, colTypes := range db.tables {
			orderedCols := db.columnOrder[tblName]
			pkCols := db.primaryKeys[tblName]
			pkMap := make(map[string]bool)
			for _, pk := range pkCols {
				pkMap[pk] = true
			}
			for _, colName := range orderedCols {
				colType := colTypes[colName]
				isNullable := "YES"
				// PRIMARY KEY columns cannot be NULL
				if pkMap[colName] {
					isNullable = "NO"
				} else if strings.Contains(strings.ToUpper(colType), "NOT NULL") {
					isNullable = "NO"
				}
				allResults = append(allResults, []interface{}{
					colName,
					tblName,
					"main",
					colType,
					isNullable,
					nil, // column_default (not tracked yet)
				})
			}
		}

	case "tables":
		columnNames = []string{"table_name", "table_schema", "table_type"}
		// Extract tables from in-memory schema
		for tblName := range db.tables {
			allResults = append(allResults, []interface{}{
				tblName,
				"main",
				"BASE TABLE",
			})
		}

	case "views":
		columnNames = []string{"table_name", "table_schema", "view_definition"}
		// No views tracked yet, return empty

	case "table_constraints":
		columnNames = []string{"constraint_name", "table_name", "table_schema", "constraint_type"}
		// Extract PRIMARY KEY constraints from in-memory schema
		for tblName, pkCols := range db.primaryKeys {
			if len(pkCols) > 0 {
				constraintName := fmt.Sprintf("pk_%s", tblName)
				allResults = append(allResults, []interface{}{
					constraintName,
					tblName,
					"main",
					"PRIMARY KEY",
				})
			}
		}

	case "referential_constraints":
		columnNames = []string{"constraint_name", "unique_constraint_schema", "unique_constraint_name"}
		// No foreign keys tracked yet, return empty

	case "key_column_usage":
		columnNames = []string{"constraint_name", "table_name", "table_schema", "column_name", "ordinal_position"}
		// Extract PRIMARY KEY column usage from in-memory schema
		for tblName, pkCols := range db.primaryKeys {
			if len(pkCols) > 0 {
				constraintName := fmt.Sprintf("pk_%s", tblName)
				for i, colName := range pkCols {
					allResults = append(allResults, []interface{}{
						constraintName,
						tblName,
						"main",
						colName,
						int64(i + 1), // ordinal position starts at 1
					})
				}
			}
		}

	default:
		return nil, fmt.Errorf("unknown information_schema view: %s", viewName)
	}

	// Filter based on WHERE clause (simple support)
	filtered := allResults
	if stmt.Where != nil {
		filtered = make([][]interface{}, 0)
		for _, row := range allResults {
			if db.matchesWhereClause(row, columnNames, stmt.Where) {
				filtered = append(filtered, row)
			}
		}
	}

	// Select specific columns or all
	var selectedCols []string
	var selectedData [][]interface{}

	// Check if SELECT *
	if len(stmt.Columns) == 1 {
		if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" {
			// SELECT * - return all columns
			selectedCols = columnNames
			selectedData = filtered
		}
	}

	if len(selectedCols) == 0 {
		// SELECT specific columns
		for _, col := range stmt.Columns {
			if cr, ok := col.(*QP.ColumnRef); ok {
				selectedCols = append(selectedCols, cr.Name)
			}
		}

		// Project columns
		for _, row := range filtered {
			projectedRow := make([]interface{}, 0)
			for _, selCol := range selectedCols {
				// Find column index
				colIdx := -1
				for i, cn := range columnNames {
					if cn == selCol {
						colIdx = i
						break
					}
				}
				if colIdx >= 0 && colIdx < len(row) {
					projectedRow = append(projectedRow, row[colIdx])
				} else {
					projectedRow = append(projectedRow, nil)
				}
			}
			selectedData = append(selectedData, projectedRow)
		}
	}

	// Apply ORDER BY if present
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		rows := &Rows{Columns: selectedCols, Data: selectedData}
		sortedRows, err := db.sortResults(rows, stmt.OrderBy)
		if err != nil {
			return nil, err
		}
		return sortedRows, nil
	}

	return &Rows{Columns: selectedCols, Data: selectedData}, nil
}

// matchesWhereClause checks if a row matches the WHERE clause
func (db *Database) matchesWhereClause(row []interface{}, columnNames []string, where QP.Expr) bool {
	// Simple WHERE filtering (only equality checks for now)
	if binExpr, ok := where.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenEq {
		if colRef, ok := binExpr.Left.(*QP.ColumnRef); ok {
			if lit, ok := binExpr.Right.(*QP.Literal); ok {
				// Find column index
				colIdx := -1
				for i, cn := range columnNames {
					if cn == colRef.Name {
						colIdx = i
						break
					}
				}
				if colIdx >= 0 && colIdx < len(row) {
					return row[colIdx] == lit.Value
				}
			}
		}
	}
	// Default to include if we can't evaluate
	return true
}

func (db *Database) combineRows(left, right map[string]interface{}, stmt *QP.SelectStmt) []interface{} {
	row := make([]interface{}, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			colName := cr.Name
			if idx := strings.Index(cr.Name, "."); idx > 0 {
				colName = cr.Name[idx+1:]
			}
			if val, ok := left[colName]; ok {
				row = append(row, val)
			} else if val, ok := right[colName]; ok {
				row = append(row, val)
			} else {
				row = append(row, nil)
			}
		}
	}
	return row
}

func (db *Database) getJoinColumns(stmt *QP.SelectStmt) []string {
	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			colName := cr.Name
			if idx := strings.Index(cr.Name, "."); idx > 0 {
				colName = cr.Name[idx+1:]
			}
			cols = append(cols, colName)
		}
	}
	return cols
}

func (db *Database) getRightColumns(right []map[string]interface{}) []string {
	if len(right) == 0 {
		return []string{}
	}
	cols := make([]string, 0)
	for k := range right[0] {
		cols = append(cols, k)
	}
	return cols
}

// execCreateView handles CREATE VIEW
func (db *Database) execCreateView(stmt *QP.CreateViewStmt, origSQL string) (Result, error) {
	util.AssertNotNil(stmt, "CreateViewStmt")
	util.Assert(stmt.Name != "", "CreateViewStmt.Name cannot be empty")
if _, exists := db.views[stmt.Name]; exists {
if stmt.IfNotExists {
return Result{}, nil
}
return Result{}, fmt.Errorf("view %s already exists", stmt.Name)
}
// Also check tables
if _, exists := db.tables[stmt.Name]; exists {
return Result{}, fmt.Errorf("view %s already exists as a table", stmt.Name)
}
// Reconstruct the SELECT SQL from the statement - we need to store it
// Find the AS keyword position in origSQL to extract SELECT part
upper := strings.ToUpper(origSQL)
asIdx := strings.Index(upper, " AS ")
if asIdx < 0 {
asIdx = strings.Index(upper, "\nAS\n")
}
var selectSQL string
if asIdx >= 0 {
selectSQL = strings.TrimSpace(origSQL[asIdx+4:])
} else {
// Fallback: rebuild from stmt
selectSQL = "SELECT * FROM unknown"
}
db.views[stmt.Name] = selectSQL
return Result{}, nil
}

// execDerivedTableQuery materializes a derived table (subquery in FROM) and executes the outer query.
// SELECT ... FROM (SELECT ...) AS alias ...
func (db *Database) execDerivedTableQuery(stmt *QP.SelectStmt) (*Rows, error) {
	subq := stmt.From.Subquery
	alias := stmt.From.Alias
	if alias == "" {
		alias = "__subq__"
	}

	// Execute the subquery to materialize its rows
	// For UNION/EXCEPT/INTERSECT subqueries, use the SetOp path to avoid VM cursor conflicts
	var subRows *Rows
	var err error
	if subq.SetOp != "" && subq.SetOpRight != nil {
		leftStmt := *subq
		leftStmt.SetOp = ""
		leftStmt.SetOpAll = false
		leftStmt.SetOpRight = nil
		leftRows, lerr := db.execSelectStmt(&leftStmt)
		if lerr != nil {
			return nil, lerr
		}
		rightRows, rerr := db.execSelectStmt(subq.SetOpRight)
		if rerr != nil {
			return nil, rerr
		}
		combined := db.applySetOp(leftRows.Data, rightRows.Data, subq.SetOp, subq.SetOpAll)
		subRows = &Rows{Columns: leftRows.Columns, Data: combined}
	} else {
		subRows, err = db.execSelectStmt(subq)
		if err != nil {
			return nil, err
		}
	}
	if subRows == nil {
		subRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}

	// Register temp table
	tempName := alias
	colTypes := make(map[string]string)
	for _, col := range subRows.Columns {
		colTypes[col] = "TEXT"
	}
	db.tables[tempName] = colTypes
	db.columnOrder[tempName] = subRows.Columns
	rowMaps := make([]map[string]interface{}, len(subRows.Data))
	for i, row := range subRows.Data {
		rm := make(map[string]interface{})
		for j, col := range subRows.Columns {
			if j < len(row) {
				rm[col] = row[j]
			}
		}
		rowMaps[i] = rm
	}
	db.data[tempName] = rowMaps

	// Rewrite stmt.From to use temp table name instead of subquery
	origFrom := stmt.From
	stmt.From = &QP.TableRef{Name: tempName, Alias: alias}

	// Execute outer query
	rows, err := db.execVMQuery("", stmt)

	// Restore and clean up
	stmt.From = origFrom
	delete(db.tables, tempName)
	delete(db.columnOrder, tempName)
	delete(db.data, tempName)

	if err != nil {
		return nil, err
	}
	if rows == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	// Apply ORDER BY and LIMIT if present
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		rows, err = db.sortResults(rows, stmt.OrderBy)
		if err != nil {
			return nil, err
		}
	}
	if stmt.Limit != nil {
		rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// queryView executes a query against a view by executing the underlying SELECT
func (db *Database) queryView(viewSQL string, outerStmt *QP.SelectStmt, origSQL string) (*Rows, error) {
// Execute the underlying view SELECT
rows, err := db.Query(viewSQL)
if err != nil {
return nil, err
}
if rows == nil {
return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
}

// Handle UNION/EXCEPT/INTERSECT with view on left side
if outerStmt.SetOp != "" && outerStmt.SetOpRight != nil {
	rightRows, err := db.execSelectStmt(outerStmt.SetOpRight)
	if err != nil {
		return nil, err
	}
	if len(rows.Columns) != len(rightRows.Columns) {
		return nil, fmt.Errorf("SELECTs to the left and right of %s do not have the same number of result columns", outerStmt.SetOp)
	}
	combined := db.applySetOp(rows.Data, rightRows.Data, outerStmt.SetOp, outerStmt.SetOpAll)
	result := &Rows{Columns: rows.Columns, Data: combined}
	if len(outerStmt.OrderBy) > 0 {
		result, err = db.sortResults(result, outerStmt.OrderBy)
		if err != nil {
			return nil, err
		}
	}
	if outerStmt.Limit != nil {
		result, err = db.applyLimit(result, outerStmt.Limit, outerStmt.Offset)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Apply outer SELECT column filtering if needed
if len(outerStmt.Columns) > 0 {
if cr, ok := outerStmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" {
// SELECT * - return all
} else {
// Filter to requested columns
reqCols := make([]string, 0)
colIdx := make(map[string]int)
for i, col := range rows.Columns {
colIdx[col] = i
}
for _, c := range outerStmt.Columns {
if cr, ok := c.(*QP.ColumnRef); ok {
reqCols = append(reqCols, cr.Name)
} else if ae, ok := c.(*QP.AliasExpr); ok {
if cr, ok := ae.Expr.(*QP.ColumnRef); ok {
reqCols = append(reqCols, cr.Name)
}
}
}
if len(reqCols) > 0 {
newData := make([][]interface{}, len(rows.Data))
for i, row := range rows.Data {
newRow := make([]interface{}, len(reqCols))
for j, col := range reqCols {
if idx, ok := colIdx[col]; ok {
newRow[j] = row[idx]
}
}
newData[i] = newRow
}
rows = &Rows{Columns: reqCols, Data: newData}
}
}
}

// Apply outer WHERE
if outerStmt.Where != nil {
colIdx := make(map[string]int)
for i, col := range rows.Columns {
colIdx[col] = i
}
filtered := make([][]interface{}, 0)
for _, row := range rows.Data {
rowMap := make(map[string]interface{})
for col, idx := range colIdx {
rowMap[col] = row[idx]
}
if db.evalWhereOnMap(outerStmt.Where, rowMap) {
filtered = append(filtered, row)
}
}
rows.Data = filtered
}

// Apply outer ORDER BY
if len(outerStmt.OrderBy) > 0 {
var err error
rows, err = db.sortResults(rows, outerStmt.OrderBy)
if err != nil {
return nil, err
}
}

// Apply outer LIMIT
if outerStmt.Limit != nil {
var err error
rows, err = db.applyLimit(rows, outerStmt.Limit, outerStmt.Offset)
if err != nil {
return nil, err
}
}

return rows, nil
}

// evalWhereOnMap evaluates a WHERE expression against a map row
func (db *Database) evalWhereOnMap(expr QP.Expr, row map[string]interface{}) bool {
if expr == nil {
return true
}
switch e := expr.(type) {
case *QP.BinaryExpr:
switch e.Op {
case QP.TokenAnd:
return db.evalWhereOnMap(e.Left, row) && db.evalWhereOnMap(e.Right, row)
case QP.TokenOr:
return db.evalWhereOnMap(e.Left, row) || db.evalWhereOnMap(e.Right, row)
default:
lv := db.evalExprOnMap(e.Left, row)
rv := db.evalExprOnMap(e.Right, row)
return db.engine.CompareVals(lv, rv) == 0
}
case *QP.UnaryExpr:
if e.Op == QP.TokenNot {
return !db.evalWhereOnMap(e.Expr, row)
}
}
return true
}

func (db *Database) evalExprOnMap(expr QP.Expr, row map[string]interface{}) interface{} {
if expr == nil {
return nil
}
switch e := expr.(type) {
case *QP.Literal:
return e.Value
case *QP.ColumnRef:
if v, ok := row[e.Name]; ok {
return v
}
if idx := strings.LastIndex(e.Name, "."); idx >= 0 {
return row[e.Name[idx+1:]]
}
return nil
}
return nil
}

// execCreateTableAsSelect handles CREATE TABLE ... AS SELECT
func (db *Database) execCreateTableAsSelect(stmt *QP.CreateTableStmt) (Result, error) {
// Execute the SELECT
// Build SQL for the inner SELECT
rows, err := db.execSelectStmt(stmt.AsSelect)
if err != nil {
return Result{}, err
}
if rows == nil {
rows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
}

// Create the table with columns from SELECT result
schema := make(map[string]QE.ColumnType)
colTypes := make(map[string]string)
db.columnDefaults[stmt.Name] = make(map[string]interface{})
db.columnNotNull[stmt.Name] = make(map[string]bool)
db.columnChecks[stmt.Name] = make(map[string]QP.Expr)

for _, col := range rows.Columns {
	// Try to infer column type from the source table schema
	colType := db.inferColumnTypeFromSelect(col, stmt.AsSelect)
	schema[col] = QE.ColumnType{Name: col, Type: colType}
	colTypes[col] = colType
}
db.engine.RegisterTable(stmt.Name, schema)
db.tables[stmt.Name] = colTypes
db.columnOrder[stmt.Name] = rows.Columns

bt := DS.NewBTree(db.pm, 0, true)
db.tableBTrees[stmt.Name] = bt

// Insert rows
db.data[stmt.Name] = make([]map[string]interface{}, 0, len(rows.Data))
for _, row := range rows.Data {
rowMap := make(map[string]interface{})
for i, col := range rows.Columns {
if i < len(row) {
rowMap[col] = row[i]
}
}
db.data[stmt.Name] = append(db.data[stmt.Name], rowMap)
}

return Result{}, nil
}

// inferColumnTypeFromSelect tries to determine a column's type from its source in a SELECT statement.
// Falls back to "TEXT" if the type cannot be determined.
func (db *Database) inferColumnTypeFromSelect(colName string, sel *QP.SelectStmt) string {
	if sel == nil || sel.From == nil {
		return "TEXT"
	}
	srcTable := sel.From.Name
	if srcTable == "" {
		return "TEXT"
	}
	srcTypes, hasSrc := db.tables[srcTable]
	// Scan SELECT columns to find matching expressions
	for _, expr := range sel.Columns {
		switch e := expr.(type) {
		case *QP.ColumnRef:
			// Direct column reference: SELECT a FROM t1 → column 'a' gets t1's type
			if e.Name == colName && hasSrc {
				if typ, ok := srcTypes[e.Name]; ok {
					return normalizeTypeForPragma(typ)
				}
			}
		case *QP.AliasExpr:
			alias := e.Alias
			if alias == colName {
				// Check if inner is a simple column ref
				if cr, ok := e.Expr.(*QP.ColumnRef); ok && hasSrc {
					if typ, ok := srcTypes[cr.Name]; ok {
						return normalizeTypeForPragma(typ)
					}
				}
				// Expression alias (e.g. a * 2 AS double_a): SQLite uses empty type
				return ""
			}
		}
	}
	// Direct column lookup in source table
	if hasSrc {
		if typ, ok := srcTypes[colName]; ok {
			return normalizeTypeForPragma(typ)
		}
	}
	return "TEXT"
}

// normalizeTypeForPragma maps declared types to the type SQLite uses in PRAGMA table_info for CTAS.
func normalizeTypeForPragma(typ string) string {
	switch strings.ToUpper(typ) {
	case "INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return "INT"
	case "REAL", "FLOAT", "DOUBLE":
		return "REAL"
	case "BLOB":
		return "BLOB"
	default:
		return typ
	}
}
func (db *Database) execAlterTable(stmt *QP.AlterTableStmt) (Result, error) {
	util.AssertNotNil(stmt, "AlterTableStmt")
	util.Assert(stmt.Table != "", "AlterTableStmt.Table cannot be empty")
switch stmt.Action {
case "ADD_COLUMN":
if _, exists := db.tables[stmt.Table]; !exists {
return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
}
col := stmt.Column
db.tables[stmt.Table][col.Name] = col.Type
db.columnOrder[stmt.Table] = append(db.columnOrder[stmt.Table], col.Name)
db.engine.RegisterTable(stmt.Table, db.buildSchema(stmt.Table))

// Add default value for existing rows
var defaultVal interface{}
if col.Default != nil {
defaultVal = db.evalConstExpr(col.Default)
}

// Update existing rows with the new column
for i := range db.data[stmt.Table] {
if db.data[stmt.Table][i] == nil {
db.data[stmt.Table][i] = make(map[string]interface{})
}
db.data[stmt.Table][i][col.Name] = defaultVal
}

if col.Default != nil {
if db.columnDefaults[stmt.Table] == nil {
db.columnDefaults[stmt.Table] = make(map[string]interface{})
}
db.columnDefaults[stmt.Table][col.Name] = col.Default
}
if col.NotNull {
if db.columnNotNull[stmt.Table] == nil {
db.columnNotNull[stmt.Table] = make(map[string]bool)
}
db.columnNotNull[stmt.Table][col.Name] = true
}
return Result{}, nil

case "RENAME_TO":
if _, exists := db.tables[stmt.Table]; !exists {
return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
}
// Rename table
db.tables[stmt.NewName] = db.tables[stmt.Table]
db.data[stmt.NewName] = db.data[stmt.Table]
db.columnOrder[stmt.NewName] = db.columnOrder[stmt.Table]
db.primaryKeys[stmt.NewName] = db.primaryKeys[stmt.Table]
db.columnDefaults[stmt.NewName] = db.columnDefaults[stmt.Table]
db.columnNotNull[stmt.NewName] = db.columnNotNull[stmt.Table]
db.columnChecks[stmt.NewName] = db.columnChecks[stmt.Table]
db.tableBTrees[stmt.NewName] = db.tableBTrees[stmt.Table]

delete(db.tables, stmt.Table)
delete(db.data, stmt.Table)
delete(db.columnOrder, stmt.Table)
delete(db.primaryKeys, stmt.Table)
delete(db.columnDefaults, stmt.Table)
delete(db.columnNotNull, stmt.Table)
delete(db.columnChecks, stmt.Table)
delete(db.tableBTrees, stmt.Table)
return Result{}, nil
}
return Result{}, nil
}

func (db *Database) buildSchema(tableName string) map[string]QE.ColumnType {
schema := make(map[string]QE.ColumnType)
for col, typ := range db.tables[tableName] {
schema[col] = QE.ColumnType{Name: col, Type: typ}
}
return schema
}

// evalConstExpr evaluates a constant expression (literal, etc.)
func (db *Database) evalConstExpr(expr QP.Expr) interface{} {
if expr == nil {
return nil
}
if lit, ok := expr.(*QP.Literal); ok {
return lit.Value
}
return nil
}

// execInsertSelect handles INSERT INTO table SELECT ...
func (db *Database) execInsertSelect(stmt *QP.InsertStmt) (Result, error) {
	util.AssertNotNil(stmt, "InsertStmt")
	util.Assert(stmt.Table != "", "InsertStmt.Table cannot be empty")
	util.Assert(stmt.SelectQuery != nil, "InsertStmt.SelectQuery cannot be nil for INSERT...SELECT")
	if _, exists := db.tables[stmt.Table]; !exists {
		return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
	}

	// Execute the SELECT query to get rows to insert
	rows, err := db.execSelectStmt(stmt.SelectQuery)
	if err != nil {
		return Result{}, fmt.Errorf("INSERT SELECT: %w", err)
	}
	if rows == nil || len(rows.Data) == 0 {
		return Result{}, nil
	}

	tableCols := db.columnOrder[stmt.Table]
	insertCols := stmt.Columns
	if len(insertCols) == 0 {
		insertCols = tableCols
	}

	var affected int64
	for _, rowData := range rows.Data {
		// Build INSERT for this row
		colParts := make([]string, 0, len(insertCols))
		valParts := make([]string, 0, len(insertCols))
		for i, col := range insertCols {
			colParts = append(colParts, col)
			var val interface{}
			if i < len(rowData) {
				val = rowData[i]
			}
			valParts = append(valParts, literalToString(val))
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			stmt.Table,
			strings.Join(colParts, ", "),
			strings.Join(valParts, ", "))
		res, err := db.execVMDML(insertSQL, stmt.Table)
		if err != nil {
			return Result{}, err
		}
		affected += res.RowsAffected
	}
	return Result{RowsAffected: affected}, nil
}

// resolveSelectAliases resolves column aliases defined in SELECT in WHERE, GROUP BY, HAVING, ORDER BY.
// This implements SQLite's extension allowing aliases like: SELECT a AS x ... GROUP BY x
func resolveSelectAliases(stmt *QP.SelectStmt) {
	// Build alias map: alias_name -> original expression
	aliasMap := make(map[string]QP.Expr)
	for _, col := range stmt.Columns {
		if alias, ok := col.(*QP.AliasExpr); ok {
			aliasMap[alias.Alias] = alias.Expr
		}
	}
	if len(aliasMap) == 0 {
		return
	}

	// Substitute in WHERE
	stmt.Where = substituteAliasExpr(stmt.Where, aliasMap)

	// Substitute in GROUP BY
	for i, expr := range stmt.GroupBy {
		stmt.GroupBy[i] = substituteAliasExpr(expr, aliasMap)
	}

	// Substitute in HAVING
	stmt.Having = substituteAliasExpr(stmt.Having, aliasMap)
}

// substituteAliasExpr recursively substitutes alias references in an expression.
func substituteAliasExpr(expr QP.Expr, aliasMap map[string]QP.Expr) QP.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Table == "" {
			if orig, ok := aliasMap[e.Name]; ok {
				return orig
			}
		}
		return expr
	case *QP.BinaryExpr:
		return &QP.BinaryExpr{Op: e.Op, Left: substituteAliasExpr(e.Left, aliasMap), Right: substituteAliasExpr(e.Right, aliasMap)}
	case *QP.UnaryExpr:
		return &QP.UnaryExpr{Op: e.Op, Expr: substituteAliasExpr(e.Expr, aliasMap)}
	case *QP.AliasExpr:
		return &QP.AliasExpr{Expr: substituteAliasExpr(e.Expr, aliasMap), Alias: e.Alias}
	default:
		return expr
	}
}

// splitStatements splits SQL on top-level semicolons (not inside strings/parens).
func splitStatements(sql string) []string {
var stmts []string
var cur strings.Builder
depth := 0
inSingle := false
inDouble := false
for i := 0; i < len(sql); i++ {
c := sql[i]
if inSingle {
cur.WriteByte(c)
if c == '\'' && i+1 < len(sql) && sql[i+1] == '\'' {
cur.WriteByte(sql[i+1])
i++
} else if c == '\'' {
inSingle = false
}
continue
}
if inDouble {
cur.WriteByte(c)
if c == '"' {
inDouble = false
}
continue
}
switch c {
case '\'':
inSingle = true
cur.WriteByte(c)
case '"':
inDouble = true
cur.WriteByte(c)
case '(':
depth++
cur.WriteByte(c)
case ')':
depth--
cur.WriteByte(c)
case ';':
if depth == 0 {
s := strings.TrimSpace(cur.String())
if s != "" {
stmts = append(stmts, s)
}
cur.Reset()
} else {
cur.WriteByte(c)
}
default:
cur.WriteByte(c)
}
}
if s := strings.TrimSpace(cur.String()); s != "" {
stmts = append(stmts, s)
}
return stmts
}
