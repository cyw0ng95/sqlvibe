package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/CG"
	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/PB"
	"github.com/sqlvibe/sqlvibe/internal/QE"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/TM"
	"github.com/sqlvibe/sqlvibe/internal/VM"
)

type Database struct {
	pm          *DS.PageManager
	engine      *QE.QueryEngine
	tx          *Transaction
	txMgr       *TM.TransactionManager
	activeTx    *TM.Transaction
	dbPath      string
	tables      map[string]map[string]string        // table name -> column name -> type
	primaryKeys map[string][]string                 // table name -> primary key column names
	columnOrder map[string][]string                 // table name -> ordered column names
	data        map[string][]map[string]interface{} // table name -> rows -> column name -> value
	indexes     map[string]*IndexInfo               // index name -> index info
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
		pm:          pm,
		engine:      engine,
		txMgr:       txMgr,
		activeTx:    nil,
		dbPath:      path,
		tables:      make(map[string]map[string]string),
		primaryKeys: make(map[string][]string),
		columnOrder: make(map[string][]string),
		data:        data,
		indexes:     make(map[string]*IndexInfo),
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
		if _, exists := db.tables[stmt.Name]; exists {
			if stmt.IfNotExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("table %s already exists", stmt.Name)
		}
		schema := make(map[string]QE.ColumnType)
		colTypes := make(map[string]string)
		var pkCols []string
		for _, col := range stmt.Columns {
			schema[col.Name] = QE.ColumnType{Name: col.Name, Type: col.Type}
			colTypes[col.Name] = col.Type
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
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
		return Result{}, nil
	case "InsertStmt":
		stmt := ast.(*QP.InsertStmt)
		return db.execVMDML(sql, stmt.Table)
	case "UpdateStmt":
		stmt := ast.(*QP.UpdateStmt)
		return db.execVMDML(sql, stmt.Table)
	case "DeleteStmt":
		stmt := ast.(*QP.DeleteStmt)
		return db.execVMDML(sql, stmt.Table)
	case "DropTableStmt":
		stmt := ast.(*QP.DropTableStmt)
		if _, exists := db.tables[stmt.Name]; exists {
			delete(db.tables, stmt.Name)
			delete(db.data, stmt.Name)
			delete(db.primaryKeys, stmt.Name)
		}
		return Result{}, nil
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
		return Result{}, nil
	case "RollbackStmt":
		if db.activeTx == nil {
			return Result{}, fmt.Errorf("no transaction active")
		}
		err := db.txMgr.RollbackTransaction(db.activeTx.ID)
		if err != nil {
			return Result{}, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		db.activeTx = nil
		return Result{}, nil
	}

	return Result{}, nil
}

func (db *Database) Query(sql string) (*Rows, error) {
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
		if stmt.From == nil {
			return db.evalConstantExpression(stmt)
		}

		var tableName string
		if stmt.From != nil {
			tableName = stmt.From.Name
		}
		if tableName == "" {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		// Handle sqlite_master virtual table
		if tableName == "sqlite_master" {
			return db.querySqliteMaster(stmt)
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

func (db *Database) applySetOp(left, right [][]interface{}, op string, all bool) [][]interface{} {
	switch op {
	case "UNION":
		return db.setOpUnion(left, right, all)
	case "EXCEPT":
		return db.setOpExcept(left, right, all)
	case "INTERSECT":
		return db.setOpIntersect(left, right, all)
	}
	return left
}

func (db *Database) setOpUnion(left, right [][]interface{}, all bool) [][]interface{} {
	if all {
		return append(left, right...)
	}
	seen := make(map[string]bool)
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	for _, row := range right {
		key := db.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func (db *Database) setOpExcept(left, right [][]interface{}, all bool) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[db.rowKey(row)] = true
	}
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if !rightSet[key] {
			if all {
				result = append(result, row)
			} else {
				rightSet[key] = true
				result = append(result, row)
			}
		}
	}
	return result
}

func (db *Database) setOpIntersect(left, right [][]interface{}, all bool) [][]interface{} {
	rightSet := make(map[string]int)
	for _, row := range right {
		key := db.rowKey(row)
		rightSet[key]++
	}
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if count, ok := rightSet[key]; ok && count > 0 {
			result = append(result, row)
			if !all {
				rightSet[key] = 0
			} else {
				rightSet[key]--
			}
		}
	}
	return result
}

func (db *Database) rowKey(row []interface{}) string {
	key := ""
	for i, v := range row {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", v)
	}
	return key
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

func (db *Database) handlePragma(stmt *QP.PragmaStmt) (*Rows, error) {
	switch stmt.Name {
	case "table_info":
		return db.pragmaTableInfo(stmt)
	case "index_list":
		return db.pragmaIndexList(stmt)
	case "database_list":
		return db.pragmaDatabaseList()
	default:
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}
}

func (db *Database) sortResults(rows *Rows, orderBy []QP.OrderBy) (*Rows, error) {
	if len(orderBy) == 0 || rows == nil || len(rows.Data) == 0 {
		return rows, nil
	}

	cols := rows.Columns
	data := rows.Data

	// Pre-evaluate ORDER BY expressions for each row
	// This is needed for non-ColumnRef expressions (e.g., val * -1, ABS(val))
	orderByValues := make([][]interface{}, len(orderBy))
	for obIdx, ob := range orderBy {
		orderByValues[obIdx] = make([]interface{}, len(data))
		for rowIdx, row := range data {
			// Convert row slice to map for EvalExpr
			rowMap := make(map[string]interface{})
			for colIdx, colName := range cols {
				rowMap[colName] = row[colIdx]
			}
			orderByValues[obIdx][rowIdx] = db.engine.EvalExpr(rowMap, ob.Expr)
		}
	}

	sorted := make([][]interface{}, len(data))
	copy(sorted, data)

	for i := range sorted {
		for j := i + 1; j < len(sorted); j++ {
			for obIdx, ob := range orderBy {
				var keyValI, keyValJ interface{}
				if colRef, ok := ob.Expr.(*QP.ColumnRef); ok {
					for ci, cn := range cols {
						if cn == colRef.Name {
							keyValI = sorted[i][ci]
							keyValJ = sorted[j][ci]
							break
						}
					}
				} else {
					// Use pre-evaluated expression values
					keyValI = orderByValues[obIdx][i]
					keyValJ = orderByValues[obIdx][j]
				}
				cmp := compareValues(keyValI, keyValJ)
				if ob.Desc {
					cmp = -cmp
				}
				if cmp > 0 {
					sorted[i], sorted[j] = sorted[j], sorted[i]
					// Also swap the pre-evaluated values to maintain consistency
					for obIdx2 := range orderBy {
						orderByValues[obIdx2][i], orderByValues[obIdx2][j] = orderByValues[obIdx2][j], orderByValues[obIdx2][i]
					}
					break
				} else if cmp < 0 {
					break
				}
				// if cmp == 0, continue to next ORDER BY column
			}
		}
	}

	return &Rows{Columns: cols, Data: sorted}, nil
}

func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func (db *Database) handleExplain(stmt *QP.ExplainStmt, sql string) (*Rows, error) {
	sqlType := stmt.Query.NodeType()
	if sqlType == "SelectStmt" {
		sel := stmt.Query.(*QP.SelectStmt)

		// Get table column map
		var tableColMap map[string]int
		if sel.From != nil {
			tableName := sel.From.Name
			if db.data[tableName] != nil {
				cols := db.columnOrder[tableName]
				tableColMap = make(map[string]int)
				for i, col := range cols {
					tableColMap[col] = i
				}
			}
		}

		// Strip "EXPLAIN" prefix from SQL and compile
		innerSQL := strings.TrimPrefix(sql, "EXPLAIN ")
		innerSQL = strings.TrimPrefix(innerSQL, "EXPLAIN")
		innerSQL = strings.TrimSpace(innerSQL)

		program, err := CG.CompileWithSchema(innerSQL, nil)
		if err != nil {
			return nil, err
		}
		return db.explainProgram(program)
	}
	return &Rows{Columns: []string{"opcode"}, Data: [][]interface{}{}}, nil
}

func (db *Database) explainProgram(program *VM.Program) (*Rows, error) {
	if program == nil || len(program.Instructions) == 0 {
		return &Rows{Columns: []string{"result"}, Data: [][]interface{}{{"no bytecode generated"}}}, nil
	}

	cols := []string{"addr", "opcode", "p1", "p2", "p3", "p4", "comment"}
	rows := make([][]interface{}, 0)

	for i, inst := range program.Instructions {
		row := []interface{}{
			i,
			VM.OpCodeInfo[inst.Op],
			inst.P1,
			inst.P2,
			inst.P3,
			fmt.Sprintf("%v", inst.P4),
			"",
		}
		rows = append(rows, row)
	}

	return &Rows{Columns: cols, Data: rows}, nil
}

func (db *Database) pragmaTableInfo(stmt *QP.PragmaStmt) (*Rows, error) {
	var tableName string
	if stmt.Value != nil {
		if lit, ok := stmt.Value.(*QP.Literal); ok {
			if s, ok := lit.Value.(string); ok {
				tableName = s
			}
		}
	}

	if tableName == "" {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	schema, exists := db.tables[tableName]
	if !exists {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	columns := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	data := make([][]interface{}, 0)
	colOrder := db.columnOrder[tableName]
	pkCols := db.primaryKeys[tableName]

	for i, colName := range colOrder {
		colType := schema[colName]
		isPK := int64(0)
		for _, pk := range pkCols {
			if pk == colName {
				isPK = 1
				break
			}
		}
		data = append(data, []interface{}{int64(i), colName, colType, int64(0), nil, isPK})
	}

	return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) pragmaIndexList(stmt *QP.PragmaStmt) (*Rows, error) {
	var tableName string
	if stmt.Value != nil {
		if lit, ok := stmt.Value.(*QP.Literal); ok {
			if s, ok := lit.Value.(string); ok {
				tableName = s
			}
		}
	}

	if tableName == "" {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	columns := []string{"seq", "name", "unique", "origin", "partial"}
	data := make([][]interface{}, 0)

	seq := 0
	for _, idx := range db.indexes {
		if idx.Table == tableName {
			unique := int64(0)
			if idx.Unique {
				unique = 1
			}
			data = append(data, []interface{}{int64(seq), idx.Name, unique, "c", int64(0)})
			seq++
		}
	}

	return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) pragmaDatabaseList() (*Rows, error) {
	columns := []string{"seq", "name", "file"}
	data := [][]interface{}{
		{int64(0), "main", ""},
	}
	return &Rows{Columns: columns, Data: data}, nil
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
	return db.extractValueTyped(expr, "")
}

func (db *Database) extractValueTyped(expr QP.Expr, colType string) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		val := e.Value
		if strVal, ok := val.(string); ok {
			converted := db.convertStringToType(strVal, colType)
			return converted
		}
		return val
	case *QP.ColumnRef:
		return e.Name
	case *QP.UnaryExpr:
		val := db.extractValueTyped(e.Expr, colType)
		if e.Op == QP.TokenMinus {
			return db.negateValue(val)
		}
		return val
	default:
		return nil
	}
}

func (db *Database) negateValue(val interface{}) interface{} {
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

func (db *Database) convertStringToType(val string, colType string) interface{} {
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

func (db *Database) MustExec(sql string, params ...interface{}) Result {
	res, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	return res
}

func (db *Database) tryUseIndex(tableName string, where QP.Expr) []map[string]interface{} {
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

	for _, idx := range db.indexes {
		if idx.Table == tableName && len(idx.Columns) > 0 && idx.Columns[0] == colName {
			return db.scanByIndexValue(tableName, colName, colValue, idx.Unique)
		}
	}

	return nil
}

func (db *Database) scanByIndexValue(tableName, colName string, value interface{}, unique bool) []map[string]interface{} {
	tableData := db.data[tableName]
	if tableData == nil {
		return nil
	}

	result := make([]map[string]interface{}, 0)
	for _, row := range tableData {
		if rowVal, ok := row[colName]; ok {
			if db.valuesEqual(rowVal, value) {
				result = append(result, row)
				if unique {
					return result
				}
			}
		}
	}
	return result
}

func (db *Database) valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		case string:
			var bvInt int64
			fmt.Sscanf(bv, "%d", &bvInt)
			return av == bvInt
		}
	case float64:
		switch bv := b.(type) {
		case int64:
			return av == float64(bv)
		case float64:
			return av == bv
		case string:
			var bvFloat float64
			fmt.Sscanf(bv, "%f", &bvFloat)
			return av == bvFloat
		}
	case string:
		switch bv := b.(type) {
		case int64:
			var avInt int64
			fmt.Sscanf(av, "%d", &avInt)
			return avInt == bv
		case float64:
			var avFloat float64
			fmt.Sscanf(av, "%f", &avFloat)
			return avFloat == bv
		case string:
			return av == bv
		}
	}
	return false
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}

func (db *Database) compareVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Normalize numeric types for consistent comparison
	aFloat, aIsNum := toFloat64(a)
	bFloat, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		if aFloat < bFloat {
			return -1
		}
		if aFloat > bFloat {
			return 1
		}
		return 0
	}

	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			}
			if float64(av) > bv {
				return 1
			}
			return 0
		}
		return 0
	case float64:
		switch bv := b.(type) {
		case int64:
			if av < float64(bv) {
				return -1
			}
			if av > float64(bv) {
				return 1
			}
			return 0
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

func (db *Database) matchLike(value, pattern string) bool {
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

func (db *Database) applyOrderBy(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	if len(orderBy) == 0 || len(data) == 0 {
		return data
	}

	sorted := make([][]interface{}, len(data))
	copy(sorted, data)

	// Pre-evaluate ORDER BY expressions for each row
	// This is needed for non-ColumnRef expressions (e.g., val * -1)
	orderByValues := make([][]interface{}, len(orderBy))
	for obIdx, ob := range orderBy {
		orderByValues[obIdx] = make([]interface{}, len(data))
		for rowIdx, row := range data {
			// Convert row slice to map for EvalExpr
			rowMap := make(map[string]interface{})
			for colIdx, colName := range cols {
				rowMap[colName] = row[colIdx]
			}
			orderByValues[obIdx][rowIdx] = db.engine.EvalExpr(rowMap, ob.Expr)
		}
	}

	for i := range sorted {
		for j := i + 1; j < len(sorted); j++ {
			for obIdx, ob := range orderBy {
				var keyValI, keyValJ interface{}
				if colRef, ok := ob.Expr.(*QP.ColumnRef); ok {
					for ci, cn := range cols {
						if cn == colRef.Name {
							keyValI = sorted[i][ci]
							keyValJ = sorted[j][ci]
							break
						}
					}
				} else {
					// Use pre-evaluated expression values
					keyValI = orderByValues[obIdx][i]
					keyValJ = orderByValues[obIdx][j]
				}
				cmp := db.compareVals(keyValI, keyValJ)
				if ob.Desc {
					cmp = -cmp
				}
				if cmp > 0 {
					sorted[i], sorted[j] = sorted[j], sorted[i]
					// Also swap the pre-evaluated values to maintain consistency
					for obIdx2 := range orderBy {
						orderByValues[obIdx2][i], orderByValues[obIdx2][j] = orderByValues[obIdx2][j], orderByValues[obIdx2][i]
					}
					break
				} else if cmp < 0 {
					break
				}
				// if cmp == 0, continue to next ORDER BY column
			}
		}
	}
	return sorted
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

	if offset >= len(rows.Data) {
		return &Rows{Columns: rows.Columns, Data: [][]interface{}{}}, nil
	}

	if limit <= 0 {
		return &Rows{Columns: rows.Columns, Data: [][]interface{}{}}, nil
	}

	end := offset + limit
	if end > len(rows.Data) {
		end = len(rows.Data)
	}

	return &Rows{Columns: rows.Columns, Data: rows.Data[offset:end]}, nil
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

	filtered := make([]map[string]interface{}, 0)
	for _, row := range allResults {
		// Simple WHERE filtering for sqlite_master (limited support)
		include := true
		if stmt.Where != nil {
			// Only support simple equality checks for now
			if binExpr, ok := stmt.Where.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenEq {
				if colRef, ok := binExpr.Left.(*QP.ColumnRef); ok {
					if lit, ok := binExpr.Right.(*QP.Literal); ok {
						// Check if column value matches literal
						if row[colRef.Name] != lit.Value {
							include = false
						}
					}
				}
			}
		}
		if include {
			filtered = append(filtered, row)
		}
	}

	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			cols = append(cols, cr.Name)
		}
	}

	resultData := make([][]interface{}, 0)
	for _, row := range filtered {
		resultRow := make([]interface{}, 0)
		for _, colName := range cols {
			resultRow = append(resultRow, row[colName])
		}
		resultData = append(resultData, resultRow)
	}

	return &Rows{Columns: cols, Data: resultData}, nil
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

// execSetOp executes SET operations (UNION, EXCEPT, INTERSECT) by running left and right separately
func (db *Database) execSetOp(stmt *QP.SelectStmt, originalSQL string) (*Rows, error) {
	// For now, use the existing direct execution path
	// This works but bypasses VM compilation for SetOps
	// TODO: Complete full VM bytecode compilation and merging

	// Create temporary left SELECT (without SetOp)
	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	// Execute left side through VM if possible, otherwise direct
	var leftRows *Rows
	var err error
	if leftStmt.From != nil {
		leftRows, err = db.execVMQuery("", &leftStmt)
	} else {
		// Handle SELECT without FROM
		leftRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}
	if err != nil {
		return nil, fmt.Errorf("SetOp left side error: %w", err)
	}

	// Execute right side
	var rightRows *Rows
	if stmt.SetOpRight != nil {
		if stmt.SetOpRight.From != nil {
			rightRows, err = db.execVMQuery("", stmt.SetOpRight)
		} else {
			rightRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
		}
		if err != nil {
			return nil, fmt.Errorf("SetOp right side error: %w", err)
		}
	}

	// Apply set operation using existing functions
	result := db.applySetOp(leftRows.Data, rightRows.Data, stmt.SetOp, stmt.SetOpAll)

	return &Rows{
		Columns: leftRows.Columns,
		Data:    result,
	}, nil
}

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

	ctx := &dbVmContext{db: db}
	vm := VM.NewVMWithContext(program, ctx)

	if tableName != "" && db.data[tableName] != nil {
		vm.Cursors().OpenTable(tableName, db.data[tableName], db.columnOrder[tableName])
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
	if db.data[tableName] == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	compiler := CG.NewCompiler()
	compiler.SetTableSchema(make(map[string]int), tableCols)
	for i, colName := range tableCols {
		compiler.SetTableSchema(map[string]int{colName: i}, tableCols)
	}

	program := compiler.CompileSelect(stmt)
	vm := VM.NewVMWithContext(program, &dbVmContext{db: db})

	// Open table cursor
	vm.Cursors().OpenTableAtID(0, tableName, db.data[tableName], tableCols)

	err := vm.Run(nil)
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Get column names from SELECT
	cols := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			cols[i] = colRef.Name
		} else {
			cols[i] = fmt.Sprintf("col%d", i)
		}
	}

	return &Rows{Columns: cols, Data: results}, nil
}

func (db *Database) execVMQuery(sql string, stmt *QP.SelectStmt) (*Rows, error) {
	tableName := stmt.From.Name
	if db.data[tableName] == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
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
		cg.SetMultiTableSchema(multiTableSchemas)
	} else {
		// Single table query - set TableColIndices normally
		cg.SetTableSchema(make(map[string]int), tableCols)
		for i, col := range tableCols {
			cg.GetVMCompiler().TableColIndices[col] = i
		}
	}

	program := cg.CompileSelect(stmt)

	ctx := &dbVmContext{db: db}
	vm := VM.NewVMWithContext(program, ctx)

	err := vm.Run(nil)
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Get column names from the SELECT statement
	cols := make([]string, 0)
	for i, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			// Handle SELECT * - expand to all table columns
			if colRef.Name == "*" {
				cols = tableCols
				break
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

func (db *Database) execVMDML(sql string, tableName string) (Result, error) {
	// Ensure table exists
	if db.data[tableName] == nil {
		db.data[tableName] = make([]map[string]interface{}, 0)
	}

	// Get table column order
	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	// Compile the DML statement
	program, err := CG.CompileWithSchema(sql, tableCols)
	if err != nil {
		return Result{}, err
	}

	// Create VM context
	ctx := &dbVmContext{db: db}
	vm := VM.NewVMWithContext(program, ctx)

	// Open table cursor
	if db.data[tableName] != nil {
		vm.Cursors().OpenTableAtID(0, tableName, db.data[tableName], tableCols)
	}

	// Execute the VM program
	err = vm.Run(nil)
	if err != nil {
		return Result{}, err
	}

	// Get rows affected from VM
	return Result{RowsAffected: vm.RowsAffected()}, nil
}
