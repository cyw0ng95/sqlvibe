package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/PB"
	"github.com/sqlvibe/sqlvibe/internal/QE"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/VM"
)

type Database struct {
	pm          *DS.PageManager
	engine      *QE.QueryEngine
	tx          *Transaction
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
	pos     int
}

func (r *Rows) Next() bool {
	if r.Data == nil {
		return false
	}
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

	return &Database{
		pm:          pm,
		engine:      engine,
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
		if db.data[stmt.Table] == nil {
			db.data[stmt.Table] = make([]map[string]interface{}, 0)
		}
		tableSchema := db.tables[stmt.Table]
		pkCols := db.primaryKeys[stmt.Table]

		colNames := stmt.Columns
		if len(colNames) == 0 && tableSchema != nil {
			colNames = db.getOrderedColumns(stmt.Table)
		}

		rowID := int64(len(db.data[stmt.Table])) + 1
		for _, rowExprs := range stmt.Values {
			row := make(map[string]interface{})
			for i, expr := range rowExprs {
				if i < len(colNames) {
					colName := colNames[i]
					colType := ""
					if tableSchema != nil {
						colType = tableSchema[colName]
					}
					val := db.extractValueTyped(expr, colType)
					row[colName] = val
				}
			}
			if len(pkCols) > 0 {
				for _, pkCol := range pkCols {
					pkVal := row[pkCol]
					for _, existingRow := range db.data[stmt.Table] {
						if existingRow[pkCol] == pkVal {
							return Result{}, fmt.Errorf("UNIQUE constraint failed: %s.%s", stmt.Table, pkCol)
						}
					}
				}
			}
			db.data[stmt.Table] = append(db.data[stmt.Table], row)

			serialized := db.serializeRow(row)
			db.engine.Insert(stmt.Table, uint64(rowID), serialized)
			rowID++
		}
		return Result{RowsAffected: int64(len(stmt.Values))}, nil
	case "UpdateStmt":
		stmt := ast.(*QP.UpdateStmt)
		if tableData, ok := db.data[stmt.Table]; ok {
			rowsAffected := int64(0)
			for i, row := range tableData {
				if db.evalWhere(row, stmt.Where) {
					for _, setClause := range stmt.Set {
						if colRef, ok := setClause.Column.(*QP.ColumnRef); ok {
							colType := ""
							if tableSchema, ok := db.tables[stmt.Table]; ok {
								colType = tableSchema[colRef.Name]
							}
							row[colRef.Name] = db.extractValueTyped(setClause.Value, colType)
						}
					}
					db.data[stmt.Table][i] = row
					rowsAffected++
				}
			}
			return Result{RowsAffected: rowsAffected}, nil
		}
		return Result{RowsAffected: 0}, nil
	case "DeleteStmt":
		stmt := ast.(*QP.DeleteStmt)
		if tableData, ok := db.data[stmt.Table]; ok {
			newData := make([]map[string]interface{}, 0)
			rowsAffected := int64(0)
			for _, row := range tableData {
				shouldDelete := db.evalWhere(row, stmt.Where)
				if shouldDelete {
					rowsAffected++
				} else {
					newData = append(newData, row)
				}
			}
			db.data[stmt.Table] = newData
			return Result{RowsAffected: rowsAffected}, nil
		}
		return Result{RowsAffected: 0}, nil
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

		// VM enabled for simple queries (no JOIN, no ORDER BY, no GROUP BY, no expressions, no LIMIT, no SetOp, no SELECT *)
		// VM supports: SELECT col1, col2 FROM table WHERE col = value
		isSimple := true
		if stmt.From != nil && stmt.From.Join != nil {
			isSimple = false
		}
		if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
			isSimple = false
		}
		if stmt.GroupBy != nil && len(stmt.GroupBy) > 0 {
			isSimple = false
		}
		if stmt.Limit != nil {
			isSimple = false
		}
		if stmt.SetOp != "" {
			isSimple = false
		}
		// Check for SELECT * - VM doesn't expand * to actual columns
		for _, col := range stmt.Columns {
			if colRef, ok := col.(*QP.ColumnRef); ok {
				if colRef.Name == "*" {
					isSimple = false
					break
				}
			}
			if _, ok := col.(*QP.ColumnRef); !ok {
				isSimple = false
				break
			}
		}
		if isSimple {
			rows, err := db.execVMQuery(sql, stmt)
			if err == nil && rows != nil && len(rows.Data) > 0 {
				return rows, nil
			}
		}

		if stmt.From != nil && stmt.From.Join != nil {
			return db.handleJoin(stmt)
		}

		tableData, ok := db.data[tableName]
		if !ok || tableData == nil {
			if len(stmt.Columns) > 0 {
				hasAggregate := false
				for _, col := range stmt.Columns {
					if fc, ok := col.(*QP.FuncCall); ok {
						if fc.Name == "COUNT" {
							hasAggregate = true
							break
						}
					}
				}
				if hasAggregate {
					return &Rows{Columns: []string{"COUNT(*)"}, Data: [][]interface{}{{int64(0)}}}, nil
				}
			}
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		hasAggregate := false
		var aggregateFunc *QP.FuncCall
		for _, col := range stmt.Columns {
			if fc, ok := col.(*QP.FuncCall); ok {
				if fc.Name == "SUM" || fc.Name == "AVG" || fc.Name == "MIN" || fc.Name == "MAX" || fc.Name == "COUNT" {
					hasAggregate = true
					aggregateFunc = fc
					break
				}
			}
		}

		if hasAggregate && stmt.Where == nil && len(stmt.Columns) == 1 {
			result := db.computeAggregate(tableData, aggregateFunc)
			return &Rows{Columns: []string{aggregateFunc.Name}, Data: [][]interface{}{{result}}}, nil
		}

		// Handle GROUP BY
		if len(stmt.GroupBy) > 0 && hasAggregate {
			result, err := db.computeGroupBy(tableData, stmt)
			if err != nil {
				return nil, err
			}
			return result, nil
		}

		var cols []string
		if len(stmt.Columns) == 1 {
			if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok {
				if cr.Name == "*" {
					cols = db.getOrderedColumns(tableName)
				} else {
					cols = []string{cr.Name}
				}
			} else {
				switch col := stmt.Columns[0].(type) {
				case *QP.AliasExpr:
					if col.Alias != "" {
						cols = []string{col.Alias}
					} else {
						cols = []string{"expr"}
					}
				case *QP.FuncCall:
					cols = []string{col.Name}
				case *QP.SubqueryExpr:
					cols = []string{"subquery"}
				default:
					cols = []string{"expr"}
				}
			}
		} else {
			exprIndex := 0
			for _, col := range stmt.Columns {
				switch c := col.(type) {
				case *QP.ColumnRef:
					cols = append(cols, c.Name)
				case *QP.AliasExpr:
					if c.Alias != "" {
						cols = append(cols, c.Alias)
					} else {
						cols = append(cols, fmt.Sprintf("expr%d", exprIndex))
						exprIndex++
					}
				case *QP.FuncCall:
					cols = append(cols, c.Name)
				case *QP.SubqueryExpr:
					cols = append(cols, "subquery")
				case *QP.BinaryExpr:
					cols = append(cols, fmt.Sprintf("expr%d", exprIndex))
					exprIndex++
				case *QP.UnaryExpr:
					cols = append(cols, fmt.Sprintf("expr%d", exprIndex))
					exprIndex++
				default:
					cols = append(cols, fmt.Sprintf("expr%d", exprIndex))
					exprIndex++
				}
			}
		}

		if len(cols) == 0 {
			cols = db.getOrderedColumns(tableName)
		}

		expressions := make([]QP.Expr, len(stmt.Columns))
		for i, col := range stmt.Columns {
			if _, ok := col.(*QP.ColumnRef); ok {
				expressions[i] = nil
			} else {
				expressions[i] = col
			}
		}

		// Set outer alias for correlated subquery evaluation
		if stmt.From != nil && stmt.From.Alias != "" {
			db.engine.SetOuterAlias(stmt.From.Alias)
		} else {
			db.engine.SetOuterAlias("")
		}

		scan := QE.NewTableScan(db.engine, tableName)
		scan.SetData(tableData)

		predicate := db.engine.BuildPredicate(stmt.Where)
		filter := QE.NewFilter(scan, predicate)

		project := db.engine.NewProjectWithExpr(filter, cols, expressions)

		var limit, offset int
		if stmt.Limit != nil {
			if lit, ok := stmt.Limit.(*QP.Literal); ok {
				if num, ok := lit.Value.(int64); ok {
					limit = int(num)
				}
			}
		}
		if stmt.Offset != nil {
			if lit, ok := stmt.Offset.(*QP.Literal); ok {
				if num, ok := lit.Value.(int64); ok {
					offset = int(num)
				}
			}
		}
		limited := QE.NewLimit(project, limit, offset)

		err = limited.Init()
		if err != nil {
			return nil, err
		}

		resultData := make([][]interface{}, 0)
		rowCount := 0
		for {
			row, err := limited.Next()
			if err != nil {
				return nil, err
			}
			if row == nil {
				break
			}
			rowCount++
			resultRow := make([]interface{}, len(cols))
			for i, colName := range cols {
				resultRow[i] = row[colName]
			}
			resultData = append(resultData, resultRow)
		}
		limited.Close()

		if len(stmt.OrderBy) > 0 {
			resultData = db.applyOrderBy(resultData, stmt.OrderBy, cols)
		}

		if stmt.SetOp != "" {
			rightRows, err := db.executeSelect(stmt.SetOpRight)
			if err != nil {
				return nil, err
			}
			resultData = db.applySetOp(resultData, rightRows.Data, stmt.SetOp, stmt.SetOpAll)
		}

		return &Rows{Columns: cols, Data: resultData, pos: -1}, nil
	}

	return nil, nil
}

func (db *Database) executeSelect(stmt *QP.SelectStmt) (*Rows, error) {
	if stmt == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

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

	if tableName == "sqlite_master" {
		return db.querySqliteMaster(stmt)
	}

	if stmt.From != nil && stmt.From.Join != nil {
		return db.handleJoin(stmt)
	}

	tableData, ok := db.data[tableName]
	if !ok || tableData == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	hasAggregate := false
	var aggregateFunc *QP.FuncCall
	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			if fc.Name == "SUM" || fc.Name == "AVG" || fc.Name == "MIN" || fc.Name == "MAX" || fc.Name == "COUNT" {
				hasAggregate = true
				aggregateFunc = fc
				break
			}
		}
	}

	if hasAggregate && stmt.Where == nil && len(stmt.Columns) == 1 {
		result := db.computeAggregate(tableData, aggregateFunc)
		return &Rows{Columns: []string{aggregateFunc.Name}, Data: [][]interface{}{{result}}}, nil
	}

	filteredData := tableData
	if stmt.Where != nil {
		if indexData := db.tryUseIndex(tableName, stmt.Where); indexData != nil {
			filteredData = indexData
		} else {
			filteredData = make([]map[string]interface{}, 0)
			for _, row := range tableData {
				if db.evalWhere(row, stmt.Where) {
					filteredData = append(filteredData, row)
				}
			}
		}
	}

	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		switch c := col.(type) {
		case *QP.ColumnRef:
			cols = append(cols, c.Name)
		case *QP.AliasExpr:
			cols = append(cols, c.Alias)
		case *QP.FuncCall:
			cols = append(cols, c.Name)
		case *QP.Literal:
			cols = append(cols, "expr")
		default:
			cols = append(cols, "expr")
		}
	}

	resultData := make([][]interface{}, 0)
	for _, row := range filteredData {
		resultRow := make([]interface{}, len(cols))
		for i, col := range stmt.Columns {
			resultRow[i] = db.engine.EvalExpr(row, col)
		}
		resultData = append(resultData, resultRow)
	}

	if len(stmt.OrderBy) > 0 {
		resultData = db.applyOrderBy(resultData, stmt.OrderBy, cols)
	}

	if stmt.SetOp != "" {
		rightRows, err := db.executeSelect(stmt.SetOpRight)
		if err != nil {
			return nil, err
		}
		resultData = db.applySetOp(resultData, rightRows.Data, stmt.SetOp, stmt.SetOpAll)
	}

	return &Rows{Columns: cols, Data: resultData}, nil
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

func (db *Database) evalWhere(row map[string]interface{}, where QP.Expr) bool {
	if where == nil {
		return true
	}
	switch e := where.(type) {
	case *QP.BinaryExpr:
		switch e.Op {
		case QP.TokenAnd:
			return db.evalWhere(row, e.Left) && db.evalWhere(row, e.Right)
		case QP.TokenOr:
			return db.evalWhere(row, e.Left) || db.evalWhere(row, e.Right)
		case QP.TokenEq:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return db.valuesEqual(leftVal, rightVal)
		case QP.TokenNe:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return !db.valuesEqual(leftVal, rightVal)
		case QP.TokenLt:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return db.compareVals(leftVal, rightVal) < 0
		case QP.TokenLe:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return db.compareVals(leftVal, rightVal) <= 0
		case QP.TokenGt:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return db.compareVals(leftVal, rightVal) > 0
		case QP.TokenGe:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			return db.compareVals(leftVal, rightVal) >= 0
		case QP.TokenIs:
			leftVal := db.evalExpr(row, e.Left)
			return leftVal == nil
		case QP.TokenIsNot:
			leftVal := db.evalExpr(row, e.Left)
			return leftVal != nil
		case QP.TokenIn:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			if rightList, ok := rightVal.([]interface{}); ok {
				for _, v := range rightList {
					if db.valuesEqual(leftVal, v) {
						return true
					}
				}
				return false
			}
			return false
		case QP.TokenLike:
			leftVal := db.evalExpr(row, e.Left)
			rightVal := db.evalExpr(row, e.Right)
			leftStr, leftOk := leftVal.(string)
			patternStr, patOk := rightVal.(string)
			if !leftOk || !patOk {
				return false
			}
			return db.matchLike(leftStr, patternStr)
		case QP.TokenBetween:
			leftVal := db.evalExpr(row, e.Left)
			if andExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				minVal := db.evalExpr(row, andExpr.Left)
				maxVal := db.evalExpr(row, andExpr.Right)
				return db.compareVals(leftVal, minVal) >= 0 && db.compareVals(leftVal, maxVal) <= 0
			}
			return false
		case QP.TokenExists:
			subq, ok := e.Left.(*QP.SubqueryExpr)
			if !ok {
				return false
			}
			result := db.evalSubquery(row, subq.Select)
			return result != nil && len(result) > 0
		case QP.TokenInSubquery:
			leftVal := db.evalExpr(row, e.Left)
			subq, ok := e.Right.(*QP.SubqueryExpr)
			if !ok {
				return false
			}
			result := db.evalSubquery(row, subq.Select)
			if result == nil || len(result) == 0 {
				return false
			}
			for _, r := range result {
				for _, v := range r {
					if db.valuesEqual(leftVal, v) {
						return true
					}
				}
			}
			return false
		case QP.TokenAll:
			rightExpr, ok := e.Right.(*QP.BinaryExpr)
			if !ok {
				return false
			}
			subq, ok := rightExpr.Right.(*QP.SubqueryExpr)
			if !ok {
				return false
			}
			result := db.evalSubquery(row, subq.Select)
			if result == nil || len(result) == 0 {
				return false
			}
			for _, r := range result {
				for _, v := range r {
					cmpExpr := &QP.BinaryExpr{
						Op:    rightExpr.Op,
						Left:  e.Left,
						Right: &QP.Literal{Value: v},
					}
					if !db.evalWhere(row, cmpExpr) {
						return false
					}
				}
			}
			return true
		case QP.TokenAny:
			rightExpr, ok := e.Right.(*QP.BinaryExpr)
			if !ok {
				return false
			}
			subq, ok := rightExpr.Right.(*QP.SubqueryExpr)
			if !ok {
				return false
			}
			result := db.evalSubquery(row, subq.Select)
			if result == nil || len(result) == 0 {
				return false
			}
			for _, r := range result {
				for _, v := range r {
					cmpExpr := &QP.BinaryExpr{
						Op:    rightExpr.Op,
						Left:  e.Left,
						Right: &QP.Literal{Value: v},
					}
					if db.evalWhere(row, cmpExpr) {
						return true
					}
				}
			}
			return false
		}
	case *QP.UnaryExpr:
		if e.Op == QP.TokenNot {
			return !db.evalWhere(row, e.Expr)
		}
	case *QP.SubqueryExpr:
		result := db.evalSubquery(row, e.Select)
		return result != nil && len(result) > 0
	}
	return true
}

func (db *Database) evalExpr(row map[string]interface{}, expr QP.Expr) interface{} {
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
	case *QP.UnaryExpr:
		if e.Op == QP.TokenMinus {
			val := db.evalExpr(row, e.Expr)
			if n, ok := val.(int64); ok {
				return -n
			}
			if n, ok := val.(float64); ok {
				return -n
			}
		}
	}
	return nil
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

func (db *Database) evalSubquery(outerRow map[string]interface{}, sel *QP.SelectStmt) []map[string]interface{} {
	if sel == nil || sel.From == nil {
		return nil
	}

	tableName := sel.From.Name
	tableData, ok := db.data[tableName]
	if !ok || tableData == nil {
		return nil
	}

	rows := []map[string]interface{}{}
	for _, row := range tableData {
		if sel.Where != nil {
			merged := make(map[string]interface{})
			for k, v := range outerRow {
				merged[k] = v
			}
			for k, v := range row {
				merged[k] = v
			}
			if !db.evalWhere(merged, sel.Where) {
				continue
			}
		}
		rows = append(rows, row)
	}

	return rows
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

func (db *Database) computeAggregate(data []map[string]interface{}, fc *QP.FuncCall) interface{} {
	funcName := fc.Name
	var colName string
	if len(fc.Args) > 0 {
		if cr, ok := fc.Args[0].(*QP.ColumnRef); ok {
			colName = cr.Name
		}
	}

	switch funcName {
	case "COUNT":
		// COUNT(*) or COUNT() without args counts all rows
		if len(fc.Args) == 0 || colName == "*" || colName == "" {
			return int64(len(data))
		}
		count := 0
		for _, r := range data {
			if r != nil {
				if val, ok := r[colName]; ok && val != nil {
					count++
				}
			}
		}
		return int64(count)
	case "SUM":
		var sum float64
		for _, r := range data {
			if r != nil {
				if val, ok := r[colName]; ok {
					switch v := val.(type) {
					case int64:
						sum += float64(v)
					case int:
						sum += float64(v)
					case float64:
						sum += v
					}
				}
			}
		}
		return sum
	case "AVG":
		var sum float64
		var count int
		for _, r := range data {
			if r != nil {
				if val, ok := r[colName]; ok && val != nil {
					switch v := val.(type) {
					case int64:
						sum += float64(v)
					case int:
						sum += float64(v)
					case float64:
						sum += v
					}
					count++
				}
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)
	case "MIN":
		var minVal interface{}
		for _, r := range data {
			if r != nil {
				if val, ok := r[colName]; ok && val != nil {
					if minVal == nil {
						minVal = val
					} else {
						if db.compareVals(val, minVal) < 0 {
							minVal = val
						}
					}
				}
			}
		}
		return minVal
	case "MAX":
		var maxVal interface{}
		for _, r := range data {
			if r != nil {
				if val, ok := r[colName]; ok && val != nil {
					if maxVal == nil {
						maxVal = val
					} else {
						if db.compareVals(val, maxVal) > 0 {
							maxVal = val
						}
					}
				}
			}
		}
		return maxVal
	}
	return nil
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

func (db *Database) computeGroupBy(data []map[string]interface{}, stmt *QP.SelectStmt) (*Rows, error) {

	groupByCols := make([]string, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		if cr, ok := gb.(*QP.ColumnRef); ok {
			groupByCols[i] = cr.Name
		}
	}

	groups := make(map[string][]map[string]interface{})
	for _, row := range data {
		key := ""
		for _, col := range groupByCols {
			if val, ok := row[col]; ok {
				key += fmt.Sprintf("%v_", val)
			}
		}
		groups[key] = append(groups[key], row)
	}

	resultCols := make([]string, 0)
	resultData := make([][]interface{}, 0)

	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			resultCols = append(resultCols, cr.Name)
		} else if fc, ok := col.(*QP.FuncCall); ok {
			colName := fc.Name
			if len(fc.Args) > 0 {
				colName = fc.Name + "("
				for i, arg := range fc.Args {
					if i > 0 {
						colName += ", "
					}
					if argCr, ok := arg.(*QP.ColumnRef); ok {
						colName += argCr.Name
					} else if lit, ok := arg.(*QP.Literal); ok {
						colName += fmt.Sprintf("%v", lit.Value)
					}
				}
				colName += ")"
			} else {
				colName = fc.Name + "(*)"
			}
			resultCols = append(resultCols, colName)
		}
	}

	aggCache := make(map[string]map[string]interface{})
	for key, rows := range groups {
		aggResults := make(map[string]interface{})
		for _, col := range stmt.Columns {
			if fc, ok := col.(*QP.FuncCall); ok {
				colName := fc.Name
				if len(fc.Args) > 0 {
					colName = fc.Name + "("
					for i, arg := range fc.Args {
						if i > 0 {
							colName += ", "
						}
						if argCr, ok := arg.(*QP.ColumnRef); ok {
							colName += argCr.Name
						} else if lit, ok := arg.(*QP.Literal); ok {
							colName += fmt.Sprintf("%v", lit.Value)
						}
					}
					colName += ")"
				} else {
					colName = fc.Name + "(*)"
				}
				aggResults[colName] = db.computeAggregate(rows, fc)
			}
		}
		aggCache[key] = aggResults
	}

	for key, rows := range groups {
		row := make([]interface{}, 0)

		for _, gbCol := range groupByCols {
			var val interface{}
			if len(rows) > 0 {
				val = rows[0][gbCol]
			}
			row = append(row, val)
		}

		for _, col := range stmt.Columns {
			if fc, ok := col.(*QP.FuncCall); ok {
				colName := fc.Name
				if len(fc.Args) > 0 {
					colName = fc.Name + "("
					for i, arg := range fc.Args {
						if i > 0 {
							colName += ", "
						}
						if argCr, ok := arg.(*QP.ColumnRef); ok {
							colName += argCr.Name
						} else if lit, ok := arg.(*QP.Literal); ok {
							colName += fmt.Sprintf("%v", lit.Value)
						}
					}
					colName += ")"
				} else {
					colName = fc.Name + "(*)"
				}
				row = append(row, aggCache[key][colName])
			}
		}

		if stmt.Having != nil {
			passesHaving := false
			for _, col := range stmt.Columns {
				if fc, ok := col.(*QP.FuncCall); ok {
					havingColName := fc.Name
					if len(fc.Args) > 0 {
						havingColName = fc.Name + "("
						for i, arg := range fc.Args {
							if i > 0 {
								havingColName += ", "
							}
							if argCr, ok := arg.(*QP.ColumnRef); ok {
								havingColName += argCr.Name
							} else if lit, ok := arg.(*QP.Literal); ok {
								havingColName += fmt.Sprintf("%v", lit.Value)
							}
						}
						havingColName += ")"
					} else {
						havingColName = fc.Name + "(*)"
					}
					havingVal := aggCache[key][havingColName]

					if pred, ok := stmt.Having.(*QP.BinaryExpr); ok {
						if lit, ok := pred.Right.(*QP.Literal); ok {
							litVal := lit.Value
							cmp := db.compareVals(havingVal, litVal)
							op := pred.Op
							if op == QP.TokenGt && cmp > 0 {
								passesHaving = true
							} else if op == QP.TokenGe && cmp >= 0 {
								passesHaving = true
							} else if op == QP.TokenLt && cmp < 0 {
								passesHaving = true
							} else if op == QP.TokenLe && cmp <= 0 {
								passesHaving = true
							} else if op == QP.TokenEq && cmp == 0 {
								passesHaving = true
							}
						}
					}
				}
			}
			if !passesHaving {
				continue
			}
		}

		resultData = append(resultData, row)
	}

	// Sort results by group by columns to match SQLite order
	if len(resultData) > 1 && len(groupByCols) > 0 {
		for i := 0; i < len(resultData)-1; i++ {
			for j := i + 1; j < len(resultData); j++ {
				for gbIdx := range groupByCols {
					cmp := db.compareVals(resultData[i][gbIdx], resultData[j][gbIdx])
					if cmp > 0 {
						resultData[i], resultData[j] = resultData[j], resultData[i]
						break
					} else if cmp < 0 {
						break
					}
				}
			}
		}
	}

	return &Rows{Columns: resultCols, Data: resultData}, nil
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
		if stmt.Where == nil || db.evalWhere(row, stmt.Where) {
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

func (db *Database) handleJoin(stmt *QP.SelectStmt) (*Rows, error) {
	join := stmt.From.Join

	leftTableName := stmt.From.Name
	leftData, ok := db.data[leftTableName]
	if !ok || leftData == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	rightTableName := join.Right.Name
	rightData, ok := db.data[rightTableName]
	if !ok || rightData == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	var result *Rows
	switch join.Type {
	case "INNER":
		result = db.innerJoin(leftData, rightData, join.Cond, stmt)
	case "LEFT":
		result = db.leftJoin(leftData, rightData, join.Cond, stmt)
	case "CROSS":
		result = db.crossJoin(leftData, rightData, stmt)
	default:
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	return result, nil
}

func (db *Database) innerJoin(left, right []map[string]interface{}, cond QP.Expr, stmt *QP.SelectStmt) *Rows {
	resultData := make([][]interface{}, 0)

	for _, lrow := range left {
		for _, rrow := range right {
			if db.evalJoinCond(lrow, rrow, cond) {
				row := db.combineRows(lrow, rrow, stmt)
				resultData = append(resultData, row)
			}
		}
	}

	cols := db.getJoinColumns(stmt)
	return &Rows{Columns: cols, Data: resultData, pos: -1}
}

func (db *Database) leftJoin(left, right []map[string]interface{}, cond QP.Expr, stmt *QP.SelectStmt) *Rows {
	resultData := make([][]interface{}, 0)
	rightCols := db.getRightColumns(right)

	for _, lrow := range left {
		found := false
		for _, rrow := range right {
			if db.evalJoinCond(lrow, rrow, cond) {
				row := db.combineRows(lrow, rrow, stmt)
				resultData = append(resultData, row)
				found = true
			}
		}
		if !found {
			row := make([]interface{}, 0)
			for _, col := range stmt.Columns {
				if cr, ok := col.(*QP.ColumnRef); ok {
					colName := cr.Name
					if idx := strings.Index(cr.Name, "."); idx > 0 {
						colName = cr.Name[idx+1:]
					}
					if val, ok := lrow[colName]; ok {
						row = append(row, val)
					} else {
						row = append(row, nil)
					}
				}
			}
			for range rightCols {
				row = append(row, nil)
			}
			resultData = append(resultData, row)
		}
	}

	cols := db.getJoinColumns(stmt)
	return &Rows{Columns: cols, Data: resultData, pos: -1}
}

func (db *Database) crossJoin(left, right []map[string]interface{}, stmt *QP.SelectStmt) *Rows {
	resultData := make([][]interface{}, 0)

	for _, lrow := range left {
		for _, rrow := range right {
			row := db.combineRows(lrow, rrow, stmt)
			resultData = append(resultData, row)
		}
	}

	cols := db.getJoinColumns(stmt)
	return &Rows{Columns: cols, Data: resultData, pos: -1}
}

func (db *Database) evalJoinCond(leftRow, rightRow map[string]interface{}, cond QP.Expr) bool {
	if cond == nil {
		return true
	}
	switch e := cond.(type) {
	case *QP.BinaryExpr:
		leftVal := db.evalJoinExpr(leftRow, rightRow, e.Left)
		rightVal := db.evalJoinExpr(leftRow, rightRow, e.Right)
		switch e.Op {
		case QP.TokenEq:
			return db.valuesEqual(leftVal, rightVal)
		case QP.TokenNe:
			return !db.valuesEqual(leftVal, rightVal)
		case QP.TokenLt:
			return db.compareVals(leftVal, rightVal) < 0
		case QP.TokenLe:
			return db.compareVals(leftVal, rightVal) <= 0
		case QP.TokenGt:
			return db.compareVals(leftVal, rightVal) > 0
		case QP.TokenGe:
			return db.compareVals(leftVal, rightVal) >= 0
		}
	}
	return true
}

func (db *Database) evalJoinExpr(leftRow, rightRow map[string]interface{}, expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		if val, ok := leftRow[e.Name]; ok {
			return val
		}
		if val, ok := rightRow[e.Name]; ok {
			return val
		}
		if idx := strings.Index(e.Name, "."); idx > 0 {
			colName := e.Name[idx+1:]
			if val, ok := leftRow[colName]; ok {
				return val
			}
			if val, ok := rightRow[colName]; ok {
				return val
			}
		}
		return nil
	}
	return nil
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

	program, err := VM.Compile(sql)
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

func (db *Database) execVMQuery(sql string, stmt *QP.SelectStmt) (*Rows, error) {
	tableName := stmt.From.Name
	if db.data[tableName] == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	// Get table column order for proper column mapping
	tableCols := db.columnOrder[tableName]
	program, err := VM.CompileWithSchema(sql, tableCols)
	if err != nil {
		return nil, err
	}

	ctx := &dbVmContext{db: db}
	vm := VM.NewVMWithContext(program, ctx)

	err = vm.Run(nil)
	if err != nil {
		return nil, err
	}

	results := vm.Results()

	// Get column names from the SELECT statement
	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			cols = append(cols, colRef.Name)
		} else if alias, ok := col.(*QP.AliasExpr); ok {
			cols = append(cols, alias.Alias)
		} else {
			// Fallback to table column order
			cols = db.columnOrder[tableName]
			break
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
