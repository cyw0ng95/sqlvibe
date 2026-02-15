package sqlvibe

import (
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/DS"
	"github.com/sqlvibe/sqlvibe/internal/PB"
	"github.com/sqlvibe/sqlvibe/internal/QE"
	"github.com/sqlvibe/sqlvibe/internal/QP"
)

type Database struct {
	pm          *DS.PageManager
	engine      *QE.QueryEngine
	tx          *Transaction
	tables      map[string]map[string]string        // table name -> column name -> type
	primaryKeys map[string][]string                 // table name -> primary key column names
	columnOrder map[string][]string                 // table name -> ordered column names
	data        map[string][]map[string]interface{} // table name -> rows -> column name -> value
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

	engine := QE.NewQueryEngine(pm)

	return &Database{
		pm:          pm,
		engine:      engine,
		tables:      make(map[string]map[string]string),
		primaryKeys: make(map[string][]string),
		columnOrder: make(map[string][]string),
		data:        make(map[string][]map[string]interface{}),
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

	if ast.NodeType() == "SelectStmt" {
		stmt := ast.(*QP.SelectStmt)

		var tableName string
		if stmt.From != nil {
			tableName = stmt.From.Name
		}
		if tableName == "" {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		fmt.Printf("DEBUG Query: tableName=%s db.data=%v\n", tableName, db.data)
		tableData, ok := db.data[tableName]
		fmt.Printf("DEBUG Query: tableData=%v ok=%v\n", tableData, ok)
		if !ok || tableData == nil {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		var cols []string
		if len(stmt.Columns) == 1 {
			if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok {
				if cr.Name == "*" {
					if tableSchema, ok := db.tables[tableName]; ok {
						for colName := range tableSchema {
							cols = append(cols, colName)
						}
					}
				} else {
					cols = []string{cr.Name}
				}
			}
		} else {
			for _, col := range stmt.Columns {
				if cr, ok := col.(*QP.ColumnRef); ok {
					cols = append(cols, cr.Name)
				} else {
					cols = append(cols, "expr")
				}
			}
		}

		if len(cols) == 0 {
			if tableSchema, ok := db.tables[tableName]; ok {
				for colName := range tableSchema {
					cols = append(cols, colName)
				}
			}
		}

		scan := QE.NewTableScan(db.engine, tableName)
		scan.SetData(tableData)
		fmt.Printf("DEBUG: scan.data = %v\n", scan.GetData())

		predicate := db.engine.BuildPredicate(stmt.Where)
		filter := QE.NewFilter(scan, predicate)

		project := QE.NewProject(filter, cols)

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

		fmt.Printf("DEBUG: Before loop, limited=%v\n", limited)

		resultData := make([][]interface{}, 0)
		for {
			row, err := limited.Next()
			fmt.Printf("DEBUG: row from Next: %v, err: %v\n", row, err)
			if err != nil {
				return nil, err
			}
			if row == nil {
				break
			}
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

		return &Rows{Columns: cols, Data: resultData, pos: -1}, nil
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
	default:
		return nil
	}
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

func (db *Database) evalWhere(row map[string]interface{}, where QP.Expr) bool {
	if where == nil {
		return true
	}
	switch e := where.(type) {
	case *QP.BinaryExpr:
		leftVal := db.evalExpr(row, e.Left)
		rightVal := db.evalExpr(row, e.Right)
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
	}
	return 0
}

func (db *Database) applyOrderBy(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	if len(orderBy) == 0 || len(data) == 0 {
		return data
	}

	sorted := make([][]interface{}, len(data))
	copy(sorted, data)

	for i := range sorted {
		for j := i + 1; j < len(sorted); j++ {
			for _, ob := range orderBy {
				var keyValI, keyValJ interface{}
				if colRef, ok := ob.Expr.(*QP.ColumnRef); ok {
					for ci, cn := range cols {
						if cn == colRef.Name {
							keyValI = sorted[i][ci]
							keyValJ = sorted[j][ci]
							break
						}
					}
				}
				cmp := db.compareVals(keyValI, keyValJ)
				if ob.Desc {
					cmp = -cmp
				}
				if cmp < 0 {
					sorted[i], sorted[j] = sorted[j], sorted[i]
					break
				}
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
