package sqlvibe

import (
	"github.com/sqlvibe/sqlvibe/internal/QP"
)

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
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				tableName = s
			}
		case *QP.ColumnRef:
			tableName = v.Name
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
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				tableName = s
			}
		case *QP.ColumnRef:
			tableName = v.Name
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
	rows := [][]interface{}{
		{int64(0), "main", db.dbPath},
	}
	return &Rows{Columns: []string{"seq", "name", "file"}, Data: rows}, nil
}
