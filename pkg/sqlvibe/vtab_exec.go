package sqlvibe

import (
	"fmt"
	"strings"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// execCreateVirtualTable handles CREATE VIRTUAL TABLE ... USING module(args).
func (db *Database) execCreateVirtualTable(stmt *QP.CreateVirtualTableStmt) error {
	mod, ok := IS.GetVTabModule(stmt.ModuleName)
	if !ok {
		return fmt.Errorf("no such module: %s", stmt.ModuleName)
	}
	vt, err := mod.Create(stmt.ModuleArgs)
	if err != nil {
		return err
	}
	db.queryMu.Lock()
	db.virtualTables[stmt.TableName] = vt
	db.queryMu.Unlock()
	return nil
}

// execVTabQuery materializes all rows from a virtual table cursor.
func (db *Database) execVTabQuery(vtab DS.VTab, cols []string) (*Rows, error) {
	cursor, err := vtab.Open()
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if err := cursor.Filter(0, "", nil); err != nil {
		return nil, err
	}

	var data [][]interface{}
	for !cursor.Eof() {
		row := make([]interface{}, len(cols))
		for i := range cols {
			v, err := cursor.Column(i)
			if err != nil {
				return nil, err
			}
			row[i] = v
		}
		data = append(data, row)
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	if data == nil {
		data = [][]interface{}{}
	}
	return &Rows{Columns: cols, Data: data}, nil
}

// execVTabQuerySelect materializes a virtual table into a temp table and executes the SELECT.
func (db *Database) execVTabQuerySelect(tableName string, vtab DS.VTab, stmt *QP.SelectStmt) (*Rows, error) {
	cols := vtab.Columns()
	rows, err := db.execVTabQuery(vtab, cols)
	if err != nil {
		return nil, err
	}

	// Infer column types from the first data row (same as execDerivedTableQuery).
	colTypes := make(map[string]string)
	for _, col := range cols {
		colTypes[col] = "TEXT"
	}
	if len(rows.Data) > 0 {
		for i, col := range cols {
			if i >= len(rows.Data[0]) {
				break
			}
			switch rows.Data[0][i].(type) {
			case int64, int:
				colTypes[col] = "INTEGER"
			case float64:
				colTypes[col] = "REAL"
			}
		}
	}

	// Materialize as temp table.
	alias := strings.ToLower(tableName)
	tempName := "__vtab_" + alias + "__"
	db.tables[tempName] = colTypes
	db.columnOrder[tempName] = cols
	rowMaps := make([]map[string]interface{}, len(rows.Data))
	for i, row := range rows.Data {
		rm := make(map[string]interface{}, len(cols))
		for j, col := range cols {
			if j < len(row) {
				rm[col] = row[j]
			}
		}
		rowMaps[i] = rm
	}
	db.data[tempName] = rowMaps

	origFrom := stmt.From
	stmt.From = &QP.TableRef{Name: tempName, Alias: origFrom.Alias}
	if stmt.From.Alias == "" {
		stmt.From.Alias = alias
	}
	result, execErr := db.execSelectStmt(stmt)
	stmt.From = origFrom

	delete(db.tables, tempName)
	delete(db.columnOrder, tempName)
	delete(db.data, tempName)

	return result, execErr
}
