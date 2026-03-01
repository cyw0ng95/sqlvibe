package sqlvibe

import (
	"fmt"
	"sort"
	"strings"
)

// TableInfo holds metadata about a table or view.
type TableInfo struct {
	Name string
	Type string // "table" or "view"
	SQL  string // CREATE statement
}

// ColumnInfo holds metadata about a single column.
type ColumnInfo struct {
	Name       string
	Type       string
	NotNull    bool
	Default    interface{}
	PrimaryKey bool
}

// GetTables returns all user tables and views (excludes sqlite_* system tables).
func (db *Database) GetTables() ([]TableInfo, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	tableNames := make([]string, 0, len(db.tables))
	for name := range db.tables {
		if !strings.HasPrefix(name, "sqlite_") {
			tableNames = append(tableNames, name)
		}
	}
	sort.Strings(tableNames)

	result := make([]TableInfo, 0, len(tableNames)+len(db.views))
	for _, name := range tableNames {
		result = append(result, TableInfo{
			Name: name,
			Type: "table",
			SQL:  db.buildCreateTableSQL(name),
		})
	}

	viewNames := make([]string, 0, len(db.views))
	for name := range db.views {
		viewNames = append(viewNames, name)
	}
	sort.Strings(viewNames)
	for _, name := range viewNames {
		result = append(result, TableInfo{
			Name: name,
			Type: "view",
			SQL:  "CREATE VIEW " + name + " AS " + db.views[name],
		})
	}

	return result, nil
}

// GetSchema returns the CREATE statement for the named table or view.
// Returns an error if the table/view does not exist.
func (db *Database) GetSchema(table string) (string, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	lower := strings.ToLower(table)
	if _, ok := db.tables[lower]; ok {
		return db.buildCreateTableSQL(lower), nil
	}
	if viewSQL, ok := db.views[lower]; ok {
		return "CREATE VIEW " + lower + " AS " + viewSQL, nil
	}
	return "", fmt.Errorf("no such table: %s", table)
}

// GetIndexes returns all indexes for the named table.
// If table is empty, all indexes across all tables are returned.
func (db *Database) GetIndexes(table string) ([]IndexInfo, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	lower := strings.ToLower(table)
	names := make([]string, 0, len(db.indexes))
	for name, idx := range db.indexes {
		if lower == "" || strings.EqualFold(idx.Table, lower) {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	result := make([]IndexInfo, 0, len(names))
	for _, name := range names {
		idx := db.indexes[name]
		result = append(result, IndexInfo{
			Name:    idx.Name,
			Table:   idx.Table,
			Columns: idx.Columns,
			Unique:  idx.Unique,
		})
	}
	return result, nil
}

// GetColumns returns column metadata for the named table.
func (db *Database) GetColumns(table string) ([]ColumnInfo, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	lower := strings.ToLower(table)
	colTypes, ok := db.tables[lower]
	if !ok {
		return nil, fmt.Errorf("no such table: %s", table)
	}

	order := db.columnOrder[lower]
	pks := make(map[string]bool, len(db.primaryKeys[lower]))
	for _, pk := range db.primaryKeys[lower] {
		pks[pk] = true
	}
	notNulls := db.columnNotNull[lower]
	defaults := db.columnDefaults[lower]

	result := make([]ColumnInfo, 0, len(order))
	for _, col := range order {
		ci := ColumnInfo{
			Name:       col,
			Type:       colTypes[col],
			NotNull:    notNulls != nil && notNulls[col],
			Default:    nil,
			PrimaryKey: pks[col],
		}
		if defaults != nil {
			ci.Default = defaults[col]
		}
		result = append(result, ci)
	}
	return result, nil
}

// buildCreateTableSQL reconstructs a CREATE TABLE statement from in-memory schema.
func (db *Database) buildCreateTableSQL(table string) string {
	colTypes := db.tables[table]
	if colTypes == nil {
		return fmt.Sprintf("CREATE TABLE %s ()", table)
	}
	order := db.columnOrder[table]
	pks := db.primaryKeys[table]
	pkSet := make(map[string]bool, len(pks))
	for _, pk := range pks {
		pkSet[pk] = true
	}
	notNulls := db.columnNotNull[table]
	defaults := db.columnDefaults[table]

	cols := make([]string, 0, len(order))
	for _, col := range order {
		def := col + " " + colTypes[col]
		if pkSet[col] {
			def += " PRIMARY KEY"
		}
		if notNulls != nil && notNulls[col] {
			def += " NOT NULL"
		}
		if defaults != nil {
			if dv, ok := defaults[col]; ok && dv != nil {
				def += fmt.Sprintf(" DEFAULT %v", dv)
			}
		}
		cols = append(cols, def)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(cols, ", "))
}

// Schema returns the DDL for tables/views in the database.
// If tableName is non-empty, returns DDL for that specific table or view.
// If tableName is empty, returns DDL for all tables and views joined by newlines.
func (db *Database) Schema(tableName string) (string, error) {
	if tableName != "" {
		return db.GetSchema(tableName)
	}
	tables, err := db.GetTables()
	if err != nil {
		return "", err
	}
	stmts := make([]string, 0, len(tables))
	for _, t := range tables {
		stmts = append(stmts, t.SQL+";")
	}
	return strings.Join(stmts, "\n"), nil
}
