package IS

import (
	"database/sql"

	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// TABLESView provides the information_schema.tables virtual table
type TABLESView struct {
	mp *MetadataProvider
}

// NewTABLESView creates a new TABLES view
func NewTABLESView(btree *DS.BTree) *TABLESView {
	return &TABLESView{
		mp: NewMetadataProvider(btree),
	}
}

// Query returns all tables/views from the database
func (tv *TABLESView) Query(schema, tableName string) ([]TableInfo, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	if tableName != "" {
		return tv.mp.GetTables()
	}

	allTables := make([]TableInfo, 0)

	// Filter by schema and type if needed
	for _, table := range tv.mp.GetTables() {
		if schema == "" || table.TableSchema == schema {
			allTables = append(allTables, table)
		}
	}

	return allTables, nil
}

// ToSQL converts table info to database/sql compatible format
func (ti TableInfo) ToSQL() []any {
	return []any{
		ti.TableName,
		ti.TableSchema,
		ti.TableType,
	}
}

// Table returns all tables in database/sql format
func (tv *TABLESView) Table(db *sql.DB) (*sql.Rows, error) {
	tables, err := tv.Query("", "")
	if err != nil {
		return nil, err
	}

	// TODO: Convert []TableInfo to sql.Rows
	// This requires implementing sql.Rows interface

	return nil, nil
}
