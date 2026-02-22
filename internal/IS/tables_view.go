package IS

import (
	"database/sql"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
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

// Query returns all tables/views from database
func (tv *TABLESView) Query(schema, tableName string) ([]TableInfo, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	if tableName != "" {
		tables, _ := tv.mp.GetTables()
		for _, table := range tables {
			if table.TableName == tableName {
				return []TableInfo{table}, nil
			}
		}
		return nil, nil
	}

	allTables, _ := tv.mp.GetTables()

	// Filter by schema if needed
	filteredTables := make([]TableInfo, 0)
	for _, table := range allTables {
		if schema == "" || table.TableSchema == schema {
			filteredTables = append(filteredTables, table)
		}
	}

	return filteredTables, nil
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
	_, err := tv.Query("", "")
	if err != nil {
		return nil, err
	}

	// TODO: Convert []TableInfo to sql.Rows
	// This requires implementing sql.Rows interface

	return nil, nil
}
