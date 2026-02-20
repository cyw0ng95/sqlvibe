package IS

import (
	"database/sql"

	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// COLUMNSView provides the information_schema.columns virtual table
type COLUMNSView struct {
	mp *MetadataProvider
}

// NewCOLUMNSView creates a new COLUMNS view
func NewCOLUMNSView(btree *DS.BTree) *COLUMNSView {
	return &COLUMNSView{
		mp: NewMetadataProvider(btree),
	}
}

// Query returns all columns from the database
func (cv *COLUMNSView) Query(schema, tableName string) ([]ColumnInfo, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	if tableName != "" {
		return cv.mp.GetColumns(tableName)
	}

	return cv.mp.GetAllColumns()
}

// ToSQL converts column info to database/sql compatible format
func (ci ColumnInfo) ToSQL() []any {
	return []any{
		ci.ColumnName,
		ci.TableName,
		ci.TableSchema,
		ci.DataType,
		ci.IsNullable,
		ci.ColumnDefault,
	}
}

// Columns returns all columns in database/sql format
func (cv *COLUMNSView) Columns(db *sql.DB) (*sql.Rows, error) {
	columns, err := cv.Query("", "")
	if err != nil {
		return nil, err
	}

	// TODO: Convert []ColumnInfo to sql.Rows
	// This requires implementing sql.Rows interface
	_ = columns

	return nil, nil
}
