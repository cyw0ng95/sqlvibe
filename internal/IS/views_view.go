package IS

import (
	"database/sql"

	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// VIEWSView provides the information_schema.views virtual table
type VIEWSView struct {
	mp *MetadataProvider
}

// NewVIEWSView creates a new VIEWS view
func NewVIEWSView(btree *DS.BTree) *VIEWSView {
	return &VIEWSView{
		mp: NewMetadataProvider(btree),
	}
}

// Query returns all views from the database
func (vv *VIEWSView) Query(schema, tableName string) ([]ViewInfo, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	views, err := vv.mp.GetViews()
	if err != nil {
		return nil, err
	}

	// Filter by table name if specified
	if tableName != "" {
		for _, view := range views {
			if view.TableName == tableName {
				return []ViewInfo{view}, nil
			}
		}
		return nil, nil
	}

	return views, nil
}

// ToSQL converts view info to database/sql compatible format
func (vi ViewInfo) ToSQL() []any {
	return []any{
		vi.TableName,
		vi.TableSchema,
		vi.ViewDefinition,
	}
}

// View returns all views in database/sql format
func (vv *VIEWSView) View(db *sql.DB) (*sql.Rows, error) {
	views, err := vv.Query("", "")
	if err != nil {
		return nil, err
	}

	// TODO: Convert []ViewInfo to sql.Rows
	// This requires implementing sql.Rows interface
	_ = views

	return nil, nil
}
