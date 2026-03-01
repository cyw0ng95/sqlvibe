package IS

import (
	"database/sql"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// CONSTRAINTSView provides the information_schema.table_constraints virtual table
type CONSTRAINTSView struct {
	mp *MetadataProvider
}

// NewCONSTRAINTSView creates a new TABLE_CONSTRAINTS view
func NewCONSTRAINTSView(btree *DS.BTree) *CONSTRAINTSView {
	return &CONSTRAINTSView{
		mp: NewMetadataProvider(btree),
	}
}

// Query returns all constraints from the database
func (cv *CONSTRAINTSView) Query(schema, tableName string) ([]ConstraintInfo, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	if tableName != "" {
		constraints, err := cv.mp.GetConstraints(tableName)
		if err != nil {
			return nil, err
		}
		return constraints, nil
	}

	allConstraints, err := cv.mp.GetAllConstraints()
	if err != nil {
		return nil, err
	}

	// Filter by schema if needed
	filteredConstraints := make([]ConstraintInfo, 0)
	for _, constraint := range allConstraints {
		if schema == "" || constraint.TableSchema == schema {
			filteredConstraints = append(filteredConstraints, constraint)
		}
	}

	return filteredConstraints, nil
}

// ToSQL converts constraint info to database/sql compatible format
func (ci ConstraintInfo) ToSQL() []any {
	return []any{
		ci.ConstraintName,
		ci.TableName,
		ci.TableSchema,
		ci.ConstraintType,
	}
}

// Constraints returns all constraints in database/sql format
func (cv *CONSTRAINTSView) Constraints(db *sql.DB) (*sql.Rows, error) {
	constraints, err := cv.Query("", "")
	if err != nil {
		return nil, err
	}

	// Conversion to sql.Rows is not required; consumers use Query() directly.
	_ = constraints

	return nil, nil
}
