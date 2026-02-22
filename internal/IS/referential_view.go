package IS

import (
	"database/sql"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// REFERENTIALView provides information_schema.referential_constraints virtual table
type REFERENTIALView struct {
	mp *MetadataProvider
}

// NewREFERENTIALView creates a new REFERENTIAL constraints view
func NewREFERENTIALView(btree *DS.BTree) *REFERENTIALView {
	return &REFERENTIALView{
		mp: NewMetadataProvider(btree),
	}
}

// Query returns all referential constraints from database
func (rv *REFERENTIALView) Query(schema, constraintName string) ([]ReferentialConstraint, error) {
	if schema != "" && schema != TableSchemaMain {
		return nil, nil
	}

	refs, err := rv.mp.GetReferentialConstraints()
	if err != nil {
		return nil, err
	}

	// Filter by constraint name if specified
	if constraintName != "" {
		filtered := make([]ReferentialConstraint, 0)
		for _, ref := range refs {
			if ref.ConstraintName == constraintName {
				filtered = append(filtered, ref)
			}
		}
		return filtered, nil
	}

	return refs, nil
}

// ToSQL converts referential constraint info to database/sql compatible format
func (rc ReferentialConstraint) ToSQL() []any {
	return []any{
		rc.ConstraintName,
		rc.UniqueConstraintSchema,
		rc.UniqueConstraintName,
	}
}

// Constraints returns all referential constraints in database/sql format
func (rv *REFERENTIALView) Constraints(db *sql.DB) (*sql.Rows, error) {
	refs, err := rv.Query("", "")
	if err != nil {
		return nil, err
	}

	// TODO: Convert []ReferentialConstraint to sql.Rows
	// This requires implementing sql.Rows interface
	_ = refs

	return nil, nil
}
