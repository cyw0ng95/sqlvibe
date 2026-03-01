package IS

import (
	"fmt"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// SchemaParser extracts schema metadata from sqlite_master table
type SchemaParser struct {
	pm *DS.PageManager
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(pm *DS.PageManager) *SchemaParser {
	return &SchemaParser{
		pm: pm,
	}
}

// ParseSchema extracts all schema information from the database
// Schema extraction is now handled via the SchemaSource interface in SchemaExtractor.
func (sp *SchemaParser) ParseSchema() ([]TableInfo, []ColumnInfo, []ViewInfo, []ConstraintInfo, []ReferentialConstraint, error) {
	tables := make([]TableInfo, 0)
	columns := make([]ColumnInfo, 0)
	views := make([]ViewInfo, 0)
	constraints := make([]ConstraintInfo, 0)
	refs := make([]ReferentialConstraint, 0)

	return tables, columns, views, constraints, refs, nil
}

// parseCreateTable parses a CREATE TABLE statement.
// Schema parsing is now handled via the SchemaSource interface in SchemaExtractor.
func (sp *SchemaParser) parseCreateTable(sql string) (*TableInfo, []ColumnInfo, []ConstraintInfo, error) {
	_ = sql
	return nil, nil, nil, fmt.Errorf("CREATE TABLE parsing not implemented in SchemaParser; use SchemaSource")
}

// parseCreateView parses a CREATE VIEW statement.
// Schema parsing is now handled via the SchemaSource interface in SchemaExtractor.
func (sp *SchemaParser) parseCreateView(sql string) (*ViewInfo, error) {
	_ = sql
	return nil, fmt.Errorf("CREATE VIEW parsing not implemented in SchemaParser; use SchemaSource")
}
