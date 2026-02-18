package IS

import (
	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// InformationSchema package provides SQLite-compatible information_schema views
// See: https://www.sqlite.org/information_schema.html

// View types for information schema
const (
	ViewTypeBaseTable = "BASE TABLE"
	ViewTypeView      = "VIEW"
)

// Constraint types for information schema
const (
	ConstraintTypePrimaryKey = "PRIMARY KEY"
	ConstraintTypeUnique     = "UNIQUE"
	ConstraintTypeCheck      = "CHECK"
	ConstraintTypeForeignKey = "FOREIGN KEY"
)

// TableSchema constant for main database
const (
	TableSchemaMain = "main"
)

// TableInfo represents information about a table or view
type TableInfo struct {
	TableName   string
	TableSchema string
	TableType   string
}

// ColumnInfo represents information about a column
type ColumnInfo struct {
	ColumnName    string
	TableName     string
	TableSchema   string
	DataType      string
	IsNullable    string
	ColumnDefault string
}

// ViewInfo represents information about a view
type ViewInfo struct {
	TableName      string
	TableSchema    string
	ViewDefinition string
}

// ConstraintInfo represents information about a constraint
type ConstraintInfo struct {
	ConstraintName string
	TableName      string
	TableSchema    string
	ConstraintType string
}

// ReferentialConstraint represents information about a foreign key relationship
type ReferentialConstraint struct {
	ConstraintName         string
	UniqueConstraintSchema string
	UniqueConstraintName   string
}

// MetadataProvider extracts metadata from the database
type MetadataProvider struct {
	ds *DS.BTree
}

// NewMetadataProvider creates a new metadata provider
func NewMetadataProvider(btree *DS.BTree) *MetadataProvider {
	return &MetadataProvider{
		ds: btree,
	}
}

// GetTables returns all tables and views in the database
func (mp *MetadataProvider) GetTables() ([]TableInfo, error) {
	tables := make([]TableInfo, 0)

	// TODO: Implement schema metadata extraction from BTree
	// This will query the schema table in the BTree
	// and build TableInfo structs

	return tables, nil
}

// GetColumns returns all columns for a given table
func (mp *MetadataProvider) GetColumns(tableName string) ([]ColumnInfo, error) {
	columns := make([]ColumnInfo, 0)

	// TODO: Implement column metadata extraction from BTree
	// This will parse column definitions from schema

	return columns, nil
}

// GetAllColumns returns all columns for all tables
func (mp *MetadataProvider) GetAllColumns() ([]ColumnInfo, error) {
	allColumns := make([]ColumnInfo, 0)

	// TODO: Iterate through all tables and collect columns

	return allColumns, nil
}

// GetViews returns all views in the database
func (mp *MetadataProvider) GetViews() ([]ViewInfo, error) {
	views := make([]ViewInfo, 0)

	// TODO: Implement view metadata extraction from BTree
	// This will query the schema for view definitions

	return views, nil
}

// GetConstraints returns all constraints for a given table
func (mp *MetadataProvider) GetConstraints(tableName string) ([]ConstraintInfo, error) {
	constraints := make([]ConstraintInfo, 0)

	// TODO: Implement constraint metadata extraction from BTree
	// This will parse PK, UNIQUE, CHECK, FK from schema

	return constraints, nil
}

// GetAllConstraints returns all constraints for all tables
func (mp *MetadataProvider) GetAllConstraints() ([]ConstraintInfo, error) {
	allConstraints := make([]ConstraintInfo, 0)

	// TODO: Iterate through all tables and collect constraints

	return allConstraints, nil
}

// GetReferentialConstraints returns all foreign key relationships
func (mp *MetadataProvider) GetReferentialConstraints() ([]ReferentialConstraint, error) {
	refs := make([]ReferentialConstraint, 0)

	// TODO: Implement FK relationship extraction
	// This will parse FK constraints from schema

	return refs, nil
}
