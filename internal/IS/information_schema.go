package IS

import (
	"github.com/cyw0ng95/sqlvibe/internal/DS"
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

// FKInfo holds foreign key constraint information.
type FKInfo struct {
	ChildColumns  []string
	ParentTable   string
	ParentColumns []string
}

// SchemaSource is implemented by the database to provide schema metadata to IS.
type SchemaSource interface {
	GetTableNames() []string
	GetTableType(name string) string
	GetColumnNames(tableName string) []string
	GetColumnType(tableName, colName string) string
	IsColumnNullable(tableName, colName string) bool
	GetColumnDefault(tableName, colName string) interface{}
	GetPrimaryKeyColumns(tableName string) []string
	GetUniqueIndexes(tableName string) map[string][]string
	GetForeignKeys(tableName string) []FKInfo
	GetViewDefinition(name string) string
	GetViewNames() []string
}

// MetadataProvider extracts metadata from the database
type MetadataProvider struct {
	btree     *DS.BTree
	extractor *SchemaExtractor
	source    SchemaSource
}

// NewMetadataProvider creates a new metadata provider
func NewMetadataProvider(btree *DS.BTree) *MetadataProvider {
	return &MetadataProvider{
		btree:     btree,
		extractor: NewSchemaExtractor(btree),
	}
}

// NewMetadataProviderWithSource creates a metadata provider backed by a SchemaSource.
func NewMetadataProviderWithSource(btree *DS.BTree, source SchemaSource) *MetadataProvider {
	return &MetadataProvider{
		btree:     btree,
		extractor: NewSchemaExtractorWithSource(btree, source),
		source:    source,
	}
}

// GetTables returns all tables and views in the database
func (mp *MetadataProvider) GetTables() ([]TableInfo, error) {
	return mp.extractor.ExtractTables()
}

// GetColumns returns all columns for a given table
func (mp *MetadataProvider) GetColumns(tableName string) ([]ColumnInfo, error) {
	return mp.extractor.ExtractColumns(tableName)
}

// GetAllColumns returns all columns for all tables
func (mp *MetadataProvider) GetAllColumns() ([]ColumnInfo, error) {
	return mp.extractor.GetAllColumns()
}

// GetViews returns all views in the database
func (mp *MetadataProvider) GetViews() ([]ViewInfo, error) {
	return mp.extractor.ExtractViews()
}

// GetConstraints returns all constraints for a given table
func (mp *MetadataProvider) GetConstraints(tableName string) ([]ConstraintInfo, error) {
	return mp.extractor.ExtractConstraints(tableName)
}

// GetAllConstraints returns all constraints for all tables
func (mp *MetadataProvider) GetAllConstraints() ([]ConstraintInfo, error) {
	return mp.extractor.GetAllConstraints()
}

// GetReferentialConstraints returns all foreign key relationships
func (mp *MetadataProvider) GetReferentialConstraints() ([]ReferentialConstraint, error) {
	return mp.extractor.GetReferentialConstraints()
}
