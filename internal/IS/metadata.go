package is

import "strings"

// ToSQL converts ColumnInfo to SQL row format
func (ci ColumnInfo) ToSQL() []any {
	return []any{ci.ColumnName, ci.TableName, ci.TableSchema, ci.DataType, ci.IsNullable, ci.ColumnDefault}
}

// ToSQL converts TableInfo to SQL row format
func (ti TableInfo) ToSQL() []any {
	return []any{ti.TableName, ti.TableSchema, ti.TableType}
}

// ToSQL converts ViewInfo to SQL row format
func (vi ViewInfo) ToSQL() []any {
	return []any{vi.TableName, vi.TableSchema, vi.ViewDefinition}
}

// ToSQL converts ConstraintInfo to SQL row format
func (ci ConstraintInfo) ToSQL() []any {
	return []any{ci.ConstraintName, ci.TableName, ci.TableSchema, ci.ConstraintType}
}

// ToSQL converts ReferentialConstraint to SQL row format
func (rc ReferentialConstraint) ToSQL() []any {
	return []any{rc.ConstraintName, rc.UniqueConstraintSchema, rc.UniqueConstraintName}
}

// MetadataProvider provides metadata from database
type MetadataProvider struct{}

// NewMetadataProvider creates a metadata provider
func NewMetadataProvider(btreeHandle interface{}) *MetadataProvider {
	return &MetadataProvider{}
}

// GetColumns gets columns for a table
func (mp *MetadataProvider) GetColumns(tableName string) ([]ColumnInfo, error) {
	if strings.ToLower(tableName) == "sqlite_master" {
		return []ColumnInfo{
			{ColumnName: "type", DataType: "TEXT"},
			{ColumnName: "name", DataType: "TEXT"},
			{ColumnName: "tbl_name", DataType: "TEXT"},
			{ColumnName: "rootpage", DataType: "INTEGER"},
			{ColumnName: "sql", DataType: "TEXT"},
		}, nil
	}
	return nil, nil
}
