package IS

import (
	"fmt"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// SchemaExtractor extracts table and column metadata from BTree schema
type SchemaExtractor struct {
	btree  *DS.BTree
	source SchemaSource
}

// NewSchemaExtractor creates a new schema extractor
func NewSchemaExtractor(btree *DS.BTree) *SchemaExtractor {
	return &SchemaExtractor{btree: btree}
}

// NewSchemaExtractorWithSource creates a schema extractor backed by a SchemaSource.
func NewSchemaExtractorWithSource(btree *DS.BTree, source SchemaSource) *SchemaExtractor {
	return &SchemaExtractor{btree: btree, source: source}
}

// ExtractTables extracts all table information from the database
func (se *SchemaExtractor) ExtractTables() ([]TableInfo, error) {
	if se.source != nil {
		tables := make([]TableInfo, 0)
		for _, name := range se.source.GetTableNames() {
			tables = append(tables, TableInfo{
				TableName:   name,
				TableSchema: TableSchemaMain,
				TableType:   se.source.GetTableType(name),
			})
		}
		for _, name := range se.source.GetViewNames() {
			tables = append(tables, TableInfo{
				TableName:   name,
				TableSchema: TableSchemaMain,
				TableType:   ViewTypeView,
			})
		}
		return tables, nil
	}
	return make([]TableInfo, 0), nil
}

// ExtractColumns extracts column information for a specific table
func (se *SchemaExtractor) ExtractColumns(tableName string) ([]ColumnInfo, error) {
	if se.source != nil {
		cols := make([]ColumnInfo, 0)
		for _, colName := range se.source.GetColumnNames(tableName) {
			nullable := "YES"
			if !se.source.IsColumnNullable(tableName, colName) {
				nullable = "NO"
			}
			defVal := ""
			if d := se.source.GetColumnDefault(tableName, colName); d != nil {
				defVal = fmt.Sprintf("%v", d)
			}
			cols = append(cols, ColumnInfo{
				ColumnName:    colName,
				TableName:     tableName,
				TableSchema:   TableSchemaMain,
				DataType:      se.source.GetColumnType(tableName, colName),
				IsNullable:    nullable,
				ColumnDefault: defVal,
			})
		}
		return cols, nil
	}
	return make([]ColumnInfo, 0), nil
}

// GetAllColumns extracts all column information from all tables
func (se *SchemaExtractor) GetAllColumns() ([]ColumnInfo, error) {
	if se.source != nil {
		allColumns := make([]ColumnInfo, 0)
		for _, tableName := range se.source.GetTableNames() {
			cols, err := se.ExtractColumns(tableName)
			if err != nil {
				return nil, err
			}
			allColumns = append(allColumns, cols...)
		}
		return allColumns, nil
	}
	return make([]ColumnInfo, 0), nil
}

// ExtractViews extracts all view information from the database
func (se *SchemaExtractor) ExtractViews() ([]ViewInfo, error) {
	if se.source != nil {
		views := make([]ViewInfo, 0)
		for _, name := range se.source.GetViewNames() {
			views = append(views, ViewInfo{
				TableName:      name,
				TableSchema:    TableSchemaMain,
				ViewDefinition: se.source.GetViewDefinition(name),
			})
		}
		return views, nil
	}
	return make([]ViewInfo, 0), nil
}

// ExtractConstraints extracts constraint information for a table
func (se *SchemaExtractor) ExtractConstraints(tableName string) ([]ConstraintInfo, error) {
	if se.source != nil {
		constraints := make([]ConstraintInfo, 0)
		pkCols := se.source.GetPrimaryKeyColumns(tableName)
		if len(pkCols) > 0 {
			constraints = append(constraints, ConstraintInfo{
				ConstraintName: tableName + "_pk",
				TableName:      tableName,
				TableSchema:    TableSchemaMain,
				ConstraintType: ConstraintTypePrimaryKey,
			})
		}
		for idxName := range se.source.GetUniqueIndexes(tableName) {
			constraints = append(constraints, ConstraintInfo{
				ConstraintName: idxName,
				TableName:      tableName,
				TableSchema:    TableSchemaMain,
				ConstraintType: ConstraintTypeUnique,
			})
		}
		for i := range se.source.GetForeignKeys(tableName) {
			constraints = append(constraints, ConstraintInfo{
				ConstraintName: fmt.Sprintf("%s_fk_%d", tableName, i),
				TableName:      tableName,
				TableSchema:    TableSchemaMain,
				ConstraintType: ConstraintTypeForeignKey,
			})
		}
		return constraints, nil
	}
	return make([]ConstraintInfo, 0), nil
}

// GetAllConstraints extracts all constraint information from all tables
func (se *SchemaExtractor) GetAllConstraints() ([]ConstraintInfo, error) {
	if se.source != nil {
		allConstraints := make([]ConstraintInfo, 0)
		for _, tableName := range se.source.GetTableNames() {
			c, err := se.ExtractConstraints(tableName)
			if err != nil {
				return nil, err
			}
			allConstraints = append(allConstraints, c...)
		}
		return allConstraints, nil
	}
	return make([]ConstraintInfo, 0), nil
}

// GetReferentialConstraints extracts all foreign key relationships
func (se *SchemaExtractor) GetReferentialConstraints() ([]ReferentialConstraint, error) {
	if se.source != nil {
		refs := make([]ReferentialConstraint, 0)
		for _, tableName := range se.source.GetTableNames() {
			for i, fk := range se.source.GetForeignKeys(tableName) {
				childConstraint := fmt.Sprintf("%s_fk_%d", tableName, i)
				parentConstraint := fmt.Sprintf("%s_pk", fk.ParentTable)
				refs = append(refs, ReferentialConstraint{
					ConstraintName:         childConstraint,
					UniqueConstraintSchema: TableSchemaMain,
					UniqueConstraintName:   parentConstraint,
				})
			}
		}
		return refs, nil
	}
	return make([]ReferentialConstraint, 0), nil
}

