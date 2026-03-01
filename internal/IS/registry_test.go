package IS

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

func TestRegistry_Creation(t *testing.T) {
	btree := &DS.BTree{}
	registry := NewRegistry(btree)

	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}
}

func TestRegistry_IsInformationSchemaTable(t *testing.T) {
	btree := &DS.BTree{}
	registry := NewRegistry(btree)

	tests := []struct {
		tableName string
		expected  bool
	}{
		{"information_schema.columns", true},
		{"information_schema.tables", true},
		{"information_schema.views", true},
		{"information_schema.table_constraints", true},
		{"information_schema.referential_constraints", true},
		{"INFORMATION_SCHEMA.COLUMNS", true},
		{"regular_table", false},
		{"main.regular_table", false},
		{"test_table", false},
	}

	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			result := registry.IsInformationSchemaTable(tt.tableName)
			if result != tt.expected {
				t.Errorf("Expected %v for %s, got %v", tt.expected, tt.tableName, result)
			}
		})
	}
}

func TestRegistry_GetColumnNames(t *testing.T) {
	btree := &DS.BTree{}
	registry := NewRegistry(btree)

	tests := []struct {
		viewName string
		expected []string
	}{
		{
			viewName: "columns",
			expected: []string{"column_name", "table_name", "table_schema", "data_type", "is_nullable", "column_default"},
		},
		{
			viewName: "tables",
			expected: []string{"table_name", "table_schema", "table_type"},
		},
		{
			viewName: "views",
			expected: []string{"table_name", "table_schema", "view_definition"},
		},
		{
			viewName: "table_constraints",
			expected: []string{"constraint_name", "table_name", "table_schema", "constraint_type"},
		},
		{
			viewName: "referential_constraints",
			expected: []string{"constraint_name", "unique_constraint_schema", "unique_constraint_name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.viewName, func(t *testing.T) {
			cols, err := registry.GetColumnNames(tt.viewName)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if len(cols) != len(tt.expected) {
				t.Errorf("Expected %d columns, got %d", len(tt.expected), len(cols))
			}

			for i, col := range cols {
				if col != tt.expected[i] {
					t.Errorf("Expected column %s at position %d, got %s", tt.expected[i], i, col)
				}
			}
		})
	}
}

func TestTypes_Constants(t *testing.T) {
	constants := []struct {
		name  string
		value string
	}{
		{"ViewTypeBaseTable", ViewTypeBaseTable},
		{"ViewTypeView", ViewTypeView},
		{"TableSchemaMain", TableSchemaMain},
		{"ConstraintTypePrimaryKey", ConstraintTypePrimaryKey},
		{"ConstraintTypeUnique", ConstraintTypeUnique},
		{"ConstraintTypeCheck", ConstraintTypeCheck},
		{"ConstraintTypeForeignKey", ConstraintTypeForeignKey},
	}

	for _, tt := range constants {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value == "" {
				t.Errorf("Expected non-empty constant for %s", tt.name)
			}
		})
	}
}
