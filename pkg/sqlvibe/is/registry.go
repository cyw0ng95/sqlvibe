package is

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// Registry manages all information_schema views using C++ implementation.
type Registry struct {
	btree  *DS.BTree
	cppReg *ISRegistry
}

// NewRegistry creates a new information schema registry using C++ implementation.
func NewRegistry(btree *DS.BTree) *Registry {
	var btreeHandle unsafe.Pointer
	if btree != nil {
		/* TODO: Get C++ btree handle from Go btree */
	}
	
	return &Registry{
		btree:  btree,
		cppReg: NewISRegistry(btreeHandle),
	}
}

// Close closes the registry and frees C++ resources.
func (r *Registry) Close() {
	if r.cppReg != nil {
		r.cppReg.Destroy()
		r.cppReg = nil
	}
}

// IsInformationSchemaTable returns true if the table name is an information_schema table
func (r *Registry) IsInformationSchemaTable(tableName string) bool {
	return IsInformationSchemaTable(tableName)
}

// QueryInformationSchema executes a query against information_schema views using C++ implementation.
func (r *Registry) QueryInformationSchema(viewName, filterSchema, filterTable string) ([][]any, error) {
	var results [][]any

	switch strings.ToLower(viewName) {
	case "columns":
		cols, err := r.cppReg.QueryColumns(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, col := range cols {
			results = append(results, col.ToSQL())
		}

	case "tables":
		tables, err := r.cppReg.QueryTables(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			results = append(results, table.ToSQL())
		}

	case "views":
		views, err := r.cppReg.QueryViews(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, view := range views {
			results = append(results, view.ToSQL())
		}

	case "table_constraints":
		constraints, err := r.cppReg.QueryConstraints(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, constraint := range constraints {
			results = append(results, constraint.ToSQL())
		}

	case "referential_constraints":
		refs, err := r.cppReg.QueryReferential(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, ref := range refs {
			results = append(results, ref.ToSQL())
		}

	default:
		return nil, fmt.Errorf("unknown information_schema view: %s", viewName)
	}

	return results, nil
}

// GetColumnNames returns column names for a given view
func (r *Registry) GetColumnNames(viewName string) ([]string, error) {
	switch strings.ToLower(viewName) {
	case "columns":
		return []string{"column_name", "table_name", "table_schema", "data_type", "is_nullable", "column_default"}, nil
	case "tables":
		return []string{"table_name", "table_schema", "table_type"}, nil
	case "views":
		return []string{"table_name", "table_schema", "view_definition"}, nil
	case "table_constraints":
		return []string{"constraint_name", "table_name", "table_schema", "constraint_type"}, nil
	case "referential_constraints":
		return []string{"constraint_name", "unique_constraint_schema", "unique_constraint_name"}, nil
	default:
		return nil, fmt.Errorf("unknown information_schema view: %s", viewName)
	}
}
