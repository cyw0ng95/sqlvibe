package IS

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// Registry manages all information_schema views
type Registry struct {
	btree           *DS.BTree
	colsView        *COLUMNSView
	tablesView      *TABLESView
	viewsView       *VIEWSView
	constraintsView *CONSTRAINTSView
	referentialView *REFERENTIALView
}

// NewRegistry creates a new information schema registry
func NewRegistry(btree *DS.BTree) *Registry {
	return &Registry{
		btree:           btree,
		colsView:        NewCOLUMNSView(btree),
		tablesView:      NewTABLESView(btree),
		viewsView:       NewVIEWSView(btree),
		constraintsView: NewCONSTRAINTSView(btree),
		referentialView: NewREFERENTIALView(btree),
	}
}

// IsInformationSchemaTable returns true if the table name is an information_schema table
func (r *Registry) IsInformationSchemaTable(tableName string) bool {
	if !strings.HasPrefix(strings.ToLower(tableName), "information_schema") {
		return false
	}

	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return false
	}

	viewName := strings.ToLower(parts[1])

	switch viewName {
	case "columns", "tables", "views", "table_constraints", "referential_constraints":
		return true
	default:
		return false
	}
}

// QueryInformationSchema executes a query against information_schema views
func (r *Registry) QueryInformationSchema(viewName, filterSchema, filterTable string) ([][]any, error) {
	var results [][]any

	switch strings.ToLower(viewName) {
	case "columns":
		cols, err := r.colsView.Query(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, col := range cols {
			results = append(results, col.ToSQL())
		}

	case "tables":
		tables, err := r.tablesView.Query(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			results = append(results, table.ToSQL())
		}

	case "views":
		views, err := r.viewsView.Query(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, view := range views {
			results = append(results, view.ToSQL())
		}

	case "table_constraints":
		constraints, err := r.constraintsView.Query(filterSchema, filterTable)
		if err != nil {
			return nil, err
		}
		for _, constraint := range constraints {
			results = append(results, constraint.ToSQL())
		}

	case "referential_constraints":
		refs, err := r.referentialView.Query(filterSchema, "")
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
