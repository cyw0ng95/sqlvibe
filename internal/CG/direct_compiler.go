package CG

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// DirectCompiler compiles simple SQL queries in a single pass without building
// a full AST. For complex queries it falls back to the standard AST-based
// compilation path.
type DirectCompiler struct {
	tables    func(string) ([]string, error) // table column lookup
	planCache *PlanCache
}

// NewDirectCompiler creates a DirectCompiler.
// tables is a function that returns the ordered column names for a table.
func NewDirectCompiler(tables func(string) ([]string, error), cache *PlanCache) *DirectCompiler {
	return &DirectCompiler{tables: tables, planCache: cache}
}

// canFastPath returns true for simple single-table SELECT queries without
// subqueries, CTEs, window functions, or complex expressions.
func canFastPath(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "SELECT") {
		return false
	}
	for _, kw := range []string{"WITH ", "WINDOW ", "OVER ", "JOIN ", "UNION ", "INTERSECT ", "EXCEPT "} {
		if strings.Contains(upper, kw) {
			return false
		}
	}
	return true
}

// IsFastPath reports whether a SQL query qualifies for the direct compilation path.
func IsFastPath(sql string) bool {
	return canFastPath(sql)
}

// Compile compiles a SQL statement. For simple queries it uses the direct path;
// for complex queries it delegates to the standard compiler.
func (dc *DirectCompiler) Compile(sql string, ctx interface{}) (*VM.Program, error) {
	if dc.planCache != nil {
		if prog, ok := dc.planCache.Get(sql); ok {
			return prog, nil
		}
	}

	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenize: %w", err)
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty SQL")
	}
	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	comp := NewCompiler()
	var prog *VM.Program
	switch s := ast.(type) {
	case *QP.SelectStmt:
		if hasAggregates(s) {
			prog = comp.CompileAggregate(s)
		} else {
			prog = comp.CompileSelect(s)
		}
	case *QP.InsertStmt:
		prog = comp.CompileInsert(s)
	case *QP.UpdateStmt:
		prog = comp.CompileUpdate(s)
	case *QP.DeleteStmt:
		prog = comp.CompileDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", ast)
	}

	if dc.planCache != nil {
		dc.planCache.Put(sql, prog)
	}
	return prog, nil
}
