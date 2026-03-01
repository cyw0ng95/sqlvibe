package compiler

import (
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HasCTEs reports whether stmt defines any CTEs (WITH clause).
func HasCTEs(stmt *QP.SelectStmt) bool {
	return stmt != nil && len(stmt.CTEs) > 0
}

// IsRecursiveCTE reports whether cte is declared as a recursive CTE.
func IsRecursiveCTE(cte *QP.CTEClause) bool {
	return cte != nil && cte.Recursive
}

// HasRecursiveCTE reports whether stmt contains at least one recursive CTE.
func HasRecursiveCTE(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for i := range stmt.CTEs {
		if stmt.CTEs[i].Recursive {
			return true
		}
	}
	return false
}

// CTENames returns the names of all CTEs defined by stmt.
func CTENames(stmt *QP.SelectStmt) []string {
	if stmt == nil || len(stmt.CTEs) == 0 {
		return nil
	}
	names := make([]string, len(stmt.CTEs))
	for i, cte := range stmt.CTEs {
		names[i] = cte.Name
	}
	return names
}

// FindCTE returns the CTEClause with the given name, or nil if not found.
func FindCTE(stmt *QP.SelectStmt, name string) *QP.CTEClause {
	if stmt == nil {
		return nil
	}
	for i := range stmt.CTEs {
		if stmt.CTEs[i].Name == name {
			return &stmt.CTEs[i]
		}
	}
	return nil
}

// CTEReferences returns the set of CTE names referenced in expr.
// This is a best-effort analysis based on TableRef names.
func CTEReferences(stmt *QP.SelectStmt) map[string]bool {
	if stmt == nil || len(stmt.CTEs) == 0 {
		return nil
	}
	defined := make(map[string]bool, len(stmt.CTEs))
	for _, cte := range stmt.CTEs {
		defined[cte.Name] = true
	}
	refs := make(map[string]bool)
	collectCTERefs(stmt.From, defined, refs)
	return refs
}

// collectCTERefs recursively collects CTE name references from a TableRef.
func collectCTERefs(ref *QP.TableRef, defined, refs map[string]bool) {
	if ref == nil {
		return
	}
	if defined[ref.Name] {
		refs[ref.Name] = true
	}
	if ref.Join != nil {
		collectCTERefs(ref.Join.Right, defined, refs)
	}
}
