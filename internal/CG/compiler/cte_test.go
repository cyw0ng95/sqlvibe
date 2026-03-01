package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestHasCTEs_True(t *testing.T) {
	stmt := parseSelect(t, "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte")
	if !compiler.HasCTEs(stmt) {
		t.Error("expected true for WITH clause")
	}
}

func TestHasCTEs_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id FROM t")
	if compiler.HasCTEs(stmt) {
		t.Error("expected false for stmt without WITH")
	}
}

func TestHasCTEs_Nil(t *testing.T) {
	if compiler.HasCTEs(nil) {
		t.Error("expected false for nil stmt")
	}
}

func TestIsRecursiveCTE_True(t *testing.T) {
	cte := &QP.CTEClause{Name: "r", Recursive: true}
	if !compiler.IsRecursiveCTE(cte) {
		t.Error("expected true for recursive CTE")
	}
}

func TestIsRecursiveCTE_False(t *testing.T) {
	cte := &QP.CTEClause{Name: "cte", Recursive: false}
	if compiler.IsRecursiveCTE(cte) {
		t.Error("expected false for non-recursive CTE")
	}
}

func TestHasRecursiveCTE(t *testing.T) {
	stmt := parseSelect(t, `
		WITH RECURSIVE cnt(n) AS (
			SELECT 1
			UNION ALL
			SELECT n+1 FROM cnt WHERE n < 5
		)
		SELECT n FROM cnt`)
	if !compiler.HasRecursiveCTE(stmt) {
		t.Error("expected true for WITH RECURSIVE")
	}
}

func TestCTENames(t *testing.T) {
	stmt := parseSelect(t, "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b")
	names := compiler.CTENames(stmt)
	if len(names) != 2 {
		t.Fatalf("expected 2 CTE names, got %d", len(names))
	}
	if names[0] != "a" || names[1] != "b" {
		t.Errorf("unexpected CTE names: %v", names)
	}
}

func TestFindCTE_Found(t *testing.T) {
	stmt := parseSelect(t, "WITH foo AS (SELECT 42 AS val) SELECT val FROM foo")
	cte := compiler.FindCTE(stmt, "foo")
	if cte == nil {
		t.Fatal("expected to find CTE 'foo'")
	}
	if cte.Name != "foo" {
		t.Errorf("expected name foo, got %s", cte.Name)
	}
}

func TestFindCTE_NotFound(t *testing.T) {
	stmt := parseSelect(t, "WITH foo AS (SELECT 1) SELECT 1")
	cte := compiler.FindCTE(stmt, "bar")
	if cte != nil {
		t.Error("expected nil for missing CTE")
	}
}

func TestCTEReferences_NoFrom(t *testing.T) {
	stmt := parseSelect(t, "WITH a AS (SELECT 1) SELECT 1")
	refs := compiler.CTEReferences(stmt)
	// With no FROM clause referencing 'a', refs should be empty
	if len(refs) != 0 {
		t.Errorf("expected 0 CTE refs, got %d", len(refs))
	}
}
