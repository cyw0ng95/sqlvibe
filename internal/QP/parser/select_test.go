package parser_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestParse_Select_Basic(t *testing.T) {
	ast := parseSQL(t, "SELECT 1")
	stmt, ok := ast.(*QP.SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Errorf("columns = %d, want 1", len(stmt.Columns))
	}
}

func TestParse_Select_WithWhere(t *testing.T) {
	ast := parseSQL(t, "SELECT id, name FROM users WHERE id = 1")
	stmt := ast.(*QP.SelectStmt)
	if stmt.From == nil {
		t.Fatal("FROM should not be nil")
	}
	if stmt.From.Name != "users" {
		t.Errorf("table = %q, want users", stmt.From.Name)
	}
	if stmt.Where == nil {
		t.Fatal("WHERE should not be nil")
	}
}

func TestParse_Select_GroupBy(t *testing.T) {
	ast := parseSQL(t, "SELECT dept, COUNT(*) FROM emp GROUP BY dept")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.GroupBy) == 0 {
		t.Error("GroupBy should not be empty")
	}
}

func TestParse_Select_OrderBy(t *testing.T) {
	ast := parseSQL(t, "SELECT id FROM t ORDER BY id DESC")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.OrderBy) == 0 {
		t.Fatal("OrderBy should not be empty")
	}
	if !stmt.OrderBy[0].Desc {
		t.Error("first ORDER BY should be DESC")
	}
}

func TestParse_Select_Limit(t *testing.T) {
	ast := parseSQL(t, "SELECT id FROM t LIMIT 10 OFFSET 5")
	stmt := ast.(*QP.SelectStmt)
	if stmt.Limit == nil {
		t.Error("Limit should not be nil")
	}
	if stmt.Offset == nil {
		t.Error("Offset should not be nil")
	}
}

func TestParse_Select_Join(t *testing.T) {
	ast := parseSQL(t, "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id")
	stmt := ast.(*QP.SelectStmt)
	if stmt.From == nil || stmt.From.Join == nil {
		t.Fatal("expected JOIN in FROM clause")
	}
}

func TestParse_Select_Subquery(t *testing.T) {
	ast := parseSQL(t, "SELECT id FROM (SELECT id FROM users) AS sub")
	stmt := ast.(*QP.SelectStmt)
	if stmt.From == nil || stmt.From.Subquery == nil {
		t.Fatal("expected subquery in FROM clause")
	}
}

func TestParse_Select_CTE(t *testing.T) {
	ast := parseSQL(t, "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.CTEs) == 0 {
		t.Fatal("expected CTE clause")
	}
	if stmt.CTEs[0].Name != "cte" {
		t.Errorf("CTE name = %q, want cte", stmt.CTEs[0].Name)
	}
}
