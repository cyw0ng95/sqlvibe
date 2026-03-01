package parser_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestParse_Expr_Arithmetic(t *testing.T) {
	ast := parseSQL(t, "SELECT 1 + 2 * 3")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.Columns) != 1 {
		t.Fatal("expected 1 column")
	}
	// Top-level should be a BinaryExpr (addition)
	if _, ok := stmt.Columns[0].(*QP.BinaryExpr); !ok {
		t.Errorf("expected BinaryExpr, got %T", stmt.Columns[0])
	}
}

func TestParse_Expr_Comparison(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE a > 5 AND b < 10")
	stmt := ast.(*QP.SelectStmt)
	if stmt.Where == nil {
		t.Fatal("WHERE should not be nil")
	}
	// AND expression at top level
	if _, ok := stmt.Where.(*QP.BinaryExpr); !ok {
		t.Errorf("expected BinaryExpr for AND, got %T", stmt.Where)
	}
}

func TestParse_Expr_Like(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE name LIKE 'A%'")
	stmt := ast.(*QP.SelectStmt)
	if stmt.Where == nil {
		t.Fatal("WHERE should not be nil")
	}
}

func TestParse_Expr_Between(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE age BETWEEN 18 AND 65")
	stmt := ast.(*QP.SelectStmt)
	if stmt.Where == nil {
		t.Fatal("WHERE should not be nil")
	}
}

func TestParse_Expr_Case(t *testing.T) {
	ast := parseSQL(t, "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t")
	stmt := ast.(*QP.SelectStmt)
	if _, ok := stmt.Columns[0].(*QP.CaseExpr); !ok {
		t.Errorf("expected CaseExpr, got %T", stmt.Columns[0])
	}
}

func TestParse_Expr_Subquery(t *testing.T) {
	ast := parseSQL(t, "SELECT (SELECT COUNT(*) FROM t) AS cnt")
	stmt := ast.(*QP.SelectStmt)
	alias, ok := stmt.Columns[0].(*QP.AliasExpr)
	if !ok {
		t.Fatalf("expected AliasExpr, got %T", stmt.Columns[0])
	}
	if _, ok := alias.Expr.(*QP.SubqueryExpr); !ok {
		t.Errorf("expected SubqueryExpr, got %T", alias.Expr)
	}
}

func TestParse_Expr_Aggregate(t *testing.T) {
	ast := parseSQL(t, "SELECT COUNT(*), SUM(val), AVG(val) FROM t")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(stmt.Columns))
	}
}

func TestParse_Expr_IsNull(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE x IS NULL")
	stmt := ast.(*QP.SelectStmt)
	if stmt.Where == nil {
		t.Fatal("WHERE should not be nil")
	}
}

func TestParse_Expr_WindowFunction(t *testing.T) {
	ast := parseSQL(t, "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t")
	stmt := ast.(*QP.SelectStmt)
	if len(stmt.Columns) != 1 {
		t.Fatal("expected 1 column")
	}
	if _, ok := stmt.Columns[0].(*QP.WindowFuncExpr); !ok {
		t.Errorf("expected WindowFuncExpr, got %T", stmt.Columns[0])
	}
}
