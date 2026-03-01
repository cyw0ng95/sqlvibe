package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestHasSubquery_True(t *testing.T) {
	subq := &QP.SubqueryExpr{Select: &QP.SelectStmt{}}
	expr := &QP.BinaryExpr{Op: QP.TokenEq, Left: &QP.ColumnRef{Name: "id"}, Right: subq}
	if !compiler.HasSubquery(expr) {
		t.Error("expected true for expr containing SubqueryExpr")
	}
}

func TestHasSubquery_False(t *testing.T) {
	expr := &QP.BinaryExpr{Op: QP.TokenEq, Left: &QP.ColumnRef{Name: "id"}, Right: &QP.Literal{Value: int64(1)}}
	if compiler.HasSubquery(expr) {
		t.Error("expected false for plain binary expr")
	}
}

func TestHasSubquery_Nil(t *testing.T) {
	if compiler.HasSubquery(nil) {
		t.Error("expected false for nil expr")
	}
}

func TestHasWhereSubquery_True(t *testing.T) {
	subq := &QP.SubqueryExpr{Select: &QP.SelectStmt{}}
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		Where:   &QP.BinaryExpr{Op: QP.TokenEq, Left: &QP.ColumnRef{Name: "id"}, Right: subq},
	}
	if !compiler.HasWhereSubquery(stmt) {
		t.Error("expected true for WHERE with subquery")
	}
}

func TestHasWhereSubquery_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id FROM t WHERE id = 1")
	if compiler.HasWhereSubquery(stmt) {
		t.Error("expected false for plain WHERE")
	}
}

func TestHasColumnSubquery_True(t *testing.T) {
	subq := &QP.SubqueryExpr{Select: &QP.SelectStmt{}}
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.AliasExpr{Expr: subq, Alias: "sub"}},
	}
	if !compiler.HasColumnSubquery(stmt) {
		t.Error("expected true for SELECT column containing subquery")
	}
}

func TestHasColumnSubquery_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id, name FROM t")
	if compiler.HasColumnSubquery(stmt) {
		t.Error("expected false for plain SELECT")
	}
}

func TestExtractSubqueries(t *testing.T) {
	subq1 := &QP.SubqueryExpr{Select: &QP.SelectStmt{}}
	subq2 := &QP.SubqueryExpr{Select: &QP.SelectStmt{}}
	expr := &QP.BinaryExpr{
		Op:    QP.TokenAnd,
		Left:  &QP.BinaryExpr{Op: QP.TokenEq, Left: &QP.ColumnRef{Name: "a"}, Right: subq1},
		Right: &QP.BinaryExpr{Op: QP.TokenEq, Left: &QP.ColumnRef{Name: "b"}, Right: subq2},
	}
	subs := compiler.ExtractSubqueries(expr)
	if len(subs) != 2 {
		t.Fatalf("expected 2 subqueries, got %d", len(subs))
	}
}
