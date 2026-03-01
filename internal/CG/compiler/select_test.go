package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func parseSelect(t *testing.T, sql string) *QP.SelectStmt {
	t.Helper()
	tok := QP.NewTokenizer(sql)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize %q: %v", sql, err)
	}
	p := QP.NewParser(tokens)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel, ok := stmt.(*QP.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	return sel
}

func TestHasAggregates_NoAgg(t *testing.T) {
	stmt := parseSelect(t, "SELECT id, name FROM users")
	if compiler.HasAggregates(stmt) {
		t.Error("expected false for plain SELECT")
	}
}

func TestHasAggregates_WithCount(t *testing.T) {
	stmt := parseSelect(t, "SELECT COUNT(*) FROM users")
	if !compiler.HasAggregates(stmt) {
		t.Error("expected true for COUNT(*)")
	}
}

func TestHasAggregates_WithGroupBy(t *testing.T) {
	stmt := parseSelect(t, "SELECT dept, COUNT(*) FROM users GROUP BY dept")
	if !compiler.HasAggregates(stmt) {
		t.Error("expected true for GROUP BY")
	}
}

func TestHasAggregates_Nil(t *testing.T) {
	if compiler.HasAggregates(nil) {
		t.Error("expected false for nil stmt")
	}
}

func TestShouldUseColumnar_AggNoJoin(t *testing.T) {
	stmt := parseSelect(t, "SELECT SUM(amount) FROM orders")
	if !compiler.ShouldUseColumnar(stmt) {
		t.Error("expected true for aggregate without join")
	}
}

func TestShouldUseColumnar_Join(t *testing.T) {
	stmt := parseSelect(t, "SELECT a.id FROM a JOIN b ON a.id = b.id")
	if compiler.ShouldUseColumnar(stmt) {
		t.Error("expected false when join is present")
	}
}

func TestShouldUseColumnar_FullScan(t *testing.T) {
	stmt := parseSelect(t, "SELECT * FROM users")
	if !compiler.ShouldUseColumnar(stmt) {
		t.Error("expected true for full scan without WHERE")
	}
}

func TestIsStarSelect_True(t *testing.T) {
	stmt := parseSelect(t, "SELECT * FROM users")
	if !compiler.IsStarSelect(stmt) {
		t.Error("expected true for SELECT *")
	}
}

func TestIsStarSelect_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id, name FROM users")
	if compiler.IsStarSelect(stmt) {
		t.Error("expected false for multi-column SELECT")
	}
}

func TestGetSelectColumnNames_Aliases(t *testing.T) {
	stmt := parseSelect(t, "SELECT id AS user_id, name AS user_name FROM users")
	names := compiler.GetSelectColumnNames(stmt)
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
	if names[0] != "user_id" {
		t.Errorf("expected user_id, got %s", names[0])
	}
	if names[1] != "user_name" {
		t.Errorf("expected user_name, got %s", names[1])
	}
}

func TestHasWindowFunctions_NoWindows(t *testing.T) {
	stmt := parseSelect(t, "SELECT id, name FROM users")
	if compiler.HasWindowFunctions(stmt) {
		t.Error("expected false for plain SELECT")
	}
}

func TestHasJoin_True(t *testing.T) {
	stmt := parseSelect(t, "SELECT a.id FROM a JOIN b ON a.id = b.id")
	if !compiler.HasJoin(stmt) {
		t.Error("expected true for JOIN")
	}
}

func TestHasJoin_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id FROM users")
	if compiler.HasJoin(stmt) {
		t.Error("expected false without JOIN")
	}
}
