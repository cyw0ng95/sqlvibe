package parser_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestParse_Insert_Basic(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t VALUES (1, 'hello')")
	stmt, ok := ast.(*QP.InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", ast)
	}
	if stmt.Table != "t" {
		t.Errorf("table = %q, want t", stmt.Table)
	}
	if len(stmt.Values) != 1 {
		t.Fatalf("value rows = %d, want 1", len(stmt.Values))
	}
	if len(stmt.Values[0]) != 2 {
		t.Errorf("value cols = %d, want 2", len(stmt.Values[0]))
	}
}

func TestParse_Insert_MultiRow(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t VALUES (1), (2), (3)")
	stmt := ast.(*QP.InsertStmt)
	if len(stmt.Values) != 3 {
		t.Errorf("value rows = %d, want 3", len(stmt.Values))
	}
}

func TestParse_Insert_WithNamedColumns(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t (id, name) VALUES (1, 'a')")
	stmt := ast.(*QP.InsertStmt)
	if len(stmt.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(stmt.Columns))
	}
	if stmt.Columns[0] != "id" || stmt.Columns[1] != "name" {
		t.Errorf("columns = %v, want [id name]", stmt.Columns)
	}
}

func TestParse_Insert_OrReplace(t *testing.T) {
	ast := parseSQL(t, "INSERT OR REPLACE INTO t VALUES (1)")
	stmt := ast.(*QP.InsertStmt)
	if stmt.OrAction != "REPLACE" {
		t.Errorf("OrAction = %q, want REPLACE", stmt.OrAction)
	}
}

func TestParse_Insert_OrIgnore(t *testing.T) {
	ast := parseSQL(t, "INSERT OR IGNORE INTO t VALUES (1)")
	stmt := ast.(*QP.InsertStmt)
	if stmt.OrAction != "IGNORE" {
		t.Errorf("OrAction = %q, want IGNORE", stmt.OrAction)
	}
}

func TestParse_Insert_Select(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t SELECT id, name FROM src")
	stmt := ast.(*QP.InsertStmt)
	if stmt.SelectQuery == nil {
		t.Fatal("SelectQuery should not be nil for INSERT ... SELECT")
	}
}

func TestParse_Insert_Returning(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t VALUES (1) RETURNING id")
	stmt := ast.(*QP.InsertStmt)
	if len(stmt.Returning) == 0 {
		t.Error("RETURNING clause should not be empty")
	}
}

func TestParse_Insert_NullValue(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO t VALUES (NULL)")
	stmt := ast.(*QP.InsertStmt)
	if len(stmt.Values) != 1 || len(stmt.Values[0]) != 1 {
		t.Fatal("expected 1 row with 1 column")
	}
	lit, ok := stmt.Values[0][0].(*QP.Literal)
	if !ok || lit.Value != nil {
		t.Errorf("expected NULL literal, got %v", stmt.Values[0][0])
	}
}
