package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func parseInsert(t *testing.T, sql string) *QP.InsertStmt {
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
	ins, ok := stmt.(*QP.InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmt)
	}
	return ins
}

func parseUpdate(t *testing.T, sql string) *QP.UpdateStmt {
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
	upd, ok := stmt.(*QP.UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", stmt)
	}
	return upd
}

func parseDelete(t *testing.T, sql string) *QP.DeleteStmt {
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
	del, ok := stmt.(*QP.DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", stmt)
	}
	return del
}

func TestInsertColumnNames_WithColumns(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t (a, b, c) VALUES (1, 2, 3)")
	names := compiler.InsertColumnNames(stmt)
	if len(names) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(names))
	}
	if names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Errorf("unexpected column names: %v", names)
	}
}

func TestInsertColumnNames_NoColumns(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t VALUES (1, 2, 3)")
	names := compiler.InsertColumnNames(stmt)
	if names != nil {
		t.Errorf("expected nil for no column list, got %v", names)
	}
}

func TestInsertValueRowCount_SingleRow(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t (a) VALUES (1)")
	if compiler.InsertValueRowCount(stmt) != 1 {
		t.Error("expected 1 value row")
	}
}

func TestInsertValueRowCount_MultiRow(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t (a) VALUES (1), (2), (3)")
	if compiler.InsertValueRowCount(stmt) != 3 {
		t.Error("expected 3 value rows")
	}
}

func TestIsInsertWithSelect(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t SELECT * FROM s")
	if !compiler.IsInsertWithSelect(stmt) {
		t.Error("expected true for INSERT...SELECT")
	}
}

func TestIsInsertWithSelect_False(t *testing.T) {
	stmt := parseInsert(t, "INSERT INTO t (a) VALUES (1)")
	if compiler.IsInsertWithSelect(stmt) {
		t.Error("expected false for INSERT VALUES")
	}
}

func TestUpdateColumnList(t *testing.T) {
	stmt := parseUpdate(t, "UPDATE t SET a = 1, b = 2 WHERE id = 5")
	cols := compiler.UpdateColumnList(stmt)
	if len(cols) != 2 {
		t.Fatalf("expected 2 updated columns, got %d", len(cols))
	}
	if cols[0] != "a" || cols[1] != "b" {
		t.Errorf("unexpected column list: %v", cols)
	}
}

func TestUpdateHasWhere_True(t *testing.T) {
	stmt := parseUpdate(t, "UPDATE t SET x = 1 WHERE id = 1")
	if !compiler.UpdateHasWhere(stmt) {
		t.Error("expected true")
	}
}

func TestDeleteHasWhere_False(t *testing.T) {
	stmt := parseDelete(t, "DELETE FROM t")
	if compiler.DeleteHasWhere(stmt) {
		t.Error("expected false for DELETE without WHERE")
	}
}
