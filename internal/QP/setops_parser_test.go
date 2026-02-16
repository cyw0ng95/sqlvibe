package QP

import (
	"testing"
)

func TestParseSetOperations(t *testing.T) {
	tests := []struct {
		sql       string
		setOp     string
		setOpAll  bool
	}{
		{
			sql:   "SELECT a FROM t1 UNION SELECT a FROM t2",
			setOp: "UNION",
		},
		{
			sql:      "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
			setOp:    "UNION",
			setOpAll: true,
		},
		{
			sql:   "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
			setOp: "EXCEPT",
		},
		{
			sql:   "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
			setOp: "INTERSECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.sql)
			tokens, _ := tokenizer.Tokenize()
			parser := NewParser(tokens)
			ast, err := parser.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			stmt, ok := ast.(*SelectStmt)
			if !ok {
				t.Fatalf("expected *SelectStmt, got %T", ast)
			}
			if stmt.SetOp != tt.setOp {
				t.Errorf("SetOp: got %q, want %q", stmt.SetOp, tt.setOp)
			}
			if stmt.SetOpAll != tt.setOpAll {
				t.Errorf("SetOpAll: got %v, want %v", stmt.SetOpAll, tt.setOpAll)
			}
			if stmt.SetOpRight == nil {
				t.Error("SetOpRight is nil")
			}
		})
	}
}

func TestParseChainedSetOperations(t *testing.T) {
	sql := "SELECT a FROM t1 UNION SELECT a FROM t2 UNION SELECT a FROM t3"
	tokenizer := NewTokenizer(sql)
	tokens, _ := tokenizer.Tokenize()
	parser := NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", ast)
	}
	if stmt.SetOp != "UNION" {
		t.Errorf("first SetOp: got %q, want UNION", stmt.SetOp)
	}
	if stmt.SetOpRight == nil {
		t.Fatal("first SetOpRight is nil")
	}
	if stmt.SetOpRight.SetOp != "UNION" {
		t.Errorf("second SetOp: got %q, want UNION", stmt.SetOpRight.SetOp)
	}
}
