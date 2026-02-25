package QP

import (
	"testing"
)

func TestParseCreateIndex(t *testing.T) {
	tests := []struct {
		sql         string
		name        string
		table       string
		columns     []string
		unique      bool
		ifNotExists bool
	}{
		{
			sql:     "CREATE INDEX idx_name ON users(name)",
			name:    "idx_name",
			table:   "users",
			columns: []string{"name"},
			unique:  false,
		},
		{
			sql:     "CREATE UNIQUE INDEX idx_email ON users(email)",
			name:    "idx_email",
			table:   "users",
			columns: []string{"email"},
			unique:  true,
		},
		{
			sql:     "CREATE INDEX idx_multi ON orders(user_id, product_id)",
			name:    "idx_multi",
			table:   "orders",
			columns: []string{"user_id", "product_id"},
			unique:  false,
		},
		{
			sql:         "CREATE INDEX IF NOT EXISTS idx_name ON users(name)",
			name:        "idx_name",
			table:       "users",
			columns:     []string{"name"},
			unique:      false,
			ifNotExists: true,
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
			stmt, ok := ast.(*CreateIndexStmt)
			if !ok {
				t.Fatalf("expected *CreateIndexStmt, got %T", ast)
			}
			if stmt.Name != tt.name {
				t.Errorf("name: got %q, want %q", stmt.Name, tt.name)
			}
			if stmt.Table != tt.table {
				t.Errorf("table: got %q, want %q", stmt.Table, tt.table)
			}
			if stmt.Unique != tt.unique {
				t.Errorf("unique: got %v, want %v", stmt.Unique, tt.unique)
			}
			if stmt.IfNotExists != tt.ifNotExists {
				t.Errorf("ifNotExists: got %v, want %v", stmt.IfNotExists, tt.ifNotExists)
			}
			if len(stmt.Columns) != len(tt.columns) {
				t.Errorf("columns: got %d, want %d", len(stmt.Columns), len(tt.columns))
			}
			for i, col := range stmt.Columns {
				if col != tt.columns[i] {
					t.Errorf("columns[%d]: got %q, want %q", i, col, tt.columns[i])
				}
			}
		})
	}
}

func TestParseDropIndex(t *testing.T) {
	sql := "DROP INDEX idx_name"
	tokenizer := NewTokenizer(sql)
	tokens, _ := tokenizer.Tokenize()
	parser := NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt, ok := ast.(*DropIndexStmt)
	if !ok {
		t.Fatalf("expected *DropIndexStmt, got %T", ast)
	}
	if stmt.Name != "idx_name" {
		t.Errorf("name: got %q, want %q", stmt.Name, "idx_name")
	}
}
