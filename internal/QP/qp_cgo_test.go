package QP

import (
	"testing"
)

func TestTokenizeC_Basic(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = 1"
	tokens, err := TokenizeC(sql)
	if err != nil {
		t.Fatalf("TokenizeC failed: %v", err)
	}

	if len(tokens) == 0 {
		t.Fatal("TokenizeC returned no tokens")
	}

	// Check first token is SELECT
	if tokens[0].Type != TokenKeyword || tokens[0].Literal != "SELECT" {
		t.Errorf("Expected SELECT keyword, got %v", tokens[0])
	}
}

func TestTokenizeC_Empty(t *testing.T) {
	tokens, err := TokenizeC("")
	if err != nil {
		t.Fatalf("TokenizeC failed on empty input: %v", err)
	}

	if len(tokens) != 1 || tokens[0].Type != TokenEOF {
		t.Errorf("Expected single EOF token for empty input, got %v", tokens)
	}
}

func TestTokenizeC_MultipleStatements(t *testing.T) {
	sql := "SELECT 1; SELECT 2;"
	tokens, err := TokenizeC(sql)
	if err != nil {
		t.Fatalf("TokenizeC failed: %v", err)
	}

	// Should have tokens for both statements
	if len(tokens) < 6 {
		t.Errorf("Expected at least 6 tokens, got %d", len(tokens))
	}
}

func TestTokenizeC_Comments(t *testing.T) {
	sql := "-- comment\nSELECT 1"
	tokens, err := TokenizeC(sql)
	if err != nil {
		t.Fatalf("TokenizeC failed: %v", err)
	}

	// Should have comment token and SELECT
	if len(tokens) < 2 {
		t.Errorf("Expected at least 2 tokens, got %d", len(tokens))
	}
}

func TestTokenizeC_Strings(t *testing.T) {
	sql := "SELECT 'hello world'"
	tokens, err := TokenizeC(sql)
	if err != nil {
		t.Fatalf("TokenizeC failed: %v", err)
	}

	// Find string token
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenString && tok.Literal == "'hello world'" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected string token, got %v", tokens)
	}
}

func TestTokenizeC_Numbers(t *testing.T) {
	sql := "SELECT 123, 45.67"
	tokens, err := TokenizeC(sql)
	if err != nil {
		t.Fatalf("TokenizeC failed: %v", err)
	}

	// Find number tokens
	numCount := 0
	for _, tok := range tokens {
		if tok.Type == TokenNumber {
			numCount++
		}
	}
	if numCount != 2 {
		t.Errorf("Expected 2 number tokens, got %d", numCount)
	}
}

func TestTokenizeC_Keywords(t *testing.T) {
	tests := []struct {
		sql      string
		expected string
	}{
		{"SELECT 1", "SELECT"},
		{"INSERT INTO t VALUES (1)", "INSERT"},
		{"UPDATE t SET x = 1", "UPDATE"},
		{"DELETE FROM t", "DELETE"},
		{"CREATE TABLE t (x INT)", "CREATE"},
	}

	for _, tt := range tests {
		tokens, err := TokenizeC(tt.sql)
		if err != nil {
			t.Errorf("TokenizeC(%q) failed: %v", tt.sql, err)
			continue
		}

		if len(tokens) == 0 || tokens[0].Literal != tt.expected {
			t.Errorf("TokenizeC(%q) = %v, expected %s", tt.sql, tokens, tt.expected)
		}
	}
}

func BenchmarkTokenizeC(b *testing.B) {
	sql := "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100 ORDER BY o.total DESC LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TokenizeC(sql)
	}
}

func BenchmarkTokenizeC_Simple(b *testing.B) {
	sql := "SELECT 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TokenizeC(sql)
	}
}
