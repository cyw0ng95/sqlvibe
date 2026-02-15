package QP

import (
	"testing"
)

func TestTokenizerSelect(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	tok := NewTokenizer(input)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	if len(tokens) < 4 {
		t.Errorf("expected at least 4 tokens, got %d", len(tokens))
	}

	if tokens[0].Type != TokenKeyword || tokens[0].Literal != "SELECT" {
		t.Errorf("expected SELECT keyword, got %v", tokens[0])
	}
}

func TestTokenizerInsert(t *testing.T) {
	input := "INSERT INTO users (name, age) VALUES ('John', 25)"
	tok := NewTokenizer(input)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	if tokens[0].Type != TokenKeyword || tokens[0].Literal != "INSERT" {
		t.Errorf("expected INSERT keyword, got %v", tokens[0])
	}
}

func TestTokenizerString(t *testing.T) {
	input := "SELECT 'hello world'"
	tok := NewTokenizer(input)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	found := false
	for _, tok := range tokens {
		if tok.Type == TokenString && tok.Literal == "hello world" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected string token 'hello world'")
	}
}

func TestTokenizerNumber(t *testing.T) {
	input := "SELECT 123, 45.67"
	tok := NewTokenizer(input)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNumber && tok.Literal == "123" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected number token '123'")
	}
}

func TestTokenizerOperators(t *testing.T) {
	input := "SELECT * FROM t WHERE a <> b AND c <= d"
	tok := NewTokenizer(input)
	_, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	t.Logf("tokens: %s", tok.String())
}

func TestTokenizerComment(t *testing.T) {
	input := "SELECT 1 -- this is a comment"
	tok := NewTokenizer(input)
	result, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize failed: %v", err)
	}

	for _, tok := range result {
		if tok.Type == TokenNumber && tok.Literal == "1" {
			return
		}
	}
	t.Error("expected number token before comment")
}
