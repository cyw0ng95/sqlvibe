package vm_test

import (
	"testing"

	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestLiteralToString_Int(t *testing.T) {
	s := svvm.LiteralToString(int64(42))
	if s != "42" {
		t.Errorf("expected '42', got %q", s)
	}
}

func TestLiteralToString_Float(t *testing.T) {
	s := svvm.LiteralToString(float64(3.14))
	if s != "3.14" {
		t.Errorf("expected '3.14', got %q", s)
	}
}

func TestLiteralToString_String(t *testing.T) {
	s := svvm.LiteralToString("hello")
	if s != "'hello'" {
		t.Errorf("expected \"'hello'\", got %q", s)
	}
}

func TestLiteralToString_StringEscape(t *testing.T) {
	s := svvm.LiteralToString("it's")
	if s != "'it''s'" {
		t.Errorf("expected \"'it''s'\", got %q", s)
	}
}

func TestLiteralToString_Bool_True(t *testing.T) {
	s := svvm.LiteralToString(true)
	if s != "1" {
		t.Errorf("expected '1', got %q", s)
	}
}

func TestLiteralToString_Bool_False(t *testing.T) {
	s := svvm.LiteralToString(false)
	if s != "0" {
		t.Errorf("expected '0', got %q", s)
	}
}

func TestLiteralToString_Nil(t *testing.T) {
	s := svvm.LiteralToString(nil)
	if s != "NULL" {
		t.Errorf("expected 'NULL', got %q", s)
	}
}

func TestLiteralToString_Bytes(t *testing.T) {
	s := svvm.LiteralToString([]byte{0xCA, 0xFE})
	if s != "X'cafe'" {
		t.Errorf("expected X'cafe', got %q", s)
	}
}
