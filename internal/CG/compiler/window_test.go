package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestIsWindowFunction_Known(t *testing.T) {
	for _, name := range []string{"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
		"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "COUNT", "SUM", "AVG"} {
		if !compiler.IsWindowFunction(name) {
			t.Errorf("expected true for %s", name)
		}
	}
}

func TestIsWindowFunction_Unknown(t *testing.T) {
	for _, name := range []string{"COALESCE", "LENGTH", "UPPER", "REPLACE"} {
		if compiler.IsWindowFunction(name) {
			t.Errorf("expected false for %s", name)
		}
	}
}

func TestExtractWindowFunctions_NoWindows(t *testing.T) {
	stmt := parseSelect(t, "SELECT id, name FROM t")
	wfns := compiler.ExtractWindowFunctions(stmt)
	if len(wfns) != 0 {
		t.Errorf("expected no window funcs, got %d", len(wfns))
	}
}

func TestHasNamedWindows_False(t *testing.T) {
	stmt := parseSelect(t, "SELECT id FROM t")
	if compiler.HasNamedWindows(stmt) {
		t.Error("expected false for stmt with no WINDOW clause")
	}
}

func TestHasNamedWindows_NilStmt(t *testing.T) {
	if compiler.HasNamedWindows(nil) {
		t.Error("expected false for nil stmt")
	}
}

func TestFrameSpecString_Nil(t *testing.T) {
	s := compiler.FrameSpecString(nil)
	if s != "" {
		t.Errorf("expected empty string for nil frame, got %q", s)
	}
}

func TestFrameSpecString_RowsBetween(t *testing.T) {
	frame := &QP.WindowFrame{
		Type:  "ROWS",
		Start: QP.FrameBound{Type: "UNBOUNDED"},
		End:   QP.FrameBound{Type: "CURRENT"},
	}
	s := compiler.FrameSpecString(frame)
	if s == "" {
		t.Error("expected non-empty frame string")
	}
	// Should contain the frame type
	if len(s) < 4 {
		t.Errorf("frame string too short: %q", s)
	}
}

func TestExtractWindowFunctions_WithWindowFuncExpr(t *testing.T) {
	wfe := &QP.WindowFuncExpr{Name: "ROW_NUMBER"}
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.AliasExpr{Expr: wfe, Alias: "rn"},
		},
	}
	wfns := compiler.ExtractWindowFunctions(stmt)
	if len(wfns) != 1 {
		t.Fatalf("expected 1 window func, got %d", len(wfns))
	}
	if wfns[0].Name != "ROW_NUMBER" {
		t.Errorf("unexpected name: %s", wfns[0].Name)
	}
}
