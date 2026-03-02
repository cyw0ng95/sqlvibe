package QP

import (
	"strings"
	"testing"
)

// TestCParser_ExprBasic tests basic expression parsing via ParseExpr.
func TestCParser_ExprBasic(t *testing.T) {
	node, errMsg := ParseExpr("1 + 2 * 3")
	if node == nil {
		t.Fatalf("ParseExpr returned nil: %s", errMsg)
	}
	if node.Type() != ASTExpr {
		t.Errorf("expected ASTExpr (%d), got %d", ASTExpr, node.Type())
	}
	where := node.Where()
	if !strings.Contains(where, "+") || !strings.Contains(where, "*") {
		t.Errorf("expected expression with + and *, got %q", where)
	}
}

// TestCParser_ExprComparison tests comparison expression parsing.
func TestCParser_ExprComparison(t *testing.T) {
	cases := []string{
		"a = 1",
		"x != 'foo'",
		"price >= 10.5",
		"count < 100",
	}
	for _, sql := range cases {
		node, errMsg := ParseExpr(sql)
		if node == nil {
			t.Errorf("ParseExpr(%q) returned nil: %s", sql, errMsg)
			continue
		}
		if node.Type() != ASTExpr {
			t.Errorf("ParseExpr(%q) type = %d; want ASTExpr (%d)", sql, node.Type(), ASTExpr)
		}
		if got := node.Where(); got == "" {
			t.Errorf("ParseExpr(%q) returned empty expression", sql)
		}
	}
}

// TestCParser_ExprLogical tests AND/OR/NOT expression parsing.
func TestCParser_ExprLogical(t *testing.T) {
	sql := "a > 1 AND b < 10"
	node, errMsg := ParseExpr(sql)
	if node == nil {
		t.Fatalf("ParseExpr(%q) returned nil: %s", sql, errMsg)
	}
	where := node.Where()
	if !strings.Contains(where, "AND") {
		t.Errorf("expected 'AND' in expression, got %q", where)
	}
}

// TestCParser_ExprFunction tests function call parsing.
func TestCParser_ExprFunction(t *testing.T) {
	sql := "LENGTH(name)"
	node, errMsg := ParseExpr(sql)
	if node == nil {
		t.Fatalf("ParseExpr(%q) returned nil: %s", sql, errMsg)
	}
	where := node.Where()
	if !strings.Contains(strings.ToUpper(where), "LENGTH") {
		t.Errorf("expected 'LENGTH' in expression, got %q", where)
	}
}

// TestCParser_ExprIsNull tests IS NULL and IS NOT NULL parsing.
func TestCParser_ExprIsNull(t *testing.T) {
	for _, sql := range []string{"x IS NULL", "y IS NOT NULL"} {
		node, errMsg := ParseExpr(sql)
		if node == nil {
			t.Errorf("ParseExpr(%q) returned nil: %s", sql, errMsg)
			continue
		}
		where := node.Where()
		if !strings.Contains(strings.ToUpper(where), "NULL") {
			t.Errorf("expected NULL in expr result for %q, got %q", sql, where)
		}
	}
}

// TestCParser_ExprLike tests LIKE expression parsing.
func TestCParser_ExprLike(t *testing.T) {
	sql := "name LIKE '%foo%'"
	node, errMsg := ParseExpr(sql)
	if node == nil {
		t.Fatalf("ParseExpr(%q) returned nil: %s", sql, errMsg)
	}
	where := node.Where()
	if !strings.Contains(strings.ToUpper(where), "LIKE") {
		t.Errorf("expected LIKE in expr result for %q, got %q", sql, where)
	}
}

// TestCParser_ExprBetween tests BETWEEN expression parsing.
func TestCParser_ExprBetween(t *testing.T) {
	sql := "age BETWEEN 18 AND 65"
	node, errMsg := ParseExpr(sql)
	if node == nil {
		t.Fatalf("ParseExpr(%q) returned nil: %s", sql, errMsg)
	}
	where := node.Where()
	if !strings.Contains(strings.ToUpper(where), "BETWEEN") {
		t.Errorf("expected BETWEEN in expr result for %q, got %q", sql, where)
	}
}
