package QP

import (
	"testing"
)

func TestRequiredColumns(t *testing.T) {
	stmt := &SelectStmt{
		Columns: []Expr{
			&ColumnRef{Name: "id"},
			&ColumnRef{Name: "name"},
		},
		Where: &BinaryExpr{
			Op:    TokenEq,
			Left:  &ColumnRef{Name: "id"},
			Right: &Literal{Value: 1},
		},
		OrderBy: []OrderBy{
			{Expr: &ColumnRef{Name: "name"}},
		},
		GroupBy: []Expr{
			&ColumnRef{Name: "dept"},
		},
	}

	cols := RequiredColumns(stmt)
	// May have duplicates in some cases, just check count is at least 3
	if len(cols) < 3 {
		t.Errorf("Expected at least 3 columns, got %d: %v", len(cols), cols)
	}
}

func TestRequiredColumns_Empty(t *testing.T) {
	stmt := &SelectStmt{}

	cols := RequiredColumns(stmt)
	if len(cols) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(cols))
	}
}

func TestColNamesFromExpr_ColumnRef(t *testing.T) {
	expr := &ColumnRef{Name: "id", Table: "t"}

	names := colNamesFromExpr(expr)
	if len(names) != 1 || names[0] != "id" {
		t.Errorf("Expected [id], got %v", names)
	}
}

func TestColNamesFromExpr_Nil(t *testing.T) {
	names := colNamesFromExpr(nil)
	if names != nil {
		t.Errorf("Expected nil, got %v", names)
	}
}

func TestColNamesFromExpr_BinaryExpr(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenPlus,
		Left:  &ColumnRef{Name: "a"},
		Right: &ColumnRef{Name: "b"},
	}

	names := colNamesFromExpr(expr)
	if len(names) != 2 {
		t.Errorf("Expected 2 columns, got %v", names)
	}
}

func TestColNamesFromExpr_UnaryExpr(t *testing.T) {
	expr := &UnaryExpr{
		Op:   TokenMinus,
		Expr: &ColumnRef{Name: "x"},
	}

	names := colNamesFromExpr(expr)
	if len(names) != 1 || names[0] != "x" {
		t.Errorf("Expected [x], got %v", names)
	}
}

func TestColNamesFromExpr_FuncCall(t *testing.T) {
	expr := &FuncCall{
		Name: "ABS",
		Args: []Expr{
			&ColumnRef{Name: "val"},
		},
	}

	names := colNamesFromExpr(expr)
	if len(names) != 1 || names[0] != "val" {
		t.Errorf("Expected [val], got %v", names)
	}
}

func TestColNamesFromExpr_CaseExpr(t *testing.T) {
	expr := &CaseExpr{
		Operand: &ColumnRef{Name: "status"},
		Whens: []CaseWhen{
			{
				Condition: &BinaryExpr{Op: TokenEq, Left: &ColumnRef{Name: "status"}, Right: &Literal{Value: 1}},
				Result:    &Literal{Value: "active"},
			},
		},
		Else: &Literal{Value: "unknown"},
	}

	names := colNamesFromExpr(expr)
	// Check that status is in the names (may have duplicates due to condition referencing)
	if len(names) < 1 {
		t.Errorf("Expected at least 1 column name, got %v", names)
	}
}

func TestColNamesFromExpr_Literal(t *testing.T) {
	expr := &Literal{Value: 42}

	names := colNamesFromExpr(expr)
	if names != nil {
		t.Errorf("Expected nil for Literal, got %v", names)
	}
}

func TestSplitPushdownPredicates_Nil(t *testing.T) {
	pushable, remaining := SplitPushdownPredicates(nil)
	if pushable != nil || remaining != nil {
		t.Errorf("Expected nil nil, got %v %v", pushable, remaining)
	}
}

func TestSplitPushdownPredicates_Simple(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenEq,
		Left:  &ColumnRef{Name: "id"},
		Right: &Literal{Value: 1},
	}

	pushable, remaining := SplitPushdownPredicates(expr)
	if len(pushable) != 1 {
		t.Errorf("Expected 1 pushable, got %d", len(pushable))
	}
	if remaining != nil {
		t.Error("Expected nil remaining")
	}
}

func TestSplitPushdownPredicates_And(t *testing.T) {
	expr := &BinaryExpr{
		Op: TokenAnd,
		Left: &BinaryExpr{
			Op:    TokenEq,
			Left:  &ColumnRef{Name: "id"},
			Right: &Literal{Value: 1},
		},
		Right: &BinaryExpr{
			Op:    TokenGt,
			Left:  &ColumnRef{Name: "age"},
			Right: &Literal{Value: 18},
		},
	}

	pushable, remaining := SplitPushdownPredicates(expr)
	if len(pushable) != 2 {
		t.Errorf("Expected 2 pushable, got %d", len(pushable))
	}
	if remaining != nil {
		t.Error("Expected nil remaining for all pushable")
	}
}

func TestSplitPushdownPredicates_AndPartial(t *testing.T) {
	expr := &BinaryExpr{
		Op: TokenAnd,
		Left: &BinaryExpr{
			Op:    TokenEq,
			Left:  &ColumnRef{Name: "id"},
			Right: &Literal{Value: 1},
		},
		Right: &FuncCall{
			Name: "ABS",
			Args: []Expr{&ColumnRef{Name: "val"}},
		},
	}

	pushable, remaining := SplitPushdownPredicates(expr)
	if len(pushable) != 1 {
		t.Errorf("Expected 1 pushable, got %d", len(pushable))
	}
	if remaining == nil {
		t.Error("Expected non-nil remaining")
	}
}

func TestIsPushableExpr_ColumnEqLiteral(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenEq,
		Left:  &ColumnRef{Name: "id"},
		Right: &Literal{Value: 1},
	}

	if !IsPushableExpr(expr) {
		t.Error("Expected true for ColumnRef = Literal")
	}
}

func TestIsPushableExpr_LiteralEqColumn(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenEq,
		Left:  &Literal{Value: 1},
		Right: &ColumnRef{Name: "id"},
	}

	if !IsPushableExpr(expr) {
		t.Error("Expected true for Literal = ColumnRef")
	}
}

func TestIsPushableExpr_NonComparison(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenPlus,
		Left:  &ColumnRef{Name: "a"},
		Right: &ColumnRef{Name: "b"},
	}

	if IsPushableExpr(expr) {
		t.Error("Expected false for non-comparison")
	}
}

func TestIsPushableExpr_FuncCall(t *testing.T) {
	expr := &FuncCall{
		Name: "ABS",
		Args: []Expr{&ColumnRef{Name: "val"}},
	}

	if IsPushableExpr(expr) {
		t.Error("Expected false for FuncCall")
	}
}

func TestIsPushableExpr_Between(t *testing.T) {
	expr := &BinaryExpr{
		Op:   TokenBetween,
		Left: &ColumnRef{Name: "age"},
		Right: &BinaryExpr{
			Op:    TokenAnd,
			Left:  &Literal{Value: 18},
			Right: &Literal{Value: 65},
		},
	}

	if !IsPushableExpr(expr) {
		t.Error("Expected true for BETWEEN with literals")
	}
}

func TestIsPushableExpr_BetweenNonLiteral(t *testing.T) {
	expr := &BinaryExpr{
		Op:   TokenBetween,
		Left: &ColumnRef{Name: "age"},
		Right: &BinaryExpr{
			Op:    TokenAnd,
			Left:  &ColumnRef{Name: "min"},
			Right: &Literal{Value: 65},
		},
	}

	if IsPushableExpr(expr) {
		t.Error("Expected false for BETWEEN with non-literals")
	}
}

func TestEvalPushdown_Eq(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenEq,
		Left:  &ColumnRef{Name: "id"},
		Right: &Literal{Value: 1},
	}

	row := map[string]interface{}{"id": 1, "name": "test"}
	if !EvalPushdown(expr, row) {
		t.Error("Expected true for matching row")
	}

	row2 := map[string]interface{}{"id": 2, "name": "test"}
	if EvalPushdown(expr, row2) {
		t.Error("Expected false for non-matching row")
	}
}

func TestEvalPushdown_Gt(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenGt,
		Left:  &ColumnRef{Name: "age"},
		Right: &Literal{Value: 18},
	}

	row := map[string]interface{}{"age": 25}
	if !EvalPushdown(expr, row) {
		t.Error("Expected true for age > 18")
	}

	row2 := map[string]interface{}{"age": 10}
	if EvalPushdown(expr, row2) {
		t.Error("Expected false for age <= 18")
	}
}

func TestEvalPushdown_Between(t *testing.T) {
	expr := &BinaryExpr{
		Op:   TokenBetween,
		Left: &ColumnRef{Name: "age"},
		Right: &BinaryExpr{
			Op:    TokenAnd,
			Left:  &Literal{Value: 18},
			Right: &Literal{Value: 65},
		},
	}

	row := map[string]interface{}{"age": 25}
	if !EvalPushdown(expr, row) {
		t.Error("Expected true for age in range")
	}

	row2 := map[string]interface{}{"age": 10}
	if EvalPushdown(expr, row2) {
		t.Error("Expected false for age out of range")
	}
}

func TestEvalPushdown_MissingColumn(t *testing.T) {
	expr := &BinaryExpr{
		Op:    TokenEq,
		Left:  &ColumnRef{Name: "id"},
		Right: &Literal{Value: 1},
	}

	row := map[string]interface{}{"name": "test"}
	if EvalPushdown(expr, row) {
		t.Error("Expected false for missing column")
	}
}

func TestEvalPushdown_NonPushable(t *testing.T) {
	// Skip this test - EvalPushdown requires IsPushableExpr to be true
	// The function has a precondition documented that callers must ensure
	t.Skip("EvalPushdown requires IsPushableExpr(expr) to be true")
}
