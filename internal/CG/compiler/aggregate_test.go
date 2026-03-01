package compiler_test

import (
	"testing"

	compiler "github.com/cyw0ng95/sqlvibe/internal/CG/compiler"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

func TestIsAggregateFunction_Known(t *testing.T) {
	for _, name := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT",
		"JSON_GROUP_ARRAY", "JSONB_GROUP_ARRAY", "JSON_GROUP_OBJECT", "JSONB_GROUP_OBJECT"} {
		if !compiler.IsAggregateFunction(name) {
			t.Errorf("expected true for %s", name)
		}
	}
}

func TestIsAggregateFunction_Unknown(t *testing.T) {
	for _, name := range []string{"COALESCE", "LENGTH", "UPPER", "ROW_NUMBER"} {
		if compiler.IsAggregateFunction(name) {
			t.Errorf("expected false for %s", name)
		}
	}
}

func TestExprHasAggregate_FuncCall(t *testing.T) {
	expr := &QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.ColumnRef{Name: "*"}}}
	if !compiler.ExprHasAggregate(expr) {
		t.Error("expected true for COUNT(*)")
	}
}

func TestExprHasAggregate_NoAgg(t *testing.T) {
	expr := &QP.ColumnRef{Name: "id"}
	if compiler.ExprHasAggregate(expr) {
		t.Error("expected false for column ref")
	}
}

func TestExprHasAggregate_Nested(t *testing.T) {
	inner := &QP.FuncCall{Name: "SUM", Args: []QP.Expr{&QP.ColumnRef{Name: "amount"}}}
	expr := &QP.BinaryExpr{Op: QP.TokenPlus, Left: inner, Right: &QP.Literal{Value: int64(1)}}
	if !compiler.ExprHasAggregate(expr) {
		t.Error("expected true for SUM embedded in binary expr")
	}
}

func TestExtractAggregates_CountStar(t *testing.T) {
	expr := &QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.ColumnRef{Name: "*"}}}
	aggInfo := &VM.AggregateInfo{}
	compiler.ExtractAggregates(expr, aggInfo)
	if len(aggInfo.Aggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(aggInfo.Aggregates))
	}
	if aggInfo.Aggregates[0].Function != "COUNT" {
		t.Errorf("unexpected function: %s", aggInfo.Aggregates[0].Function)
	}
}

func TestExtractAggregates_Dedup(t *testing.T) {
	expr1 := &QP.FuncCall{Name: "SUM", Args: []QP.Expr{&QP.ColumnRef{Name: "amount"}}}
	expr2 := &QP.FuncCall{Name: "SUM", Args: []QP.Expr{&QP.ColumnRef{Name: "amount"}}}
	combined := &QP.BinaryExpr{Op: QP.TokenPlus, Left: expr1, Right: expr2}
	aggInfo := &VM.AggregateInfo{}
	compiler.ExtractAggregates(combined, aggInfo)
	if len(aggInfo.Aggregates) != 1 {
		t.Errorf("expected dedup to 1, got %d", len(aggInfo.Aggregates))
	}
}

func TestCountAggregateFunctions(t *testing.T) {
	expr := &QP.FuncCall{Name: "AVG", Args: []QP.Expr{&QP.ColumnRef{Name: "score"}}}
	n := compiler.CountAggregateFunctions(expr)
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestExtractAggregates_Nil(t *testing.T) {
	aggInfo := &VM.AggregateInfo{}
	compiler.ExtractAggregates(nil, aggInfo)
	if len(aggInfo.Aggregates) != 0 {
		t.Error("expected no aggregates for nil expr")
	}
}
