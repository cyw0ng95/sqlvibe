package compiler

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// aggregateFuncs is the set of recognised SQL aggregate function names (upper-case).
var aggregateFuncs = map[string]bool{
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	"TOTAL": true, "GROUP_CONCAT": true,
	"JSON_GROUP_ARRAY": true, "JSONB_GROUP_ARRAY": true,
	"JSON_GROUP_OBJECT": true, "JSONB_GROUP_OBJECT": true,
	"ANY_VALUE": true, "MODE": true,
}

// IsAggregateFunction reports whether name is a known aggregate function.
func IsAggregateFunction(name string) bool {
	return aggregateFuncs[strings.ToUpper(name)]
}

// ExprHasAggregate recursively checks if an expression contains an aggregate function.
func ExprHasAggregate(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.FuncCall:
		if IsAggregateFunction(e.Name) {
			return true
		}
	case *QP.BinaryExpr:
		return ExprHasAggregate(e.Left) || ExprHasAggregate(e.Right)
	case *QP.UnaryExpr:
		return ExprHasAggregate(e.Expr)
	case *QP.AliasExpr:
		return ExprHasAggregate(e.Expr)
	case *QP.CastExpr:
		return ExprHasAggregate(e.Expr)
	case *QP.CaseExpr:
		if ExprHasAggregate(e.Operand) {
			return true
		}
		for _, when := range e.Whens {
			if ExprHasAggregate(when.Condition) || ExprHasAggregate(when.Result) {
				return true
			}
		}
		return ExprHasAggregate(e.Else)
	}
	return false
}

// ExtractAggregates extracts all aggregate function calls from expr into aggInfo.
// Duplicate aggregates (same function + same args) are skipped.
func ExtractAggregates(expr QP.Expr, aggInfo *VM.AggregateInfo) {
	if expr == nil || aggInfo == nil {
		return
	}
	switch e := expr.(type) {
	case *QP.FuncCall:
		upper := strings.ToUpper(e.Name)
		if !aggregateFuncs[upper] {
			return
		}
		argKey := argsKey(e.Args)
		for _, existing := range aggInfo.Aggregates {
			if existing.Function == upper && argsKey(existing.Args) == argKey {
				return
			}
		}
		aggInfo.Aggregates = append(aggInfo.Aggregates, VM.AggregateDef{
			Function: upper,
			Args:     e.Args,
			Distinct: e.Distinct,
		})
	case *QP.BinaryExpr:
		ExtractAggregates(e.Left, aggInfo)
		ExtractAggregates(e.Right, aggInfo)
	case *QP.UnaryExpr:
		ExtractAggregates(e.Expr, aggInfo)
	case *QP.AliasExpr:
		ExtractAggregates(e.Expr, aggInfo)
	case *QP.CaseExpr:
		ExtractAggregates(e.Operand, aggInfo)
		for _, when := range e.Whens {
			ExtractAggregates(when.Condition, aggInfo)
			ExtractAggregates(when.Result, aggInfo)
		}
		ExtractAggregates(e.Else, aggInfo)
	case *QP.CastExpr:
		ExtractAggregates(e.Expr, aggInfo)
	}
}

// argsKey returns a string key for a slice of expressions (used for deduplication).
func argsKey(args []QP.Expr) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, len(args))
	for i, a := range args {
		if a == nil {
			parts[i] = "<nil>"
		} else {
			parts[i] = fmt.Sprintf("%T", a)
		}
	}
	return strings.Join(parts, ",")
}

// CountAggregateFunctions counts the number of distinct aggregate function calls in expr.
func CountAggregateFunctions(expr QP.Expr) int {
	if expr == nil {
		return 0
	}
	aggInfo := &VM.AggregateInfo{}
	ExtractAggregates(expr, aggInfo)
	return len(aggInfo.Aggregates)
}
