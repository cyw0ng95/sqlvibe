package QP

import (
	"strconv"
)

// PredicatePushdown analyses a WHERE expression and classifies sub-predicates
// into two groups:
//
//  1. "Pushable" conditions — simple comparisons of the form column OP constant
//     (e.g. id = 100, age > 25, name = 'Alice').  These can be evaluated at the
//     Go layer directly on row maps before the VM processes the row, reducing
//     the number of rows the VM must handle.
//
//  2. "Kept" conditions — everything else (subqueries, function calls, column
//     OP column, NOT, …).  These are left in the WHERE clause for the VM.
//
// Typical usage:
//
//	pushable, remaining := SplitPushdownPredicates(stmt.Where)
//	tableData = ApplyPushdownFilter(tableData, pushable)
//	stmt.Where = remaining

// SplitPushdownPredicates splits expr into (pushable, remaining).
// pushable is nil when no sub-expression can be pushed down.
// remaining is nil when all sub-expressions are pushable.
//
// For a top-level AND node each branch is recursively split; for any other
// node the whole expression is either pushable or kept as-is.
func SplitPushdownPredicates(expr Expr) (pushable []Expr, remaining Expr) {
	if expr == nil {
		return nil, nil
	}
	if bin, ok := expr.(*BinaryExpr); ok && bin.Op == TokenAnd {
		// AND: split both sides independently and recombine
		lPush, lRemain := SplitPushdownPredicates(bin.Left)
		rPush, rRemain := SplitPushdownPredicates(bin.Right)
		pushable = append(pushable, lPush...)
		pushable = append(pushable, rPush...)
		switch {
		case lRemain == nil && rRemain == nil:
			remaining = nil
		case lRemain == nil:
			remaining = rRemain
		case rRemain == nil:
			remaining = lRemain
		default:
			remaining = &BinaryExpr{Op: TokenAnd, Left: lRemain, Right: rRemain}
		}
		return pushable, remaining
	}
	if IsPushableExpr(expr) {
		return []Expr{expr}, nil
	}
	return nil, expr
}

// IsPushableExpr reports whether expr is a simple comparison that can be
// evaluated at the Go layer directly against a row map.
//
// A pushable expression has the form:
//
//	ColumnRef OP Literal   — e.g. age > 25
//	Literal   OP ColumnRef — e.g. 25 < age (operands flipped)
//
// where OP ∈ {=, !=, <, <=, >, >=}.
func IsPushableExpr(expr Expr) bool {
	bin, ok := expr.(*BinaryExpr)
	if !ok {
		return false
	}
	switch bin.Op {
	case TokenEq, TokenNe, TokenLt, TokenLe, TokenGt, TokenGe:
		_, lCol := bin.Left.(*ColumnRef)
		_, rLit := bin.Right.(*Literal)
		if lCol && rLit {
			return true
		}
		_, lLit := bin.Left.(*Literal)
		_, rCol := bin.Right.(*ColumnRef)
		return lLit && rCol
	case TokenBetween:
		// col BETWEEN low AND high — both bounds must be literals
		_, isCol := bin.Left.(*ColumnRef)
		if !isCol {
			return false
		}
		rangeBin, ok := bin.Right.(*BinaryExpr)
		if !ok || rangeBin.Op != TokenAnd {
			return false
		}
		_, loLit := rangeBin.Left.(*Literal)
		_, hiLit := rangeBin.Right.(*Literal)
		return loLit && hiLit
	}
	return false
}

// EvalPushdown evaluates a single pushable predicate against row.
// Returns true when row satisfies the predicate.
// Callers must only pass expressions where IsPushableExpr(expr) is true.
func EvalPushdown(expr Expr, row map[string]interface{}) bool {
	bin := expr.(*BinaryExpr)

	// Handle BETWEEN separately: col BETWEEN lo AND hi
	if bin.Op == TokenBetween {
		colRef := bin.Left.(*ColumnRef)
		rangeBin := bin.Right.(*BinaryExpr)
		lo := rangeBin.Left.(*Literal).Value
		hi := rangeBin.Right.(*Literal).Value
		val := row[colRef.Name]
		if val == nil {
			return false
		}
		return pdCompare(val, lo) >= 0 && pdCompare(val, hi) <= 0
	}

	var colRef *ColumnRef
	var lit *Literal
	flipped := false

	if c, ok := bin.Left.(*ColumnRef); ok {
		colRef = c
		lit = bin.Right.(*Literal)
	} else {
		lit = bin.Left.(*Literal)
		colRef = bin.Right.(*ColumnRef)
		flipped = true
	}

	val := row[colRef.Name]
	if val == nil {
		return false // NULL comparisons with = / < / > / etc. always false
	}

	cmp := pdCompare(val, lit.Value)

	op := bin.Op
	if flipped {
		// lit OP col → col (flipped_OP) lit
		switch op {
		case TokenLt:
			op = TokenGt
		case TokenLe:
			op = TokenGe
		case TokenGt:
			op = TokenLt
		case TokenGe:
			op = TokenLe
		}
	}
	switch op {
	case TokenEq:
		return cmp == 0
	case TokenNe:
		return cmp != 0
	case TokenLt:
		return cmp < 0
	case TokenLe:
		return cmp <= 0
	case TokenGt:
		return cmp > 0
	case TokenGe:
		return cmp >= 0
	}
	return false
}

// ApplyPushdownFilter returns the subset of rows that satisfy all pushable
// predicates.  When pushable is empty, the original slice is returned unchanged.
func ApplyPushdownFilter(rows []map[string]interface{}, pushable []Expr) []map[string]interface{} {
	if len(pushable) == 0 {
		return rows
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		keep := true
		for _, pred := range pushable {
			if !EvalPushdown(pred, row) {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, row)
		}
	}
	return out
}

// pdCompare compares row value a against literal value b, returning:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
//
// Numeric types (int64, float64, int) are compared numerically; strings
// lexicographically.  Mixed numeric/string pairs coerce to string for
// comparison.
func pdCompare(a, b interface{}) int {
	af, aNum := pdToFloat(a)
	bf, bNum := pdToFloat(b)
	if aNum && bNum {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	as, aStr := a.(string)
	bs, bStr := b.(string)
	if aStr && bStr {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}

	// Fallback: coerce both to string
	as2 := pdToStr(a)
	bs2 := pdToStr(b)
	if as2 < bs2 {
		return -1
	}
	if as2 > bs2 {
		return 1
	}
	return 0
}

func pdToFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	}
	return 0, false
}

func pdToStr(v interface{}) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case bool:
		if x {
			return "1"
		}
		return "0"
	}
	return ""
}

// IndexMetaQP mirrors DS.IndexMeta but in the QP package to avoid circular deps.
type IndexMetaQP struct {
Name      string
TableName string
Columns   []string
}

// CoversColumns returns true if all required columns are present in this index.
func (im *IndexMetaQP) CoversColumns(required []string) bool {
colSet := make(map[string]bool, len(im.Columns))
for _, c := range im.Columns {
colSet[c] = true
}
for _, r := range required {
if !colSet[r] {
return false
}
}
return true
}

// FindCoveringIndex returns the first index that covers all required columns, or nil.
func FindCoveringIndex(indexes []*IndexMetaQP, required []string) *IndexMetaQP {
for _, idx := range indexes {
if idx.CoversColumns(required) {
return idx
}
}
return nil
}

// SelectBestIndex picks the best index for a query given a filter column and
// required output columns.
func SelectBestIndex(indexes []*IndexMetaQP, filterCol string, required []string) *IndexMetaQP {
for _, idx := range indexes {
if len(idx.Columns) > 0 && idx.Columns[0] == filterCol && idx.CoversColumns(required) {
return idx
}
}
for _, idx := range indexes {
if len(idx.Columns) > 0 && idx.Columns[0] == filterCol {
return idx
}
}
return FindCoveringIndex(indexes, required)
}

// skipScanCardinalityRatio is the maximum leading-column cardinality relative
// to table row count for which a skip scan is considered cost-effective.
const skipScanCardinalityRatio = 10

// skipScanAbsoluteThreshold is the absolute leading-column cardinality below
// which a skip scan is always considered cost-effective regardless of table size.
const skipScanAbsoluteThreshold = 100

// CanSkipScan returns true when a skip scan on index is cost-effective.
func CanSkipScan(indexCols []string, filterCols []string, leadingCardinality, rowCount int) bool {
	if len(indexCols) <= len(filterCols) {
		return false
	}
	offset := len(indexCols) - len(filterCols)
	for i, fc := range filterCols {
		if indexCols[offset+i] != fc {
			return false
		}
	}
	return leadingCardinality < rowCount/skipScanCardinalityRatio || leadingCardinality < skipScanAbsoluteThreshold
}
