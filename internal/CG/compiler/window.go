package compiler

import (
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// windowFuncs is the set of recognised window function names (upper-case).
var windowFuncs = map[string]bool{
	"ROW_NUMBER": true, "RANK": true, "DENSE_RANK": true,
	"PERCENT_RANK": true, "CUME_DIST": true, "NTILE": true,
	"LAG": true, "LEAD": true,
	"FIRST_VALUE": true, "LAST_VALUE": true, "NTH_VALUE": true,
	// Aggregate functions also used as window functions
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
}

// IsWindowFunction reports whether name is a known window function.
func IsWindowFunction(name string) bool {
	return windowFuncs[strings.ToUpper(name)]
}

// ExtractWindowFunctions collects all WindowFuncExpr nodes from stmt.Columns.
func ExtractWindowFunctions(stmt *QP.SelectStmt) []*QP.WindowFuncExpr {
	if stmt == nil {
		return nil
	}
	var out []*QP.WindowFuncExpr
	for _, col := range stmt.Columns {
		collectWindowFuncs(col, &out)
	}
	return out
}

// FrameSpecString converts a WindowFrame to its SQL string representation.
// Returns an empty string when frame is nil.
func FrameSpecString(frame *QP.WindowFrame) string {
	if frame == nil {
		return ""
	}
	return frame.Type + " BETWEEN " + boundStr(frame.Start) + " AND " + boundStr(frame.End)
}

// HasNamedWindows reports whether stmt defines any WINDOW clause windows.
func HasNamedWindows(stmt *QP.SelectStmt) bool {
	return stmt != nil && len(stmt.Windows) > 0
}

// collectWindowFuncs recursively collects WindowFuncExpr nodes into out.
func collectWindowFuncs(expr QP.Expr, out *[]*QP.WindowFuncExpr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *QP.WindowFuncExpr:
		*out = append(*out, e)
	case *QP.BinaryExpr:
		collectWindowFuncs(e.Left, out)
		collectWindowFuncs(e.Right, out)
	case *QP.UnaryExpr:
		collectWindowFuncs(e.Expr, out)
	case *QP.AliasExpr:
		collectWindowFuncs(e.Expr, out)
	case *QP.CastExpr:
		collectWindowFuncs(e.Expr, out)
	case *QP.CaseExpr:
		collectWindowFuncs(e.Operand, out)
		for _, when := range e.Whens {
			collectWindowFuncs(when.Condition, out)
			collectWindowFuncs(when.Result, out)
		}
		collectWindowFuncs(e.Else, out)
	}
}

// boundStr converts a FrameBound to its SQL text.
func boundStr(b QP.FrameBound) string {
	switch b.Type {
	case "UNBOUNDED":
		return "UNBOUNDED PRECEDING"
	case "CURRENT":
		return "CURRENT ROW"
	case "PRECEDING":
		if b.Value == nil {
			return "UNBOUNDED PRECEDING"
		}
		return "N PRECEDING"
	case "FOLLOWING":
		if b.Value == nil {
			return "UNBOUNDED FOLLOWING"
		}
		return "N FOLLOWING"
	default:
		return b.Type
	}
}
