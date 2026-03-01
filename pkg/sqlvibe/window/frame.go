package window

import (
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// ResolveFrameBounds returns the inclusive [start, end] positions within a sorted
// partition for the given frame spec and current position.
// Default behaviour (frame == nil, with ORDER BY): RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
func ResolveFrameBounds(frame *QP.WindowFrame, pos, total int) (start, end int) {
	if total == 0 {
		return 0, 0
	}
	if frame == nil {
		return 0, pos
	}
	start = ResolveFramePos(frame.Start, pos, total, true)
	end = ResolveFramePos(frame.End, pos, total, false)
	if start < 0 {
		start = 0
	}
	if end >= total {
		end = total - 1
	}
	if start > end {
		start = end
	}
	return start, end
}

// ResolveFramePos resolves a FrameBound to an absolute position in the sorted partition.
// isStart indicates whether this is the start bound.
func ResolveFramePos(fb QP.FrameBound, pos, total int, isStart bool) int {
	switch fb.Type {
	case "UNBOUNDED":
		if isStart {
			return 0
		}
		return total - 1
	case "CURRENT":
		return pos
	case "PRECEDING":
		offset := FrameBoundOffset(fb.Value)
		return pos - offset
	case "FOLLOWING":
		offset := FrameBoundOffset(fb.Value)
		return pos + offset
	default:
		if isStart {
			return 0
		}
		return total - 1
	}
}

// FrameBoundOffset extracts the integer offset from a FrameBound value expression.
func FrameBoundOffset(expr QP.Expr) int {
	if expr == nil {
		return 0
	}
	if lit, ok := expr.(*QP.Literal); ok {
		switch v := lit.Value.(type) {
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return 0
}
