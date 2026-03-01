package window

import (
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// ComputePartitionValues computes a value for each row based on its partition.
// compute is called with the slice of row indices in the partition and the current row index.
func ComputePartitionValues(rs *RowSet, wf *QP.WindowFuncExpr, compute func(partRows []int, rowIdx int) interface{}) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)
	for _, group := range partGroups {
		for _, ri := range group {
			result[ri] = compute(group, ri)
		}
	}
	return result
}

// ComputeWindowAgg computes a window aggregate for each row.
// When wf has ORDER BY or a Frame spec, computes per-row frame values;
// otherwise aggregates over the full partition.
func ComputeWindowAgg(rs *RowSet, wf *QP.WindowFuncExpr, agg func(rowIndices []int) interface{}) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)

	if len(wf.OrderBy) > 0 || wf.Frame != nil {
		for _, group := range partGroups {
			sortedGroup := SortRowIndices(rs, group, wf.OrderBy)
			total := len(sortedGroup)
			posMap := make(map[int]int, total)
			for pos, ri := range sortedGroup {
				posMap[ri] = pos
			}
			for _, ri := range group {
				pos := posMap[ri]
				start, end := ResolveFrameBounds(wf.Frame, pos, total)
				frameIndices := sortedGroup[start : end+1]
				result[ri] = agg(frameIndices)
			}
		}
		return result
	}

	for _, group := range partGroups {
		val := agg(group)
		for _, ri := range group {
			result[ri] = val
		}
	}
	return result
}

// ComputeOrderedWindowValues computes per-row values based on position within an ordered partition.
// computeFn receives the sorted row indices and the position of the current row within it.
func ComputeOrderedWindowValues(rs *RowSet, wf *QP.WindowFuncExpr, computeFn func(sortedIndices []int, posInPartition int) interface{}) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := SortRowIndices(rs, group, wf.OrderBy)
		posMap := make(map[int]int, len(sortedGroup))
		for pos, ri := range sortedGroup {
			posMap[ri] = pos
		}
		for _, ri := range group {
			pos := posMap[ri]
			result[ri] = computeFn(sortedGroup, pos)
		}
	}
	return result
}

// ComputeRankValues computes RANK or DENSE_RANK for each row.
func ComputeRankValues(rs *RowSet, wf *QP.WindowFuncExpr, dense bool) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := SortRowIndices(rs, group, wf.OrderBy)
		rank := int64(1)
		denseRank := int64(1)
		for pos, ri := range sortedGroup {
			if pos > 0 {
				prevRi := sortedGroup[pos-1]
				if !SameOrderKey(rs, prevRi, ri, wf.OrderBy) {
					if dense {
						denseRank++
					} else {
						rank = int64(pos) + 1
					}
				}
			}
			if dense {
				result[ri] = denseRank
			} else {
				result[ri] = rank
			}
		}
	}
	return result
}

// ComputeRankFloat computes PERCENT_RANK for each row (returns float64 in [0,1]).
func ComputeRankFloat(rs *RowSet, wf *QP.WindowFuncExpr) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := SortRowIndices(rs, group, wf.OrderBy)
		total := len(sortedGroup)
		if total <= 1 {
			for _, ri := range sortedGroup {
				result[ri] = float64(0)
			}
			continue
		}
		rank := 1
		for pos, ri := range sortedGroup {
			if pos > 0 {
				prevRi := sortedGroup[pos-1]
				if !SameOrderKey(rs, prevRi, ri, wf.OrderBy) {
					rank = pos + 1
				}
			}
			result[ri] = float64(rank-1) / float64(total-1)
		}
	}
	return result
}

// ComputeCumeDist computes CUME_DIST for each row.
func ComputeCumeDist(rs *RowSet, wf *QP.WindowFuncExpr) []interface{} {
	n := len(rs.Data)
	result := make([]interface{}, n)
	partGroups := BuildPartitionGroups(rs, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := SortRowIndices(rs, group, wf.OrderBy)
		total := len(sortedGroup)
		pos := 0
		for pos < len(sortedGroup) {
			end := pos + 1
			for end < len(sortedGroup) && SameOrderKey(rs, sortedGroup[pos], sortedGroup[end], wf.OrderBy) {
				end++
			}
			cumeDist := float64(end) / float64(total)
			for i := pos; i < end; i++ {
				result[sortedGroup[i]] = cumeDist
			}
			pos = end
		}
	}
	return result
}

// getLagLeadOffset extracts the offset argument from LAG/LEAD (default 1).
func getLagLeadOffset(wf *QP.WindowFuncExpr) int {
	if len(wf.Args) >= 2 {
		if lit, ok := wf.Args[1].(*QP.Literal); ok {
			if n, ok := lit.Value.(int64); ok {
				return int(n)
			}
		}
	}
	return 1
}

// EvalConstArg evaluates a constant expression for a window function default value.
func EvalConstArg(expr QP.Expr) interface{} {
	if lit, ok := expr.(*QP.Literal); ok {
		return lit.Value
	}
	return nil
}

// ComputeLag computes LAG values for each row.
func ComputeLag(rs *RowSet, wf *QP.WindowFuncExpr) []interface{} {
	offset := getLagLeadOffset(wf)
	return ComputeOrderedWindowValues(rs, wf, func(sortedIndices []int, posInPartition int) interface{} {
		prevPos := posInPartition - offset
		if prevPos < 0 || prevPos >= len(sortedIndices) {
			if len(wf.Args) >= 3 {
				return EvalConstArg(wf.Args[2])
			}
			return nil
		}
		return GetArgVal(rs, sortedIndices[prevPos], wf.Args[:1])
	})
}

// ComputeLead computes LEAD values for each row.
func ComputeLead(rs *RowSet, wf *QP.WindowFuncExpr) []interface{} {
	offset := getLagLeadOffset(wf)
	return ComputeOrderedWindowValues(rs, wf, func(sortedIndices []int, posInPartition int) interface{} {
		nextPos := posInPartition + offset
		if nextPos < 0 || nextPos >= len(sortedIndices) {
			if len(wf.Args) >= 3 {
				return EvalConstArg(wf.Args[2])
			}
			return nil
		}
		return GetArgVal(rs, sortedIndices[nextPos], wf.Args[:1])
	})
}
