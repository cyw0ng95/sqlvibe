package sqlvibe

import (
	"fmt"
	"sort"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// windowFuncInfo tracks a window function column position and its definition.
type windowFuncInfo struct {
	colIndex int                 // position in the SELECT column list
	expr     *QP.WindowFuncExpr // the window function expression
	alias    string              // output column name
}

// extractWindowFunctions finds WindowFuncExpr in SELECT columns and replaces them
// with NULL literals for the base VM query, returning info for post-processing.
// It also adds any PARTITION BY / ORDER BY column references as extra hidden columns.
func extractWindowFunctions(stmt *QP.SelectStmt) ([]windowFuncInfo, int) {
	var funcs []windowFuncInfo
	for i, col := range stmt.Columns {
		switch e := col.(type) {
		case *QP.WindowFuncExpr:
			funcs = append(funcs, windowFuncInfo{colIndex: i, expr: e, alias: fmt.Sprintf("win_%d", i)})
			stmt.Columns[i] = &QP.Literal{Value: nil} // replace with NULL for VM
		case *QP.AliasExpr:
			if wf, ok := e.Expr.(*QP.WindowFuncExpr); ok {
				funcs = append(funcs, windowFuncInfo{colIndex: i, expr: wf, alias: e.Alias})
				stmt.Columns[i] = &QP.AliasExpr{Expr: &QP.Literal{Value: nil}, Alias: e.Alias}
			}
		}
	}
	if len(funcs) == 0 {
		return nil, 0
	}
	// Collect extra columns needed for PARTITION BY and ORDER BY expressions
	extraAdded := 0
	existingCols := make(map[string]bool)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			existingCols[cr.Name] = true
		} else if ae, ok := col.(*QP.AliasExpr); ok {
			if cr, ok2 := ae.Expr.(*QP.ColumnRef); ok2 {
				existingCols[cr.Name] = true
			}
		}
	}
	for _, wf := range funcs {
		for _, pExpr := range wf.expr.Partition {
			if cr, ok := pExpr.(*QP.ColumnRef); ok && !existingCols[cr.Name] {
				stmt.Columns = append(stmt.Columns, &QP.ColumnRef{Table: cr.Table, Name: cr.Name})
				existingCols[cr.Name] = true
				extraAdded++
			}
		}
		for _, ob := range wf.expr.OrderBy {
			if cr, ok := ob.Expr.(*QP.ColumnRef); ok && !existingCols[cr.Name] {
				stmt.Columns = append(stmt.Columns, &QP.ColumnRef{Table: cr.Table, Name: cr.Name})
				existingCols[cr.Name] = true
				extraAdded++
			}
		}
		// Add the first arg of LAG/LEAD/FIRST_VALUE/LAST_VALUE if it's a column not in select
		name := strings.ToUpper(wf.expr.Name)
		if name == "LAG" || name == "LEAD" || name == "FIRST_VALUE" || name == "LAST_VALUE" {
			if len(wf.expr.Args) > 0 {
				if cr, ok := wf.expr.Args[0].(*QP.ColumnRef); ok && !existingCols[cr.Name] {
					stmt.Columns = append(stmt.Columns, &QP.ColumnRef{Table: cr.Table, Name: cr.Name})
					existingCols[cr.Name] = true
					extraAdded++
				}
			}
		}
	}
	return funcs, extraAdded
}

// applyWindowFunctionsToRows computes window function values and injects them into rows.
// extraCols is the number of extra columns added to the end of the result for partition/order purposes.
func applyWindowFunctionsToRows(rows *Rows, funcs []windowFuncInfo, extraCols int) (*Rows, error) {
	if len(funcs) == 0 || rows == nil || len(rows.Data) == 0 {
		// Strip extra columns if any
		if extraCols > 0 && rows != nil {
			rows = stripExtraColumns(rows, extraCols)
		}
		return rows, nil
	}

	n := len(rows.Data)

	for _, wf := range funcs {
		colIdx := wf.colIndex
		if colIdx >= len(rows.Columns) {
			continue
		}

		name := strings.ToUpper(wf.expr.Name)

		// Compute a value for each row depending on the function
		var values []interface{}
		switch name {
		case "COUNT":
			// COUNT(*) OVER () = total rows
			// COUNT(*) OVER (PARTITION BY ...) = count in same partition
			if len(wf.expr.Partition) == 0 {
				// Total count: same value for all rows
				total := int64(n)
				values = make([]interface{}, n)
				for i := range values {
					values[i] = total
				}
			} else {
				// Partition count
				partitionCounts := computePartitionValues(rows, wf.expr, func(partRows []int, _ int) interface{} {
					return int64(len(partRows))
				})
				values = partitionCounts
			}

		case "SUM":
			// SUM(expr) OVER (PARTITION BY ...)
			values = computeWindowAgg(rows, wf.expr, func(rowIndices []int) interface{} {
				var sum float64
				hasVal := false
				for _, ri := range rowIndices {
					v := getRowColumnValue(rows, ri, wf.expr.Args)
					if v == nil {
						continue
					}
					f := toFloat64Window(v)
					sum += f
					hasVal = true
				}
				if !hasVal {
					return nil
				}
				return sum
			})

		case "AVG":
			values = computeWindowAgg(rows, wf.expr, func(rowIndices []int) interface{} {
				var sum float64
				count := 0
				for _, ri := range rowIndices {
					v := getRowColumnValue(rows, ri, wf.expr.Args)
					if v == nil {
						continue
					}
					sum += toFloat64Window(v)
					count++
				}
				if count == 0 {
					return nil
				}
				return sum / float64(count)
			})

		case "MIN":
			values = computeWindowAgg(rows, wf.expr, func(rowIndices []int) interface{} {
				var minVal interface{}
				for _, ri := range rowIndices {
					v := getRowColumnValue(rows, ri, wf.expr.Args)
					if v == nil {
						continue
					}
					if minVal == nil || compareWindowVals(v, minVal) < 0 {
						minVal = v
					}
				}
				return minVal
			})

		case "MAX":
			values = computeWindowAgg(rows, wf.expr, func(rowIndices []int) interface{} {
				var maxVal interface{}
				for _, ri := range rowIndices {
					v := getRowColumnValue(rows, ri, wf.expr.Args)
					if v == nil {
						continue
					}
					if maxVal == nil || compareWindowVals(v, maxVal) > 0 {
						maxVal = v
					}
				}
				return maxVal
			})

		case "ROW_NUMBER":
			// ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1-based rank within partition
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, posInPartition int) interface{} {
				return int64(posInPartition + 1)
			})

		case "RANK":
			values = computeRankValues(rows, wf.expr, false)

		case "DENSE_RANK":
			values = computeRankValues(rows, wf.expr, true)

		case "NTILE":
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, posInPartition int) interface{} {
				n := int64(1)
				if len(wf.expr.Args) > 0 {
					if lit, ok := wf.expr.Args[0].(*QP.Literal); ok {
						if iv, ok2 := lit.Value.(int64); ok2 && iv > 0 {
							n = iv
						}
					}
				}
				total := int64(len(sortedIndices))
				if total == 0 || n <= 0 {
					return int64(0)
				}
				bucket := (int64(posInPartition)*n)/total + 1
				return bucket
			})

		case "PERCENT_RANK":
			values = computeRankValuesFloat(rows, wf.expr)

		case "CUME_DIST":
			values = computeCumeDist(rows, wf.expr)

		case "LAG":
			offset := getLagLeadOffset(wf.expr)
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, posInPartition int) interface{} {
				prevPos := posInPartition - offset
				if prevPos < 0 || prevPos >= len(sortedIndices) {
					// Return default (3rd arg) or NULL
					if len(wf.expr.Args) >= 3 {
						return evalConstWindowArg(wf.expr.Args[2])
					}
					return nil
				}
				return getRowColumnValue(rows, sortedIndices[prevPos], wf.expr.Args[:1])
			})

		case "LEAD":
			offset := getLagLeadOffset(wf.expr)
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, posInPartition int) interface{} {
				nextPos := posInPartition + offset
				if nextPos < 0 || nextPos >= len(sortedIndices) {
					if len(wf.expr.Args) >= 3 {
						return evalConstWindowArg(wf.expr.Args[2])
					}
					return nil
				}
				return getRowColumnValue(rows, sortedIndices[nextPos], wf.expr.Args[:1])
			})

		case "FIRST_VALUE":
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, _ int) interface{} {
				if len(sortedIndices) == 0 {
					return nil
				}
				return getRowColumnValue(rows, sortedIndices[0], wf.expr.Args)
			})

		case "LAST_VALUE":
			// Default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			// So LAST_VALUE returns the value at the current position in the window
			values = computeOrderedWindowValues(rows, wf.expr, func(sortedIndices []int, posInPartition int) interface{} {
				if len(sortedIndices) == 0 {
					return nil
				}
				// Use current row (posInPartition) as the last row in the default frame
				return getRowColumnValue(rows, sortedIndices[posInPartition], wf.expr.Args)
			})

		default:
			// Unknown window function: fill with NULL
			values = make([]interface{}, n)
		}

		// Write values back to rows
		if values != nil {
			for i, row := range rows.Data {
				if colIdx < len(row) && i < len(values) {
					row[colIdx] = values[i]
				}
			}
		}
	}

	// Sort rows by partition key if any window function has PARTITION BY (matching SQLite behavior)
	// Do this BEFORE stripping extra columns so partition keys are accessible
	for _, wf := range funcs {
		if len(wf.expr.Partition) > 0 {
			origOrder := make([]int, len(rows.Data))
			for i := range origOrder {
				origOrder[i] = i
			}
			sort.SliceStable(origOrder, func(a, b int) bool {
				ra := makeRowMap(rows.Columns, rows.Data[origOrder[a]])
				rb := makeRowMap(rows.Columns, rows.Data[origOrder[b]])
				for _, pExpr := range wf.expr.Partition {
					va := evalWindowExprOnRow(ra, rows.Columns, pExpr)
					vb := evalWindowExprOnRow(rb, rows.Columns, pExpr)
					cmp := compareWindowVals(va, vb)
					if cmp != 0 {
						return cmp < 0
					}
				}
				return false
			})
			newData := make([][]interface{}, len(rows.Data))
			for i, origIdx := range origOrder {
				newData[i] = rows.Data[origIdx]
			}
			rows = &Rows{Columns: rows.Columns, Data: newData}
			break // only sort once
		}
	}

	// Strip extra columns that were added for PARTITION BY / ORDER BY
	if extraCols > 0 {
		rows = stripExtraColumns(rows, extraCols)
	}

	return rows, nil
}

// stripExtraColumns removes the last n columns from rows.
func stripExtraColumns(rows *Rows, n int) *Rows {
	if n <= 0 || rows == nil || len(rows.Columns) <= n {
		return rows
	}
	newCols := rows.Columns[:len(rows.Columns)-n]
	newData := make([][]interface{}, len(rows.Data))
	for i, row := range rows.Data {
		if len(row) > n {
			newData[i] = row[:len(row)-n]
		} else {
			newData[i] = row
		}
	}
	return &Rows{Columns: newCols, Data: newData}
}

// computePartitionValues computes a value for each row based on its partition.
func computePartitionValues(rows *Rows, wf *QP.WindowFuncExpr, compute func(partRows []int, rowIdx int) interface{}) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	for _, group := range partGroups {
		for _, ri := range group {
			result[ri] = compute(group, ri)
		}
	}
	return result
}

// computeWindowAgg computes a window aggregate for each row.
// If wf has a Frame spec, uses frame-based per-row computation; otherwise full-partition agg.
func computeWindowAgg(rows *Rows, wf *QP.WindowFuncExpr, agg func(rowIndices []int) interface{}) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	// If there are ORDER BY expressions or a frame spec, compute per-row frame values
	if len(wf.OrderBy) > 0 || wf.Frame != nil {
		for _, group := range partGroups {
			sortedGroup := sortRowIndices(rows, group, wf.OrderBy)
			total := len(sortedGroup)
			// Build reverse map: original row index → position in sorted group
			posMap := make(map[int]int, total)
			for pos, ri := range sortedGroup {
				posMap[ri] = pos
			}
			for _, ri := range group {
				pos := posMap[ri]
				start, end := resolveFrameBounds(wf.Frame, pos, total)
				frameIndices := sortedGroup[start : end+1]
				result[ri] = agg(frameIndices)
			}
		}
		return result
	}

	// No ORDER BY / frame: aggregate over full partition
	for _, group := range partGroups {
		val := agg(group)
		for _, ri := range group {
			result[ri] = val
		}
	}
	return result
}

// resolveFrameBounds returns the [start, end] (inclusive) positions within a sorted partition
// for the given frame spec and current position. Default frame when no spec:
//   - With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//   - Without ORDER BY (handled by caller): full partition
func resolveFrameBounds(frame *QP.WindowFrame, pos, total int) (start, end int) {
	if total == 0 {
		return 0, 0
	}
	if frame == nil {
		// Default with ORDER BY: from beginning to current row
		return 0, pos
	}
	start = resolveFramePos(frame.Start, pos, total, true)
	end = resolveFramePos(frame.End, pos, total, false)
	// Clamp
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

// resolveFramePos resolves a FrameBound to an absolute position.
// isStart indicates whether this is the start bound (for FOLLOWING, use pos+offset).
func resolveFramePos(fb QP.FrameBound, pos, total int, isStart bool) int {
	switch fb.Type {
	case "UNBOUNDED":
		if isStart {
			return 0
		}
		return total - 1
	case "CURRENT":
		return pos
	case "PRECEDING":
		offset := frameBoundOffset(fb.Value)
		return pos - offset
	case "FOLLOWING":
		offset := frameBoundOffset(fb.Value)
		return pos + offset
	default:
		if isStart {
			return 0
		}
		return total - 1
	}
}

// frameBoundOffset extracts the integer offset from a FrameBound value expression.
func frameBoundOffset(expr QP.Expr) int {
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

// computeOrderedWindowValues computes per-row values based on position within ordered partition.
// computeFn receives the sorted row indices for the partition and the position of the current row within it.
func computeOrderedWindowValues(rows *Rows, wf *QP.WindowFuncExpr, computeFn func(sortedIndices []int, posInPartition int) interface{}) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := sortRowIndices(rows, group, wf.OrderBy)
		// Build a map from original row index to position in sortedGroup
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

// computeRankValues computes RANK or DENSE_RANK window values.
func computeRankValues(rows *Rows, wf *QP.WindowFuncExpr, dense bool) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := sortRowIndices(rows, group, wf.OrderBy)
		rank := int64(1)
		denseRank := int64(1)
		for pos, ri := range sortedGroup {
			if pos > 0 {
				prevRi := sortedGroup[pos-1]
				if !sameOrderKey(rows, prevRi, ri, wf.OrderBy) {
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

func computeRankValuesFloat(rows *Rows, wf *QP.WindowFuncExpr) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := sortRowIndices(rows, group, wf.OrderBy)
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
				if !sameOrderKey(rows, prevRi, ri, wf.OrderBy) {
					rank = pos + 1
				}
			}
			result[ri] = float64(rank-1) / float64(total-1)
		}
	}
	return result
}

func computeCumeDist(rows *Rows, wf *QP.WindowFuncExpr) []interface{} {
	n := len(rows.Data)
	result := make([]interface{}, n)
	partGroups := buildPartitionGroups(rows, wf.Partition)

	for _, group := range partGroups {
		sortedGroup := sortRowIndices(rows, group, wf.OrderBy)
		total := len(sortedGroup)
		pos := 0
		for pos < len(sortedGroup) {
			end := pos + 1
			for end < len(sortedGroup) && sameOrderKey(rows, sortedGroup[pos], sortedGroup[end], wf.OrderBy) {
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

// buildPartitionGroups groups row indices by PARTITION BY key.
func buildPartitionGroups(rows *Rows, partExprs []QP.Expr) [][]int {
	if len(partExprs) == 0 {
		// No partition: single group with all rows
		all := make([]int, len(rows.Data))
		for i := range all {
			all[i] = i
		}
		return [][]int{all}
	}

	groupMap := make(map[string][]int)
	var groupOrder []string

	for i, rowData := range rows.Data {
		row := makeRowMap(rows.Columns, rowData)
		key := computeWindowKey(row, rows.Columns, partExprs)
		if _, exists := groupMap[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groupMap[key] = append(groupMap[key], i)
	}

	result := make([][]int, len(groupOrder))
	for i, key := range groupOrder {
		result[i] = groupMap[key]
	}
	return result
}

// sortRowIndices sorts a slice of row indices by the ORDER BY expressions.
func sortRowIndices(rows *Rows, indices []int, orderExprs []QP.WindowOrderBy) []int {
	if len(orderExprs) == 0 {
		return indices
	}
	sorted := make([]int, len(indices))
	copy(sorted, indices)
	sort.SliceStable(sorted, func(a, b int) bool {
		ra := makeRowMap(rows.Columns, rows.Data[sorted[a]])
		rb := makeRowMap(rows.Columns, rows.Data[sorted[b]])
		for _, ob := range orderExprs {
			va := evalWindowExprOnRow(ra, rows.Columns, ob.Expr)
			vb := evalWindowExprOnRow(rb, rows.Columns, ob.Expr)
			cmp := compareWindowVals(va, vb)
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	return sorted
}

// sameOrderKey returns true if two rows have the same ORDER BY key values.
func sameOrderKey(rows *Rows, ri, rj int, orderExprs []QP.WindowOrderBy) bool {
	ra := makeRowMap(rows.Columns, rows.Data[ri])
	rb := makeRowMap(rows.Columns, rows.Data[rj])
	for _, ob := range orderExprs {
		va := evalWindowExprOnRow(ra, rows.Columns, ob.Expr)
		vb := evalWindowExprOnRow(rb, rows.Columns, ob.Expr)
		if compareWindowVals(va, vb) != 0 {
			return false
		}
	}
	return true
}

// getRowColumnValue evaluates the first arg expression against a row, returning its value.
func getRowColumnValue(rows *Rows, rowIdx int, args []QP.Expr) interface{} {
	if len(args) == 0 || rowIdx < 0 || rowIdx >= len(rows.Data) {
		return nil
	}
	rowData := rows.Data[rowIdx]
	row := makeRowMap(rows.Columns, rowData)
	return evalWindowExprOnRow(row, rows.Columns, args[0])
}

// computeWindowKey computes a string key from partition expressions for a row.
func computeWindowKey(row map[string]interface{}, columns []string, exprs []QP.Expr) string {
	parts := make([]string, len(exprs))
	for i, expr := range exprs {
		v := evalWindowExprOnRow(row, columns, expr)
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// evalWindowExprOnRow evaluates a simple expression (ColumnRef or Literal) against a row.
func evalWindowExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) interface{} {
	switch e := expr.(type) {
	case *QP.ColumnRef:
		// Try qualified name first
		if e.Table != "" {
			if v, ok := row[e.Table+"."+e.Name]; ok {
				return v
			}
		}
		if v, ok := row[e.Name]; ok {
			return v
		}
		// Try case-insensitive
		lower := strings.ToLower(e.Name)
		for k, v := range row {
			if strings.ToLower(k) == lower {
				return v
			}
		}
		// Try by index in columns
		for i, col := range columns {
			if strings.ToLower(col) == lower {
				if rowData, ok := row["__data__"]; ok {
					if rd, ok2 := rowData.([]interface{}); ok2 && i < len(rd) {
						return rd[i]
					}
				}
				return row[col]
			}
		}
		return nil
	case *QP.Literal:
		return e.Value
	case *QP.AliasExpr:
		return evalWindowExprOnRow(row, columns, e.Expr)
	default:
		return nil
	}
}

// makeRowMap creates a map from column names to values for a row.
func makeRowMap(columns []string, rowData []interface{}) map[string]interface{} {
	row := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(rowData) {
			row[col] = rowData[i]
		}
	}
	return row
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

// evalConstWindowArg evaluates a constant expression for window function default.
func evalConstWindowArg(expr QP.Expr) interface{} {
	if lit, ok := expr.(*QP.Literal); ok {
		return lit.Value
	}
	return nil
}

// toFloat64Window converts a value to float64 for window aggregation.
func toFloat64Window(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	case string:
		var f float64
		fmt.Sscanf(n, "%f", &f)
		return f
	}
	return 0
}

// compareWindowVals compares two window values for ordering.
func compareWindowVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	// Try numeric comparison
	fa, aOk := toFloat64WindowOk(a)
	fb, bOk := toFloat64WindowOk(b)
	if aOk && bOk {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	// String comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func toFloat64WindowOk(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
