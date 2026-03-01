package sqlvibe

import (
	"fmt"
	"sort"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	svwin "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/window"
)

// windowFuncInfo tracks a window function column position and its definition.
type windowFuncInfo struct {
	colIndex int                // position in the SELECT column list
	expr     *QP.WindowFuncExpr // the window function expression
	alias    string             // output column name
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
			rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
			values = svwin.ComputeLag(rs, wf.expr)

		case "LEAD":
			rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
			values = svwin.ComputeLead(rs, wf.expr)

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
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputePartitionValues(rs, wf, compute)
}

// computeWindowAgg computes a window aggregate for each row.
// Delegates to the window subpackage.
func computeWindowAgg(rows *Rows, wf *QP.WindowFuncExpr, agg func(rowIndices []int) interface{}) []interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputeWindowAgg(rs, wf, agg)
}

// resolveFrameBounds returns the [start, end] (inclusive) positions within a sorted partition.
// Delegates to the window subpackage.
func resolveFrameBounds(frame *QP.WindowFrame, pos, total int) (start, end int) {
	return svwin.ResolveFrameBounds(frame, pos, total)
}

// resolveFramePos resolves a FrameBound to an absolute position.
// Delegates to the window subpackage.
func resolveFramePos(fb QP.FrameBound, pos, total int, isStart bool) int {
	return svwin.ResolveFramePos(fb, pos, total, isStart)
}

// frameBoundOffset extracts the integer offset from a FrameBound value expression.
// Delegates to the window subpackage.
func frameBoundOffset(expr QP.Expr) int {
	return svwin.FrameBoundOffset(expr)
}

// computeOrderedWindowValues computes per-row values based on position within ordered partition.
// Delegates to the window subpackage.
func computeOrderedWindowValues(rows *Rows, wf *QP.WindowFuncExpr, computeFn func(sortedIndices []int, posInPartition int) interface{}) []interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputeOrderedWindowValues(rs, wf, computeFn)
}

// computeRankValues computes RANK or DENSE_RANK window values.
// Delegates to the window subpackage.
func computeRankValues(rows *Rows, wf *QP.WindowFuncExpr, dense bool) []interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputeRankValues(rs, wf, dense)
}

func computeRankValuesFloat(rows *Rows, wf *QP.WindowFuncExpr) []interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputeRankFloat(rs, wf)
}

func computeCumeDist(rows *Rows, wf *QP.WindowFuncExpr) []interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.ComputeCumeDist(rs, wf)
}

// buildPartitionGroups groups row indices by PARTITION BY key.
// Delegates to the window subpackage.
func buildPartitionGroups(rows *Rows, partExprs []QP.Expr) [][]int {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.BuildPartitionGroups(rs, partExprs)
}

// sortRowIndices sorts a slice of row indices by the ORDER BY expressions.
// Delegates to the window subpackage.
func sortRowIndices(rows *Rows, indices []int, orderExprs []QP.WindowOrderBy) []int {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.SortRowIndices(rs, indices, orderExprs)
}

// sameOrderKey returns true if two rows have the same ORDER BY key values.
// Delegates to the window subpackage.
func sameOrderKey(rows *Rows, ri, rj int, orderExprs []QP.WindowOrderBy) bool {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.SameOrderKey(rs, ri, rj, orderExprs)
}

// getRowColumnValue evaluates the first arg expression against a row.
// Delegates to the window subpackage.
func getRowColumnValue(rows *Rows, rowIdx int, args []QP.Expr) interface{} {
	rs := &svwin.RowSet{Columns: rows.Columns, Data: rows.Data}
	return svwin.GetArgVal(rs, rowIdx, args)
}

// computeWindowKey computes a string key from partition expressions for a row.
func computeWindowKey(row map[string]interface{}, columns []string, exprs []QP.Expr) string {
	return svwin.ComputeKey(row, columns, exprs)
}

// evalWindowExprOnRow evaluates a simple expression against a row map.
// Delegates to the window subpackage.
func evalWindowExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) interface{} {
	return svwin.EvalExprOnRow(row, columns, expr)
}

// makeRowMap creates a map from column names to values for a row.
// Delegates to the window subpackage.
func makeRowMap(columns []string, rowData []interface{}) map[string]interface{} {
	return svwin.MakeRowMap(columns, rowData)
}

// compareWindowVals compares two window values for ordering.
// Delegates to the window subpackage.
func compareWindowVals(a, b interface{}) int {
	return svwin.CompareVals(a, b)
}

// toFloat64Window converts a value to float64 for window aggregation.
// Delegates to the window subpackage.
func toFloat64Window(v interface{}) float64 {
	return svwin.ToFloat64(v)
}



