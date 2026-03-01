// Package window provides pure helper functions for SQL window function computation.
// Functions operate on RowSet (columns + data) without depending on the Database struct,
// making them independently testable.
package window

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// RowSet is a minimal column-oriented result set used by window helpers.
type RowSet struct {
	Columns []string
	Data    [][]interface{}
}

// MakeRowMap creates a column→value map from a single data row.
func MakeRowMap(columns []string, rowData []interface{}) map[string]interface{} {
	row := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(rowData) {
			row[col] = rowData[i]
		}
	}
	return row
}

// EvalExprOnRow evaluates a simple expression (ColumnRef or Literal) against a row map.
func EvalExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) interface{} {
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Table != "" {
			if v, ok := row[e.Table+"."+e.Name]; ok {
				return v
			}
		}
		if v, ok := row[e.Name]; ok {
			return v
		}
		lower := strings.ToLower(e.Name)
		for k, v := range row {
			if strings.ToLower(k) == lower {
				return v
			}
		}
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
		return EvalExprOnRow(row, columns, e.Expr)
	default:
		return nil
	}
}

// ComputeKey returns a string partition key from a row and a list of partition expressions.
func ComputeKey(row map[string]interface{}, columns []string, exprs []QP.Expr) string {
	parts := make([]string, len(exprs))
	for i, expr := range exprs {
		v := EvalExprOnRow(row, columns, expr)
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// BuildPartitionGroups groups row indices by PARTITION BY key.
// If partExprs is empty, all rows are placed in a single group.
func BuildPartitionGroups(rs *RowSet, partExprs []QP.Expr) [][]int {
	if len(partExprs) == 0 {
		all := make([]int, len(rs.Data))
		for i := range all {
			all[i] = i
		}
		return [][]int{all}
	}

	groupMap := make(map[string][]int)
	var groupOrder []string

	for i, rowData := range rs.Data {
		row := MakeRowMap(rs.Columns, rowData)
		key := ComputeKey(row, rs.Columns, partExprs)
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

// SortRowIndices sorts a slice of row indices by the given ORDER BY expressions.
func SortRowIndices(rs *RowSet, indices []int, orderExprs []QP.WindowOrderBy) []int {
	if len(orderExprs) == 0 {
		return indices
	}
	sorted := make([]int, len(indices))
	copy(sorted, indices)
	sort.SliceStable(sorted, func(a, b int) bool {
		ra := MakeRowMap(rs.Columns, rs.Data[sorted[a]])
		rb := MakeRowMap(rs.Columns, rs.Data[sorted[b]])
		for _, ob := range orderExprs {
			va := EvalExprOnRow(ra, rs.Columns, ob.Expr)
			vb := EvalExprOnRow(rb, rs.Columns, ob.Expr)
			cmp := CompareVals(va, vb)
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

// SameOrderKey reports whether two rows have identical ORDER BY key values.
func SameOrderKey(rs *RowSet, ri, rj int, orderExprs []QP.WindowOrderBy) bool {
	ra := MakeRowMap(rs.Columns, rs.Data[ri])
	rb := MakeRowMap(rs.Columns, rs.Data[rj])
	for _, ob := range orderExprs {
		va := EvalExprOnRow(ra, rs.Columns, ob.Expr)
		vb := EvalExprOnRow(rb, rs.Columns, ob.Expr)
		if CompareVals(va, vb) != 0 {
			return false
		}
	}
	return true
}

// GetArgVal evaluates the first arg expression for the row at rowIdx.
func GetArgVal(rs *RowSet, rowIdx int, args []QP.Expr) interface{} {
	if len(args) == 0 || rowIdx < 0 || rowIdx >= len(rs.Data) {
		return nil
	}
	row := MakeRowMap(rs.Columns, rs.Data[rowIdx])
	return EvalExprOnRow(row, rs.Columns, args[0])
}

// CompareVals compares two window values for ordering purposes.
// NULL sorts before all other values.
func CompareVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	fa, aOk := toFloat64(a)
	fb, bOk := toFloat64(b)
	if aOk && bOk {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
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

// ToFloat64 converts a value to float64 for window aggregation (returns 0 for non-numeric types).
func ToFloat64(v interface{}) float64 {
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

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
