// Package vm provides pure helper functions for query execution.
// These helpers are stateless and operate only on QP/DS types, keeping
// pkg/sqlvibe/vm_exec.go focused on high-level orchestration.
package vm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// ExtractLimitInt computes the number of rows to read given LIMIT + OFFSET
// expressions. Returns 0 when no LIMIT is set (meaning unlimited).
func ExtractLimitInt(limitExpr, offsetExpr QP.Expr) int {
	if limitExpr == nil {
		return 0
	}
	lim := 0
	off := 0
	if lit, ok := limitExpr.(*QP.Literal); ok {
		if n, ok := lit.Value.(int64); ok {
			lim = int(n)
		}
	}
	if offsetExpr != nil {
		if lit, ok := offsetExpr.(*QP.Literal); ok {
			if n, ok := lit.Value.(int64); ok {
				off = int(n)
			}
		}
	}
	if lim <= 0 {
		return 0
	}
	return off + lim
}

// IsSimpleSelectStar reports whether stmt is a plain SELECT * FROM table with
// no WHERE, GROUP BY, ORDER BY, DISTINCT, LIMIT, JOINs, or subqueries.
// Such queries are eligible for the execSelectStarFast fast path.
func IsSimpleSelectStar(stmt *QP.SelectStmt) bool {
	if stmt == nil || stmt.From == nil {
		return false
	}
	if stmt.From.Join != nil || stmt.From.Subquery != nil {
		return false
	}
	if stmt.Where != nil {
		return false
	}
	if stmt.GroupBy != nil {
		return false
	}
	if len(stmt.OrderBy) > 0 {
		return false
	}
	if stmt.Distinct {
		return false
	}
	if stmt.Limit != nil {
		return false
	}
	if len(stmt.Columns) != 1 {
		return false
	}
	cr, ok := stmt.Columns[0].(*QP.ColumnRef)
	return ok && cr.Name == "*" && cr.Table == ""
}

// DeduplicateRows removes duplicate rows, preserving the first occurrence of
// each unique row. Uses strings.Builder + type switch to avoid fmt.Sprintf
// overhead.
func DeduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]interface{}, 0, len(rows))
	var b strings.Builder
	for _, row := range rows {
		b.Reset()
		writeRowKey(&b, row, len(row))
		key := b.String()
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// DeduplicateRowsN is like DeduplicateRows but uses only the first n columns as
// the dedup key. This is used when rows have extra ORDER BY columns appended
// beyond the projected SELECT columns; only the SELECT columns should determine
// uniqueness.
func DeduplicateRowsN(rows [][]interface{}, n int) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]interface{}, 0, len(rows))
	var b strings.Builder
	for _, row := range rows {
		b.Reset()
		limit := n
		if limit > len(row) {
			limit = len(row)
		}
		writeRowKey(&b, row, limit)
		key := b.String()
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// writeRowKey appends a canonical string representation of the first limit
// values of row into b. limit must be <= len(row).
func writeRowKey(b *strings.Builder, row []interface{}, limit int) {
	if limit > len(row) {
		limit = len(row)
	}
	for i, v := range row[:limit] {
		if i > 0 {
			b.WriteByte(',')
		}
		switch val := v.(type) {
		case int64:
			b.WriteString(strconv.FormatInt(val, 10))
		case float64:
			b.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
		case string:
			b.WriteString(val)
		case bool:
			if val {
				b.WriteString("true")
			} else {
				b.WriteString("false")
			}
		case []byte:
			b.WriteString(string(val))
		case nil:
			b.WriteString("<nil>")
		default:
			fmt.Fprintf(b, "%v", val)
		}
	}
}
