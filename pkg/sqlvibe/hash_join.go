package sqlvibe

import (
	"fmt"
	"strings"
	"sync"

	"github.com/sqlvibe/sqlvibe/internal/QP"
)

// mergedRowPool is a pool of map[string]interface{} objects used as scratch
// space in buildJoinMergedRow.  Each map is cleared before re-use.
var mergedRowPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 32)
	},
}

// getMergedRow obtains a merged-row map from the pool, clearing any
// leftover entries from the previous use.
func getMergedRow() map[string]interface{} {
	m := mergedRowPool.Get().(map[string]interface{})
	for k := range m {
		delete(m, k)
	}
	return m
}

// putMergedRow returns a merged-row map to the pool.
func putMergedRow(m map[string]interface{}) {
	mergedRowPool.Put(m)
}

// normalizeJoinKey converts a value to a form that is safe as an interface{}
// map key.  []byte is not comparable in Go so it is converted to string; all
// other types that reach here (int64, float64, string, bool) are left as-is.
func normalizeJoinKey(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// allColumnsAreStar reports whether every SELECT column is an unqualified or
// qualified star (i.e. no expressions, no function calls, no aliases with
// non-star source).  When true and the query has no WHERE clause the hash join
// can skip building the merged-row map entirely.
func allColumnsAreStar(cols []QP.Expr) bool {
	for _, col := range cols {
		switch c := col.(type) {
		case *QP.ColumnRef:
			if c.Name != "*" {
				return false
			}
		default:
			return false
		}
	}
	return len(cols) > 0
}

// hashJoinInfo holds the extracted parameters for a 2-table equi-join.
type hashJoinInfo struct {
	leftTable    string
	rightTable   string
	leftAlias    string
	rightAlias   string
	leftJoinKey  string // column name in the left table to match on
	rightJoinKey string // column name in the right table to match on
}

// extractHashJoinInfo inspects stmt and returns hash join metadata if the query is
// eligible for Go-level hash join execution.  Returns nil when not eligible.
// Eligible queries must be:
//   - Exactly two tables (no 3-table chains, no derived tables)
//   - INNER join (empty type string is also treated as INNER)
//   - A simple equi-join condition: <colA> = <colB>
func extractHashJoinInfo(stmt *QP.SelectStmt) *hashJoinInfo {
	if stmt == nil || stmt.From == nil || stmt.From.Join == nil {
		return nil
	}
	join := stmt.From.Join
	joinType := strings.ToUpper(join.Type)
	if joinType != "" && joinType != "INNER" && joinType != "CROSS" {
		return nil
	}
	// No 3-table chains
	if join.Right == nil || join.Right.Join != nil {
		return nil
	}
	// Derived table (subquery in FROM) not supported here
	if stmt.From.Subquery != nil || join.Right.Subquery != nil {
		return nil
	}
	// Skip if any SELECT column uses aggregate functions or qualified star (e.g., t.*)
	// since the hash join does not implement aggregation.
	for _, col := range stmt.Columns {
		if selectColNeedsVM(col) {
			return nil
		}
	}
	cond := join.Cond
	if cond == nil {
		return nil
	}
	binExpr, ok := cond.(*QP.BinaryExpr)
	if !ok || binExpr.Op != QP.TokenEq {
		return nil
	}
	leftCol, lOk := binExpr.Left.(*QP.ColumnRef)
	rightCol, rOk := binExpr.Right.(*QP.ColumnRef)
	if !lOk || !rOk {
		return nil
	}

	leftTable := stmt.From.Name
	leftAlias := stmt.From.Alias
	rightTable := join.Right.Name
	rightAlias := join.Right.Alias

	leftRef := leftTable
	if leftAlias != "" {
		leftRef = leftAlias
	}
	rightRef := rightTable
	if rightAlias != "" {
		rightRef = rightAlias
	}

	// Determine which side of the equality belongs to which table.
	// A ColumnRef.Table qualifier of "" means unqualified; we treat it as belonging
	// to the side that matches the position in the binary expression.
	var leftJoinKey, rightJoinKey string
	lTbl := strings.ToLower(leftCol.Table)
	rTbl := strings.ToLower(rightCol.Table)
	lRef := strings.ToLower(leftRef)
	rRef := strings.ToLower(rightRef)

	switch {
	case (lTbl == "" || lTbl == lRef || lTbl == strings.ToLower(leftTable)) &&
		(rTbl == "" || rTbl == rRef || rTbl == strings.ToLower(rightTable)):
		leftJoinKey = leftCol.Name
		rightJoinKey = rightCol.Name
	case (lTbl == rRef || lTbl == strings.ToLower(rightTable)) &&
		(rTbl == "" || rTbl == lRef || rTbl == strings.ToLower(leftTable)):
		// AST left side refers to the right table
		leftJoinKey = rightCol.Name
		rightJoinKey = leftCol.Name
	default:
		return nil
	}

	return &hashJoinInfo{
		leftTable:    leftTable,
		rightTable:   rightTable,
		leftAlias:    leftAlias,
		rightAlias:   rightAlias,
		leftJoinKey:  leftJoinKey,
		rightJoinKey: rightJoinKey,
	}
}

// selectColNeedsVM reports whether a SELECT column expression requires the full VM
// (e.g., aggregate functions), which the hash join does not handle.
func selectColNeedsVM(col QP.Expr) bool {
	switch c := col.(type) {
	case *QP.FuncCall:
		return true // aggregate or scalar function
	case *QP.ColumnRef:
		_ = c // plain column refs and qualified stars (t.*) are handled by hash join
	case *QP.AliasExpr:
		return selectColNeedsVM(c.Expr)
	case *QP.WindowFuncExpr:
		return true
	}
	return false
}

// execHashJoin executes a 2-table equi-join using a hash table built on the right side.
// It returns (rows, columns, true) when hash join was used, or (nil, nil, false) when
// the query is not eligible or if table data is unavailable.
func (db *Database) execHashJoin(stmt *QP.SelectStmt) ([][]interface{}, []string, bool) {
	info := extractHashJoinInfo(stmt)
	if info == nil {
		return nil, nil, false
	}

	leftTable := db.resolveTableName(info.leftTable)
	rightTable := db.resolveTableName(info.rightTable)

	leftData, leftExists := db.data[leftTable]
	rightData, rightExists := db.data[rightTable]
	if !leftExists || !rightExists {
		return nil, nil, false
	}

	leftCols := db.columnOrder[leftTable]
	rightCols := db.columnOrder[rightTable]
	if leftCols == nil || rightCols == nil {
		return nil, nil, false
	}

	leftAlias := info.leftAlias
	if leftAlias == "" {
		leftAlias = leftTable
	}
	rightAlias := info.rightAlias
	if rightAlias == "" {
		rightAlias = rightTable
	}

	// Build hash table keyed by the right table's join column value.
	// Use interface{} keys directly (no fmt.Sprintf string conversion) to
	// avoid one heap allocation per key.  []byte is converted to string first
	// because []byte is not comparable as a map key.
	// NULL values are excluded since NULL ≠ NULL in SQL equi-joins.
	hashTable := make(map[interface{}][]map[string]interface{}, len(rightData))
	for _, row := range rightData {
		joinVal := row[info.rightJoinKey]
		if joinVal == nil {
			continue // NULL does not match anything in an equi-join
		}
		key := normalizeJoinKey(joinVal)
		hashTable[key] = append(hashTable[key], row)
	}

	// Determine SELECT column names for the output.
	allCols := append(append([]string{}, leftCols...), rightCols...)
	cols := make([]string, 0, len(stmt.Columns))
	for i, col := range stmt.Columns {
		switch c := col.(type) {
		case *QP.ColumnRef:
			if c.Name == "*" && c.Table == "" {
				// Unqualified star: expand all columns from both tables.
				cols = append(cols, allCols...)
			} else if c.Name == "*" {
				// Qualified star (e.g. a.*, b.*): expand columns of matching side.
				tbl := strings.ToLower(c.Table)
				if tbl == strings.ToLower(leftAlias) || tbl == strings.ToLower(leftTable) {
					cols = append(cols, leftCols...)
				} else {
					cols = append(cols, rightCols...)
				}
			} else {
				cols = append(cols, c.Name)
			}
		case *QP.AliasExpr:
			cols = append(cols, c.Alias)
		default:
			cols = append(cols, fmt.Sprintf("col_%d", i))
		}
	}

	// Probe the hash table for each left row.
	// Fast path: when WHERE is absent and every SELECT column is a star (unqualified
	// or qualified), skip buildJoinMergedRow entirely — the output row is built
	// directly from the source rows without going through a merged map.
	starOnly := stmt.Where == nil && allColumnsAreStar(stmt.Columns)

	var results [][]interface{}
	for _, leftRow := range leftData {
		joinVal := leftRow[info.leftJoinKey]
		if joinVal == nil {
			continue // NULL never matches in an equi-join
		}
		key := normalizeJoinKey(joinVal)
		matches := hashTable[key]
		if len(matches) == 0 {
			continue
		}
		for _, rightRow := range matches {
			if starOnly {
				// Fast path: build output directly from source rows.
				row := make([]interface{}, 0, len(allCols))
				for _, col := range stmt.Columns {
					c := col.(*QP.ColumnRef) // allColumnsAreStar guarantees this
					if c.Table == "" {
						// Unqualified *: left columns then right columns.
						for _, colName := range leftCols {
							row = append(row, leftRow[colName])
						}
						for _, colName := range rightCols {
							row = append(row, rightRow[colName])
						}
					} else {
						tbl := strings.ToLower(c.Table)
						if tbl == strings.ToLower(leftAlias) || tbl == strings.ToLower(leftTable) {
							for _, colName := range leftCols {
								row = append(row, leftRow[colName])
							}
						} else {
							for _, colName := range rightCols {
								row = append(row, rightRow[colName])
							}
						}
					}
				}
				results = append(results, row)
				continue
			}

			merged := buildJoinMergedRow(leftRow, leftTable, leftAlias, leftCols,
				rightRow, rightTable, rightAlias, rightCols)

			if stmt.Where != nil && !db.engine.EvalBool(merged, stmt.Where) {
				putMergedRow(merged)
				continue
			}

			row := make([]interface{}, 0, len(stmt.Columns))
			for _, col := range stmt.Columns {
				switch c := col.(type) {
				case *QP.ColumnRef:
					if c.Name == "*" && c.Table == "" {
						// Unqualified *: all left columns then all right columns.
						for _, colName := range leftCols {
							row = append(row, leftRow[colName])
						}
						for _, colName := range rightCols {
							row = append(row, rightRow[colName])
						}
					} else if c.Name == "*" {
						// Qualified star (e.g. a.*, b.*): columns of matching side.
						tbl := strings.ToLower(c.Table)
						if tbl == strings.ToLower(leftAlias) || tbl == strings.ToLower(leftTable) {
							for _, colName := range leftCols {
								row = append(row, leftRow[colName])
							}
						} else {
							for _, colName := range rightCols {
								row = append(row, rightRow[colName])
							}
						}
					} else {
						row = append(row, db.engine.EvalExpr(merged, c))
					}
				default:
					row = append(row, db.engine.EvalExpr(merged, col))
				}
			}
			putMergedRow(merged)
			results = append(results, row)
		}
	}

	return results, cols, true
}

// buildJoinMergedRow creates a merged row map for hash join result evaluation.
// Keys are stored in "alias.col", "tableName.col", and unqualified "col" forms.
// Left table columns take priority for unqualified names.
// The returned map is obtained from mergedRowPool; callers must call putMergedRow
// when they are done with it to return it to the pool.
func buildJoinMergedRow(
	leftRow map[string]interface{}, leftTable, leftAlias string, leftCols []string,
	rightRow map[string]interface{}, rightTable, rightAlias string, rightCols []string,
) map[string]interface{} {
	merged := getMergedRow()

	for _, col := range leftCols {
		val := leftRow[col]
		merged[leftTable+"."+col] = val
		if leftAlias != leftTable {
			merged[leftAlias+"."+col] = val
		}
		merged[col] = val // left takes priority for unqualified names
	}
	for _, col := range rightCols {
		val := rightRow[col]
		merged[rightTable+"."+col] = val
		if rightAlias != rightTable {
			merged[rightAlias+"."+col] = val
		}
		// Only set unqualified if not already present (left takes priority)
		if _, exists := merged[col]; !exists {
			merged[col] = val
		}
	}
	return merged
}
