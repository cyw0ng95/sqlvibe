package SQLValidator

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// QueryResult holds the outcome of running a single SQL statement.
type QueryResult struct {
	// Columns is nil for non-SELECT statements.
	Columns []string
	// Rows holds the result rows; each row is a slice of Go values
	// (int64, float64, string, or nil for NULL).
	Rows [][]interface{}
	// Err is non-nil when the statement caused an error.
	Err error
}

// Mismatch describes a single discrepancy found by the validator.
type Mismatch struct {
	Query         string
	SQLiteResult  QueryResult
	SQLVibeResult QueryResult
	Reason        string
}

const floatTolerance = 1e-9

// Compare returns a Mismatch if the two results differ, or nil if they match.
//
// Comparison rules:
//  1. Both error → match (we don't compare error message text).
//  2. One error, other success → mismatch.
//  3. Both success → rows are sorted and compared cell-by-cell with float
//     tolerance; NULL == NULL is true.
func Compare(query string, liteRes, svibeRes QueryResult) *Mismatch {
	liteErr := liteRes.Err != nil
	svibeErr := svibeRes.Err != nil

	if liteErr && svibeErr {
		// Both returned errors — consider this a match regardless of message.
		return nil
	}
	if liteErr != svibeErr {
		reason := ""
		if liteErr {
			reason = fmt.Sprintf("SQLite returned error %q but SQLVibe succeeded", liteRes.Err)
		} else {
			reason = fmt.Sprintf("SQLVibe returned error %q but SQLite succeeded", svibeRes.Err)
		}
		return &Mismatch{Query: query, SQLiteResult: liteRes, SQLVibeResult: svibeRes, Reason: reason}
	}

	// Both succeeded — compare result sets.
	liteRows := normaliseRows(liteRes.Rows)
	svibeRows := normaliseRows(svibeRes.Rows)

	if len(liteRows) != len(svibeRows) {
		return &Mismatch{
			Query:         query,
			SQLiteResult:  liteRes,
			SQLVibeResult: svibeRes,
			Reason: fmt.Sprintf("row count differs: SQLite=%d SQLVibe=%d",
				len(liteRows), len(svibeRows)),
		}
	}

	for i, lr := range liteRows {
		sr := svibeRows[i]
		if len(lr) != len(sr) {
			return &Mismatch{
				Query:         query,
				SQLiteResult:  liteRes,
				SQLVibeResult: svibeRes,
				Reason: fmt.Sprintf("row %d column count differs: SQLite=%d SQLVibe=%d",
					i, len(lr), len(sr)),
			}
		}
		for j := range lr {
			if !cellsEqual(lr[j], sr[j]) {
				return &Mismatch{
					Query:         query,
					SQLiteResult:  liteRes,
					SQLVibeResult: svibeRes,
					Reason: fmt.Sprintf("row %d col %d differs: SQLite=%v (%T) SQLVibe=%v (%T)",
						i, j, lr[j], lr[j], sr[j], sr[j]),
				}
			}
		}
	}
	return nil
}

// normaliseRows sorts rows into a canonical order so comparison is
// order-independent. Each cell is converted to a string key first.
func normaliseRows(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}
	cp := make([][]interface{}, len(rows))
	copy(cp, rows)
	sort.Slice(cp, func(i, j int) bool {
		ri, rj := cp[i], cp[j]
		for k := 0; k < len(ri) && k < len(rj); k++ {
			si := cellKey(ri[k])
			sj := cellKey(rj[k])
			if si != sj {
				return si < sj
			}
		}
		return len(ri) < len(rj)
	})
	return cp
}

// cellKey converts a cell value to a string for sorting purposes.
func cellKey(v interface{}) string {
	if v == nil {
		return "\x00NULL"
	}
	switch x := v.(type) {
	case int64:
		return fmt.Sprintf("i:%020d", x)
	case float64:
		return fmt.Sprintf("f:%+.9e", x)
	case string:
		return "s:" + x
	case []byte:
		return "b:" + string(x)
	case bool:
		if x {
			return "b:true"
		}
		return "b:false"
	default:
		return fmt.Sprintf("?:%v", v)
	}
}

// cellsEqual returns true if two cell values are logically equal.
// NULL == NULL is true. Floats are compared with tolerance.
func cellsEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Normalise both to a canonical type before comparing.
	an := normaliseCell(a)
	bn := normaliseCell(b)

	switch av := an.(type) {
	case int64:
		switch bv := bn.(type) {
		case int64:
			return av == bv
		case float64:
			return math.Abs(float64(av)-bv) <= floatTolerance
		}
	case float64:
		switch bv := bn.(type) {
		case float64:
			if math.IsNaN(av) && math.IsNaN(bv) {
				return true
			}
			return math.Abs(av-bv) <= floatTolerance
		case int64:
			return math.Abs(av-float64(bv)) <= floatTolerance
		}
	case string:
		if bv, ok := bn.(string); ok {
			return av == bv
		}
	case []byte:
		if bv, ok := bn.([]byte); ok {
			return string(av) == string(bv)
		}
	}
	return fmt.Sprintf("%v", an) == fmt.Sprintf("%v", bn)
}

// normaliseCell coerces a cell value to int64, float64, string, or nil.
// This bridges differences between SQLite (database/sql) and sqlvibe return types.
func normaliseCell(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case uint8:
		return int64(x)
	case float64:
		return x
	case float32:
		return float64(x)
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		if x {
			return int64(1)
		}
		return int64(0)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// fmtResult formats a QueryResult as a short human-readable string for error messages.
func fmtResult(r QueryResult) string {
	if r.Err != nil {
		return fmt.Sprintf("error(%q)", r.Err.Error())
	}
	if len(r.Rows) == 0 {
		return "(0 rows)"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "(%d rows)[", len(r.Rows))
	for i, row := range r.Rows {
		if i > 0 {
			sb.WriteString("|")
		}
		for j, cell := range row {
			if j > 0 {
				sb.WriteString(",")
			}
			if cell == nil {
				sb.WriteString("NULL")
			} else {
				fmt.Fprintf(&sb, "%v", cell)
			}
		}
	}
	sb.WriteString("]")
	return sb.String()
}
