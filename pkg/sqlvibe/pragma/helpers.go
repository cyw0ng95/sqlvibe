package pragma

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// IntValue extracts an int64 from a pragma expression.
// String tokens "ON"/"FULL"/"EXCLUSIVE" map to 1; "OFF"/"NONE"/"NORMAL" map to 0.
func IntValue(expr QP.Expr) int64 {
	switch v := expr.(type) {
	case *QP.Literal:
		if n, ok := v.Value.(int64); ok {
			return n
		}
		if f, ok := v.Value.(float64); ok {
			return int64(f)
		}
		if s, ok := v.Value.(string); ok {
			return pragmaKeywordToInt(s)
		}
	case *QP.ColumnRef:
		return pragmaKeywordToInt(v.Name)
	}
	return 0
}

// pragmaKeywordToInt converts SQLite pragma keyword strings to int64.
func pragmaKeywordToInt(s string) int64 {
	switch strings.ToUpper(s) {
	case "ON", "FULL", "EXCLUSIVE":
		return 1
	case "OFF", "NONE", "NORMAL":
		return 0
	}
	return 0
}

// StrValue extracts a string from a pragma expression.
func StrValue(expr QP.Expr) string {
	switch v := expr.(type) {
	case *QP.Literal:
		return fmt.Sprintf("%v", v.Value)
	case *QP.ColumnRef:
		return v.Name
	}
	return ""
}

// Result builds a simple single-column single-row pragma result.
func Result(colName string, value interface{}) ([]string, [][]interface{}) {
	return []string{colName}, [][]interface{}{{value}}
}

// EmptyResult returns an empty pragma result (used when setting values).
func EmptyResult() ([]string, [][]interface{}) {
	return []string{}, [][]interface{}{}
}
