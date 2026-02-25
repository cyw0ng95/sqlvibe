package driver

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// toDriverValue converts a sqlvibe value to a driver.Value.
// Supported types: nil, int64, float64, string, []byte, time.Time.
func toDriverValue(v interface{}) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case string:
		return val, nil
	case []byte:
		return val, nil
	case bool:
		if val {
			return int64(1), nil
		}
		return int64(0), nil
	case time.Time:
		return val, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// fromNamedValues converts []driver.NamedValue to positional []interface{}
// and named map[string]interface{} for use with ExecWithParams/QueryNamed.
// If any arg has a non-empty Name, the named map is populated; otherwise
// only the positional slice is populated.
func fromNamedValues(args []driver.NamedValue) ([]interface{}, map[string]interface{}) {
	pos := make([]interface{}, len(args))
	named := make(map[string]interface{})
	hasNamed := false
	for _, a := range args {
		if a.Name != "" {
			named[a.Name] = a.Value
			hasNamed = true
		}
		// Always fill positional slice in ordinal order (Ordinal is 1-based)
		idx := a.Ordinal - 1
		if idx >= 0 && idx < len(pos) {
			pos[idx] = a.Value
		}
	}
	if !hasNamed {
		named = nil
	}
	return pos, named
}
