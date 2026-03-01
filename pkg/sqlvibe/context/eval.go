// Package context provides pure helper functions for expression evaluation,
// CHECK constraint checking, and aggregate accumulation.
// These helpers are stateless and operate only on QP types and plain Go values,
// making them independently testable without the full Database struct.
package context

// IsTruthy reports whether a SQL value is logically true (non-zero, non-null, non-empty).
func IsTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case float64:
		return v != 0.0
	case string:
		return len(v) > 0
	default:
		return true
	}
}

// AddVals adds two SQL values. Returns nil if types are incompatible.
func AddVals(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l + r
		}
		if r, ok := right.(float64); ok {
			return float64(l) + r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l + float64(r)
		}
		if r, ok := right.(float64); ok {
			return l + r
		}
	}
	return nil
}

// SubVals subtracts right from left. Returns nil if types are incompatible.
func SubVals(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l - r
		}
		if r, ok := right.(float64); ok {
			return float64(l) - r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l - float64(r)
		}
		if r, ok := right.(float64); ok {
			return l - r
		}
	}
	return nil
}

// MulVals multiplies two SQL values. Returns nil if types are incompatible.
func MulVals(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l * r
		}
		if r, ok := right.(float64); ok {
			return float64(l) * r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			return l * float64(r)
		}
		if r, ok := right.(float64); ok {
			return l * r
		}
	}
	return nil
}

// DivVals divides left by right. Returns nil on division by zero or incompatible types.
func DivVals(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			if r != 0 {
				return l / r
			}
		}
		if r, ok := right.(float64); ok {
			if r != 0.0 {
				return float64(l) / r
			}
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(int64); ok {
			if r != 0 {
				return l / float64(r)
			}
		}
		if r, ok := right.(float64); ok {
			if r != 0.0 {
				return l / r
			}
		}
	}
	return nil
}
