package DS

import (
	"fmt"
	"math"
	"strconv"
)

// ValueType enumerates the supported column types.
type ValueType int

const (
	TypeNull   ValueType = iota
	TypeInt              // int64
	TypeFloat            // float64
	TypeString           // string
	TypeBytes            // []byte
	TypeBool             // bool
)

// Value holds a single typed datum.
type Value struct {
	Type  ValueType
	Int   int64
	Float float64
	Str   string
	Bytes []byte
}

func NullValue() Value           { return Value{Type: TypeNull} }
func IntValue(v int64) Value     { return Value{Type: TypeInt, Int: v} }
func FloatValue(v float64) Value { return Value{Type: TypeFloat, Float: v} }
func StringValue(v string) Value { return Value{Type: TypeString, Str: v} }
func BoolValue(v bool) Value {
	b := int64(0)
	if v {
		b = 1
	}
	return Value{Type: TypeBool, Int: b}
}
func BytesValue(v []byte) Value { return Value{Type: TypeBytes, Bytes: v} }

// IsNull returns true if the value is NULL.
func (v Value) IsNull() bool { return v.Type == TypeNull }

// String returns a human-readable representation.
func (v Value) String() string {
	switch v.Type {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return fmt.Sprintf("%d", v.Int)
	case TypeFloat:
		return fmt.Sprintf("%g", v.Float)
	case TypeString:
		return v.Str
	case TypeBool:
		if v.Int != 0 {
			return "true"
		}
		return "false"
	case TypeBytes:
		return fmt.Sprintf("%x", v.Bytes)
	default:
		return "?"
	}
}

// Equal returns true when the two values are equal (NULL != NULL per SQL semantics).
func (v Value) Equal(other Value) bool {
	if v.Type == TypeNull || other.Type == TypeNull {
		return false
	}
	return Compare(v, other) == 0
}

// Compare compares two Values. Returns -1, 0, or 1.
// NULL is treated as less than everything else.
// When types differ the numeric types are coerced; otherwise type order is used.
func Compare(a, b Value) int {
	// NULL ordering
	if a.Type == TypeNull && b.Type == TypeNull {
		return 0
	}
	if a.Type == TypeNull {
		return -1
	}
	if b.Type == TypeNull {
		return 1
	}

	// Numeric coercion: Int ↔ Float
	af, aIsNum := toFloat(a)
	bf, bIsNum := toFloat(b)
	if aIsNum && bIsNum {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	// Same non-numeric type
	if a.Type == b.Type {
		switch a.Type {
		case TypeString:
			return cmpString(a.Str, b.Str)
		case TypeBytes:
			return cmpBytes(a.Bytes, b.Bytes)
		case TypeBool:
			return cmpInt(a.Int, b.Int)
		}
	}

	// Fall back to type order
	return cmpInt(int64(a.Type), int64(b.Type))
}

func toFloat(v Value) (float64, bool) {
	switch v.Type {
	case TypeInt:
		return float64(v.Int), true
	case TypeFloat:
		return v.Float, true
	case TypeBool:
		return float64(v.Int), true
	}
	return math.NaN(), false
}

func cmpInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// ParseValue parses a string (as produced by Value.String()) back to a Value.
// It tries int64, then float64, then falls back to string.
func ParseValue(s string) Value {
	if s == "NULL" {
		return NullValue()
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return IntValue(i)
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return FloatValue(f)
	}
	return StringValue(s)
}
