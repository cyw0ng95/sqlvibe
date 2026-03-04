// Package DS - minimal Value type for C++ wrapper compatibility
package DS

// ValueType represents the type of a database value.
type ValueType int

const (
	TypeNull ValueType = iota
	TypeInt
	TypeFloat
	TypeString
	TypeBlob
	TypeBytes // Alias for Blob
)

// Value represents a database value (type-safe union).
// This is a minimal type definition for C++ wrapper compatibility.
// The actual value handling is done in C++.
type Value struct {
	Type  ValueType
	Int   int64
	Float float64
	Str   string
	Bytes []byte
	Blob  []byte // Alias for Bytes
}

// GetBytes returns the blob/bytes value.
func (v Value) GetBytes() []byte {
	if v.Bytes != nil {
		return v.Bytes
	}
	return v.Blob
}

// Compare compares two values.
func Compare(a, b Value) int {
	if a.Type != b.Type {
		if a.Type < b.Type {
			return -1
		}
		return 1
	}
	switch a.Type {
	case TypeNull:
		return 0
	case TypeInt:
		if a.Int < b.Int {
			return -1
		} else if a.Int > b.Int {
			return 1
		}
		return 0
	case TypeFloat:
		if a.Float < b.Float {
			return -1
		} else if a.Float > b.Float {
			return 1
		}
		return 0
	case TypeString:
		if a.Str < b.Str {
			return -1
		} else if a.Str > b.Str {
			return 1
		}
		return 0
	case TypeBlob:
		aBytes := a.Bytes
		if aBytes == nil {
			aBytes = a.Blob
		}
		bBytes := b.Bytes
		if bBytes == nil {
			bBytes = b.Blob
		}
		for i := 0; i < len(aBytes) && i < len(bBytes); i++ {
			if aBytes[i] < bBytes[i] {
				return -1
			} else if aBytes[i] > bBytes[i] {
				return 1
			}
		}
		if len(aBytes) < len(bBytes) {
			return -1
		} else if len(aBytes) > len(bBytes) {
			return 1
		}
		return 0
	default:
		return 0
	}
}
