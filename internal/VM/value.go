package VM

import (
	"fmt"
	"math"
)

// ValTag identifies the runtime type of a VmVal.
type ValTag uint8

const (
	TagNull  ValTag = 0 // SQL NULL
	TagInt   ValTag = 1 // int64
	TagFloat ValTag = 2 // float64 bits stored in N via math.Float64bits
	TagText  ValTag = 3 // string
	TagBlob  ValTag = 4 // []byte stored as string
	TagBool  ValTag = 5 // bool (N==1 is true, N==0 is false)
)

// VmVal is a 32-byte typed SQL scalar (no interface{} boxing).
// Layout on amd64: N(8) + S(16) + T(1) + Fl(1) + _pad(6) = 32 bytes.
type VmVal struct {
	N    int64  // int64 value; also stores float64 bits (math.Float64bits)
	S    string // TagText: string content; TagBlob: blob cast to string
	T    ValTag // type tag (one of TagXxx constants)
	Fl   uint8  // internal flags (reserved, currently unused)
	_pad [6]byte
}

// Constructors — VmNull, VmInt, VmFloat never allocate.
func VmNull() VmVal             { return VmVal{T: TagNull} }
func VmInt(n int64) VmVal       { return VmVal{T: TagInt, N: n} }
func VmFloat(f float64) VmVal   { return VmVal{T: TagFloat, N: int64(math.Float64bits(f))} }
func VmText(s string) VmVal     { return VmVal{T: TagText, S: s} }
func VmBlob(b []byte) VmVal     { return VmVal{T: TagBlob, S: string(b)} }
func VmBool(b bool) VmVal {
	if b {
		return VmVal{T: TagBool, N: 1}
	}
	return VmVal{T: TagBool, N: 0}
}

// Accessors
func (v VmVal) IsNull() bool  { return v.T == TagNull }
func (v VmVal) Int() int64    { return v.N }
func (v VmVal) Float() float64 { return math.Float64frombits(uint64(v.N)) }
func (v VmVal) Text() string  { return v.S }
func (v VmVal) Blob() []byte  { return []byte(v.S) }
func (v VmVal) Bool() bool    { return v.N != 0 }

// ToInterface converts a VmVal to interface{} for legacy API compatibility.
// Only called at result projection boundaries, not in the hot path.
func (v VmVal) ToInterface() interface{} {
	switch v.T {
	case TagNull:
		return nil
	case TagInt:
		return v.N
	case TagFloat:
		return v.Float()
	case TagText:
		return v.S
	case TagBlob:
		return v.Blob()
	case TagBool:
		if v.N != 0 {
			return true
		}
		return false
	}
	return nil
}

// FromInterface converts a legacy interface{} value to VmVal.
// Called at row-read boundaries when consuming existing storage results.
func FromInterface(val interface{}) VmVal {
	if val == nil {
		return VmNull()
	}
	switch v := val.(type) {
	case int64:
		return VmInt(v)
	case int:
		return VmInt(int64(v))
	case int32:
		return VmInt(int64(v))
	case float64:
		return VmFloat(v)
	case float32:
		return VmFloat(float64(v))
	case string:
		return VmText(v)
	case []byte:
		return VmBlob(v)
	case bool:
		if v {
			return VmInt(1)
		}
		return VmInt(0)
	}
	return VmText(fmt.Sprintf("%v", val))
}

// addVmVal adds two VmVals numerically, NULL-propagating.
func AddVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	if a.T == TagFloat || b.T == TagFloat {
		return VmFloat(toFloat(a) + toFloat(b))
	}
	if a.T == TagInt && b.T == TagInt {
		return VmInt(a.N + b.N)
	}
	return VmFloat(toFloat(a) + toFloat(b))
}

func SubVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	if a.T == TagFloat || b.T == TagFloat {
		return VmFloat(toFloat(a) - toFloat(b))
	}
	if a.T == TagInt && b.T == TagInt {
		return VmInt(a.N - b.N)
	}
	return VmFloat(toFloat(a) - toFloat(b))
}

func MulVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	if a.T == TagFloat || b.T == TagFloat {
		return VmFloat(toFloat(a) * toFloat(b))
	}
	if a.T == TagInt && b.T == TagInt {
		return VmInt(a.N * b.N)
	}
	return VmFloat(toFloat(a) * toFloat(b))
}

func DivVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	if a.T == TagFloat || b.T == TagFloat {
		bf := toFloat(b)
		if bf == 0 {
			return VmNull()
		}
		return VmFloat(toFloat(a) / bf)
	}
	if a.T == TagInt && b.T == TagInt {
		if b.N == 0 {
			return VmNull()
		}
		return VmInt(a.N / b.N)
	}
	bf := toFloat(b)
	if bf == 0 {
		return VmNull()
	}
	return VmFloat(toFloat(a) / bf)
}

func ModVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	if a.T == TagInt && b.T == TagInt {
		if b.N == 0 {
			return VmNull()
		}
		return VmInt(a.N % b.N)
	}
	bf := toFloat(b)
	if bf == 0 {
		return VmNull()
	}
	return VmFloat(math.Mod(toFloat(a), bf))
}

func NegVmVal(a VmVal) VmVal {
	if a.T == TagNull {
		return VmNull()
	}
	if a.T == TagFloat {
		return VmFloat(-a.Float())
	}
	if a.T == TagInt {
		return VmInt(-a.N)
	}
	return VmNull()
}

func ConcatVmVal(a, b VmVal) VmVal {
	if a.T == TagNull || b.T == TagNull {
		return VmNull()
	}
	return VmText(vmValToText(a) + vmValToText(b))
}

// CompareVmVal returns -1, 0, or 1 for a <, ==, > b.
// NULL comparisons always return 0 (callers check IsNull separately).
func CompareVmVal(a, b VmVal) int {
	if a.T == TagNull && b.T == TagNull {
		return 0
	}
	if a.T == TagNull {
		return -1
	}
	if b.T == TagNull {
		return 1
	}
	// Numeric comparison
	if (a.T == TagInt || a.T == TagFloat || a.T == TagBool) &&
		(b.T == TagInt || b.T == TagFloat || b.T == TagBool) {
		af, bf := toFloat(a), toFloat(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	// Text comparison
	as, bs := vmValToText(a), vmValToText(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func toFloat(v VmVal) float64 {
	switch v.T {
	case TagFloat:
		return v.Float()
	case TagInt, TagBool:
		return float64(v.N)
	case TagText:
		var f float64
		fmt.Sscanf(v.S, "%g", &f)
		return f
	}
	return 0
}

func vmValToText(v VmVal) string {
	switch v.T {
	case TagText, TagBlob:
		return v.S
	case TagInt:
		return fmt.Sprintf("%d", v.N)
	case TagFloat:
		return fmt.Sprintf("%g", v.Float())
	case TagBool:
		if v.N != 0 {
			return "1"
		}
		return "0"
	}
	return ""
}
