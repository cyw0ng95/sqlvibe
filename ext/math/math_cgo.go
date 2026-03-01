//go:build SVDB_ENABLE_CGO
// +build SVDB_ENABLE_CGO

package math

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_math
#cgo CFLAGS: -I${SRCDIR}
#include "math.h"
#include <stdlib.h>
*/
import "C"

import (
	"github.com/cyw0ng95/sqlvibe/ext"
)

type MathExtension struct{}

func (e *MathExtension) Name() string        { return "math" }
func (e *MathExtension) Description() string { return "Math extension (CGO)" }

func (e *MathExtension) Functions() []string {
	return []string{
		"ABS", "CEIL", "CEILING", "FLOOR", "ROUND",
		"POWER", "POW", "SQRT", "MOD", "PI",
		"EXP", "LN", "LOG", "LOG2", "LOG10", "SIGN",
		"RANDOM", "RANDOMBLOB", "ZEROBLOB",
	}
}

func (e *MathExtension) Opcodes() []ext.Opcode { return nil }

func (e *MathExtension) Register(db interface{}) error { return nil }

func (e *MathExtension) Close() error { return nil }

func (e *MathExtension) CallFunc(name string, args []interface{}) interface{} {
	switch name {
	case "ABS":
		return callAbs(args)
	case "CEIL", "CEILING":
		return callCeil(args)
	case "FLOOR":
		return callFloor(args)
	case "ROUND":
		return callRound(args)
	case "POWER", "POW":
		return callPower(args)
	case "SQRT":
		return callSqrt(args)
	case "MOD":
		return callMod(args)
	case "PI":
		return callPi()
	case "EXP":
		return callExp(args)
	case "LN":
		return callLn(args)
	case "LOG":
		return callLog(args)
	case "LOG2":
		return callLog2(args)
	case "LOG10":
		return callLog10(args)
	case "SIGN":
		return callSign(args)
	case "RANDOM":
		return callRandom()
	case "RANDOMBLOB":
		return callRandomblob(args)
	case "ZEROBLOB":
		return callZeroblob(args)
	}
	return nil
}

func init() {
	ext.Register("math", &MathExtension{})
}

func callAbs(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return C.svdb_abs_int(C.int64_t(v))
	case float64:
		return C.svdb_abs_double(C.double(v))
	}
	return val
}

func callCeil(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return v
	case float64:
		return C.svdb_ceil(C.double(v))
	}
	return val
}

func callFloor(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return v
	case float64:
		return C.svdb_floor(C.double(v))
	}
	return val
}

func callRound(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	decimals := 0
	if len(args) >= 2 {
		if decVal := args[1]; decVal != nil {
			switch d := decVal.(type) {
			case int64:
				decimals = int(d)
			case float64:
				decimals = int(d)
			}
		}
	}
	switch v := val.(type) {
	case int64:
		return C.svdb_round(C.double(v), C.int(decimals))
	case float64:
		return C.svdb_round(C.double(v), C.int(decimals))
	}
	return val
}

func callPower(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	base, ok1 := toFloat64CGO(args[0])
	exp, ok2 := toFloat64CGO(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	return C.svdb_power(C.double(base), C.double(exp))
}

func callSqrt(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64CGO(args[0])
	if !ok {
		return nil
	}
	if v < 0 {
		return nil
	}
	return C.svdb_sqrt(C.double(v))
}

func callMod(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	a, ok1 := toFloat64CGO(args[0])
	b, ok2 := toFloat64CGO(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	if b == 0 {
		return nil
	}
	return C.svdb_mod(C.double(a), C.double(b))
}

func callPi() interface{} {
	return C.SVDB_PI
}

func callExp(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64CGO(args[0])
	if !ok {
		return nil
	}
	return C.svdb_exp(C.double(v))
}

func callLn(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64CGO(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return C.svdb_ln(C.double(v))
}

func callLog(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	if len(args) == 1 {
		v, ok := toFloat64CGO(args[0])
		if !ok || v <= 0 {
			return nil
		}
		return C.svdb_ln(C.double(v))
	}
	b, ok1 := toFloat64CGO(args[0])
	x, ok2 := toFloat64CGO(args[1])
	if !ok1 || !ok2 || b <= 0 || b == 1 || x <= 0 {
		return nil
	}
	return C.svdb_log(C.double(b), C.double(x))
}

func callLog2(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64CGO(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return C.svdb_log2(C.double(v))
}

func callLog10(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64CGO(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return C.svdb_log10(C.double(v))
}

func callSign(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return C.svdb_sign_int(C.int64_t(v))
	case float64:
		return C.svdb_sign_double(C.double(v))
	}
	return nil
}

func callRandom() interface{} {
	return int64(C.svdb_random())
}

func callRandomblob(args []interface{}) interface{} {
	n := toInt64CGOOrDefault(args[0], 0)
	if n <= 0 {
		return []byte{}
	}

	ptr := C.svdb_randomblob(C.int64_t(n))
	if ptr == nil {
		return []byte{}
	}
	defer C.free(ptr)

	cSlice := (*[1 << 30]byte)(ptr)[:n:n]
	result := make([]byte, n)
	copy(result, cSlice)
	return result
}

func callZeroblob(args []interface{}) interface{} {
	n := toInt64CGOOrDefault(args[0], 0)
	if n <= 0 {
		return []byte{}
	}

	ptr := C.svdb_zeroblob(C.int64_t(n))
	if ptr == nil {
		return []byte{}
	}
	defer C.free(ptr)

	cSlice := (*[1 << 30]byte)(ptr)[:n:n]
	result := make([]byte, n)
	copy(result, cSlice)
	return result
}

func toFloat64CGO(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func toInt64CGOOrDefault(v interface{}, def int64) int64 {
	if v == nil {
		return def
	}
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return def
}
