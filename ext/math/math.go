// Package math implements the sqlvibe Math extension, providing additional
// mathematical SQL functions.
//
// Register this extension by building with the SVDB_EXT_MATH build tag:
//
//	go build -tags SVDB_EXT_MATH ./...
//
// Note: Without SVDB_EXT_MATH, ABS, CEIL, CEILING, FLOOR, ROUND will NOT be available.
package math

import (
	gomath "math"
	mathrand "math/rand"
	"strings"

	"github.com/cyw0ng95/sqlvibe/ext"
)

// MathExtension implements the Math extension.
type MathExtension struct{}

func (e *MathExtension) Name() string        { return "math" }
func (e *MathExtension) Description() string { return "Math extension" }

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
	switch strings.ToUpper(name) {
	case "ABS":
		return evalAbs(args)
	case "CEIL", "CEILING":
		return evalCeil(args)
	case "FLOOR":
		return evalFloor(args)
	case "ROUND":
		return evalRound(args)
	case "POWER", "POW":
		return evalPower(args)
	case "SQRT":
		return evalSqrt(args)
	case "MOD":
		return evalMod(args)
	case "PI":
		return gomath.Pi
	case "EXP":
		return evalExp(args)
	case "LN":
		return evalLn(args)
	case "LOG":
		return evalLog(args)
	case "LOG2":
		return evalLog2(args)
	case "LOG10":
		return evalLog10(args)
	case "SIGN":
		return evalSign(args)
	case "RANDOM":
		return evalRandom(args)
	case "RANDOMBLOB":
		return evalRandombLOB(args)
	case "ZEROBLOB":
		return evalZerobLOB(args)
	}
	return nil
}

func init() {
	ext.Register("math", &MathExtension{})
}

// ---------- helpers ----------

func toFloat64Math(v interface{}) (float64, bool) {
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

// ---------- function implementations ----------

func evalAbs(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	val := args[0]
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		if v < 0 {
			return -v
		}
		return v
	case float64:
		return gomath.Abs(v)
	}
	return val
}

func evalCeil(args []interface{}) interface{} {
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
		return gomath.Ceil(v)
	}
	return val
}

func evalFloor(args []interface{}) interface{} {
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
		return gomath.Floor(v)
	}
	return val
}

func evalRound(args []interface{}) interface{} {
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
		if decimals == 0 {
			return v
		}
		if decimals < 0 {
			divisor := gomath.Pow10(-decimals)
			return int64(gomath.Round(float64(v)/divisor) * divisor)
		}
		divisor := gomath.Pow10(decimals)
		return gomath.Round(float64(v)*divisor) / divisor
	case float64:
		if decimals == 0 {
			return gomath.Round(v)
		}
		if decimals < 0 {
			divisor := gomath.Pow10(-decimals)
			return gomath.Round(v/divisor) * divisor
		}
		divisor := gomath.Pow10(decimals)
		return gomath.Round(v*divisor) / divisor
	}
	return val
}

func evalPower(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	base, ok1 := toFloat64Math(args[0])
	exp, ok2 := toFloat64Math(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	return gomath.Pow(base, exp)
}

func evalSqrt(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64Math(args[0])
	if !ok {
		return nil
	}
	if v < 0 {
		return nil
	}
	return gomath.Sqrt(v)
}

func evalMod(args []interface{}) interface{} {
	if len(args) < 2 {
		return nil
	}
	switch a := args[0].(type) {
	case int64:
		switch b := args[1].(type) {
		case int64:
			if b == 0 {
				return nil
			}
			return a % b
		}
	}
	af, ok1 := toFloat64Math(args[0])
	bf, ok2 := toFloat64Math(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	if bf == 0 {
		return nil
	}
	return gomath.Mod(af, bf)
}

func evalExp(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64Math(args[0])
	if !ok {
		return nil
	}
	return gomath.Exp(v)
}

func evalLn(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64Math(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return gomath.Log(v)
}

func evalLog(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	if len(args) == 1 {
		// log(X) = natural log (same as LN)
		v, ok := toFloat64Math(args[0])
		if !ok || v <= 0 {
			return nil
		}
		return gomath.Log(v)
	}
	// log(B, X) = log base B of X
	b, ok1 := toFloat64Math(args[0])
	x, ok2 := toFloat64Math(args[1])
	if !ok1 || !ok2 || b <= 0 || b == 1 || x <= 0 {
		return nil
	}
	return gomath.Log(x) / gomath.Log(b)
}

func evalLog2(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64Math(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return gomath.Log2(v)
}

func evalLog10(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	v, ok := toFloat64Math(args[0])
	if !ok || v <= 0 {
		return nil
	}
	return gomath.Log10(v)
}

func evalSign(args []interface{}) interface{} {
	if len(args) < 1 {
		return nil
	}
	switch x := args[0].(type) {
	case int64:
		if x > 0 {
			return int64(1)
		} else if x < 0 {
			return int64(-1)
		}
		return int64(0)
	case float64:
		if x > 0 {
			return float64(1)
		} else if x < 0 {
			return float64(-1)
		}
		return float64(0)
	}
	return nil
}

func evalRandom(args []interface{}) interface{} {
	return int64(mathrand.Uint64())
}

func evalRandombLOB(args []interface{}) interface{} {
	if len(args) < 1 {
		return []byte{}
	}
	n, ok := toInt64Math(args[0])
	if !ok || n <= 0 {
		return []byte{}
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(mathrand.Intn(256))
	}
	return b
}

func evalZerobLOB(args []interface{}) interface{} {
	if len(args) < 1 {
		return []byte{}
	}
	n, ok := toInt64Math(args[0])
	if !ok || n <= 0 {
		return []byte{}
	}
	return make([]byte, n)
}

func toInt64Math(v interface{}) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), true
	}
	return 0, false
}
