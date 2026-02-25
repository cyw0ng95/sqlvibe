package VM

import (
	"math"
	"math/big"
	"strconv"
)

type ExprEvaluator struct {
	vm *VM
}

func NewExprEvaluator(vm *VM) *ExprEvaluator {
	return &ExprEvaluator{vm: vm}
}

func (e *ExprEvaluator) Eval(expr interface{}) (interface{}, error) {
	switch v := expr.(type) {
	case nil:
		return nil, nil
	case int:
		return v, nil
	case int64:
		return v, nil
	case float64:
		return v, nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return nil, nil
	}
}

func (e *ExprEvaluator) Compare(op OpCode, a, b interface{}) (int, error) {
	aVal, bVal := e.toFloat64(a), e.toFloat64(b)
	if aVal < bVal {
		return -1, nil
	}
	if aVal > bVal {
		return 1, nil
	}
	return 0, nil
}

func (e *ExprEvaluator) toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func (e *ExprEvaluator) BinaryOp(op OpCode, a, b interface{}) (interface{}, error) {
	switch op {
	case OpAdd:
		return e.add(a, b), nil
	case OpSubtract:
		return e.sub(a, b), nil
	case OpMultiply:
		return e.mul(a, b), nil
	case OpDivide:
		return e.div(a, b), nil
	case OpRemainder:
		return e.mod(a, b), nil
	case OpConcat:
		if a == nil || b == nil {
			return nil, nil
		}
		return e.toString(a) + e.toString(b), nil
	default:
		return nil, nil
	}
}

func (e *ExprEvaluator) add(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	if isFloat64(a) || isFloat64(b) {
		av := e.toFloat64(a)
		bv := e.toFloat64(b)
		return av + bv
	}

	aInt, aIsInt := e.toInteger(a)
	bInt, bIsInt := e.toInteger(b)

	if aIsInt && bIsInt {
		ai := new(big.Int).SetInt64(aInt)
		bi := new(big.Int).SetInt64(bInt)
		result := new(big.Int).Add(ai, bi)
		if result.IsInt64() {
			return result.Int64()
		}
		f, _ := result.Float64()
		return f
	}

	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	return av + bv
}

func (e *ExprEvaluator) sub(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	if isFloat64(a) || isFloat64(b) {
		av := e.toFloat64(a)
		bv := e.toFloat64(b)
		return av - bv
	}

	aInt, aIsInt := e.toInteger(a)
	bInt, bIsInt := e.toInteger(b)

	if aIsInt && bIsInt {
		ai := new(big.Int).SetInt64(aInt)
		bi := new(big.Int).SetInt64(bInt)
		result := new(big.Int).Sub(ai, bi)
		if result.IsInt64() {
			return result.Int64()
		}
		f, _ := result.Float64()
		return f
	}

	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	return av - bv
}

func (e *ExprEvaluator) mul(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	if isFloat64(a) || isFloat64(b) {
		av := e.toFloat64(a)
		bv := e.toFloat64(b)
		return av * bv
	}

	aInt, aIsInt := e.toInteger(a)
	bInt, bIsInt := e.toInteger(b)

	if aIsInt && bIsInt {
		ai := new(big.Int).SetInt64(aInt)
		bi := new(big.Int).SetInt64(bInt)
		result := new(big.Int).Mul(ai, bi)
		if result.IsInt64() {
			return result.Int64()
		}
		f, _ := result.Float64()
		return f
	}

	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	return av * bv
}

func (e *ExprEvaluator) div(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	if isFloat64(a) || isFloat64(b) {
		av := e.toFloat64(a)
		bv := e.toFloat64(b)
		if bv == 0 {
			return nil
		}
		return av / bv
	}

	aIsInt := isInteger(a)
	bIsInt := isInteger(b)
	if aIsInt && bIsInt {
		aInt, _ := e.toInteger(a)
		bInt, _ := e.toInteger(b)
		if bInt == 0 {
			return nil
		}
		ai := new(big.Int).SetInt64(aInt)
		bi := new(big.Int).SetInt64(bInt)

		absA := new(big.Int).Abs(ai)
		absB := new(big.Int).Abs(bi)
		quotient := new(big.Int).Div(absA, absB)

		if (aInt < 0 && bInt > 0) || (aInt > 0 && bInt < 0) {
			quotient.Neg(quotient)
		}

		if quotient.IsInt64() {
			return quotient.Int64()
		}
		f, _ := quotient.Float64()
		return f
	}
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	if bv == 0 {
		return nil
	}
	return av / bv
}

// toInteger attempts to convert a value to int64 for integer operations
// Returns false for float64 values - they should be treated as floats, not integers
func (e *ExprEvaluator) toInteger(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		if math.IsInf(val, 0) || math.IsNaN(val) {
			return 0, false
		}
		if val == float64(int64(val)) {
			return int64(val), true
		}
		return 0, false
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, true
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			if math.IsInf(f, 0) || math.IsNaN(f) {
				return 0, false
			}
			if f == float64(int64(f)) {
				return int64(f), true
			}
			return 0, false
		}
		return 0, false
	default:
		return 0, false
	}
}

// isInteger checks if a value is an integer type
func isInteger(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return true
	}
	return false
}

func isFloat64(v interface{}) bool {
	switch v.(type) {
	case float64:
		return true
	}
	return false
}

func (e *ExprEvaluator) mod(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	if isFloat64(a) || isFloat64(b) {
		av := e.toFloat64(a)
		bv := e.toFloat64(b)
		if bv == 0 {
			return nil
		}
		return float64(int64(av) % int64(bv))
	}

	aIsInt := isInteger(a)
	bIsInt := isInteger(b)
	if aIsInt && bIsInt {
		aInt, _ := e.toInteger(a)
		bInt, _ := e.toInteger(b)
		if bInt == 0 {
			return nil
		}
		ai := new(big.Int).SetInt64(aInt)
		bi := new(big.Int).SetInt64(bInt)

		absA := new(big.Int).Abs(ai)
		absB := new(big.Int).Abs(bi)
		quotient := new(big.Int).Div(absA, absB)
		if (aInt < 0 && bInt > 0) || (aInt > 0 && bInt < 0) {
			quotient.Neg(quotient)
		}

		quotient.Mul(quotient, bi)
		result := new(big.Int).Sub(ai, quotient)

		if result.IsInt64() {
			return result.Int64()
		}
		f, _ := result.Float64()
		return f
	}
	return nil
}

func (e *ExprEvaluator) toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int64, float64:
		return strconv.FormatFloat(e.toFloat64(val), 'f', -1, 64)
	default:
		return ""
	}
}

func (e *ExprEvaluator) IsNull(v interface{}) bool {
	return v == nil
}
