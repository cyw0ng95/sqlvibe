package QE

import (
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
		return e.toString(a) + e.toString(b), nil
	default:
		return nil, nil
	}
}

func (e *ExprEvaluator) add(a, b interface{}) interface{} {
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	if av == float64(int(av)) && bv == float64(int(bv)) {
		return int(av) + int(bv)
	}
	return av + bv
}

func (e *ExprEvaluator) sub(a, b interface{}) interface{} {
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	return av - bv
}

func (e *ExprEvaluator) mul(a, b interface{}) interface{} {
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	return av * bv
}

func (e *ExprEvaluator) div(a, b interface{}) interface{} {
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	if bv == 0 {
		return nil
	}
	// SQLite does integer division when both operands are integers
	aIsInt := isInteger(a)
	bIsInt := isInteger(b)
	if aIsInt && bIsInt {
		return int64(int(av) / int(bv))
	}
	return av / bv
}

func isInteger(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return true
	}
	return false
}

func (e *ExprEvaluator) mod(a, b interface{}) interface{} {
	av := e.toFloat64(a)
	bv := e.toFloat64(b)
	if bv == 0 {
		return nil
	}
	return int(av) % int(bv)
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
