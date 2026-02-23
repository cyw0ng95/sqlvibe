package VM

import "math"

// Eval evaluates the expression bytecode against a row, returning the result.
func (eb *ExprBytecode) Eval(row []interface{}) interface{} {
	stack := make([]interface{}, 0, 8)
	argIdx := 0

	for _, op := range eb.ops {
		switch op {
		case EOpLoadColumn:
			colIdx := int(eb.args[argIdx])
			argIdx++
			if colIdx >= 0 && colIdx < len(row) {
				stack = append(stack, row[colIdx])
			} else {
				stack = append(stack, nil)
			}

		case EOpLoadConst:
			cIdx := int(eb.args[argIdx])
			argIdx++
			if cIdx >= 0 && cIdx < len(eb.consts) {
				stack = append(stack, eb.consts[cIdx])
			} else {
				stack = append(stack, nil)
			}

		case EOpAdd:
			b, a := exprPop(&stack), exprPop(&stack)
			stack = append(stack, exprNumericOp(a, b, 0))
		case EOpSub:
			b, a := exprPop(&stack), exprPop(&stack)
			stack = append(stack, exprNumericOp(a, b, 1))
		case EOpMul:
			b, a := exprPop(&stack), exprPop(&stack)
			stack = append(stack, exprNumericOp(a, b, 2))
		case EOpDiv:
			b, a := exprPop(&stack), exprPop(&stack)
			stack = append(stack, exprNumericOp(a, b, 3))
		case EOpMod:
			b, a := exprPop(&stack), exprPop(&stack)
			stack = append(stack, exprNumericOp(a, b, 4))

		case EOpEq:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) == 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpNe:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) != 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpLt:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) < 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpLe:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) <= 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpGt:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) > 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpGe:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprCompare(a, b) >= 0 {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}

		case EOpAnd:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprTruthy(a) && exprTruthy(b) {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpOr:
			b, a := exprPop(&stack), exprPop(&stack)
			if exprTruthy(a) || exprTruthy(b) {
				stack = append(stack, int64(1))
			} else {
				stack = append(stack, int64(0))
			}
		case EOpNot:
			a := exprPop(&stack)
			if exprTruthy(a) {
				stack = append(stack, int64(0))
			} else {
				stack = append(stack, int64(1))
			}
		case EOpNeg:
			a := exprPop(&stack)
			switch av := a.(type) {
			case int64:
				stack = append(stack, -av)
			case float64:
				stack = append(stack, -av)
			default:
				stack = append(stack, nil)
			}
		}	}

	if len(stack) > 0 {
		return stack[len(stack)-1]
	}
	return nil
}

func exprPop(stack *[]interface{}) interface{} {
	s := *stack
	if len(s) == 0 {
		return nil
	}
	v := s[len(s)-1]
	*stack = s[:len(s)-1]
	return v
}

func exprToFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	}
	return 0, false
}

// exprNumericOp applies op (0=add,1=sub,2=mul,3=div,4=mod) to a and b.
func exprNumericOp(a, b interface{}, op int) interface{} {
	af, aok := exprToFloat(a)
	bf, bok := exprToFloat(b)
	if aok && bok {
		// Return int64 when both inputs are integer-like and op is not division.
		ai, aIsInt := a.(int64)
		bi, bIsInt := b.(int64)
		if aIsInt && bIsInt {
			switch op {
			case 0:
				return ai + bi
			case 1:
				return ai - bi
			case 2:
				return ai * bi
			case 4:
				if bi == 0 {
					return nil
				}
				return ai % bi
			}
		}
		switch op {
		case 0:
			return af + bf
		case 1:
			return af - bf
		case 2:
			return af * bf
		case 3:
			if bf == 0 {
				return nil
			}
			return af / bf
		case 4:
			if bf == 0 {
				return nil
			}
			return math.Mod(af, bf)
		}
	}
	return nil
}

func exprCompare(a, b interface{}) int {
	af, aok := exprToFloat(a)
	bf, bok := exprToFloat(b)
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	as, aStr := a.(string)
	bs, bStr := b.(string)
	if aStr && bStr {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
	return 0
}

func exprTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch x := v.(type) {
	case int64:
		return x != 0
	case float64:
		return x != 0
	case int:
		return x != 0
	case string:
		return x != ""
	case bool:
		return x
	}
	return true
}
