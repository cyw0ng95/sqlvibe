package VM

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

func (vm *VM) Exec(ctx interface{}) error {
	for {
		inst := vm.GetInstruction()

		switch inst.Op {
		case OpNoop:
			continue

		case OpHalt:
			vm.err = nil
			if inst.P4 != nil {
				if err, ok := inst.P4.(error); ok {
					return err
				}
			}
			return nil

		case OpGoto:
			if inst.P2 != 0 {
				vm.pc = int(inst.P2)
			}
			continue

		case OpGosub:
			vm.subReturn = append(vm.subReturn, vm.pc)
			if inst.P2 != 0 {
				vm.pc = int(inst.P2)
			}
			continue

		case OpReturn:
			if len(vm.subReturn) > 0 {
				n := len(vm.subReturn)
				vm.pc = vm.subReturn[n-1]
				vm.subReturn = vm.subReturn[:n-1]
			}
			continue

		case OpInit:
			if inst.P2 != 0 {
				vm.pc = int(inst.P2)
			}
			continue

		case OpLoadConst:
			vm.registers[inst.P1] = inst.P4
			continue

		case OpNull:
			vm.registers[inst.P1] = nil
			continue

		case OpConstNull:
			vm.registers[inst.P1] = nil
			continue

		case OpMove:
			vm.registers[inst.P2] = vm.registers[inst.P1]
			continue

		case OpCopy:
			if inst.P1 != inst.P2 {
				vm.registers[inst.P2] = vm.registers[inst.P1]
			}
			continue

		case OpSCopy:
			vm.registers[inst.P2] = vm.registers[inst.P1]
			continue

		case OpIntCopy:
			if v, ok := vm.registers[inst.P1].(int64); ok {
				vm.registers[inst.P2] = v
			} else if v, ok := vm.registers[inst.P1].(int); ok {
				vm.registers[inst.P2] = int64(v)
			}
			continue

		case OpResultRow:
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				vm.results = append(vm.results, row)
			}
			continue

		case OpEq:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) == 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpNe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) != 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpLt:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) < 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpLe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) <= 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpGt:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) > 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpGe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := compareVals(lhs, rhs) >= 0
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpIs:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := (lhs == nil && rhs == nil) || (lhs != nil && rhs != nil && compareVals(lhs, rhs) == 0)
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpIsNot:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := (lhs == nil && rhs != nil) || (lhs != nil && rhs == nil) || (lhs != nil && rhs != nil && compareVals(lhs, rhs) != 0)
			if result && inst.P4 != nil {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
				}
			}
			continue

		case OpIsNull:
			if vm.registers[inst.P1] == nil {
				if inst.P2 != 0 {
					vm.pc = int(inst.P2)
				}
			}
			continue

		case OpNotNull:
			if vm.registers[inst.P1] != nil {
				if inst.P2 != 0 {
					vm.pc = int(inst.P2)
				}
			}
			continue

		case OpIfNull:
			if vm.registers[inst.P1] == nil {
				vm.registers[inst.P1] = inst.P4
			}
			continue

		case OpAdd:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericAdd(lhs, rhs)
			}
			continue

		case OpSubtract:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericSubtract(lhs, rhs)
			}
			continue

		case OpMultiply:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericMultiply(lhs, rhs)
			}
			continue

		case OpDivide:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericDivide(lhs, rhs)
			}
			continue

		case OpRemainder:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericRemainder(lhs, rhs)
			}
			continue

		case OpAddImm:
			if v, ok := vm.registers[inst.P1].(int64); ok {
				vm.registers[inst.P1] = v + int64(inst.P2)
			} else if v, ok := vm.registers[inst.P1].(float64); ok {
				vm.registers[inst.P1] = v + float64(inst.P2)
			}
			continue

		case OpConcat:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = stringConcat(lhs, rhs)
			}
			continue

		case OpSubstr:
			src := vm.registers[inst.P1]
			start := int64(1)
			length := int64(-1)
			if inst.P2 != 0 {
				if v, ok := vm.registers[inst.P2].(int64); ok {
					start = v
				}
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = stringSubstr(src, start, length)
			}
			continue

		case OpLength:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getLength(src)
			}
			continue

		case OpUpper:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getUpper(src)
			}
			continue

		case OpLower:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getLower(src)
			}
			continue

		case OpTrim, OpLTrim, OpRTrim:
			src := vm.registers[inst.P1]
			chars := " "
			if v, ok := vm.registers[inst.P2].(string); ok {
				chars = v
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getTrim(src, chars, inst.Op == OpTrim, inst.Op == OpLTrim, inst.Op == OpRTrim)
			}
			continue

		case OpReplace:
			src := vm.registers[inst.P1]
			from := ""
			to := ""
			if v, ok := vm.registers[inst.P2].(string); ok {
				from = v
			}
			if v, ok := inst.P4.(string); ok {
				to = v
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = strings.Replace(fmt.Sprintf("%v", src), from, to, -1)
			}
			continue

		case OpInstr:
			haystack := ""
			needle := ""
			if v, ok := vm.registers[inst.P1].(string); ok {
				haystack = v
			}
			if v, ok := vm.registers[inst.P2].(string); ok {
				needle = v
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = int64(strings.Index(haystack, needle) + 1)
			}
			continue

		case OpLike:
			pattern := ""
			str := ""
			if v, ok := vm.registers[inst.P1].(string); ok {
				str = v
			}
			if v, ok := vm.registers[inst.P2].(string); ok {
				pattern = v
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = int64(0)
				if likeMatch(str, pattern) {
					vm.registers[dst] = int64(1)
				}
			}
			continue

		case OpAbs:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getAbs(src)
			}
			continue

		case OpRound:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getRound(src, 0)
			}
			continue

		case OpCeil, OpCeiling:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getCeil(src)
			}
			continue

		case OpFloor:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getFloor(src)
			}
			continue

		case OpSqrt:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getSqrt(src)
			}
			continue

		case OpPow:
			base := vm.registers[inst.P1]
			exp := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getPow(base, exp)
			}
			continue

		case OpMod:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericRemainder(lhs, rhs)
			}
			continue

		case OpExp:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getExp(src)
			}
			continue

		case OpLog, OpLog10:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getLog(src)
			}
			continue

		case OpLn:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getLn(src)
			}
			continue

		case OpSin:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getSin(src)
			}
			continue

		case OpCos:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getCos(src)
			}
			continue

		case OpTan:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getTan(src)
			}
			continue

		case OpAsin:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getAsin(src)
			}
			continue

		case OpAcos:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getAcos(src)
			}
			continue

		case OpAtan:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getAtan(src)
			}
			continue

		case OpAtan2:
			y := vm.registers[inst.P1]
			x := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getAtan2(y, x)
			}
			continue

		case OpDegToRad:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getDegToRad(src)
			}
			continue

		case OpRadToDeg:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getRadToDeg(src)
			}
			continue

		case OpToText:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = toText(src)
			}
			continue

		case OpToInt:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = toInt(src)
			}
			continue

		case OpToReal:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = toReal(src)
			}
			continue

		case OpCast:
			vm.registers[inst.P1] = vm.registers[inst.P1]
			continue

		case OpColumn:
			cursorID := int(inst.P1)
			colIdx := int(inst.P2)
			dst := inst.P4
			cursor := vm.cursors.Get(cursorID)
			if cursor != nil && cursor.Data != nil && cursor.Index >= 0 && cursor.Index < len(cursor.Data) {
				row := cursor.Data[cursor.Index]
				if colIdx >= 0 && colIdx < len(cursor.Columns) {
					colName := cursor.Columns[colIdx]
					if dstReg, ok := dst.(int); ok {
						vm.registers[dstReg] = row[colName]
					}
				}
			}
			continue

		case OpOpenRead:
			tableName := inst.P3
			if tableName == "" {
				continue
			}
			vm.cursors.OpenTable(tableName, nil, nil)
			continue

		case OpRewind:
			cursorID := int(inst.P1)
			cursor := vm.cursors.Get(cursorID)
			if cursor != nil {
				cursor.Index = -1
				cursor.EOF = len(cursor.Data) == 0
			}
			continue

		case OpNext:
			cursorID := int(inst.P1)
			target := int(inst.P2)
			cursor := vm.cursors.Get(cursorID)
			if cursor != nil {
				cursor.Index++
				if cursor.Index >= len(cursor.Data) {
					cursor.EOF = true
					if target > 0 {
						vm.pc = target
					}
				} else {
					cursor.EOF = false
				}
			}
			continue

		default:
			return fmt.Errorf("unimplemented opcode: %v", inst.Op)
		}
	}
}

func compareVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	av := reflectVal(a)
	bv := reflectVal(b)

	if av.typ != bv.typ {
		if av.isNumeric() && bv.isNumeric() {
			af := av.toFloat()
			bf := bv.toFloat()
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
		return strings.Compare(av.String(), bv.String())
	}

	switch a.(type) {
	case int64:
		ai := a.(int64)
		bi := b.(int64)
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0

	case float64:
		af := a.(float64)
		bf := b.(float64)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0

	case string:
		as := a.(string)
		bs := b.(string)
		return strings.Compare(as, bs)

	case []byte:
		as := a.([]byte)
		bs := b.([]byte)
		if len(as) < len(bs) {
			return -1
		}
		if len(as) > len(bs) {
			return 1
		}
		for i := range as {
			if as[i] < bs[i] {
				return -1
			}
			if as[i] > bs[i] {
				return 1
			}
		}
		return 0
	}

	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

type refVal struct {
	v   interface{}
	typ string
}

func reflectVal(v interface{}) refVal {
	switch v.(type) {
	case nil:
		return refVal{v: v, typ: "null"}
	case int64:
		return refVal{v: v, typ: "int"}
	case int:
		return refVal{v: int64(v.(int)), typ: "int"}
	case float64:
		return refVal{v: v, typ: "real"}
	case string:
		return refVal{v: v, typ: "text"}
	case []byte:
		return refVal{v: v, typ: "blob"}
	default:
		return refVal{v: v, typ: fmt.Sprintf("%T", v)}
	}
}

func (r refVal) isNumeric() bool {
	return r.typ == "int" || r.typ == "real"
}

func (r refVal) toFloat() float64 {
	switch r.v.(type) {
	case int64:
		return float64(r.v.(int64))
	case int:
		return float64(r.v.(int))
	case float64:
		return r.v.(float64)
	case string:
		if f, err := strconv.ParseFloat(r.v.(string), 64); err == nil {
			return f
		}
	}
	return 0
}

func (r refVal) String() string {
	return fmt.Sprintf("%v", r.v)
}

func numericAdd(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	av := reflectVal(a)
	bv := reflectVal(b)

	if av.typ == "int" && bv.typ == "int" {
		return av.v.(int64) + bv.v.(int64)
	}
	return av.toFloat() + bv.toFloat()
}

func numericSubtract(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	av := reflectVal(a)
	bv := reflectVal(b)

	if av.typ == "int" && bv.typ == "int" {
		return av.v.(int64) - bv.v.(int64)
	}
	return av.toFloat() - bv.toFloat()
}

func numericMultiply(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	av := reflectVal(a)
	bv := reflectVal(b)

	if av.typ == "int" && bv.typ == "int" {
		return av.v.(int64) * bv.v.(int64)
	}
	return av.toFloat() * bv.toFloat()
}

func numericDivide(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	av := reflectVal(a)
	bv := reflectVal(b)

	if bv.toFloat() == 0 {
		return nil
	}

	if av.typ == "int" && bv.typ == "int" && bv.v.(int64) != 0 {
		return av.v.(int64) / bv.v.(int64)
	}
	return av.toFloat() / bv.toFloat()
}

func numericRemainder(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	av := reflectVal(a)
	bv := reflectVal(b)

	if av.typ == "int" && bv.typ == "int" {
		if bv.v.(int64) == 0 {
			return nil
		}
		return av.v.(int64) % bv.v.(int64)
	}
	return nil
}

func stringConcat(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}
	return fmt.Sprintf("%v%v", a, b)
}

func stringSubstr(s interface{}, start, length int64) interface{} {
	if s == nil {
		return nil
	}
	ss := fmt.Sprintf("%v", s)
	runes := []rune(ss)

	startIdx := int(start) - 1
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(runes) {
		return ""
	}

	endIdx := len(runes)
	if length >= 0 {
		endIdx = startIdx + int(length)
		if endIdx > len(runes) {
			endIdx = len(runes)
		}
	}

	return string(runes[startIdx:endIdx])
}

func getLength(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case string:
		return int64(utf8.RuneCountInString(v.(string)))
	case []byte:
		return int64(len(v.([]byte)))
	case int64:
		return int64(len(fmt.Sprintf("%d", v.(int64))))
	}
	return int64(0)
}

func getUpper(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return strings.ToUpper(s)
	}
	return fmt.Sprintf("%v", v)
}

func getLower(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return strings.ToLower(s)
	}
	return fmt.Sprintf("%v", v)
}

func getTrim(s interface{}, chars string, trimAll, left, right bool) interface{} {
	if s == nil {
		return nil
	}
	ss := fmt.Sprintf("%v", s)
	if trimAll {
		return strings.Trim(ss, chars)
	}
	if left {
		ss = strings.TrimLeft(ss, chars)
	}
	if right {
		ss = strings.TrimRight(ss, chars)
	}
	return ss
}

func getAbs(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case int64:
		if v.(int64) < 0 {
			return -v.(int64)
		}
		return v.(int64)
	case float64:
		if v.(float64) < 0 {
			return -v.(float64)
		}
		return v.(float64)
	}
	return nil
}

func getRound(v interface{}, decimals int) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	if decimals == 0 {
		return int64(math.Round(f))
	}
	m := math.Pow10(decimals)
	return math.Round(f*m) / m
}

func getCeil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Ceil(f)
}

func getFloor(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Floor(f)
}

func getSqrt(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Sqrt(f)
}

func getPow(base, exp interface{}) interface{} {
	if base == nil || exp == nil {
		return nil
	}
	b := reflectVal(base).toFloat()
	e := reflectVal(exp).toFloat()
	return math.Pow(b, e)
}

func getExp(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Exp(f)
}

func getLog(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	if f <= 0 {
		return nil
	}
	return math.Log10(f)
}

func getLn(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	if f <= 0 {
		return nil
	}
	return math.Log(f)
}

func getSin(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Sin(f)
}

func getCos(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Cos(f)
}

func getTan(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Tan(f)
}

func getAsin(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Asin(f)
}

func getAcos(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Acos(f)
}

func getAtan(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return math.Atan(f)
}

func getAtan2(y, x interface{}) interface{} {
	if y == nil || x == nil {
		return nil
	}
	yf := reflectVal(y).toFloat()
	xf := reflectVal(x).toFloat()
	return math.Atan2(yf, xf)
}

func getDegToRad(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return f * math.Pi / 180
}

func getRadToDeg(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	f := reflectVal(v).toFloat()
	return f * 180 / math.Pi
}

func toText(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return fmt.Sprintf("%v", v)
}

func toInt(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case int64:
		return v.(int64)
	case int:
		return int64(v.(int))
	case float64:
		return int64(v.(float64))
	case string:
		if i, err := strconv.ParseInt(v.(string), 10, 64); err == nil {
			return i
		}
	}
	return int64(0)
}

func toReal(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case float64:
		return v.(float64)
	case int64:
		return float64(v.(int64))
	case int:
		return float64(v.(int))
	case string:
		if f, err := strconv.ParseFloat(v.(string), 64); err == nil {
			return f
		}
	}
	return float64(0)
}

func likeMatch(str, pattern string) bool {
	return likeMatchRecursive(str, pattern, 0, 0)
}

func likeMatchRecursive(str string, pattern string, si, pi int) bool {
	for pi < len(pattern) && si < len(str) {
		pch := pattern[pi]

		if pch == '%' {
			for i := pi + 1; i <= len(pattern); i++ {
				if likeMatchRecursive(str, pattern, si, i) {
					return true
				}
			}
			return false
		}

		if pch == '_' {
			si++
			pi++
			continue
		}

		if pch == '\\' && pi+1 < len(pattern) {
			pi++
			pch = pattern[pi]
		}

		if si < len(str) && str[si] == pch {
			si++
			pi++
			continue
		}

		return false
	}

	for pi < len(pattern) {
		if pattern[pi] != '%' {
			return false
		}
		pi++
	}

	return si == len(str)
}

var ErrValueTooBig = errors.New("value too big")
