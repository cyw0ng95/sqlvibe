package VM

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// MaxVMIterations is the maximum number of VM instructions that can be executed
// before the VM panics with an assertion error. This prevents infinite loops.
const MaxVMIterations = 1000000

func (vm *VM) Exec(ctx interface{}) error {
	iterationCount := 0
	checkInterval := 1000 // Check for infinite loop every 1000 iterations
	for {
		// Check for infinite loop less frequently for better performance
		iterationCount++
		if iterationCount%checkInterval == 0 && iterationCount > MaxVMIterations {
			util.Assert(false, "VM execution exceeded %d iterations - possible infinite loop detected. This usually indicates a bug in the query compiler (e.g., incorrect jump targets in JOIN compilation).", MaxVMIterations)
		}

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
				n := len(regs)
				start := len(vm.flatBuf)
				needed := start + n
				if needed > cap(vm.flatBuf) {
					// Grow with 2× amortised doubling plus a minimum pad so that the
					// first growth of a zero-capacity buffer allocates more than one row.
					const flatBufMinGrowth = 64
					newCap := needed*2 + flatBufMinGrowth
					newBuf := make([]interface{}, start, newCap)
					copy(newBuf, vm.flatBuf)
					vm.flatBuf = newBuf
				}
				vm.flatBuf = vm.flatBuf[:start+n]
				for i, reg := range regs {
					vm.flatBuf[start+i] = vm.registers[reg]
				}
				vm.results = append(vm.results, vm.flatBuf[start:start+n])
			}
			continue

		case OpIf:
			// Jump to P2 if register P1 is true (non-zero, non-null)
			val := vm.registers[inst.P1]
			shouldJump := false
			if val != nil {
				switch v := val.(type) {
				case int64:
					shouldJump = v != 0
				case float64:
					shouldJump = v != 0.0
				case bool:
					shouldJump = v
				case string:
					shouldJump = len(v) > 0
				default:
					shouldJump = true
				}
			}
			if shouldJump && inst.P2 != 0 {
				vm.pc = int(inst.P2)
			}
			continue

		case OpIfNot:
			// Jump to P2 if register P1 is false (zero, null, or empty)
			val := vm.registers[inst.P1]
			shouldJump := false
			if val == nil {
				shouldJump = true
			} else {
				switch v := val.(type) {
				case int64:
					shouldJump = v == 0
				case float64:
					shouldJump = v == 0.0
				case bool:
					shouldJump = !v
				case string:
					shouldJump = len(v) == 0
				default:
					shouldJump = false
				}
			}
			if shouldJump && inst.P2 != 0 {
				vm.pc = int(inst.P2)
			}
			continue

		case OpEq:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) == 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpNe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) != 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpLt:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) < 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpLe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) <= 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpGt:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) > 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpGe:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]

			// Handle NULL comparisons - any comparison with NULL returns NULL
			if lhs == nil || rhs == nil {
				if inst.P4 != nil {
					if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
						vm.registers[dst] = nil
					}
				}
				continue
			}

			result := compareVals(lhs, rhs) >= 0

			// Store result as 0 or 1 if P4 is a destination register
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// P4 is a jump target for WHERE clauses
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpIs:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := (lhs == nil && rhs == nil) || (lhs != nil && rhs != nil && compareVals(lhs, rhs) == 0)

			// P4 can be either a register (for storing result) or a jump target
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					// Store boolean result in register
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// Jump if result is true
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
				}
			}
			continue

		case OpIsNot:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			result := (lhs == nil && rhs != nil) || (lhs != nil && rhs == nil) || (lhs != nil && rhs != nil && compareVals(lhs, rhs) != 0)

			// P4 can be either a register (for storing result) or a jump target
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					// Store boolean result in register
					if result {
						vm.registers[dst] = int64(1)
					} else {
						vm.registers[dst] = int64(0)
					}
				} else if result {
					// Jump if result is true
					if target, ok := inst.P4.(int); ok {
						vm.pc = target
					}
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

		case OpIfNull2:
			src := vm.registers[inst.P1]
			fallback := vm.registers[inst.P2]
			if dst, ok := inst.P4.(int); ok {
				if src == nil {
					vm.registers[dst] = fallback
				} else {
					vm.registers[dst] = src
				}
			}
			continue

		case OpAdd:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if inst.HasDst {
				vm.registers[inst.DstReg] = numericAdd(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericAdd(lhs, rhs)
			}
			continue

		case OpSubtract:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if inst.HasDst {
				vm.registers[inst.DstReg] = numericSubtract(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericSubtract(lhs, rhs)
			}
			continue

		case OpMultiply:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if inst.HasDst {
				vm.registers[inst.DstReg] = numericMultiply(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericMultiply(lhs, rhs)
			}
			continue

		case OpDivide:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if inst.HasDst {
				vm.registers[inst.DstReg] = numericDivide(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericDivide(lhs, rhs)
			}
			continue

		case OpRemainder:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			if inst.HasDst {
				vm.registers[inst.DstReg] = numericRemainder(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = numericRemainder(lhs, rhs)
			}
			continue

		case OpBitAnd:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			// If either operand is NULL, result is NULL
			if lhs == nil || rhs == nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					vm.registers[dst] = nil
				}
				continue
			}
			if dst, ok := inst.P4.(int); ok {
				// Bitwise AND: convert to int64 and AND
				lhsInt := toInt64(lhs)
				rhsInt := toInt64(rhs)
				vm.registers[dst] = lhsInt & rhsInt
			}
			continue

		case OpBitOr:
			lhs := vm.registers[inst.P1]
			rhs := vm.registers[inst.P2]
			// If either operand is NULL, result is NULL
			if lhs == nil || rhs == nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					vm.registers[dst] = nil
				}
				continue
			}
			if dst, ok := inst.P4.(int); ok {
				// Bitwise OR: convert to int64 and OR
				lhsInt := toInt64(lhs)
				rhsInt := toInt64(rhs)
				vm.registers[dst] = lhsInt | rhsInt
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
			if inst.HasDst {
				vm.registers[inst.DstReg] = stringConcat(lhs, rhs)
			} else if dst, ok := inst.P4.(int); ok {
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
			// Check P3 for length register (format: "len:<register>")
			if strings.HasPrefix(inst.P3, "len:") {
				if lenReg, err := strconv.Atoi(inst.P3[4:]); err == nil {
					if v, ok := vm.registers[lenReg].(int64); ok {
						length = v
					}
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
			// P2=0 means no characters specified, use default space
			if inst.P2 != 0 {
				if v, ok := vm.registers[inst.P2].(string); ok {
					chars = v
				}
			}
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = getTrim(src, chars, inst.Op == OpTrim, inst.Op == OpLTrim, inst.Op == OpRTrim)
			}
			continue

		case OpReplace:
			dst, ok := inst.P4.(int)
			if !ok {
				continue
			}
			srcVal := vm.registers[dst]
			if srcVal == nil {
				// REPLACE(NULL, ...) = NULL
				continue
			}
			srcStr := fmt.Sprintf("%v", srcVal)
			from := ""
			to := ""
			if v, ok := vm.registers[inst.P1].(string); ok {
				from = v
			}
			if v, ok := vm.registers[inst.P2].(string); ok {
				to = v
			}
			vm.registers[dst] = strings.Replace(srcStr, from, to, -1)
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
			// NULL LIKE anything = NULL (treat as NULL/false)
			if vm.registers[inst.P1] == nil || vm.registers[inst.P2] == nil {
				if dst, ok := inst.P4.(int); ok {
					vm.registers[dst] = nil
				}
				continue
			}
			pattern := ""
			str := ""
			if v, ok := vm.registers[inst.P1].(string); ok {
				str = v
			} else {
				str = fmt.Sprintf("%v", vm.registers[inst.P1])
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

		case OpNotLike:
			// NULL NOT LIKE anything = NULL (treat as NULL/false)
			if vm.registers[inst.P1] == nil || vm.registers[inst.P2] == nil {
				if dst, ok := inst.P4.(int); ok {
					vm.registers[dst] = nil
				}
				continue
			}
			str := ""
			pattern := ""
			if v, ok := vm.registers[inst.P1].(string); ok {
				str = v
			} else {
				str = fmt.Sprintf("%v", vm.registers[inst.P1])
			}
			if v, ok := vm.registers[inst.P2].(string); ok {
				pattern = v
			}
			if dst, ok := inst.P4.(int); ok {
				if likeMatch(str, pattern) {
					vm.registers[dst] = int64(0)
				} else {
					vm.registers[dst] = int64(1)
				}
			}
			continue

		case OpGlob:
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
				if globMatch(str, pattern) {
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

		case OpTypeof:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				if src == nil {
					vm.registers[dst] = "null"
				} else {
					switch src.(type) {
					case int64:
						vm.registers[dst] = "integer"
					case float64:
						vm.registers[dst] = "real"
					case string:
						vm.registers[dst] = "text"
					case []byte:
						vm.registers[dst] = "blob"
					default:
						vm.registers[dst] = "text"
					}
				}
			}
			continue

		case OpRandom:
			if dst, ok := inst.P4.(int); ok {
				vm.registers[dst] = rand.Int63()
			}
			continue

		case OpCallScalar:
			if info, ok := inst.P4.(*ScalarCallInfo); ok {
				// Build synthetic FuncCall with arg values from registers
				argExprs := make([]QP.Expr, len(info.ArgRegs))
				for i, reg := range info.ArgRegs {
					argExprs[i] = &QP.Literal{Value: vm.registers[reg]}
				}
				syntheticCall := &QP.FuncCall{Name: info.Name, Args: argExprs}
				vm.registers[info.Dst] = vm.evaluateFuncCallOnRow(nil, nil, syntheticCall)
			}
			continue

		case OpRound:
			src := vm.registers[inst.P1]
			if dst, ok := inst.P4.(int); ok {
				decimals := 0
				if inst.P2 != 0 {
					if dv, ok2 := vm.registers[inst.P2].(int64); ok2 {
						decimals = int(dv)
					} else if dv, ok2 := vm.registers[inst.P2].(float64); ok2 {
						decimals = int(dv)
					}
				}
				vm.registers[dst] = getRound(src, decimals)
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
			val := vm.registers[inst.P1]

			// Try to get TypeSpec from P4, fall back to string for backward compatibility
			var typeName string

			if typeSpec, ok := inst.P4.(QP.TypeSpec); ok {
				typeName = typeSpec.Name
			} else if typeStr, ok := inst.P4.(string); ok {
				// Backward compatibility: P4 is a string
				typeName = typeStr
			}

			if val != nil && typeName != "" {
				upperType := strings.ToUpper(typeName)
				switch upperType {
				case "INTEGER", "INT":
					if s, ok := val.(string); ok {
						if iv, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
							vm.registers[inst.P1] = iv
						} else if fv, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
							vm.registers[inst.P1] = int64(fv)
						} else {
							// SQLite-style: parse numeric prefix from string
							vm.registers[inst.P1] = parseNumericPrefix(s)
						}
					} else if fv, ok := val.(float64); ok {
						vm.registers[inst.P1] = int64(fv)
					}
				case "REAL", "FLOAT", "DOUBLE":
					if s, ok := val.(string); ok {
						if fv, err := strconv.ParseFloat(s, 64); err == nil {
							vm.registers[inst.P1] = fv
						} else {
							vm.registers[inst.P1] = float64(0)
						}
					} else if iv, ok := val.(int64); ok {
						vm.registers[inst.P1] = float64(iv)
					}
				case "NUMERIC", "DECIMAL":
					// Handle NUMERIC/DECIMAL with precision and scale
					var floatVal float64

					if s, ok := val.(string); ok {
						if fv, err := strconv.ParseFloat(s, 64); err == nil {
							floatVal = fv
						} else {
							floatVal = 0.0
						}
					} else if iv, ok := val.(int64); ok {
						floatVal = float64(iv)
					} else if fv, ok := val.(float64); ok {
						floatVal = fv
					}

					// Apply precision and scale if specified
					// SQLite compatibility: DECIMAL/NUMERIC CAST does not round to scale, just returns float value
					vm.registers[inst.P1] = floatVal
				case "TEXT", "VARCHAR", "CHAR", "CHARACTER":
					if s, ok := val.(string); ok {
						vm.registers[inst.P1] = s
					} else if iv, ok := val.(int64); ok {
						vm.registers[inst.P1] = strconv.FormatInt(iv, 10)
					} else if fv, ok := val.(float64); ok {
						vm.registers[inst.P1] = strconv.FormatFloat(fv, 'f', -1, 64)
					} else if bv, ok := val.([]byte); ok {
						vm.registers[inst.P1] = string(bv)
					} else {
						vm.registers[inst.P1] = fmt.Sprintf("%v", val)
					}
				case "BLOB":
					// Handle BLOB type
					if s, ok := val.(string); ok {
						vm.registers[inst.P1] = []byte(s)
					} else if bv, ok := val.([]byte); ok {
						vm.registers[inst.P1] = bv
					} else {
						vm.registers[inst.P1] = []byte(fmt.Sprintf("%v", val))
					}
				case "DATE", "TIME", "TIMESTAMP", "DATETIME", "YEAR":
					// SQLite treats DATE/TIME/TIMESTAMP as NUMERIC affinity (leading-integer parsing)
					if s, ok := val.(string); ok {
						vm.registers[inst.P1] = parseNumericPrefix(s)
					} else if fv, ok := val.(float64); ok {
						vm.registers[inst.P1] = int64(fv)
					}
				}
			}
			continue

		case OpColumn:
			cursorID := int(inst.P1)
			colIdx := int(inst.P2)
			tableQualifier := inst.P3 // Table qualifier if present (string), or "table.column" for outer ref
			dst := inst.P4
			cursor := vm.cursors.Get(cursorID)

			// Special case: colIdx=-1 means this is definitely an outer reference
			// P3 contains "table.column" format
			if colIdx == -1 && tableQualifier != "" && vm.ctx != nil {
				type OuterContextProvider interface {
					GetOuterRowValue(columnName string) (interface{}, bool)
				}

				if outerCtx, ok := vm.ctx.(OuterContextProvider); ok {
					// Parse "table.column" to extract column name
					parts := strings.Split(tableQualifier, ".")
					if len(parts) == 2 {
						colName := parts[1]
						// fmt.Printf("DEBUG OpColumn: colIdx=-1 (outer reference), trying GetOuterRowValue(%q)\n", colName)
						if val, found := outerCtx.GetOuterRowValue(colName); found {
							// fmt.Printf("DEBUG OpColumn: found in outer context: %v\n", val)
							if dstReg, ok := dst.(int); ok {
								vm.registers[dstReg] = val
								continue
							}
						} else {
							// fmt.Printf("DEBUG OpColumn: NOT found in outer context\n")
						}
					}
				}
				// If not found in outer context, emit NULL
				if dstReg, ok := dst.(int); ok {
					vm.registers[dstReg] = nil
				}
				continue
			}

			// For correlation: if we have a table qualifier and outer context,
			// check if this might be an outer reference
			shouldTryOuter := false
			if tableQualifier != "" && vm.ctx != nil && cursor != nil {
				// If the table qualifier doesn't match the current cursor's table name,
				// or if the current table is aliased differently, try outer context
				// fmt.Printf("DEBUG OpColumn: tableQualifier=%q, cursor.TableName=%q\n", tableQualifier, cursor.TableName)
				if cursor.TableName == "" || tableQualifier != cursor.TableName {
					shouldTryOuter = true
					// fmt.Printf("DEBUG OpColumn: shouldTryOuter=true\n")
				}
			}

			// Try to resolve from outer context first if needed
			if shouldTryOuter {
				type OuterContextProvider interface {
					GetOuterRowValue(columnName string) (interface{}, bool)
				}

				if outerCtx, ok := vm.ctx.(OuterContextProvider); ok {
					if colIdx >= 0 && colIdx < len(cursor.Columns) {
						colName := cursor.Columns[colIdx]
						// Try to get from outer context
						// fmt.Printf("DEBUG OpColumn: trying GetOuterRowValue(%q)\n", colName)
						if val, found := outerCtx.GetOuterRowValue(colName); found {
							// fmt.Printf("DEBUG OpColumn: found in outer context: %v\n", val)
							if dstReg, ok := dst.(int); ok {
								vm.registers[dstReg] = val
								continue
							}
						} else {
							// fmt.Printf("DEBUG OpColumn: NOT found in outer context\n")
						}
					}
				}
			}

			// Default: load from current cursor
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

		case OpScalarSubquery:
			// Execute a scalar subquery and store the result in a register
			// P1 = destination register
			// P4 = SelectStmt to execute
			dstReg := int(inst.P1)

			// For non-correlated scalar subqueries, use cache to avoid re-execution
			if selectStmt, ok := inst.P4.(*QP.SelectStmt); ok && !isSubqueryCorrelated(selectStmt) {
				if vm.subqueryCache == nil {
					vm.subqueryCache = newSubqueryResultCache()
				}
				if cachedVal, hit := vm.subqueryCache.scalars[selectStmt]; hit {
					vm.registers[dstReg] = cachedVal
					continue
				}
				// Execute once and cache
				if vm.ctx != nil {
					type SubqueryExecutor interface {
						ExecuteSubquery(subquery interface{}) (interface{}, error)
					}
					if executor, ok2 := vm.ctx.(SubqueryExecutor); ok2 {
						if result, err2 := executor.ExecuteSubquery(selectStmt); err2 == nil {
							vm.subqueryCache.scalars[selectStmt] = result
							vm.registers[dstReg] = result
							continue
						}
					}
				}
			}

			// Try to execute the subquery through the context
			if vm.ctx != nil {
				// Try context-aware executor first (for correlated subqueries)
				type SubqueryExecutorWithContext interface {
					ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					if result, err := executor.ExecuteSubqueryWithContext(inst.P4, currentRow); err == nil {
						vm.registers[dstReg] = result
					} else {
						vm.registers[dstReg] = nil
					}
					continue
				}

				// Fallback to non-context executor
				type SubqueryExecutor interface {
					ExecuteSubquery(subquery interface{}) (interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryExecutor); ok {
					if result, err := executor.ExecuteSubquery(inst.P4); err == nil {
						vm.registers[dstReg] = result
					} else {
						vm.registers[dstReg] = nil
					}
				} else {
					vm.registers[dstReg] = nil
				}
			} else {
				vm.registers[dstReg] = nil
			}
			continue

		case OpExistsSubquery:
			// Execute EXISTS subquery: returns 1 if subquery returns any rows, 0 otherwise
			// P1 = destination register
			// P4 = SelectStmt to execute
			dstReg := int(inst.P1)

			// For non-correlated EXISTS subqueries, use cache
			if selectStmt, ok := inst.P4.(*QP.SelectStmt); ok && !isSubqueryCorrelated(selectStmt) {
				if vm.subqueryCache == nil {
					vm.subqueryCache = newSubqueryResultCache()
				}
				if cachedExists, hit := vm.subqueryCache.exists[selectStmt]; hit {
					if cachedExists {
						vm.registers[dstReg] = int64(1)
					} else {
						vm.registers[dstReg] = int64(0)
					}
					continue
				}
				if vm.ctx != nil {
					type SubqueryRowsExecutor interface {
						ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
					}
					if executor, ok2 := vm.ctx.(SubqueryRowsExecutor); ok2 {
						if rows, err2 := executor.ExecuteSubqueryRows(selectStmt); err2 == nil {
							hasRows := len(rows) > 0
							vm.subqueryCache.exists[selectStmt] = hasRows
							if hasRows {
								vm.registers[dstReg] = int64(1)
							} else {
								vm.registers[dstReg] = int64(0)
							}
							continue
						}
					}
				}
			}

			if vm.ctx != nil {
				// Fast path: use EXISTS-optimised executor (LIMIT 1 short-circuit).
				type ExistsSubqueryExecutor interface {
					ExecuteExistsSubquery(subquery interface{}, outerRow map[string]interface{}) (bool, error)
				}
				if executor, ok := vm.ctx.(ExistsSubqueryExecutor); ok {
					currentRow := vm.getCurrentRow(0)
					if hasRows, err := executor.ExecuteExistsSubquery(inst.P4, currentRow); err == nil {
						if hasRows {
							vm.registers[dstReg] = int64(1)
						} else {
							vm.registers[dstReg] = int64(0)
						}
						continue
					}
				}

				// Fallback: context-aware full row executor
				type SubqueryRowsExecutorWithContext interface {
					ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					if rows, err := executor.ExecuteSubqueryRowsWithContext(inst.P4, currentRow); err == nil && len(rows) > 0 {
						vm.registers[dstReg] = int64(1)
					} else {
						vm.registers[dstReg] = int64(0)
					}
					continue
				}

				// Fallback to non-context executor
				type SubqueryRowsExecutor interface {
					ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutor); ok {
					if rows, err := executor.ExecuteSubqueryRows(inst.P4); err == nil && len(rows) > 0 {
						vm.registers[dstReg] = int64(1)
					} else {
						vm.registers[dstReg] = int64(0)
					}
				} else {
					vm.registers[dstReg] = int64(0)
				}
			} else {
				vm.registers[dstReg] = int64(0)
			}
			continue

		case OpNotExistsSubquery:
			// Execute NOT EXISTS subquery: returns 1 if subquery returns no rows, 0 otherwise
			// P1 = destination register
			// P4 = SelectStmt to execute
			dstReg := int(inst.P1)

			// For non-correlated NOT EXISTS subqueries, use cache
			if selectStmt, ok := inst.P4.(*QP.SelectStmt); ok && !isSubqueryCorrelated(selectStmt) {
				if vm.subqueryCache == nil {
					vm.subqueryCache = newSubqueryResultCache()
				}
				if cachedExists, hit := vm.subqueryCache.exists[selectStmt]; hit {
					if cachedExists {
						vm.registers[dstReg] = int64(0) // rows exist → NOT EXISTS = false
					} else {
						vm.registers[dstReg] = int64(1) // no rows → NOT EXISTS = true
					}
					continue
				}
				if vm.ctx != nil {
					type SubqueryRowsExecutor interface {
						ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
					}
					if executor, ok2 := vm.ctx.(SubqueryRowsExecutor); ok2 {
						if rows, err2 := executor.ExecuteSubqueryRows(selectStmt); err2 == nil {
							hasRows := len(rows) > 0
							vm.subqueryCache.exists[selectStmt] = hasRows
							if hasRows {
								vm.registers[dstReg] = int64(0)
							} else {
								vm.registers[dstReg] = int64(1)
							}
							continue
						}
					}
				}
			}

			if vm.ctx != nil {
				// Fast path: use EXISTS-optimised executor (LIMIT 1 short-circuit).
				type ExistsSubqueryExecutor interface {
					ExecuteExistsSubquery(subquery interface{}, outerRow map[string]interface{}) (bool, error)
				}
				if executor, ok := vm.ctx.(ExistsSubqueryExecutor); ok {
					currentRow := vm.getCurrentRow(0)
					if hasRows, err := executor.ExecuteExistsSubquery(inst.P4, currentRow); err == nil {
						if hasRows {
							vm.registers[dstReg] = int64(0) // rows exist → NOT EXISTS = false
						} else {
							vm.registers[dstReg] = int64(1) // no rows → NOT EXISTS = true
						}
						continue
					}
				}

				// Fallback: context-aware full row executor
				type SubqueryRowsExecutorWithContext interface {
					ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					if rows, err := executor.ExecuteSubqueryRowsWithContext(inst.P4, currentRow); err == nil && len(rows) > 0 {
						vm.registers[dstReg] = int64(0) // rows exist, so NOT EXISTS is false
					} else {
						vm.registers[dstReg] = int64(1) // no rows, so NOT EXISTS is true
					}
					continue
				}

				// Fallback to non-context executor
				type SubqueryRowsExecutor interface {
					ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutor); ok {
					if rows, err := executor.ExecuteSubqueryRows(inst.P4); err == nil && len(rows) > 0 {
						vm.registers[dstReg] = int64(0) // rows exist, so NOT EXISTS is false
					} else {
						vm.registers[dstReg] = int64(1) // no rows, so NOT EXISTS is true
					}
				} else {
					vm.registers[dstReg] = int64(1)
				}
			} else {
				vm.registers[dstReg] = int64(1)
			}
			continue

		case OpInSubquery:
			// Execute IN subquery: check if value is in the result set
			// P1 = destination register
			// P2 = value register to check
			// P4 = SelectStmt to execute
			dstReg := int(inst.P1)
			valueReg := int(inst.P2)
			value := vm.registers[valueReg]

			// For non-correlated IN subqueries, materialize into a hash set once
			if selectStmt, ok := inst.P4.(*QP.SelectStmt); ok && !isSubqueryCorrelated(selectStmt) {
				if vm.subqueryCache == nil {
					vm.subqueryCache = newSubqueryResultCache()
				}
				hashSet, hit := vm.subqueryCache.hashSets[selectStmt]
				if !hit {
					// Execute once and build hash set
					if vm.ctx != nil {
						type SubqueryRowsExecutor interface {
							ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
						}
						if executor, ok2 := vm.ctx.(SubqueryRowsExecutor); ok2 {
							if rows, err2 := executor.ExecuteSubqueryRows(selectStmt); err2 == nil {
								hashSet = make(map[string]bool, len(rows))
								for _, row := range rows {
									if len(row) > 0 {
										hashSet[subqueryHashKey(row[0])] = true
									}
								}
								vm.subqueryCache.hashSets[selectStmt] = hashSet
								hit = true
							}
						}
					}
				}
				if hit {
					if value != nil && hashSet[subqueryHashKey(value)] {
						vm.registers[dstReg] = int64(1)
					} else {
						vm.registers[dstReg] = int64(0)
					}
					continue
				}
			}

			if vm.ctx != nil {
				// Try context-aware executor first (for correlated subqueries)
				type SubqueryRowsExecutorWithContext interface {
					ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					if rows, err := executor.ExecuteSubqueryRowsWithContext(inst.P4, currentRow); err == nil {
						// Check if value matches any row's first column
						found := false
						for _, row := range rows {
							if len(row) > 0 && compareVals(value, row[0]) == 0 {
								found = true
								break
							}
						}
						if found {
							vm.registers[dstReg] = int64(1)
						} else {
							vm.registers[dstReg] = int64(0)
						}
						continue
					}
				}

				// Fallback to non-context executor
				type SubqueryRowsExecutor interface {
					ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutor); ok {
					if rows, err := executor.ExecuteSubqueryRows(inst.P4); err == nil {
						// Check if value matches any row's first column
						found := false
						for _, row := range rows {
							if len(row) > 0 && compareVals(value, row[0]) == 0 {
								found = true
								break
							}
						}
						if found {
							vm.registers[dstReg] = int64(1)
						} else {
							vm.registers[dstReg] = int64(0)
						}
					} else {
						vm.registers[dstReg] = int64(0)
					}
				} else {
					vm.registers[dstReg] = int64(0)
				}
			} else {
				vm.registers[dstReg] = int64(0)
			}
			continue

		case OpNotInSubquery:
			// Execute NOT IN subquery: check if value is NOT in the result set
			// P1 = destination register
			// P2 = value register to check
			// P4 = SelectStmt to execute
			dstReg := int(inst.P1)
			valueReg := int(inst.P2)
			value := vm.registers[valueReg]

			// NULL NOT IN (...) = NULL (per SQL standard)
			if value == nil {
				vm.registers[dstReg] = nil
				continue
			}

			// For non-correlated NOT IN subqueries, materialize into a hash set once
			if selectStmt, ok := inst.P4.(*QP.SelectStmt); ok && !isSubqueryCorrelated(selectStmt) {
				if vm.subqueryCache == nil {
					vm.subqueryCache = newSubqueryResultCache()
				}
				hashSet, hit := vm.subqueryCache.hashSets[selectStmt]
				if !hit {
					if vm.ctx != nil {
						type SubqueryRowsExecutor interface {
							ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
						}
						if executor, ok2 := vm.ctx.(SubqueryRowsExecutor); ok2 {
							if rows, err2 := executor.ExecuteSubqueryRows(selectStmt); err2 == nil {
								hashSet = make(map[string]bool, len(rows))
								for _, row := range rows {
									if len(row) > 0 {
										hashSet[subqueryHashKey(row[0])] = true
									}
								}
								vm.subqueryCache.hashSets[selectStmt] = hashSet
								hit = true
							}
						}
					}
				}
				if hit {
					if hashSet[subqueryHashKey(value)] {
						vm.registers[dstReg] = int64(0) // found, so NOT IN is false
					} else {
						vm.registers[dstReg] = int64(1) // not found, so NOT IN is true
					}
					continue
				}
			}

			if vm.ctx != nil {
				// Try context-aware executor first (for correlated subqueries)
				type SubqueryRowsExecutorWithContext interface {
					ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					if rows, err := executor.ExecuteSubqueryRowsWithContext(inst.P4, currentRow); err == nil {
						// Check if value matches any row's first column
						found := false
						for _, row := range rows {
							if len(row) > 0 && compareVals(value, row[0]) == 0 {
								found = true
								break
							}
						}
						if found {
							vm.registers[dstReg] = int64(0) // found, so NOT IN is false
						} else {
							vm.registers[dstReg] = int64(1) // not found, so NOT IN is true
						}
						continue
					}
				}

				// Fallback to non-context executor
				type SubqueryRowsExecutor interface {
					ExecuteSubqueryRows(subquery interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutor); ok {
					if rows, err := executor.ExecuteSubqueryRows(inst.P4); err == nil {
						// Check if value matches any row's first column
						found := false
						for _, row := range rows {
							if len(row) > 0 && compareVals(value, row[0]) == 0 {
								found = true
								break
							}
						}
						if found {
							vm.registers[dstReg] = int64(0) // found, so NOT IN is false
						} else {
							vm.registers[dstReg] = int64(1) // not found, so NOT IN is true
						}
					} else {
						vm.registers[dstReg] = int64(1)
					}
				} else {
					vm.registers[dstReg] = int64(1)
				}
			} else {
				vm.registers[dstReg] = int64(1)
			}
			continue

		case OpAggregate:
			// Execute aggregate query with GROUP BY
			// P1 = cursor ID
			// P4 = AggregateInfo structure
			cursorID := int(inst.P1)
			cursor := vm.cursors.Get(cursorID)

			if cursor == nil || cursor.Data == nil {
				continue
			}

			aggInfo, ok := inst.P4.(*AggregateInfo)
			if !ok {
				continue
			}

			// Execute aggregation: scan all rows, group, accumulate, emit
			vm.executeAggregation(cursor, aggInfo)
			continue

		case OpOpenRead:
			cursorID := int(inst.P1)
			util.Assert(cursorID >= 0 && cursorID < MaxCursors, "cursor ID %d out of bounds [0, %d)", cursorID, MaxCursors)

			tableName := inst.P3
			if tableName == "" {
				continue
			}

			// If cursor is already manually opened (e.g., for correlated subqueries),
			// don't reopen it to preserve the alias
			existingCursor := vm.cursors.Get(cursorID)
			if existingCursor != nil {
				// fmt.Printf("DEBUG OpOpenRead: cursor %d already open with name %q, skipping reopen to %q\n", cursorID, existingCursor.TableName, tableName)
				continue
			}

			// fmt.Printf("DEBUG OpOpenRead: opening cursor %d with tableName=%q\n", cursorID, tableName)
			if vm.ctx != nil {
				if data, err := vm.ctx.GetTableData(tableName); err == nil && data != nil {
					if cols, err := vm.ctx.GetTableColumns(tableName); err == nil {
						vm.cursors.OpenTableAtID(cursorID, tableName, data, cols)
					}
				}
			}
			continue

		case OpRewind:
			cursorID := int(inst.P1)
			util.Assert(cursorID >= 0 && cursorID < MaxCursors, "cursor ID %d out of bounds", cursorID)

			target := int(inst.P2)
			cursor := vm.cursors.Get(cursorID)
			if cursor != nil {
				cursor.Index = 0
				cursor.EOF = len(cursor.Data) == 0
				if cursor.EOF && target > 0 {
					vm.pc = target
				}
			}
			continue

		case OpNext:
			cursorID := int(inst.P1)
			util.Assert(cursorID >= 0 && cursorID < MaxCursors, "cursor ID %d out of bounds", cursorID)

			target := int(inst.P2)
			cursor := vm.cursors.Get(cursorID)
			if cursor != nil {
				instPC := vm.pc - 1 // PC of the OpNext instruction
				// Fast path: predictor says loop will continue.
				if vm.bp.Predict(instPC) {
					cursor.Index++
					if cursor.Index < len(cursor.Data) {
						// Prediction correct: loop continues.
						vm.bp.Update(instPC, true)
						continue
					}
					// Prediction wrong: cursor exhausted.
					cursor.EOF = true
					if target > 0 {
						vm.pc = target
					}
					vm.bp.Update(instPC, false)
				} else {
					// Predictor says loop will NOT continue (or cold start).
					cursor.Index++
					if cursor.Index >= len(cursor.Data) {
						cursor.EOF = true
						if target > 0 {
							vm.pc = target
						}
						vm.bp.Update(instPC, false)
					} else {
						vm.bp.Update(instPC, true)
					}
				}
			}
			continue

		case OpInsert:
			// P1 = cursor ID (table)
			// P4 = []int (register indices) OR map[string]int (column name to register)
			// Inserts a row into the table
			cursorID := int(inst.P1)
			cursor := vm.cursors.Get(cursorID)
			if cursor == nil {
				return fmt.Errorf("OpInsert: cursor %d not found", cursorID)
			}

			// Build row from registers
			row := make(map[string]interface{})

			// Check if P4 is a map (columns specified) or slice (positional)
			switch v := inst.P4.(type) {
			case map[string]int:
				// Columns specified: map column names to values
				for colName, regIdx := range v {
					row[colName] = vm.registers[regIdx]
				}
			case []int:
				// No columns specified: use table column order
				cols := cursor.Columns
				// fmt.Printf("DEBUG OpInsert: cols=%v, registers len=%d\n", cols, len(vm.registers))
				for i, reg := range v {
					// fmt.Printf("DEBUG OpInsert: i=%d, reg=%d, value=%v\n", i, reg, vm.registers[reg])
					if i < len(cols) {
						row[cols[i]] = vm.registers[reg]
					}
				}
			default:
				return fmt.Errorf("OpInsert: invalid P4 type")
			}

			// Insert via context
			if vm.ctx != nil {
				err := vm.ctx.InsertRow(cursor.TableName, row)
				if err != nil {
					return err
				}
				vm.rowsAffected++
			}
			continue

		case OpUpdate:
			// P1 = cursor ID
			// P4 = map[string]int mapping column name to register index
			// Updates the current row in the cursor
			cursorID := int(inst.P1)
			cursor := vm.cursors.Get(cursorID)
			if cursor == nil {
				return fmt.Errorf("OpUpdate: cursor %d not found", cursorID)
			}

			setInfo, ok := inst.P4.(map[string]int)
			if !ok {
				return fmt.Errorf("OpUpdate: invalid P4 type, expected map[string]int")
			}

			// Get the current row and update specified columns
			if cursor.Index < 0 || cursor.Index >= len(cursor.Data) {
				return fmt.Errorf("OpUpdate: invalid cursor position %d", cursor.Index)
			}
			row := cursor.Data[cursor.Index]

			// Update only the columns specified in SET clause
			for colName, regIdx := range setInfo {
				row[colName] = vm.registers[regIdx]
			}

			// Update via context
			if vm.ctx != nil {
				err := vm.ctx.UpdateRow(cursor.TableName, cursor.Index, row)
				if err != nil {
					return err
				}
				vm.rowsAffected++
			}
			continue

		case OpDelete:
			// P1 = cursor ID
			// Deletes the current row in the cursor
			cursorID := int(inst.P1)
			cursor := vm.cursors.Get(cursorID)
			if cursor == nil {
				return fmt.Errorf("OpDelete: cursor %d not found", cursorID)
			}

			// Delete via context
			if vm.ctx != nil {
				err := vm.ctx.DeleteRow(cursor.TableName, cursor.Index)
				if err != nil {
					return err
				}
				vm.rowsAffected++
				// After deletion, reload the cursor data
				data, err := vm.ctx.GetTableData(cursor.TableName)
				if err == nil {
					cursor.Data = data
				}
				// Decrement cursor.Index to compensate for the upcoming OpNext
				// increment. After deletion the slice shifts down: the old index
				// now points to what was previously the next row. OpNext will
				// then advance it back to that position.
				cursor.Index--
			}
			continue

		case OpEphemeralCreate:
			// Create an ephemeral table for set operations
			tableID := int(inst.P1)
			vm.ephemeralTbls[tableID] = make(map[string]bool)
			continue

		case OpEphemeralInsert:
			// Insert a row into ephemeral table
			tableID := int(inst.P1)
			// P4 contains the register array for the row
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				key := makeRowKey(row)
				if tbl, exists := vm.ephemeralTbls[tableID]; exists {
					tbl[key] = true
				}
			}
			continue

		case OpEphemeralFind:
			// Check if a row exists in ephemeral table
			tableID := int(inst.P1)
			targetAddr := int(inst.P2)
			// P4 contains the register array for the row
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				key := makeRowKey(row)
				if tbl, exists := vm.ephemeralTbls[tableID]; exists {
					if tbl[key] {
						// Row found - jump to target
						if targetAddr > 0 {
							vm.pc = targetAddr
						}
					}
				}
			}
			continue

		case OpUnionAll:
			// Union ALL: combine two result sets keeping duplicates
			// P1 = left result register array
			// P2 = right result register array
			// Results are already in vm.results from previous operations
			// This opcode is typically not needed as results stream directly
			continue

		case OpUnionDistinct:
			// Union DISTINCT: combine two result sets removing duplicates
			// Use ephemeral table to track seen rows
			// P1 = ephemeral table ID
			// P4 = register array for current row
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				key := makeRowKey(row)
				tableID := int(inst.P1)
				if tbl, exists := vm.ephemeralTbls[tableID]; exists {
					if !tbl[key] {
						// New row - add to results
						tbl[key] = true
						vm.results = append(vm.results, row)
					}
				}
			}
			continue

		case OpExcept:
			// EXCEPT: rows in left but not in right
			// P1 = ephemeral table ID (contains right-side rows)
			// P2 = jump address if row should be excluded
			// P4 = register array for current row
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				key := makeRowKey(row)
				tableID := int(inst.P1)
				if tbl, exists := vm.ephemeralTbls[tableID]; exists {
					if tbl[key] {
						// Row exists in right side - skip it
						if inst.P2 > 0 {
							vm.pc = int(inst.P2)
						}
					}
				}
			}
			continue

		case OpIntersect:
			// INTERSECT: rows that exist in both left and right
			// P1 = ephemeral table ID (contains right-side rows)
			// P2 = jump address if row should be excluded
			// P4 = register array for current row
			if regs, ok := inst.P4.([]int); ok {
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				key := makeRowKey(row)
				tableID := int(inst.P1)
				if tbl, exists := vm.ephemeralTbls[tableID]; exists {
					if !tbl[key] {
						// Row doesn't exist in right side - skip it
						if inst.P2 > 0 {
							vm.pc = int(inst.P2)
						}
					} else {
						// Mark as used for DISTINCT handling
						tbl[key] = false
					}
				}
			}
			continue

		case OpColumnarScan:
			// P1=tableID (unused), P2=destReg, P4=tableName string
			tableName := ""
			if inst.P4 != nil {
				if s, ok := inst.P4.(string); ok {
					tableName = s
				}
			}
			if vm.ctx != nil && tableName != "" {
				rows, err := vm.ctx.GetTableData(tableName)
				if err == nil {
					vm.registers[inst.P2] = rows
				} else {
					vm.registers[inst.P2] = []map[string]interface{}{}
				}
			} else {
				vm.registers[inst.P2] = []map[string]interface{}{}
			}
			continue

		case OpColumnarFilter:
			// P1=srcReg (rows), P2=valReg (filter value), P4=*ColumnarFilterSpec (includes DstReg)
			rows, _ := vm.registers[inst.P1].([]map[string]interface{})
			filterVal := vm.registers[inst.P2]
			spec, _ := inst.P4.(*ColumnarFilterSpec)
			if spec == nil || rows == nil {
				if spec != nil {
					vm.registers[spec.DstReg] = rows
				}
				continue
			}
			filtered := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				v := row[spec.ColName]
				if columnarCompare(v, filterVal, spec.Op) {
					filtered = append(filtered, row)
				}
			}
			vm.registers[spec.DstReg] = filtered
			continue

		case OpColumnarAgg:
			// P1=srcReg (rows), P2=aggType, P4=*ColumnarAggSpec (includes colName and DstReg)
			rows, _ := vm.registers[inst.P1].([]map[string]interface{})
			aggSpec, _ := inst.P4.(*ColumnarAggSpec)
			if aggSpec == nil {
				continue
			}
			aggType := int(inst.P2)
			vm.registers[aggSpec.DstReg] = columnarAggregate(rows, aggSpec.ColName, aggType)
			continue

		case OpColumnarProject:
			// P1=srcReg (rows), P2=colNamesReg ([]string), P4=destReg as int
			rows, _ := vm.registers[inst.P1].([]map[string]interface{})
			colNames, _ := vm.registers[inst.P2].([]string)
			destReg, _ := inst.P4.(int)
			if colNames == nil || rows == nil {
				vm.registers[destReg] = rows
				continue
			}
			projected := make([][]interface{}, 0, len(rows))
			for _, row := range rows {
				r := make([]interface{}, len(colNames))
				for i, c := range colNames {
					r[i] = row[c]
				}
				projected = append(projected, r)
			}
			vm.registers[destReg] = projected
			continue

		case OpTopK:
			// P1=k, P2=srcReg ([][]interface{}), P4=destReg as int
			k := int(inst.P1)
			rows, _ := vm.registers[inst.P2].([][]interface{})
			destReg, _ := inst.P4.(int)
			if k <= 0 || rows == nil {
				vm.registers[destReg] = rows
				continue
			}
			if k >= len(rows) {
				vm.registers[destReg] = rows
			} else {
				vm.registers[destReg] = rows[:k]
			}
			continue

		default:
			return fmt.Errorf("unimplemented opcode: %v", inst.Op)
		}
	}
}

// makeRowKey creates a unique key for a row for deduplication
func makeRowKey(row []interface{}) string {
	key := ""
	for i, v := range row {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", v)
	}
	return key
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

// getCurrentRow retrieves the current row from a cursor for correlated subquery context
func (vm *VM) getCurrentRow(cursorID int) map[string]interface{} {
	cursor := vm.cursors.Get(cursorID)
	if cursor == nil || cursor.Index < 0 || cursor.Index >= len(cursor.Data) {
		return nil
	}

	// Return the current row
	return cursor.Data[cursor.Index]
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

	// Both integers
	if av.typ == "int" && bv.typ == "int" {
		if bv.v.(int64) == 0 {
			return nil
		}
		return av.v.(int64) % bv.v.(int64)
	}

	// At least one is float - use math.Mod
	if av.isNumeric() && bv.isNumeric() {
		bFloat := bv.toFloat()
		if bFloat == 0 {
			return nil
		}
		return math.Mod(av.toFloat(), bFloat)
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

	// Handle negative start - count from end
	startIdx := int(start)
	if startIdx < 0 {
		startIdx = len(runes) + startIdx
		if startIdx < 0 {
			startIdx = 0
		}
	} else {
		// SQLite: start=0 is treated as 1, BUT excludes position 0
		// SUBSTR('hello', 0, 5) = 'hell' (not 'hello')
		// So start=0 or start=1 both start from position 1 (index 0)
		// But 0 means "start from beginning but exclude first char" for length purposes
		if startIdx == 0 {
			// Start=0: exclude first character
			startIdx = 0
		} else {
			startIdx = startIdx - 1 // Convert to 0-based
		}
	}

	if startIdx >= len(runes) {
		return ""
	}

	endIdx := len(runes)
	// SQLite: if length is 0, return empty string
	if length == 0 {
		return ""
	}
	// If start was 0, length is reduced by 1 to exclude first char
	if start == 0 && length > 0 {
		length = length - 1
	}
	if length > 0 {
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

func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case bool:
		if val {
			return 1
		}
		return 0
	case string:
		// Try to parse string as number
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		return 0
	}
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
	// SQLite LIKE is case-insensitive by default (for ASCII characters)
	return likeMatchRecursive(strings.ToUpper(str), strings.ToUpper(pattern), 0, 0)
}

func likeMatchRecursive(str string, pattern string, si, pi int) bool {
	// Check if pattern is exhausted
	if pi >= len(pattern) {
		// Pattern exhausted - match if string also exhausted
		return si >= len(str)
	}

	// Check if string is exhausted but pattern still has content
	if si >= len(str) {
		// String exhausted - remaining pattern must all be %
		for i := pi; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	pch := pattern[pi]

	if pch == '%' {
		// % matches zero or more characters
		// Try matching with zero characters (skip %)
		if likeMatchRecursive(str, pattern, si, pi+1) {
			return true
		}
		// Try matching with one or more characters (consume one char and try % again)
		return likeMatchRecursive(str, pattern, si+1, pi)
	}

	if pch == '_' {
		// _ matches any single character
		return likeMatchRecursive(str, pattern, si+1, pi+1)
	}

	// Handle escape character
	if pch == '\\' && pi+1 < len(pattern) {
		pi++
		pch = pattern[pi]
	}

	// Check for literal match
	if str[si] == pch {
		return likeMatchRecursive(str, pattern, si+1, pi+1)
	}

	return false
}

func globMatch(str, pattern string) bool {
	// GLOB is case-sensitive and uses * for zero or more, ? for exactly one, [] for character classes
	return globMatchRecursive(str, pattern, 0, 0)
}

func globMatchRecursive(str string, pattern string, si, pi int) bool {
	// Check if pattern is exhausted
	if pi >= len(pattern) {
		return si >= len(str)
	}

	// Check if string is exhausted but pattern still has content
	if si >= len(str) {
		for i := pi; i < len(pattern); i++ {
			if pattern[i] != '*' {
				return false
			}
		}
		return true
	}

	pch := pattern[pi]

	if pch == '*' {
		// * matches zero or more characters
		// Try matching with zero characters (skip *)
		if globMatchRecursive(str, pattern, si, pi+1) {
			return true
		}
		// Try matching with one or more characters (consume one char and try * again)
		return globMatchRecursive(str, pattern, si+1, pi)
	}

	if pch == '?' {
		// ? matches any single character
		return globMatchRecursive(str, pattern, si+1, pi+1)
	}

	// Handle character class [...]
	if pch == '[' {
		// Find closing ]
		closeIdx := -1
		for i := pi + 1; i < len(pattern); i++ {
			if pattern[i] == ']' {
				closeIdx = i
				break
			}
		}
		if closeIdx > pi+1 {
			// Check if current char matches any in the class
			match := false
			strChar := str[si]
			negate := false
			start := pi + 1
			if pattern[start] == '^' {
				negate = true
				start++
			}
			for j := start; j < closeIdx; j++ {
				if j+2 < closeIdx && pattern[j+1] == '-' {
					// Range
					low := pattern[j]
					high := pattern[j+2]
					if strChar >= low && strChar <= high {
						match = true
						break
					}
					j += 2
				} else if pattern[j] == strChar {
					match = true
					break
				}
			}
			if negate {
				match = !match
			}
			if match {
				return globMatchRecursive(str, pattern, si+1, closeIdx+1)
			}
			return false
		}
	}

	// Handle escape character
	if pch == '\\' && pi+1 < len(pattern) {
		pi++
		pch = pattern[pi]
	}

	// Check for literal match (case-sensitive)
	if str[si] == pch {
		return globMatchRecursive(str, pattern, si+1, pi+1)
	}

	return false
}

var ErrValueTooBig = errors.New("value too big")

// executeAggregation performs GROUP BY and aggregate function execution
func (vm *VM) executeAggregation(cursor *Cursor, aggInfo *AggregateInfo) {
	// Map from group key to aggregate state.
	// interface{} keys: for single-expr GROUP BY the raw value is used directly
	// (no string allocation per row); for multi-expr GROUP BY a composite string
	// produced by computeGroupKey is boxed.
	groups := make(map[interface{}]*AggregateState)

	// Scan all rows and accumulate aggregates per group
	for rowIdx := 0; rowIdx < len(cursor.Data); rowIdx++ {
		row := cursor.Data[rowIdx]

		// Apply WHERE filter if present
		if aggInfo.WhereExpr != nil {
			if !vm.evaluateBoolExprOnRow(row, cursor.Columns, aggInfo.WhereExpr) {
				continue
			}
		}

		// Compute group key from GROUP BY expressions.
		// For single-expression GROUP BY the raw value is used directly as the
		// map key (avoids a strings.Builder.String() allocation per row).
		groupKey := vm.computeGroupKeyIface(row, cursor.Columns, aggInfo.GroupByExprs)

		// Get or create aggregate state for this group
		state, exists := groups[groupKey]
		if !exists {
			distinctSets := make([]map[string]bool, len(aggInfo.Aggregates))
			for i, aggDef := range aggInfo.Aggregates {
				if aggDef.Distinct {
					distinctSets[i] = make(map[string]bool)
				}
			}
			state = &AggregateState{
				GroupKey:     groupKey,
				Count:        0,
				Counts:       make([]int, len(aggInfo.Aggregates)),
				SumsInt:      make([]int64, len(aggInfo.Aggregates)),
				SumsFloat:    make([]float64, len(aggInfo.Aggregates)),
				SumsIsFloat:  make([]bool, len(aggInfo.Aggregates)),
				SumsHasVal:   make([]bool, len(aggInfo.Aggregates)),
				Mins:         make([]interface{}, len(aggInfo.Aggregates)),
				Maxs:         make([]interface{}, len(aggInfo.Aggregates)),
				NonAggValues: make([]interface{}, len(aggInfo.NonAggCols)),
				DistinctSets: distinctSets,
			}
			groups[groupKey] = state
		}

		// Update aggregate state
		state.Count++

		// Evaluate aggregate functions
		for aggIdx, aggDef := range aggInfo.Aggregates {
			value := vm.evaluateAggregateArg(row, cursor.Columns, aggDef.Args)

			// For DISTINCT aggregates, skip if value already seen
			if aggDef.Distinct && state.DistinctSets[aggIdx] != nil {
				valKey := fmt.Sprintf("%v", value)
				if state.DistinctSets[aggIdx][valKey] {
					continue
				}
				state.DistinctSets[aggIdx][valKey] = true
			}

			switch aggDef.Function {
			case "COUNT":
				// COUNT(col) only counts non-NULL values; COUNT(*) counts all rows via state.Count
				if value != nil {
					state.Counts[aggIdx]++
				}
			case "SUM":
				// Use typed accumulators to avoid per-row interface{} boxing.
				if value != nil {
					state.SumsHasVal[aggIdx] = true
					switch v := value.(type) {
					case int64:
						if state.SumsIsFloat[aggIdx] {
							state.SumsFloat[aggIdx] += float64(v)
						} else {
							state.SumsInt[aggIdx] += v
						}
					case float64:
						if !state.SumsIsFloat[aggIdx] {
							// Promote integer accumulator to float64.
							state.SumsFloat[aggIdx] = float64(state.SumsInt[aggIdx]) + v
							state.SumsIsFloat[aggIdx] = true
						} else {
							state.SumsFloat[aggIdx] += v
						}
					}
				}
			case "AVG":
				// AVG = SUM / COUNT of non-NULL values; accumulate in float64 to match SQLite precision
				if value != nil {
					var fv float64
					switch v := value.(type) {
					case int64:
						fv = float64(v)
					case float64:
						fv = v
					default:
						continue
					}
					state.SumsFloat[aggIdx] += fv
					state.SumsHasVal[aggIdx] = true
					state.Counts[aggIdx]++
				}
			case "MIN":
				if value != nil && (state.Mins[aggIdx] == nil || vm.compareVals(value, state.Mins[aggIdx]) < 0) {
					state.Mins[aggIdx] = value
				}
			case "MAX":
				if value != nil && (state.Maxs[aggIdx] == nil || vm.compareVals(value, state.Maxs[aggIdx]) > 0) {
					state.Maxs[aggIdx] = value
				}
			}
		}

		// Store first non-aggregate column values for this group
		if !exists {
			for i, expr := range aggInfo.NonAggCols {
				state.NonAggValues[i] = vm.evaluateExprOnRow(row, cursor.Columns, expr)
			}
		}
	}

	// Emit result rows (one per group)
	// Sort groups by key for deterministic output
	groupKeys := make([]interface{}, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	// Sort the keys: NULL groups sort first (matching SQLite behavior),
	// then numeric (int64/float64) before string, with numeric string keys
	// compared as numbers.
	sort.SliceStable(groupKeys, func(i, j int) bool {
		return compareGroupKeyIface(groupKeys[i], groupKeys[j])
	})

	// SQL standard: aggregate without GROUP BY on empty table must return 1 row
	// with COUNT=0 and NULL for other aggregates.
	if len(groupKeys) == 0 && len(aggInfo.GroupByExprs) == 0 {
		resultRow := make([]interface{}, 0, len(aggInfo.NonAggCols)+len(aggInfo.Aggregates))
		for range aggInfo.NonAggCols {
			resultRow = append(resultRow, nil)
		}
		for _, aggDef := range aggInfo.Aggregates {
			if aggDef.Function == "COUNT" {
				resultRow = append(resultRow, int64(0))
			} else {
				resultRow = append(resultRow, nil)
			}
		}
		vm.results = append(vm.results, resultRow)
		return
	}

	for _, key := range groupKeys {
		state := groups[key]

		// Apply HAVING filter if present
		if aggInfo.HavingExpr != nil {
			if !vm.evaluateHaving(state, aggInfo) {
				continue
			}
		}

		// If SelectExprs is set, use them for the result row ordering
		if len(aggInfo.SelectExprs) > 0 {
			resultRow := make([]interface{}, 0, len(aggInfo.SelectExprs))
			for _, expr := range aggInfo.SelectExprs {
				resultRow = append(resultRow, vm.resolveSelectExpr(expr, state, aggInfo))
			}
			vm.results = append(vm.results, resultRow)
			continue
		}

		// Build result row: non-agg columns + aggregates
		resultRow := make([]interface{}, 0)

		// Add non-aggregate columns
		for _, val := range state.NonAggValues {
			resultRow = append(resultRow, val)
		}

		// Add aggregates
		for aggIdx, aggDef := range aggInfo.Aggregates {
			var aggValue interface{}

			switch aggDef.Function {
			case "COUNT":
				if aggDef.Distinct && state.DistinctSets[aggIdx] != nil {
					aggValue = int64(len(state.DistinctSets[aggIdx]))
				} else if isCountStar(aggDef) {
					aggValue = int64(state.Count)
				} else {
					aggValue = int64(state.Counts[aggIdx])
				}
			case "SUM":
				if state.SumsHasVal[aggIdx] {
					if state.SumsIsFloat[aggIdx] {
						aggValue = state.SumsFloat[aggIdx]
					} else {
						aggValue = state.SumsInt[aggIdx]
					}
				}
			case "AVG":
				if state.Counts[aggIdx] > 0 && state.SumsHasVal[aggIdx] {
					aggValue = state.SumsFloat[aggIdx] / float64(state.Counts[aggIdx])
				}
			case "MIN":
				aggValue = state.Mins[aggIdx]
			case "MAX":
				aggValue = state.Maxs[aggIdx]
			}

			resultRow = append(resultRow, aggValue)
		}

		vm.results = append(vm.results, resultRow)
	}
}

// AggregateInfo contains information about aggregate queries
type AggregateInfo struct {
	GroupByExprs []QP.Expr      // GROUP BY expressions
	Aggregates   []AggregateDef // Aggregate functions in SELECT
	NonAggCols   []QP.Expr      // Non-aggregate columns in SELECT
	HavingExpr   QP.Expr        // HAVING clause expression
	WhereExpr    QP.Expr        // WHERE clause expression
	SelectExprs  []QP.Expr      // Full original SELECT expressions (for post-agg eval)
}

// ScalarCallInfo holds info for OpCallScalar: a function name, arg register indices, and a dst register.
type ScalarCallInfo struct {
	Name    string       // function name (uppercase)
	ArgRegs []int        // register indices for arguments
	Dst     int          // destination register
	Call    *QP.FuncCall // original call expression (nil args are evaluated at runtime)
}

// AggregateDef defines an aggregate function
type AggregateDef struct {
	Function string    // COUNT, SUM, AVG, MIN, MAX
	Args     []QP.Expr // Arguments to the aggregate
	Distinct bool      // true if DISTINCT was specified (e.g. COUNT(DISTINCT col))
	Alias    string    // Optional output alias (e.g. COUNT(*) AS cnt)
}

// AggregateState tracks aggregate values for a group
type AggregateState struct {
	GroupKey interface{}
	Count    int
	Counts   []int // per-aggregate non-NULL counts (for COUNT(col) and AVG)
	// Typed sum accumulators avoid per-row interface{} boxing.
	SumsInt      []int64   // integer partial sums (SUM of integer values)
	SumsFloat    []float64 // float64 partial sums (SUM promoted to float, or AVG)
	SumsIsFloat  []bool    // true when the aggregate has been promoted to float64
	SumsHasVal   []bool    // true when at least one non-NULL value was accumulated
	Mins         []interface{}
	Maxs         []interface{}
	NonAggValues []interface{}
	DistinctSets []map[string]bool // per-aggregate distinct value sets (for DISTINCT aggregates)
}

// isCountStar reports whether an aggregate definition is COUNT(*) or COUNT() with no args.
func isCountStar(aggDef AggregateDef) bool {
	if len(aggDef.Args) == 0 {
		return true
	}
	if len(aggDef.Args) == 1 {
		if colRef, ok := aggDef.Args[0].(*QP.ColumnRef); ok && colRef.Name == "*" {
			return true
		}
	}
	return false
}

// computeGroupKey generates a string key from GROUP BY expressions.
// Uses a strings.Builder + type switch to avoid per-value fmt.Sprintf allocations.
func (vm *VM) computeGroupKey(row map[string]interface{}, columns []string, groupByExprs []QP.Expr) string {
	if len(groupByExprs) == 0 {
		return "" // Single group for aggregates without GROUP BY
	}

	var buf strings.Builder
	for i, expr := range groupByExprs {
		if i > 0 {
			buf.WriteByte('|')
		}
		value := vm.evaluateExprOnRow(row, columns, expr)
		switch v := value.(type) {
		case int64:
			buf.WriteString(strconv.FormatInt(v, 10))
		case float64:
			buf.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		case string:
			buf.WriteString(v)
		case bool:
			if v {
				buf.WriteString("true")
			} else {
				buf.WriteString("false")
			}
		case nil:
			buf.WriteString("<nil>")
		default:
			fmt.Fprintf(&buf, "%v", value)
		}
	}
	return buf.String()
}

// computeGroupKeyIface returns the GROUP BY key as an interface{}.
// For a single GROUP BY expression the raw evaluated value is returned directly,
// avoiding the strings.Builder.String() allocation that computeGroupKey does on
// every row.  For multiple expressions it falls back to computeGroupKey and
// boxes the resulting string.
func (vm *VM) computeGroupKeyIface(row map[string]interface{}, columns []string, groupByExprs []QP.Expr) interface{} {
	if len(groupByExprs) == 0 {
		return "" // single group for aggregates without GROUP BY
	}
	if len(groupByExprs) == 1 {
		v := vm.evaluateExprOnRow(row, columns, groupByExprs[0])
		// Only directly-comparable types are safe as interface{} map keys.
		switch v.(type) {
		case int64, float64, string, bool, nil:
			return v
		case []byte:
			return string(v.([]byte)) // []byte is not comparable; convert once
		default:
			return fmt.Sprintf("%v", v) // fallback: stringify
		}
	}
	// Multi-column GROUP BY: composite string key (one allocation per unique group,
	// not per row, because map lookup reuses the existing string for known groups).
	return vm.computeGroupKey(row, columns, groupByExprs)
}

// compareGroupKeyIface returns true when ki should sort before kj.
// NULL sorts first; numeric types compare numerically; strings compare with
// numeric fallback (matching SQLite's behaviour for numeric-string GROUP BY keys).
func compareGroupKeyIface(ki, kj interface{}) bool {
	if ki == nil {
		return kj != nil // nil < everything else
	}
	if kj == nil {
		return false // nothing < nil (nil is already "smallest")
	}
	switch vi := ki.(type) {
	case int64:
		switch vj := kj.(type) {
		case int64:
			return vi < vj
		case float64:
			return float64(vi) < vj
		}
	case float64:
		switch vj := kj.(type) {
		case float64:
			return vi < vj
		case int64:
			return vi < float64(vj)
		}
	case string:
		if vj, ok := kj.(string); ok {
			// Try numeric comparison first (matches SQLite for numeric-string keys)
			if fi, erri := strconv.ParseFloat(vi, 64); erri == nil {
				if fj, errj := strconv.ParseFloat(vj, 64); errj == nil {
					return fi < fj
				}
			}
			return vi < vj
		}
	case bool:
		if vj, ok := kj.(bool); ok {
			return !vi && vj // false < true
		}
	}
	// Fallback for composite string keys (multi-column GROUP BY) and any other
	// comparable type that reaches here.  fmt.Sprintf is acceptable because this
	// path is only hit once per unique group pair during sorting, not once per row.
	return fmt.Sprintf("%v", ki) < fmt.Sprintf("%v", kj)
}
func (vm *VM) evaluateAggregateArg(row map[string]interface{}, columns []string, args []QP.Expr) interface{} {
	if len(args) == 0 {
		return nil
	}

	// For COUNT(*), return 1
	if len(args) == 1 {
		if colRef, ok := args[0].(*QP.ColumnRef); ok && colRef.Name == "*" {
			return int64(1)
		}
	}

	return vm.evaluateExprOnRow(row, columns, args[0])
}

// applyTypeCast applies a SQL type cast to a value, returning the cast result.
func (vm *VM) applyTypeCast(val interface{}, typeName string) interface{} {
	if val == nil {
		return nil
	}
	upperType := strings.ToUpper(typeName)
	switch upperType {
	case "INTEGER", "INT":
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case string:
			if iv, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return iv
			} else if fv, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return int64(fv)
			}
			return int64(0)
		}
	case "REAL", "FLOAT", "DOUBLE":
		switch v := val.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case string:
			if fv, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return fv
			}
			return float64(0)
		}
	case "TEXT":
		if b, ok := val.([]byte); ok {
			return string(b)
		}
		return fmt.Sprintf("%v", val)
	case "BLOB":
		if s, ok := val.(string); ok {
			return []byte(s)
		}
		if b, ok := val.([]byte); ok {
			return b
		}
	case "DATE", "TIME", "TIMESTAMP", "DATETIME", "YEAR":
		// SQLite treats DATE/TIME/TIMESTAMP as NUMERIC affinity (equivalent to INTEGER cast)
		// Uses leading-integer parsing for strings (SQLite's sqlite3Atoi64 behavior)
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case string:
			return parseNumericPrefix(v)
		}
	}
	return val
}

// evaluateExprOnRow evaluates an expression against a row
func (vm *VM) evaluateExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		// When a table qualifier is present, try the qualified key first so that
		// rows built from JOINs (which store values under "alias.col" keys) resolve
		// to the correct table's value even when multiple tables share a column name.
		if e.Table != "" {
			qualKey := e.Table + "." + e.Name
			if val, ok := row[qualKey]; ok {
				return val
			}
		}
		if val, ok := row[e.Name]; ok {
			return val
		}
		// Try without table prefix (handles "t.col" stored in Name field)
		name := e.Name
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			name = name[idx+1:]
		}
		return row[name]
	case *QP.AliasExpr:
		return vm.evaluateExprOnRow(row, columns, e.Expr)
	case *QP.UnaryExpr:
		val := vm.evaluateExprOnRow(row, columns, e.Expr)
		if e.Op == QP.TokenMinus {
			switch v := val.(type) {
			case int64:
				return -v
			case float64:
				return -v
			}
		}
		return val
	case *QP.BinaryExpr:
		left := vm.evaluateExprOnRow(row, columns, e.Left)
		right := vm.evaluateExprOnRow(row, columns, e.Right)
		return vm.evaluateBinaryOp(left, right, e.Op)
	case *QP.FuncCall:
		return vm.evaluateFuncCallOnRow(row, columns, e)
	case *QP.CastExpr:
		val := vm.evaluateExprOnRow(row, columns, e.Expr)
		return vm.applyTypeCast(val, e.TypeSpec.Name)
	case *QP.SubqueryExpr:
		// Scalar subquery: execute and return the single value.
		if vm.ctx != nil {
			type SubqueryExecutorCtx interface {
				ExecuteSubqueryWithContext(subquery interface{}, outerRow map[string]interface{}) (interface{}, error)
			}
			if exec, ok := vm.ctx.(SubqueryExecutorCtx); ok {
				if result, err := exec.ExecuteSubqueryWithContext(e.Select, row); err == nil {
					return result
				}
			}
			type SubqueryExec interface {
				ExecuteSubquery(subquery interface{}) (interface{}, error)
			}
			if exec, ok := vm.ctx.(SubqueryExec); ok {
				if result, err := exec.ExecuteSubquery(e.Select); err == nil {
					return result
				}
			}
		}
		return nil
	default:
		return nil
	}
}

// evaluateFuncCallOnRow evaluates a function call against a row for aggregate WHERE/CHECK filtering
func (vm *VM) evaluateFuncCallOnRow(row map[string]interface{}, columns []string, e *QP.FuncCall) interface{} {
	funcName := strings.ToUpper(e.Name)
	switch funcName {
	case "LENGTH", "LEN":
		if len(e.Args) > 0 {
			val := vm.evaluateExprOnRow(row, columns, e.Args[0])
			if val == nil {
				return nil
			}
			return int64(len(fmt.Sprintf("%v", val)))
		}
	case "UPPER":
		if len(e.Args) > 0 {
			val := vm.evaluateExprOnRow(row, columns, e.Args[0])
			if val == nil {
				return nil
			}
			return strings.ToUpper(fmt.Sprintf("%v", val))
		}
	case "LOWER":
		if len(e.Args) > 0 {
			val := vm.evaluateExprOnRow(row, columns, e.Args[0])
			if val == nil {
				return nil
			}
			return strings.ToLower(fmt.Sprintf("%v", val))
		}
	case "TYPEOF":
		if len(e.Args) > 0 {
			val := vm.evaluateExprOnRow(row, columns, e.Args[0])
			if val == nil {
				return "null"
			}
			switch val.(type) {
			case int64:
				return "integer"
			case float64:
				return "real"
			case string:
				return "text"
			case []byte:
				return "blob"
			default:
				return "text"
			}
		}
	}
	// Date/time functions
	switch funcName {
	case "CURRENT_DATE":
		return time.Now().UTC().Format("2006-01-02")
	case "CURRENT_TIME":
		return time.Now().UTC().Format("15:04:05")
	case "CURRENT_TIMESTAMP":
		return time.Now().UTC().Format("2006-01-02 15:04:05")
	case "LOCALTIME":
		return time.Now().Local().Format("15:04:05")
	case "LOCALTIMESTAMP":
		return time.Now().Local().Format("2006-01-02 15:04:05")
	case "DATE":
		t := parseDateTimeValue(vm.evaluateExprOnRow(row, columns, safeArg(e.Args, 0)))
		if t.IsZero() {
			t = time.Now().UTC()
		}
		for i := 1; i < len(e.Args); i++ {
			mod, _ := vm.evaluateExprOnRow(row, columns, e.Args[i]).(string)
			t = applyDateModifier(t, mod)
		}
		return t.Format("2006-01-02")
	case "TIME":
		t := parseDateTimeValue(vm.evaluateExprOnRow(row, columns, safeArg(e.Args, 0)))
		if t.IsZero() {
			t = time.Now().UTC()
		}
		for i := 1; i < len(e.Args); i++ {
			mod, _ := vm.evaluateExprOnRow(row, columns, e.Args[i]).(string)
			t = applyDateModifier(t, mod)
		}
		return t.Format("15:04:05")
	case "DATETIME":
		t := parseDateTimeValue(vm.evaluateExprOnRow(row, columns, safeArg(e.Args, 0)))
		if t.IsZero() {
			t = time.Now().UTC()
		}
		for i := 1; i < len(e.Args); i++ {
			mod, _ := vm.evaluateExprOnRow(row, columns, e.Args[i]).(string)
			t = applyDateModifier(t, mod)
		}
		return t.Format("2006-01-02 15:04:05")
	case "STRFTIME":
		if len(e.Args) < 2 {
			return nil
		}
		fmtStr, _ := vm.evaluateExprOnRow(row, columns, e.Args[0]).(string)
		t := parseDateTimeValue(vm.evaluateExprOnRow(row, columns, e.Args[1]))
		if t.IsZero() {
			t = time.Now().UTC()
		}
		for i := 2; i < len(e.Args); i++ {
			mod, _ := vm.evaluateExprOnRow(row, columns, e.Args[i]).(string)
			t = applyDateModifier(t, mod)
		}
		goFmt := strings.NewReplacer(
			"%Y", "2006", "%m", "01", "%d", "02",
			"%H", "15", "%M", "04", "%S", "05",
			"%j", "002", "%f", "05.000000",
		).Replace(fmtStr)
		return t.Format(goFmt)
	}
	return nil
}

// evaluateBoolExprOnRow evaluates a WHERE-clause expression as a boolean against a row
func (vm *VM) evaluateBoolExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case *QP.BinaryExpr:
		switch e.Op {
		case QP.TokenAnd:
			return vm.evaluateBoolExprOnRow(row, columns, e.Left) && vm.evaluateBoolExprOnRow(row, columns, e.Right)
		case QP.TokenOr:
			return vm.evaluateBoolExprOnRow(row, columns, e.Left) || vm.evaluateBoolExprOnRow(row, columns, e.Right)
		case QP.TokenIs:
			left := vm.evaluateExprOnRow(row, columns, e.Left)
			right := vm.evaluateExprOnRow(row, columns, e.Right)
			if right == nil {
				return left == nil
			}
			return left == right
		case QP.TokenIsNot:
			left := vm.evaluateExprOnRow(row, columns, e.Left)
			right := vm.evaluateExprOnRow(row, columns, e.Right)
			if right == nil {
				return left != nil
			}
			return left != right
		case QP.TokenLike:
			left := vm.evaluateExprOnRow(row, columns, e.Left)
			right := vm.evaluateExprOnRow(row, columns, e.Right)
			if left == nil || right == nil {
				return false
			}
			return vmMatchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right))
		case QP.TokenNotLike:
			left := vm.evaluateExprOnRow(row, columns, e.Left)
			right := vm.evaluateExprOnRow(row, columns, e.Right)
			if left == nil || right == nil {
				return false
			}
			return !vmMatchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right))
		case QP.TokenBetween:
			val := vm.evaluateExprOnRow(row, columns, e.Left)
			if rangeExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				lo := vm.evaluateExprOnRow(row, columns, rangeExpr.Left)
				hi := vm.evaluateExprOnRow(row, columns, rangeExpr.Right)
				return vm.compareVals(val, lo) >= 0 && vm.compareVals(val, hi) <= 0
			}
			return false
		case QP.TokenNotBetween:
			val := vm.evaluateExprOnRow(row, columns, e.Left)
			if rangeExpr, ok := e.Right.(*QP.BinaryExpr); ok {
				lo := vm.evaluateExprOnRow(row, columns, rangeExpr.Left)
				hi := vm.evaluateExprOnRow(row, columns, rangeExpr.Right)
				return vm.compareVals(val, lo) < 0 || vm.compareVals(val, hi) > 0
			}
			return false
		case QP.TokenIn:
			val := vm.evaluateExprOnRow(row, columns, e.Left)
			if listLit, ok := e.Right.(*QP.Literal); ok {
				if items, ok := listLit.Value.([]interface{}); ok {
					for _, item := range items {
						if vm.compareVals(val, item) == 0 {
							return true
						}
					}
					return false
				}
			}
			result := vm.evaluateBinaryOp(val, vm.evaluateExprOnRow(row, columns, e.Right), e.Op)
			if bv, ok := result.(bool); ok {
				return bv
			}
			return false
		case QP.TokenNotIn:
			val := vm.evaluateExprOnRow(row, columns, e.Left)
			if val == nil {
				// NULL NOT IN (...) → NULL (falsy) per SQL standard
				return false
			}
			if listLit, ok := e.Right.(*QP.Literal); ok {
				if items, ok := listLit.Value.([]interface{}); ok {
					for _, item := range items {
						if vm.compareVals(val, item) == 0 {
							return false
						}
					}
					return true
				}
			}
			return true
		default:
			result := vm.evaluateBinaryOp(
				vm.evaluateExprOnRow(row, columns, e.Left),
				vm.evaluateExprOnRow(row, columns, e.Right),
				e.Op,
			)
			if bv, ok := result.(bool); ok {
				return bv
			}
			return false
		}
	case *QP.UnaryExpr:
		if e.Op == QP.TokenNot {
			return !vm.evaluateBoolExprOnRow(row, columns, e.Expr)
		}
		result := vm.evaluateExprOnRow(row, columns, e.Expr)
		if bv, ok := result.(bool); ok {
			return bv
		}
		return false
	default:
		result := vm.evaluateExprOnRow(row, columns, expr)
		if bv, ok := result.(bool); ok {
			return bv
		}
		return result != nil
	}
}

// vmMatchLike matches a string against a LIKE pattern
func vmMatchLike(value, pattern string) bool {
	if pattern == "" {
		return value == ""
	}
	if pattern == "%" {
		return true
	}

	// Fast path: check if pattern is a simple prefix (e.g., "Alice%")
	// This avoids expensive recursive matching
	hasWildcard := false
	for _, c := range pattern {
		if c == '%' || c == '_' {
			hasWildcard = true
			break
		}
	}
	if !hasWildcard {
		// Exact match (no wildcards)
		return strings.EqualFold(value, pattern)
	}

	// Check for prefix-only pattern like "Alice%"
	// This is very common and can be optimized
	if strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[:len(pattern)-1], "%") && !strings.Contains(pattern[:len(pattern)-1], "_") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(prefix))
	}

	// Check for suffix-only pattern like "%abc"
	if strings.HasPrefix(pattern, "%") && !strings.Contains(pattern[1:], "%") && !strings.Contains(pattern[1:], "_") {
		suffix := pattern[1:]
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(suffix))
	}

	// Fall back to full recursive matching
	return matchLikePattern(value, pattern, 0, 0)
}

// parseNumericPrefix parses a numeric prefix from a string like SQLite does.
// E.g., "2024year" -> 2024, "  3.14abc" -> 3, "abc" -> 0
func parseNumericPrefix(s string) int64 {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0
	}
	// Find the longest numeric prefix
	end := 0
	for end < len(s) && (s[end] >= '0' && s[end] <= '9' || (end == 0 && (s[end] == '-' || s[end] == '+'))) {
		end++
	}
	if end == 0 {
		return 0
	}
	if iv, err := strconv.ParseInt(s[:end], 10, 64); err == nil {
		return iv
	}
	if fv, err := strconv.ParseFloat(s[:end], 64); err == nil {
		return int64(fv)
	}
	return 0
}

// parseDateTimeValue parses a value into a time.Time, trying common SQLite date formats.
// Returns zero time if parsing fails.
func parseDateTimeValue(val interface{}) time.Time {
	var s string
	switch v := val.(type) {
	case string:
		s = v
	case nil:
		return time.Time{}
	default:
		return time.Time{}
	}
	if strings.ToLower(s) == "now" {
		return time.Now().UTC()
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02", "15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// applyDateModifier applies a SQLite date modifier string (e.g., "+1 day") to a time.Time.
func applyDateModifier(t time.Time, mod string) time.Time {
	mod = strings.TrimSpace(mod)
	if mod == "" {
		return t
	}
	// Parse sign
	sign := 1
	if len(mod) > 0 && mod[0] == '-' {
		sign = -1
		mod = mod[1:]
	} else if len(mod) > 0 && mod[0] == '+' {
		mod = mod[1:]
	}
	mod = strings.TrimSpace(mod)
	// Parse number and unit
	var numStr string
	var unit string
	for i, ch := range mod {
		if ch >= '0' && ch <= '9' || (ch == '.' && numStr != "") {
			numStr += string(ch)
		} else {
			unit = strings.ToLower(strings.TrimSpace(mod[i:]))
			break
		}
	}
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return t
	}
	n *= sign
	switch unit {
	case "day", "days":
		return t.AddDate(0, 0, n)
	case "month", "months":
		return t.AddDate(0, n, 0)
	case "year", "years":
		return t.AddDate(n, 0, 0)
	case "hour", "hours":
		return t.Add(time.Duration(n) * time.Hour)
	case "minute", "minutes":
		return t.Add(time.Duration(n) * time.Minute)
	case "second", "seconds":
		return t.Add(time.Duration(n) * time.Second)
	}
	return t
}

// safeArg returns e.Args[i] or nil if out of bounds.
func safeArg(args []QP.Expr, i int) QP.Expr {
	if i >= 0 && i < len(args) {
		return args[i]
	}
	return nil
}

func matchLikePattern(value, pattern string, vi, pi int) bool {
	if pi >= len(pattern) {
		return vi >= len(value)
	}
	if vi >= len(value) {
		for ; pi < len(pattern); pi++ {
			if pattern[pi] != '%' {
				return false
			}
		}
		return true
	}
	pchar := pattern[pi]
	if pchar == '%' {
		for i := vi; i <= len(value); i++ {
			if matchLikePattern(value, pattern, i, pi+1) {
				return true
			}
		}
		return false
	}
	if pchar == '_' || strings.ToUpper(string(pchar)) == strings.ToUpper(string(value[vi])) {
		return matchLikePattern(value, pattern, vi+1, pi+1)
	}
	return false
}

func (vm *VM) evaluateBinaryOp(left, right interface{}, op QP.TokenType) interface{} {
	// Simple implementation for common operators
	switch op {
	case QP.TokenPlus:
		return vm.addValues(left, right)
	case QP.TokenMinus:
		return vm.subtractValues(left, right)
	case QP.TokenAsterisk:
		return vm.multiplyValues(left, right)
	case QP.TokenSlash:
		return vm.divideValues(left, right)
	case QP.TokenPercent:
		return vm.moduloValues(left, right)
	case QP.TokenGt:
		return vm.compareVals(left, right) > 0
	case QP.TokenGe:
		return vm.compareVals(left, right) >= 0
	case QP.TokenLt:
		return vm.compareVals(left, right) < 0
	case QP.TokenLe:
		return vm.compareVals(left, right) <= 0
	case QP.TokenEq:
		return vm.compareVals(left, right) == 0
	case QP.TokenNe:
		return vm.compareVals(left, right) != 0
	default:
		return nil
	}
}

// resolveSelectExpr evaluates a SELECT expression in the context of computed aggregate state.
// It resolves aggregate functions (MAX, MIN, SUM, etc.) to their computed values.
func (vm *VM) resolveSelectExpr(expr QP.Expr, state *AggregateState, aggInfo *AggregateInfo) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		// Check NonAggCols - match by column name through direct ColumnRef or AliasExpr
		for i, nonAggExpr := range aggInfo.NonAggCols {
			if colRef, ok := nonAggExpr.(*QP.ColumnRef); ok && colRef.Name == e.Name {
				if i < len(state.NonAggValues) {
					return state.NonAggValues[i]
				}
			}
			// Match through alias expression
			if aliasExpr, ok := nonAggExpr.(*QP.AliasExpr); ok {
				if innerColRef, ok2 := aliasExpr.Expr.(*QP.ColumnRef); ok2 && innerColRef.Name == e.Name {
					if i < len(state.NonAggValues) {
						return state.NonAggValues[i]
					}
				}
			}
		}
		return nil
	case *QP.FuncCall:
		return vm.resolveHavingOperand(expr, state, aggInfo)
	case *QP.AliasExpr:
		return vm.resolveSelectExpr(e.Expr, state, aggInfo)
	case *QP.CastExpr:
		// First check if this exact CastExpr is stored as a NonAggCol (by pointer equality)
		for i, nonAggExpr := range aggInfo.NonAggCols {
			if nonAggExpr == expr {
				if i < len(state.NonAggValues) {
					return state.NonAggValues[i]
				}
			}
		}
		// CAST(expr AS type) wrapping an aggregate - resolve inner expr then apply cast
		val := vm.resolveSelectExpr(e.Expr, state, aggInfo)
		return vm.applyTypeCast(val, e.TypeSpec.Name)
	case *QP.BinaryExpr:
		left := vm.resolveSelectExpr(e.Left, state, aggInfo)
		right := vm.resolveSelectExpr(e.Right, state, aggInfo)
		return vm.evaluateBinaryOp(left, right, e.Op)
	case *QP.UnaryExpr:
		val := vm.resolveSelectExpr(e.Expr, state, aggInfo)
		if e.Op == QP.TokenMinus {
			switch v := val.(type) {
			case int64:
				return -v
			case float64:
				return -v
			}
		}
		return val
	case *QP.CaseExpr:
		if e.Operand != nil {
			// Simple CASE: CASE operand WHEN val THEN result
			operandVal := vm.resolveSelectExpr(e.Operand, state, aggInfo)
			for _, when := range e.Whens {
				whenVal := vm.resolveSelectExpr(when.Condition, state, aggInfo)
				if vm.compareVals(operandVal, whenVal) == 0 {
					return vm.resolveSelectExpr(when.Result, state, aggInfo)
				}
			}
		} else {
			// Searched CASE: CASE WHEN condition THEN result
			for _, when := range e.Whens {
				condVal := vm.resolveSelectExpr(when.Condition, state, aggInfo)
				var isTruthy bool
				if b, ok := condVal.(bool); ok {
					isTruthy = b
				} else if i, ok := condVal.(int64); ok {
					isTruthy = i != 0
				} else if f, ok := condVal.(float64); ok {
					isTruthy = f != 0
				} else {
					isTruthy = condVal != nil
				}
				if isTruthy {
					return vm.resolveSelectExpr(when.Result, state, aggInfo)
				}
			}
		}
		if e.Else != nil {
			return vm.resolveSelectExpr(e.Else, state, aggInfo)
		}
		return nil
	default:
		// Look for this expression in NonAggCols by pointer equality (for unknown expr types)
		for i, nonAggExpr := range aggInfo.NonAggCols {
			if nonAggExpr == expr {
				if i < len(state.NonAggValues) {
					return state.NonAggValues[i]
				}
			}
		}
		return nil
	}
}

// evaluateHaving checks if a group passes the HAVING filter
func (vm *VM) evaluateHaving(state *AggregateState, aggInfo *AggregateInfo) bool {
	if aggInfo.HavingExpr == nil {
		return true
	}

	if binExpr, ok := aggInfo.HavingExpr.(*QP.BinaryExpr); ok {
		left := vm.resolveHavingOperand(binExpr.Left, state, aggInfo)
		right := vm.resolveHavingOperand(binExpr.Right, state, aggInfo)
		result := vm.evaluateBinaryOp(left, right, binExpr.Op)
		if boolVal, ok := result.(bool); ok {
			return boolVal
		}
	}

	return true
}

// resolveHavingOperand resolves a HAVING expression operand to its value,
// handling aggregate function references, column references, and literals.
func (vm *VM) resolveHavingOperand(expr QP.Expr, state *AggregateState, aggInfo *AggregateInfo) interface{} {
	switch e := expr.(type) {
	case *QP.FuncCall:
		funcName := strings.ToUpper(e.Name)
		argKey := fmt.Sprintf("%v", e.Args)
		for i, aggDef := range aggInfo.Aggregates {
			if aggDef.Function != funcName {
				continue
			}
			// Match by args too (e.g., SUM(i) vs SUM(r))
			if fmt.Sprintf("%v", aggDef.Args) != argKey {
				continue
			}
			switch funcName {
			case "COUNT":
				if aggDef.Distinct && state.DistinctSets[i] != nil {
					return int64(len(state.DistinctSets[i]))
				}
				if isCountStar(aggDef) {
					return int64(state.Count)
				}
				return int64(state.Counts[i])
			case "SUM":
				return state.sumResult(i)
			case "AVG":
				if state.Counts[i] > 0 && state.SumsHasVal[i] {
					return state.SumsFloat[i] / float64(state.Counts[i])
				}
				return nil
			case "MIN":
				return state.Mins[i]
			case "MAX":
				return state.Maxs[i]
			}
		}
		// Fallback: if no arg-matching entry found, try first matching function name
		for i, aggDef := range aggInfo.Aggregates {
			if aggDef.Function != funcName {
				continue
			}
			switch funcName {
			case "COUNT":
				if aggDef.Distinct && state.DistinctSets[i] != nil {
					return int64(len(state.DistinctSets[i]))
				}
				if isCountStar(aggDef) {
					return int64(state.Count)
				}
				return int64(state.Counts[i])
			case "SUM":
				return state.sumResult(i)
			case "AVG":
				if state.Counts[i] > 0 && state.SumsHasVal[i] {
					return state.SumsFloat[i] / float64(state.Counts[i])
				}
				return nil
			case "MIN":
				return state.Mins[i]
			case "MAX":
				return state.Maxs[i]
			}
		}
		return nil
	case *QP.ColumnRef:
		for i, nonAggExpr := range aggInfo.NonAggCols {
			if colRef, ok := nonAggExpr.(*QP.ColumnRef); ok && colRef.Name == e.Name {
				return state.NonAggValues[i]
			}
			// Match through alias expression
			if aliasExpr, ok := nonAggExpr.(*QP.AliasExpr); ok {
				if innerColRef, ok2 := aliasExpr.Expr.(*QP.ColumnRef); ok2 && innerColRef.Name == e.Name {
					if i < len(state.NonAggValues) {
						return state.NonAggValues[i]
					}
				}
			}
		}
		return nil
	default:
		return vm.evaluateExprOnRow(nil, nil, expr)
	}
}

// sumResult returns the accumulated sum as an interface{} (nil if no values were seen).
// Boxing occurs once at read time rather than once per accumulated row.
func (s *AggregateState) sumResult(idx int) interface{} {
	if !s.SumsHasVal[idx] {
		return nil
	}
	if s.SumsIsFloat[idx] {
		return s.SumsFloat[idx]
	}
	return s.SumsInt[idx]
}

// addValues adds two values (handles nil and type conversion)
func (vm *VM) addValues(a, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		return aInt + bInt
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return aFloat + bFloat
	}
	if aIsInt && bIsFloat {
		return float64(aInt) + bFloat
	}
	if aIsFloat && bIsInt {
		return aFloat + float64(bInt)
	}

	return nil
}

// multiplyValues multiplies two values
func (vm *VM) multiplyValues(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		return aInt * bInt
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return aFloat * bFloat
	}
	if aIsInt && bIsFloat {
		return float64(aInt) * bFloat
	}
	if aIsFloat && bIsInt {
		return aFloat * float64(bInt)
	}

	return nil
}

// moduloValues computes the modulo of two values
func (vm *VM) moduloValues(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		if bInt == 0 {
			return nil
		}
		return aInt % bInt
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat || aIsInt {
		af := aFloat
		if aIsInt {
			af = float64(aInt)
		}
		bf := bFloat
		if bIsFloat {
			bf = bFloat
		} else if bIsInt {
			bf = float64(bInt)
		}
		if bf == 0 {
			return nil
		}
		// Float modulo via truncation
		return af - float64(int64(af/bf))*bf
	}

	return nil
}

// subtractValues subtracts two values
func (vm *VM) subtractValues(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		return aInt - bInt
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return aFloat - bFloat
	}
	if aIsInt && bIsFloat {
		return float64(aInt) - bFloat
	}
	if aIsFloat && bIsInt {
		return aFloat - float64(bInt)
	}

	return nil
}

// divideValues divides two values
func (vm *VM) divideValues(a, b interface{}) interface{} {
	if a == nil || b == nil {
		return nil
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		if bInt == 0 {
			return nil
		}
		return float64(aInt) / float64(bInt)
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		if bFloat == 0 {
			return nil
		}
		return aFloat / bFloat
	}
	if aIsInt && bIsFloat {
		if bFloat == 0 {
			return nil
		}
		return float64(aInt) / bFloat
	}
	if aIsFloat && bIsInt {
		if bInt == 0 {
			return nil
		}
		return aFloat / float64(bInt)
	}

	return nil
}

// compareValues compares two values (-1, 0, 1)
func (vm *VM) compareVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aInt, aIsInt := a.(int64)
	bInt, bIsInt := b.(int64)
	if aIsInt && bIsInt {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}
	if aIsInt && bIsFloat {
		aFloat = float64(aInt)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}
	if aIsFloat && bIsInt {
		bFloat = float64(bInt)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	return 0
}

// ColumnarFilterSpec specifies a columnar filter predicate.
type ColumnarFilterSpec struct {
	ColName string
	Op      string // "=", "!=", "<", "<=", ">", ">="
	DstReg  int    // destination register for filtered rows
}

// ColumnarAggSpec specifies a columnar aggregation.
type ColumnarAggSpec struct {
	ColName string
	DstReg  int // destination register for the aggregate result
}

// columnarCompare compares a row value against a filter value using the given operator.
func columnarCompare(rowVal, filterVal interface{}, op string) bool {
	cmp := compareVals(rowVal, filterVal)
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}
	return false
}

// columnarAggregate computes an aggregate over rows for a given column.
// aggType: 0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG
func columnarAggregate(rows []map[string]interface{}, colName string, aggType int) interface{} {
	switch aggType {
	case 0: // COUNT
		return int64(len(rows))
	case 1: // SUM
		var sum float64
		for _, row := range rows {
			v := row[colName]
			switch n := v.(type) {
			case int64:
				sum += float64(n)
			case float64:
				sum += n
			}
		}
		return sum
	case 2: // MIN
		var minVal interface{}
		for _, row := range rows {
			v := row[colName]
			if v == nil {
				continue
			}
			if minVal == nil || compareVals(v, minVal) < 0 {
				minVal = v
			}
		}
		return minVal
	case 3: // MAX
		var maxVal interface{}
		for _, row := range rows {
			v := row[colName]
			if v == nil {
				continue
			}
			if maxVal == nil || compareVals(v, maxVal) > 0 {
				maxVal = v
			}
		}
		return maxVal
	case 4: // AVG
		var sum float64
		count := 0
		for _, row := range rows {
			v := row[colName]
			switch n := v.(type) {
			case int64:
				sum += float64(n)
				count++
			case float64:
				sum += n
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)
	}
	return nil
}
