package VM

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/util"
)

// MaxVMIterations is the maximum number of VM instructions that can be executed
// before the VM panics with an assertion error. This prevents infinite loops.
const MaxVMIterations = 1000000

func (vm *VM) Exec(ctx interface{}) error {
	iterationCount := 0
	for {
		// Check for infinite loop
		iterationCount++
		if iterationCount > MaxVMIterations {
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
				row := make([]interface{}, len(regs))
				for i, reg := range regs {
					row[i] = vm.registers[reg]
				}
				vm.results = append(vm.results, row)
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
			val := vm.registers[inst.P1]
			
			// Try to get TypeSpec from P4, fall back to string for backward compatibility
			var typeName string
			var precision, scale int
			
			if typeSpec, ok := inst.P4.(QP.TypeSpec); ok {
				typeName = typeSpec.Name
				precision = typeSpec.Precision
				scale = typeSpec.Scale
			} else if typeStr, ok := inst.P4.(string); ok {
				// Backward compatibility: P4 is a string
				typeName = typeStr
			}
			
			if val != nil && typeName != "" {
				upperType := strings.ToUpper(typeName)
				switch upperType {
				case "INTEGER", "INT":
					if s, ok := val.(string); ok {
						if iv, err := strconv.ParseInt(s, 10, 64); err == nil {
							vm.registers[inst.P1] = iv
						} else if fv, err := strconv.ParseFloat(s, 64); err == nil {
							vm.registers[inst.P1] = int64(fv)
						} else {
							vm.registers[inst.P1] = int64(0)
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
					if precision > 0 && scale > 0 {
						// Round to specified scale
						multiplier := math.Pow(10, float64(scale))
						floatVal = math.Round(floatVal*multiplier) / multiplier
					}
					
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

			if vm.ctx != nil {
				// Try context-aware executor first (for correlated subqueries)
				type SubqueryRowsExecutorWithContext interface {
					ExecuteSubqueryRowsWithContext(subquery interface{}, outerRow map[string]interface{}) ([][]interface{}, error)
				}

				if executor, ok := vm.ctx.(SubqueryRowsExecutorWithContext); ok {
					// Get current row from cursor 0 (if available)
					currentRow := vm.getCurrentRow(0)
					// fmt.Printf("DEBUG OpExistsSubquery: currentRow=%v\n", currentRow)
					if rows, err := executor.ExecuteSubqueryRowsWithContext(inst.P4, currentRow); err == nil && len(rows) > 0 {
						// fmt.Printf("DEBUG OpExistsSubquery: got %d rows from subquery\n", len(rows))
						vm.registers[dstReg] = int64(1)
					} else {
						// fmt.Printf("DEBUG OpExistsSubquery: got 0 rows from subquery (err=%v)\n", err)
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

			if vm.ctx != nil {
				// Try context-aware executor first (for correlated subqueries)
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
				cursor.Index++
				if cursor.Index >= len(cursor.Data) {
					cursor.EOF = true
					if target > 0 {
						vm.pc = target
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
				// After deletion, we need to adjust the cursor
				// Reload the cursor data
				if vm.ctx != nil {
					data, err := vm.ctx.GetTableData(cursor.TableName)
					if err == nil {
						cursor.Data = data
						// Keep cursor at same index (which now points to next row)
						if cursor.Index >= len(cursor.Data) {
							cursor.EOF = true
						}
					}
				}
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
	// Map from group key (string) to aggregate state
	groups := make(map[string]*AggregateState)

	// Scan all rows and accumulate aggregates per group
	for rowIdx := 0; rowIdx < len(cursor.Data); rowIdx++ {
		row := cursor.Data[rowIdx]

		// Compute group key from GROUP BY expressions
		groupKey := vm.computeGroupKey(row, cursor.Columns, aggInfo.GroupByExprs)

		// Get or create aggregate state for this group
		state, exists := groups[groupKey]
		if !exists {
			state = &AggregateState{
				GroupKey:     groupKey,
				Count:        0,
				Sums:         make([]interface{}, len(aggInfo.Aggregates)),
				Mins:         make([]interface{}, len(aggInfo.Aggregates)),
				Maxs:         make([]interface{}, len(aggInfo.Aggregates)),
				NonAggValues: make([]interface{}, len(aggInfo.NonAggCols)),
			}
			groups[groupKey] = state
		}

		// Update aggregate state
		state.Count++

		// Evaluate aggregate functions
		for aggIdx, aggDef := range aggInfo.Aggregates {
			value := vm.evaluateAggregateArg(row, cursor.Columns, aggDef.Args)

			switch aggDef.Function {
			case "COUNT":
			// COUNT is already tracked by state.Count
			case "SUM":
				state.Sums[aggIdx] = vm.addValues(state.Sums[aggIdx], value)
			case "AVG":
				// AVG = SUM / COUNT, we accumulate SUM here
				state.Sums[aggIdx] = vm.addValues(state.Sums[aggIdx], value)
			case "MIN":
				if state.Mins[aggIdx] == nil || vm.compareVals(value, state.Mins[aggIdx]) < 0 {
					state.Mins[aggIdx] = value
				}
			case "MAX":
				if state.Maxs[aggIdx] == nil || vm.compareVals(value, state.Maxs[aggIdx]) > 0 {
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
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	// Sort the keys
	for i := 0; i < len(groupKeys); i++ {
		for j := i + 1; j < len(groupKeys); j++ {
			if groupKeys[i] > groupKeys[j] {
				groupKeys[i], groupKeys[j] = groupKeys[j], groupKeys[i]
			}
		}
	}

	for _, key := range groupKeys {
		state := groups[key]
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
				aggValue = int64(state.Count)
			case "SUM":
				aggValue = state.Sums[aggIdx]
			case "AVG":
				if state.Count > 0 && state.Sums[aggIdx] != nil {
					aggValue = vm.divideValues(state.Sums[aggIdx], int64(state.Count))
				}
			case "MIN":
				aggValue = state.Mins[aggIdx]
			case "MAX":
				aggValue = state.Maxs[aggIdx]
			}

			resultRow = append(resultRow, aggValue)
		}

		// Apply HAVING filter if present
		if aggInfo.HavingExpr != nil {
			// Evaluate HAVING on the result row
			// For simplicity, check if any non-agg column matches the HAVING condition
			if !vm.evaluateHaving(state, aggInfo) {
				continue
			}
		}

		vm.results = append(vm.results, resultRow)
	}
}

// AggregateState tracks aggregate values for a group
type AggregateState struct {
	GroupKey     string
	Count        int
	Sums         []interface{}
	Mins         []interface{}
	Maxs         []interface{}
	NonAggValues []interface{}
}

// computeGroupKey generates a string key from GROUP BY expressions
func (vm *VM) computeGroupKey(row map[string]interface{}, columns []string, groupByExprs []QP.Expr) string {
	if len(groupByExprs) == 0 {
		return "" // Single group for aggregates without GROUP BY
	}

	keyParts := make([]string, 0)
	for _, expr := range groupByExprs {
		value := vm.evaluateExprOnRow(row, columns, expr)
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyParts, "|")
}

// evaluateAggregateArg evaluates the argument of an aggregate function
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

// evaluateExprOnRow evaluates an expression against a row
func (vm *VM) evaluateExprOnRow(row map[string]interface{}, columns []string, expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		return row[e.Name]
	case *QP.BinaryExpr:
		left := vm.evaluateExprOnRow(row, columns, e.Left)
		right := vm.evaluateExprOnRow(row, columns, e.Right)
		return vm.evaluateBinaryOp(left, right, e.Op)
	default:
		return nil
	}
}

// evaluateBinaryOp evaluates a binary operation
func (vm *VM) evaluateBinaryOp(left, right interface{}, op QP.TokenType) interface{} {
	// Simple implementation for common operators
	switch op {
	case QP.TokenPlus:
		return vm.addValues(left, right)
	case QP.TokenMinus:
		return vm.subtractValues(left, right)
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

// evaluateHaving checks if a group passes the HAVING filter
func (vm *VM) evaluateHaving(state *AggregateState, aggInfo *AggregateInfo) bool {
	// For now, implement simple HAVING logic
	// Full implementation would need to evaluate the HAVING expression with aggregate values
	// This is a simplified version that assumes HAVING uses non-agg columns

	if aggInfo.HavingExpr == nil {
		return true
	}

	// Try to evaluate the HAVING expression
	// For simplicity, check if it's a comparison on a non-agg column
	if binExpr, ok := aggInfo.HavingExpr.(*QP.BinaryExpr); ok {
		if colRef, ok := binExpr.Left.(*QP.ColumnRef); ok {
			// Find the column value in NonAggValues
			for i, expr := range aggInfo.NonAggCols {
				if nonAggCol, ok := expr.(*QP.ColumnRef); ok && nonAggCol.Name == colRef.Name {
					left := state.NonAggValues[i]

					// Evaluate the right side - handle subqueries specially
					var right interface{}
					if subqExpr, ok := binExpr.Right.(*QP.SubqueryExpr); ok {
						// Execute the subquery through the context
						if vm.ctx != nil {
							type SubqueryExecutor interface {
								ExecuteSubquery(subquery interface{}) (interface{}, error)
							}

							if executor, ok := vm.ctx.(SubqueryExecutor); ok {
								if result, err := executor.ExecuteSubquery(subqExpr.Select); err == nil {
									right = result
								} else {
									right = nil
								}
							} else {
								right = nil
							}
						} else {
							right = nil
						}
					} else {
						right = vm.evaluateExprOnRow(nil, nil, binExpr.Right)
					}

					result := vm.evaluateBinaryOp(left, right, binExpr.Op)
					if boolVal, ok := result.(bool); ok {
						return boolVal
					}
				}
			}
		}
	}

	return true
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
