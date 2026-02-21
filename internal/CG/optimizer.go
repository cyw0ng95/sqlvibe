package CG

import (
	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

type Optimizer struct{}

func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

// Optimize runs all optimization passes on the program.
func (o *Optimizer) Optimize(program *VM.Program) *VM.Program {
	if program == nil || len(program.Instructions) == 0 {
		return program
	}
	o.foldConstants(program)
	o.eliminateDeadCode(program)
	return program
}

// foldConstants performs constant folding: replaces arithmetic/concat operations
// on compile-time constants with a single OpLoadConst instruction.
func (o *Optimizer) foldConstants(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)

	// Count how many times each register is read across the program.
	readCount := make(map[int32]int)
	for _, inst := range insts {
		switch inst.Op {
		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder, VM.OpConcat:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpMove, VM.OpCopy, VM.OpSCopy:
			readCount[inst.P1]++
		case VM.OpResultRow:
			if regs, ok := inst.P4.([]int); ok {
				for _, r := range regs {
					readCount[int32(r)]++
				}
			}
		case VM.OpEq, VM.OpNe, VM.OpLt, VM.OpLe, VM.OpGt, VM.OpGe, VM.OpIs, VM.OpIsNot:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpIsNull, VM.OpNotNull, VM.OpIfNull, VM.OpIfNull2:
			readCount[inst.P1]++
		case VM.OpIf, VM.OpIfNot:
			readCount[inst.P1]++
		}
	}

	// Track which registers hold compile-time constants and where they were defined.
	type constDef struct {
		value  interface{}
		defIdx int
	}
	constRegs := make(map[int32]constDef)

	for i := 0; i < n; i++ {
		inst := insts[i]

		switch inst.Op {
		case VM.OpLoadConst:
			constRegs[inst.P1] = constDef{inst.P4, i}

		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder:
			r1, r2 := inst.P1, inst.P2
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			c1, ok1 := constRegs[r1]
			c2, ok2 := constRegs[r2]
			if !ok1 || !ok2 {
				delete(constRegs, int32(dst))
				break
			}
			result := foldArith(inst.Op, c1.value, c2.value)
			if result == nil {
				delete(constRegs, int32(dst))
				break
			}
			// Replace arithmetic op with a constant load into dst.
			program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: result}
			constRegs[int32(dst)] = constDef{result, i}

			// NOP the source definitions if those registers are no longer read.
			readCount[r1]--
			if readCount[r1] <= 0 {
				program.Instructions[c1.defIdx].Op = VM.OpNoop
				delete(constRegs, r1)
			}
			readCount[r2]--
			if readCount[r2] <= 0 {
				program.Instructions[c2.defIdx].Op = VM.OpNoop
				delete(constRegs, r2)
			}

		case VM.OpConcat:
			r1, r2 := inst.P1, inst.P2
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			c1, ok1 := constRegs[r1]
			c2, ok2 := constRegs[r2]
			if !ok1 || !ok2 {
				delete(constRegs, int32(dst))
				break
			}
			s1, isStr1 := c1.value.(string)
			s2, isStr2 := c2.value.(string)
			if !isStr1 || !isStr2 {
				delete(constRegs, int32(dst))
				break
			}
			result := s1 + s2
			program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: result}
			constRegs[int32(dst)] = constDef{result, i}

			readCount[r1]--
			if readCount[r1] <= 0 {
				program.Instructions[c1.defIdx].Op = VM.OpNoop
				delete(constRegs, r1)
			}
			readCount[r2]--
			if readCount[r2] <= 0 {
				program.Instructions[c2.defIdx].Op = VM.OpNoop
				delete(constRegs, r2)
			}

		default:
			// If this instruction writes a register (as P1 or P4 int dest), invalidate it.
			delete(constRegs, inst.P1)
			if dst, ok := inst.P4.(int); ok {
				delete(constRegs, int32(dst))
			}
		}
	}
}

// foldArith evaluates a binary arithmetic operation on two constant values.
// Returns nil if the operation cannot be folded (e.g., division by zero, wrong types).
func foldArith(op VM.OpCode, v1, v2 interface{}) interface{} {
	// Convert both values to float64 for arithmetic.
	f1, ok1 := toFloat64(v1)
	f2, ok2 := toFloat64(v2)
	if !ok1 || !ok2 {
		return nil
	}

	switch op {
	case VM.OpAdd:
		result := f1 + f2
		return toSameType(v1, v2, result)
	case VM.OpSubtract:
		result := f1 - f2
		return toSameType(v1, v2, result)
	case VM.OpMultiply:
		result := f1 * f2
		return toSameType(v1, v2, result)
	case VM.OpDivide:
		if f2 == 0 {
			return nil // avoid division by zero
		}
		result := f1 / f2
		return toSameType(v1, v2, result)
	case VM.OpRemainder:
		i1, isInt1 := toInt64(v1)
		i2, isInt2 := toInt64(v2)
		if !isInt1 || !isInt2 || i2 == 0 {
			return nil
		}
		return i1 % i2
	}
	return nil
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float64:
		if val == float64(int64(val)) {
			return int64(val), true
		}
	}
	return 0, false
}

// toSameType preserves the type: if both inputs are integers, return int64; otherwise float64.
func toSameType(v1, v2 interface{}, result float64) interface{} {
	_, i1 := v1.(int64)
	_, i2 := v2.(int64)
	if !i1 {
		_, i1 = v1.(int)
	}
	if !i2 {
		_, i2 = v2.(int)
	}
	if i1 && i2 {
		return int64(result)
	}
	return result
}

// eliminateDeadCode removes unreachable instructions following unconditional
// control flow (OpHalt, OpGoto) by replacing them with OpNoop.
func (o *Optimizer) eliminateDeadCode(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)
	if n == 0 {
		return
	}

	// Collect all jump targets so we know which instructions are reachable entry points.
	jumpTargets := make(map[int]bool)
	for _, inst := range insts {
		// P2 is a jump target for most control-flow instructions.
		if inst.Op.IsJump() || inst.Op == VM.OpRewind || inst.Op == VM.OpNext ||
			inst.Op == VM.OpPrev || inst.Op == VM.OpInit {
			if inst.P2 > 0 {
				jumpTargets[int(inst.P2)] = true
			}
		}
		// P4 may be an integer jump target (WHERE fixups use P4 for jump addresses).
		if target, ok := inst.P4.(int); ok && target > 0 && target <= len(insts) {
			jumpTargets[target] = true
		}
	}

	dead := false
	for i := range insts {
		if jumpTargets[i] {
			dead = false
		}
		if dead {
			program.Instructions[i].Op = VM.OpNoop
			program.Instructions[i].P1 = 0
			program.Instructions[i].P2 = 0
			program.Instructions[i].P3 = ""
			program.Instructions[i].P4 = nil
			continue
		}
		switch insts[i].Op {
		case VM.OpHalt:
			dead = true
		case VM.OpGoto:
			// OpGoto is unconditional; code after it is dead unless it's a jump target.
			dead = true
		}
	}
}
