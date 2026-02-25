package VM

import (
	"fmt"
	"strings"
)

// OpHandler is the function signature for a dispatch-table opcode handler.
// Returns true to advance PC by 1; returns false if the handler already
// modified vm.pc.
type OpHandler func(vm *VM, inst *Instruction) bool

// dispatchTable maps OpCode values to their fast-path handlers.
// Opcodes without an entry fall back to the standard switch dispatch in Exec.
var dispatchTable [256]OpHandler

func init() {
	// Arithmetic
	dispatchTable[OpAdd] = execDispatchAdd
	dispatchTable[OpSubtract] = execDispatchSub
	dispatchTable[OpMultiply] = execDispatchMul
	dispatchTable[OpDivide] = execDispatchDiv

	// Register ops
	dispatchTable[OpNull] = execDispatchNull
	dispatchTable[OpLoadConst] = execDispatchLoadConst
	dispatchTable[OpMove] = execDispatchMove
	dispatchTable[OpCopy] = execDispatchCopy

	// String ops
	dispatchTable[OpUpper] = execDispatchUpper
	dispatchTable[OpLower] = execDispatchLower
	dispatchTable[OpLength] = execDispatchLength
	dispatchTable[OpConcat] = execDispatchConcat

	// Extended string ops (v0.9.3)
	dispatchTable[OpTrim] = execDispatchTrim
	dispatchTable[OpLTrim] = execDispatchLTrim
	dispatchTable[OpRTrim] = execDispatchRTrim
	dispatchTable[OpReplace] = execDispatchReplace
	dispatchTable[OpInstr] = execDispatchInstr

	// Comparison ops (v0.9.3)
	dispatchTable[OpEq] = execDispatchCmp(func(c int) bool { return c == 0 })
	dispatchTable[OpNe] = execDispatchCmp(func(c int) bool { return c != 0 })
	dispatchTable[OpLt] = execDispatchCmp(func(c int) bool { return c < 0 })
	dispatchTable[OpLe] = execDispatchCmp(func(c int) bool { return c <= 0 })
	dispatchTable[OpGt] = execDispatchCmp(func(c int) bool { return c > 0 })
	dispatchTable[OpGe] = execDispatchCmp(func(c int) bool { return c >= 0 })

	// NULL-check ops (v0.9.16)
	dispatchTable[OpIsNull] = execDispatchIsNull
	dispatchTable[OpNotNull] = execDispatchNotNull

	// Bitwise ops (v0.9.16)
	dispatchTable[OpBitAnd] = execDispatchBitAnd
	dispatchTable[OpBitOr] = execDispatchBitOr

	// Remainder (v0.9.16)
	dispatchTable[OpRemainder] = execDispatchRemainder
}

// ExecDirect executes the program using the dispatch table for supported opcodes.
// Falls back to the full switch-based Exec for opcodes not in the dispatch table.
// This method exists to support benchmarking and future migration.
func (vm *VM) ExecDirect(ctx interface{}) error {
	return vm.Exec(ctx)
}

// HasDispatchHandler returns true if opcode has a registered fast-path handler.
func HasDispatchHandler(op OpCode) bool {
	return int(op) < len(dispatchTable) && dispatchTable[op] != nil
}

func execDispatchAdd(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	result := numericAdd(lhs, rhs)
	if inst.HasDst {
		vm.registers[inst.DstReg] = result
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = result
	}
	return true
}

func execDispatchSub(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	result := numericSubtract(lhs, rhs)
	if inst.HasDst {
		vm.registers[inst.DstReg] = result
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = result
	}
	return true
}

func execDispatchMul(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	result := numericMultiply(lhs, rhs)
	if inst.HasDst {
		vm.registers[inst.DstReg] = result
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = result
	}
	return true
}

func execDispatchDiv(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	result := numericDivide(lhs, rhs)
	if inst.HasDst {
		vm.registers[inst.DstReg] = result
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = result
	}
	return true
}

func execDispatchNull(vm *VM, inst *Instruction) bool {
	vm.registers[inst.P1] = nil
	return true
}

func execDispatchLoadConst(vm *VM, inst *Instruction) bool {
	vm.registers[inst.P1] = inst.P4
	return true
}

func execDispatchMove(vm *VM, inst *Instruction) bool {
	vm.registers[inst.P2] = vm.registers[inst.P1]
	return true
}

func execDispatchCopy(vm *VM, inst *Instruction) bool {
	if inst.P1 != inst.P2 {
		vm.registers[inst.P2] = vm.registers[inst.P1]
	}
	return true
}

func execDispatchUpper(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getUpper(src)
	}
	return true
}

func execDispatchLower(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getLower(src)
	}
	return true
}

func execDispatchLength(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getLength(src)
	}
	return true
}

func execDispatchConcat(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	if inst.HasDst {
		vm.registers[inst.DstReg] = stringConcat(lhs, rhs)
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = stringConcat(lhs, rhs)
	}
	return true
}

// execDispatchTrim handles OpTrim (both sides).
func execDispatchTrim(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	chars := " "
	if inst.P2 != 0 {
		if v, ok := vm.registers[inst.P2].(string); ok {
			chars = v
		}
	}
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getTrim(src, chars, true /*both*/, false /*left*/, false /*right*/)
	}
	return true
}

// execDispatchLTrim handles OpLTrim (left trim).
func execDispatchLTrim(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	chars := " "
	if inst.P2 != 0 {
		if v, ok := vm.registers[inst.P2].(string); ok {
			chars = v
		}
	}
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getTrim(src, chars, false /*both*/, true /*left*/, false /*right*/)
	}
	return true
}

// execDispatchRTrim handles OpRTrim (right trim).
func execDispatchRTrim(vm *VM, inst *Instruction) bool {
	src := vm.registers[inst.P1]
	chars := " "
	if inst.P2 != 0 {
		if v, ok := vm.registers[inst.P2].(string); ok {
			chars = v
		}
	}
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = getTrim(src, chars, false /*both*/, false /*left*/, true /*right*/)
	}
	return true
}

// execDispatchReplace handles OpReplace.
func execDispatchReplace(vm *VM, inst *Instruction) bool {
	dst, ok := inst.P4.(int)
	if !ok {
		return true
	}
	srcVal := vm.registers[dst]
	if srcVal == nil {
		return true
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
	return true
}

// execDispatchInstr handles OpInstr.
func execDispatchInstr(vm *VM, inst *Instruction) bool {
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
	return true
}

// execDispatchCmp returns an OpHandler for a comparison opcode.
// The predicate fn maps the compareVals result to true/false.
func execDispatchCmp(fn func(int) bool) OpHandler {
	return func(vm *VM, inst *Instruction) bool {
		lhs := vm.registers[inst.P1]
		rhs := vm.registers[inst.P2]
		if lhs == nil || rhs == nil {
			if inst.P4 != nil {
				if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
					vm.registers[dst] = nil
				}
			}
			return true
		}
		result := fn(compareVals(lhs, rhs))
		if inst.P4 != nil {
			if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
				if result {
					vm.registers[dst] = int64(1)
				} else {
					vm.registers[dst] = int64(0)
				}
			} else if result {
				if target, ok := inst.P4.(int); ok {
					vm.pc = target
					return false
				}
			}
		}
		return true
	}
}

// execDispatchIsNull handles OpIsNull: if register P1 is nil, jump to P2.
func execDispatchIsNull(vm *VM, inst *Instruction) bool {
	if vm.registers[inst.P1] == nil {
		if inst.P2 != 0 {
			vm.pc = int(inst.P2)
			return false
		}
	}
	return true
}

// execDispatchNotNull handles OpNotNull: if register P1 is not nil, jump to P2.
func execDispatchNotNull(vm *VM, inst *Instruction) bool {
	if vm.registers[inst.P1] != nil {
		if inst.P2 != 0 {
			vm.pc = int(inst.P2)
			return false
		}
	}
	return true
}

// execDispatchBitAnd handles OpBitAnd: dst = P1 & P2.
func execDispatchBitAnd(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	if lhs == nil || rhs == nil {
		if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
			vm.registers[dst] = nil
		}
		return true
	}
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = toInt64(lhs) & toInt64(rhs)
	}
	return true
}

// execDispatchBitOr handles OpBitOr: dst = P1 | P2.
func execDispatchBitOr(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	if lhs == nil || rhs == nil {
		if dst, ok := inst.P4.(int); ok && dst < vm.program.NumRegs {
			vm.registers[dst] = nil
		}
		return true
	}
	if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = toInt64(lhs) | toInt64(rhs)
	}
	return true
}

// execDispatchRemainder handles OpRemainder: dst = P1 % P2.
func execDispatchRemainder(vm *VM, inst *Instruction) bool {
	lhs := vm.registers[inst.P1]
	rhs := vm.registers[inst.P2]
	result := numericRemainder(lhs, rhs)
	if inst.HasDst {
		vm.registers[inst.DstReg] = result
	} else if dst, ok := inst.P4.(int); ok {
		vm.registers[dst] = result
	}
	return true
}
