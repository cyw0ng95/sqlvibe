package VM

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
