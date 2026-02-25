package VM

import "fmt"

// BcVmContext provides the BytecodeVM with access to database storage.
type BcVmContext interface {
	// GetTableRows returns all rows and the ordered column list for table.
	GetTableRows(table string) ([]map[string]interface{}, []string, error)
	// GetTableSchema returns column-name → type-string for table.
	GetTableSchema(table string) map[string]string
}

// bcCursor is a VM-internal table cursor.
type bcCursor struct {
	rows    []map[string]interface{}
	colOrder []string
	pos     int // -1 before Rewind, ≥0 after
}

// BcOpHandler is the function type for all bytecode handlers.
// Returns the next PC (usually vm.pc+1) or an error.
type BcOpHandler func(vm *BytecodeVM, inst Instr) (int, error)

// aggState holds aggregate accumulator state.
type aggState struct {
	name  string      // function name (lowercase)
	count int64       // rows seen
	sum   VmVal       // running sum
	min   VmVal       // running min
	max   VmVal       // running max
	vals  []VmVal     // all values (for non-streaming aggregates)
	allNull bool      // true while no non-NULL value seen
}

// BytecodeVM executes a BytecodeProg.
type BytecodeVM struct {
	prog    *BytecodeProg
	regs    []VmVal
	pc      int
	ctx     BcVmContext
	cursors [256]*bcCursor
	aggSlots [256]*aggState
	bcTable [NumBcOpcodes]BcOpHandler
	resultCols []string
	resultRows [][]VmVal
}

// NewBytecodeVM creates a VM for prog using ctx for storage access.
func NewBytecodeVM(prog *BytecodeProg, ctx BcVmContext) *BytecodeVM {
	vm := &BytecodeVM{
		prog: prog,
		regs: make([]VmVal, maxInt(prog.NumRegs, 16)),
		ctx:  ctx,
	}
	vm.installHandlers()
	return vm
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Run executes the program from PC=0 until BcHalt or error.
func (vm *BytecodeVM) Run() error {
	vm.pc = 0
	vm.resultRows = vm.resultRows[:0]
	instrs := vm.prog.Instrs
	for vm.pc < len(instrs) {
		inst := instrs[vm.pc]
		op := BcOpCode(inst.Op)
		if int(op) >= int(NumBcOpcodes) {
			return fmt.Errorf("bytecode VM: unknown opcode %d at PC %d", op, vm.pc)
		}
		h := vm.bcTable[op]
		if h == nil {
			return fmt.Errorf("bytecode VM: no handler for opcode %s at PC %d", BcOpName[op], vm.pc)
		}
		nextPC, err := h(vm, inst)
		if err != nil {
			return err
		}
		vm.pc = nextPC
	}
	return nil
}

// ResultRows returns the accumulated result rows as [][]interface{}.
func (vm *BytecodeVM) ResultRows() [][]interface{} {
	out := make([][]interface{}, len(vm.resultRows))
	for i, row := range vm.resultRows {
		r := make([]interface{}, len(row))
		for j, v := range row {
			r[j] = v.ToInterface()
		}
		out[i] = r
	}
	return out
}

// ResultColNames returns the column names from the program.
func (vm *BytecodeVM) ResultColNames() []string {
	return vm.prog.ColNames
}

// reg returns a pointer to the register at index r.
// Grows the slice if needed.
func (vm *BytecodeVM) reg(r int32) *VmVal {
	if int(r) >= len(vm.regs) {
		extra := make([]VmVal, int(r)+1-len(vm.regs))
		vm.regs = append(vm.regs, extra...)
	}
	return &vm.regs[r]
}

// constVal returns the constant at index i.
func (vm *BytecodeVM) constVal(i int32) VmVal {
	if int(i) < len(vm.prog.Consts) {
		return vm.prog.Consts[i]
	}
	return VmNull()
}

func (vm *BytecodeVM) installHandlers() {
	vm.bcTable[BcNoop] = bcOpNoop
	vm.bcTable[BcLoadConst] = bcOpLoadConst
	vm.bcTable[BcLoadReg] = bcOpLoadReg
	vm.bcTable[BcAdd] = bcOpAdd
	vm.bcTable[BcAddInt] = bcOpAddInt
	vm.bcTable[BcSub] = bcOpSub
	vm.bcTable[BcMul] = bcOpMul
	vm.bcTable[BcDiv] = bcOpDiv
	vm.bcTable[BcMod] = bcOpMod
	vm.bcTable[BcNeg] = bcOpNeg
	vm.bcTable[BcConcat] = bcOpConcat
	vm.bcTable[BcEq] = bcOpEq
	vm.bcTable[BcNe] = bcOpNe
	vm.bcTable[BcLt] = bcOpLt
	vm.bcTable[BcLe] = bcOpLe
	vm.bcTable[BcGt] = bcOpGt
	vm.bcTable[BcGe] = bcOpGe
	vm.bcTable[BcAnd] = bcOpAnd
	vm.bcTable[BcOr] = bcOpOr
	vm.bcTable[BcNot] = bcOpNot
	vm.bcTable[BcIsNull] = bcOpIsNull
	vm.bcTable[BcNotNull] = bcOpNotNull
	vm.bcTable[BcJump] = bcOpJump
	vm.bcTable[BcJumpTrue] = bcOpJumpTrue
	vm.bcTable[BcJumpFalse] = bcOpJumpFalse
	vm.bcTable[BcOpenCursor] = bcOpOpenCursor
	vm.bcTable[BcRewind] = bcOpRewind
	vm.bcTable[BcNext] = bcOpNext
	vm.bcTable[BcColumn] = bcOpColumn
	vm.bcTable[BcRowid] = bcOpRowid
	vm.bcTable[BcResultRow] = bcOpResultRow
	vm.bcTable[BcHalt] = bcOpHalt
	vm.bcTable[BcAggInit] = bcOpAggInit
	vm.bcTable[BcAggStep] = bcOpAggStep
	vm.bcTable[BcAggFinal] = bcOpAggFinal
	vm.bcTable[BcCall] = bcOpCall
}
