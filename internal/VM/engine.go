package VM

import (
	"errors"
	"fmt"
)

var (
	ErrHalt      = errors.New("execution halted")
	ErrDone      = errors.New("execution done")
	ErrBadCursor = errors.New("bad cursor")
)

type VmError struct {
	Code    int
	Message string
}

func (e *VmError) Error() string {
	return fmt.Sprintf("VM error %d: %s", e.Code, e.Message)
}

type VmContext interface {
	GetTableData(tableName string) ([]map[string]interface{}, error)
	GetTableColumns(tableName string) ([]string, error)
	InsertRow(tableName string, row map[string]interface{}) error
	UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error
	DeleteRow(tableName string, rowIndex int) error
}

type VM struct {
	program        *Program
	pc             int
	registers      []interface{}
	cursors        *CursorArray
	subReturn      []int
	affinity       int
	undo           [][]interface{}
	errorcnt       int
	err            error
	ctx            VmContext
	results        [][]interface{}
	flatBuf        []interface{} // flat backing array for zero-alloc result rows
	rowsAffected   int64
	ephemeralTbls  map[int]map[string]bool // ephemeral tables for SetOps (table_id -> row_key -> exists)
	subqueryCache  *subqueryResultCache    // caches non-correlated subquery results per execution
}

func NewVM(program *Program) *VM {
	return NewVMWithContext(program, nil)
}

func NewVMWithContext(program *Program, ctx VmContext) *VM {
	numRegs := program.NumRegs
	if numRegs < 32 {
		numRegs = 32
	}
	return &VM{
		program:       program,
		pc:            0,
		registers:     make([]interface{}, numRegs),
		cursors:       NewCursorArray(),
		subReturn:     make([]int, 0),
		undo:          make([][]interface{}, 0),
		errorcnt:      0,
		err:           nil,
		ctx:           ctx,
		results:       make([][]interface{}, 0),
		ephemeralTbls: make(map[int]map[string]bool),
	}
}

func (vm *VM) SetContext(ctx VmContext) {
	vm.ctx = ctx
}

func (vm *VM) Context() VmContext {
	return vm.ctx
}

func (vm *VM) Reset() {
	vm.pc = 0
	for i := range vm.registers {
		vm.registers[i] = nil
	}
	vm.cursors.Reset()
	vm.subReturn = vm.subReturn[:0]
	vm.affinity = 0
	vm.errorcnt = 0
	vm.err = nil
	vm.results = vm.results[:0]   // reuse pre-allocated capacity
	vm.flatBuf = vm.flatBuf[:0]   // reuse flat backing buffer
	vm.rowsAffected = 0
	for k := range vm.ephemeralTbls {
		delete(vm.ephemeralTbls, k)
	}
}

func (vm *VM) PC() int {
	return vm.pc
}

func (vm *VM) SetPC(pc int) {
	vm.pc = pc
}

func (vm *VM) GetRegister(reg int) interface{} {
	if reg < 0 || reg >= len(vm.registers) {
		return nil
	}
	return vm.registers[reg]
}

func (vm *VM) SetRegister(reg int, value interface{}) {
	if reg >= 0 && reg < len(vm.registers) {
		vm.registers[reg] = value
	}
}

func (vm *VM) GetInstruction() Instruction {
	if vm.pc >= len(vm.program.Instructions) {
		return Instruction{Op: OpNoop}
	}
	inst := vm.program.Instructions[vm.pc]
	vm.pc++
	return inst
}

func (vm *VM) Program() *Program {
	return vm.program
}

func (vm *VM) Cursors() *CursorArray {
	return vm.cursors
}

func (vm *VM) Error() error {
	return vm.err
}

func (vm *VM) SetError(err error) {
	vm.err = err
}

func (vm *VM) ErrorCode() int {
	return vm.errorcnt
}

func (vm *VM) Results() [][]interface{} {
	return vm.results
}

// PreallocResults pre-allocates the results slice to avoid repeated reallocations
// when the expected row count is known.
func (vm *VM) PreallocResults(n int) {
	if n > 0 && cap(vm.results) < n {
		vm.results = make([][]interface{}, 0, n)
	}
}

// PreallocResultsFlat pre-allocates both the results header slice and the flat
// backing buffer for zero-alloc per-row result storage.
// rows × cols gives the total number of value slots in the flat buffer.
func (vm *VM) PreallocResultsFlat(rows, cols int) {
	if rows <= 0 || cols <= 0 {
		return
	}
	if cap(vm.results) < rows {
		vm.results = make([][]interface{}, 0, rows)
	}
	needed := rows * cols
	if cap(vm.flatBuf) < needed {
		vm.flatBuf = make([]interface{}, 0, needed)
	}
}

func (vm *VM) RowsAffected() int64 {
	return vm.rowsAffected
}

func (vm *VM) Run(ctx interface{}) error {
	vm.Reset()
	vm.pc = 0

	err := vm.Exec(ctx)
	if err == ErrHalt {
		return nil
	}
	return err
}

func (vm *VM) Step(ctx interface{}) (done bool, err error) {
	if vm.pc >= len(vm.program.Instructions) {
		return true, nil
	}

	inst := vm.GetInstruction()

	switch inst.Op {
	case OpGoto:
		if inst.P2 > 0 {
			vm.pc = int(inst.P2)
		}
		return false, nil

	case OpGosub:
		vm.subReturn = append(vm.subReturn, vm.pc)
		if inst.P2 > 0 {
			vm.pc = int(inst.P2)
		}
		return false, nil

	case OpReturn:
		if len(vm.subReturn) > 0 {
			n := len(vm.subReturn)
			vm.pc = vm.subReturn[n-1]
			vm.subReturn = vm.subReturn[:n-1]
		}
		return false, nil

	case OpHalt:
		if inst.P4 != nil {
			if err, ok := inst.P4.(error); ok {
				return true, err
			}
		}
		return true, ErrHalt

	case OpNoop:
		return false, nil

	default:
		return false, fmt.Errorf("unimplemented opcode: %v", inst.Op)
	}
}
