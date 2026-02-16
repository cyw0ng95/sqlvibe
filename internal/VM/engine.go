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

type VM struct {
	program   *Program
	pc        int
	registers []interface{}
	cursors   *CursorArray
	subReturn []int
	affinity  int
	undo      [][]interface{}
	errorcnt  int
	err       error
}

func NewVM(program *Program) *VM {
	numRegs := program.NumRegs
	if numRegs < 16 {
		numRegs = 16
	}
	return &VM{
		program:   program,
		pc:        0,
		registers: make([]interface{}, numRegs),
		cursors:   NewCursorArray(),
		subReturn: make([]int, 0),
		undo:      make([][]interface{}, 0),
		errorcnt:  0,
		err:       nil,
	}
}

func (vm *VM) Reset() {
	vm.pc = 0
	for i := range vm.registers {
		vm.registers[i] = nil
	}
	vm.cursors.Reset()
	vm.subReturn = make([]int, 0)
	vm.affinity = 0
	vm.errorcnt = 0
	vm.err = nil
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

func (vm *VM) Run(ctx interface{}) error {
	vm.Reset()
	vm.pc = 0

	for {
		inst := vm.GetInstruction()

		switch inst.Op {
		case OpGoto:
			if inst.P2 > 0 {
				vm.pc = int(inst.P2)
			}

		case OpGosub:
			vm.subReturn = append(vm.subReturn, vm.pc)
			if inst.P2 > 0 {
				vm.pc = int(inst.P2)
			}

		case OpReturn:
			if len(vm.subReturn) > 0 {
				n := len(vm.subReturn)
				vm.pc = vm.subReturn[n-1]
				vm.subReturn = vm.subReturn[:n-1]
			}

		case OpHalt:
			if inst.P4 != nil {
				if err, ok := inst.P4.(error); ok {
					return err
				}
			}
			return ErrHalt

		case OpNoop:

		default:
			return fmt.Errorf("unimplemented opcode: %v", inst.Op)
		}

		if vm.pc >= len(vm.program.Instructions) {
			break
		}

		if vm.pc < 0 {
			return fmt.Errorf("negative PC: %d", vm.pc)
		}
	}

	return nil
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
