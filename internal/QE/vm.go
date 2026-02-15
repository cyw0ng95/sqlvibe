package QE

type OpCode int

const (
	OpNull OpCode = iota
	OpTransaction
	OpAutoCommit
	OpOpenRead
	OpOpenWrite
	OpOpenAutoCommit
	OpOpenWriteCursor
	OpRewind
	OpNext
	OpPrev
	OpSeek
	OpSeekGT
	OpSeekGE
	OpSeekLT
	OpFound
	OpNotFound
	OpLast
	OpReset
	OpResultColumn
	OpResultRow
	OpConstNull
	OpFunction
	OpFunction0
	OpMakeRecord
	OpInit
	OpGoto
	OpGosub
	OpReturn
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpRemainder
	OpConcat
	OpBitAnd
	OpBitOr
	OpShiftLeft
	OpShiftRight
	OpAddImm
	OpAggStep
	OpAggFinal
	OpCount
	OpIsNull
	OpNotNull
	OpIfNull
	OpColumn
	OpAffinity
	OpString
	OpBlob
	OpInteger
	OpReal
	OpInteger16
	OpTransientBlob
	OpZeroBlob
	OpBlobZero
	OpCopy
	OpSCopy
	OpIntCopy
	OpStrReset
	OpStrGet
	OpStrLength
	OpSubstr
	OpStrSub
	OpStrCompare
	OpStrAppend
	OpStrGroupConcat
	OpNumericLoad
	OpColumnAvg
	OpAcos
	OpAcosh
	OpSin
	OpAsin
	OpAtan
	OpAtan2
	OpCeil
	OpCeiling
	OpCos
	OpCosh
	OpDegToRad
	OpExp
	OpFloor
	OpLn
	OpLog
	OpLog10
	OpMod
	OpPow
	OpRadToDeg
	OpSinh
	OpSqrt
	OpTan
	OpTanh
	OpTrim
	OpLTrim
	OpRTrim
	OpReplace
	OpOverlaps
	OpList
	OpListPush
	OpListPop
	OpListToArray
	OpArrayToList
	OpSql
	OpCursorHint
	OpNoop
	OpExplain
	OpBlobAppend
	OpRealToInt
)

type Instruction struct {
	Op OpCode
	P1 int32
	P2 int32
	P3 string
	P4 interface{}
}

type VM struct {
	instructions []Instruction
	pc           int
	registers    []interface{}
	numRegs      int
}

func NewVM(instructions []Instruction, numRegs int) *VM {
	return &VM{
		instructions: instructions,
		pc:           0,
		registers:    make([]interface{}, numRegs),
		numRegs:      numRegs,
	}
}

func (vm *VM) Reset() {
	vm.pc = 0
	for i := range vm.registers {
		vm.registers[i] = nil
	}
}

func (vm *VM) GetRegister(reg int) interface{} {
	if reg < 0 || reg >= vm.numRegs {
		return nil
	}
	return vm.registers[reg]
}

func (vm *VM) SetRegister(reg int, value interface{}) {
	if reg >= 0 && reg < vm.numRegs {
		vm.registers[reg] = value
	}
}

func (vm *VM) PC() int {
	return vm.pc
}

func (vm *VM) SetPC(pc int) {
	vm.pc = pc
}

func (vm *VM) NextInstruction() Instruction {
	if vm.pc >= len(vm.instructions) {
		return Instruction{Op: OpNoop}
	}
	inst := vm.instructions[vm.pc]
	vm.pc++
	return inst
}

func (vm *VM) Program() []Instruction {
	return vm.instructions
}
