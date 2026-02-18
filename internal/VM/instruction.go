package VM

type Instruction struct {
	Op OpCode
	P1 int32
	P2 int32
	P3 string
	P4 interface{}
}

func NewInstruction(op OpCode) Instruction {
	return Instruction{Op: op}
}

func (i *Instruction) SetP1(p1 int32) *Instruction {
	i.P1 = p1
	return i
}

func (i *Instruction) SetP2(p2 int32) *Instruction {
	i.P2 = p2
	return i
}

func (i *Instruction) SetP3(p3 string) *Instruction {
	i.P3 = p3
	return i
}

func (i *Instruction) SetP4(p4 interface{}) *Instruction {
	i.P4 = p4
	return i
}
