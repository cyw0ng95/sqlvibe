package VM

type Program struct {
	Instructions []Instruction
	NumRegs      int
	NumCursors   int
	NumAgg       int
	whereFixups  []whereFixupInfo
}

type whereFixupInfo struct {
	idx   int
	useP2 bool
}

func NewProgram() *Program {
	return &Program{
		Instructions: make([]Instruction, 0),
		NumRegs:      0,
		NumCursors:   0,
		NumAgg:       0,
		whereFixups:  make([]whereFixupInfo, 0),
	}
}

func (p *Program) AddInstruction(inst Instruction) {
	p.Instructions = append(p.Instructions, inst)
}

func (p *Program) Emit(op OpCode) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{Op: op})
	return idx
}

func (p *Program) EmitOp(op OpCode, p1, p2 int32) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
	})
	if int(p1) >= p.NumRegs {
		p.NumRegs = int(p1) + 1
	}
	if int(p2) >= p.NumRegs {
		p.NumRegs = int(p2) + 1
	}
	return idx
}

func (p *Program) EmitOpWithDst(op OpCode, p1, p2 int32, dst int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
		P4: dst,
	})
	if dst >= p.NumRegs {
		p.NumRegs = dst + 1
	}
	if int(p1) >= p.NumRegs {
		p.NumRegs = int(p1) + 1
	}
	return idx
}

func (p *Program) EmitLoadConst(reg int, value interface{}) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpLoadConst,
		P1: int32(reg),
		P4: value,
	})
	if reg >= p.NumRegs {
		p.NumRegs = reg + 1
	}
	return idx
}

func (p *Program) EmitMove(src, dst int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpMove,
		P1: int32(src),
		P2: int32(dst),
	})
	if dst >= p.NumRegs {
		p.NumRegs = dst + 1
	}
	return idx
}

func (p *Program) EmitCopy(src, dst int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpCopy,
		P1: int32(src),
		P2: int32(dst),
	})
	if dst >= p.NumRegs {
		p.NumRegs = dst + 1
	}
	return idx
}

func (p *Program) EmitGoto(target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpGoto,
		P2: int32(target),
	})
	return idx
}

func (p *Program) EmitGosub(target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpGosub,
		P2: int32(target),
	})
	return idx
}

func (p *Program) EmitReturn() int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpReturn,
	})
	return idx
}

func (p *Program) EmitEq(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpEq,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitNe(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpNe,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitLt(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpLt,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitLe(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpLe,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitGt(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpGt,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitGe(p1, p2, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpGe,
		P1: int32(p1),
		P2: int32(p2),
		P3: "",
		P4: target,
	})
	if p1 >= p.NumRegs || p2 >= p.NumRegs {
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitIsNull(reg, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpIsNull,
		P1: int32(reg),
		P2: int32(target),
	})
	if reg >= p.NumRegs {
		p.NumRegs = reg + 1
	}
	return idx
}

func (p *Program) EmitNotNull(reg, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpNotNull,
		P1: int32(reg),
		P2: int32(target),
	})
	if reg >= p.NumRegs {
		p.NumRegs = reg + 1
	}
	return idx
}

func (p *Program) EmitAdd(dst, p1, p2 int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpAdd,
		P1: int32(p1),
		P2: int32(p2),
		P4: dst,
	})
	if dst >= p.NumRegs || p1 >= p.NumRegs || p2 >= p.NumRegs {
		if dst >= p.NumRegs {
			p.NumRegs = dst + 1
		}
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitSubtract(dst, p1, p2 int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpSubtract,
		P1: int32(p1),
		P2: int32(p2),
		P4: dst,
	})
	if dst >= p.NumRegs || p1 >= p.NumRegs || p2 >= p.NumRegs {
		if dst >= p.NumRegs {
			p.NumRegs = dst + 1
		}
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitMultiply(dst, p1, p2 int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpMultiply,
		P1: int32(p1),
		P2: int32(p2),
		P4: dst,
	})
	if dst >= p.NumRegs || p1 >= p.NumRegs || p2 >= p.NumRegs {
		if dst >= p.NumRegs {
			p.NumRegs = dst + 1
		}
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitDivide(dst, p1, p2 int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpDivide,
		P1: int32(p1),
		P2: int32(p2),
		P4: dst,
	})
	if dst >= p.NumRegs || p1 >= p.NumRegs || p2 >= p.NumRegs {
		if dst >= p.NumRegs {
			p.NumRegs = dst + 1
		}
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitConcat(dst, p1, p2 int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpConcat,
		P1: int32(p1),
		P2: int32(p2),
		P4: dst,
	})
	if dst >= p.NumRegs || p1 >= p.NumRegs || p2 >= p.NumRegs {
		if dst >= p.NumRegs {
			p.NumRegs = dst + 1
		}
		if p1 >= p.NumRegs {
			p.NumRegs = p1 + 1
		}
		if p2 >= p.NumRegs {
			p.NumRegs = p2 + 1
		}
	}
	return idx
}

func (p *Program) EmitColumn(dst, cursor, col int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpColumn,
		P1: int32(cursor),
		P2: int32(col),
		P4: dst,
	})
	if dst >= p.NumRegs {
		p.NumRegs = dst + 1
	}
	if cursor >= p.NumCursors {
		p.NumCursors = cursor + 1
	}
	return idx
}

// EmitColumnWithTable emits column load with table qualifier for correlation check
func (p *Program) EmitColumnWithTable(dst, cursor, col int, tableQualifier string) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpColumn,
		P1: int32(cursor),
		P2: int32(col),
		P3: tableQualifier,
		P4: dst,
	})
	if dst >= p.NumRegs {
		p.NumRegs = dst + 1
	}
	if cursor >= p.NumCursors {
		p.NumCursors = cursor + 1
	}
	return idx
}

func (p *Program) EmitResultRow(regs []int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpResultRow,
		P1: int32(len(regs)),
		P4: regs,
	})
	for _, r := range regs {
		if r >= p.NumRegs {
			p.NumRegs = r + 1
		}
	}
	return idx
}

func (p *Program) EmitHalt(err error) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpHalt,
		P1: 0,
		P4: err,
	})
	return idx
}

func (p *Program) EmitOpenTable(cursorID int, tableName string) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpOpenRead,
		P1: int32(cursorID),
		P3: tableName,
	})
	if cursorID >= p.NumCursors {
		p.NumCursors = cursorID + 1
	}
	return idx
}

func (p *Program) EmitRewind(cursorID int, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpRewind,
		P1: int32(cursorID),
		P2: int32(target),
	})
	return idx
}

func (p *Program) EmitNext(cursorID int, target int) int {
	idx := len(p.Instructions)
	p.Instructions = append(p.Instructions, Instruction{
		Op: OpNext,
		P1: int32(cursorID),
		P2: int32(target),
	})
	return idx
}

func (p *Program) Fixup(idx int) {
	p.Instructions[idx].P2 = int32(len(p.Instructions))
	if p.Instructions[idx].P4 != nil {
		if _, ok := p.Instructions[idx].P4.(int); ok {
			p.Instructions[idx].P4 = len(p.Instructions)
		}
	}
}

func (p *Program) FixupWithPos(idx, target int) {
	p.Instructions[idx].P2 = int32(target)
	if p.Instructions[idx].P4 != nil {
		if _, ok := p.Instructions[idx].P4.(int); ok {
			p.Instructions[idx].P4 = target
		}
	}
}

func (p *Program) GetInstruction(idx int) Instruction {
	if idx >= 0 && idx < len(p.Instructions) {
		return p.Instructions[idx]
	}
	return Instruction{Op: OpNoop}
}

func (p *Program) MarkFixup(idx int) {
	p.whereFixups = append(p.whereFixups, whereFixupInfo{idx: idx, useP2: false})
}

func (p *Program) MarkFixupP2(idx int) {
	p.whereFixups = append(p.whereFixups, whereFixupInfo{idx: idx, useP2: true})
}

func (p *Program) ApplyWhereFixups() {
	target := len(p.Instructions)
	for _, fixup := range p.whereFixups {
		idx := fixup.idx
		if idx >= 0 && idx < len(p.Instructions) {
			if fixup.useP2 {
				p.Instructions[idx].P2 = int32(target)
			} else {
				p.Instructions[idx].P4 = target
			}
		}
	}
	p.whereFixups = p.whereFixups[:0]
}
