package VM

// BcOpCode is the 16-bit opcode for the bytecode VM.
type BcOpCode uint16

const (
	BcNoop       BcOpCode = iota // no-op
	BcLoadConst                  // regs[C] = consts[B]
	BcLoadReg                    // regs[C] = regs[A]
	BcAdd                        // regs[C] = regs[A] + regs[B]
	BcAddInt                     // regs[C] = regs[A] + regs[B] (int fast-path)
	BcSub                        // regs[C] = regs[A] - regs[B]
	BcMul                        // regs[C] = regs[A] * regs[B]
	BcDiv                        // regs[C] = regs[A] / regs[B]
	BcMod                        // regs[C] = regs[A] % regs[B]
	BcNeg                        // regs[C] = -regs[A]
	BcConcat                     // regs[C] = regs[A] || regs[B]
	BcEq                         // regs[C] = regs[A] == regs[B]
	BcNe                         // regs[C] = regs[A] != regs[B]
	BcLt                         // regs[C] = regs[A] < regs[B]
	BcLe                         // regs[C] = regs[A] <= regs[B]
	BcGt                         // regs[C] = regs[A] > regs[B]
	BcGe                         // regs[C] = regs[A] >= regs[B]
	BcAnd                        // regs[C] = regs[A] AND regs[B]
	BcOr                         // regs[C] = regs[A] OR regs[B]
	BcNot                        // regs[C] = NOT regs[A]
	BcIsNull                     // regs[C] = (regs[A] IS NULL)
	BcNotNull                    // regs[C] = (regs[A] IS NOT NULL)
	BcJump                       // pc = C (unconditional)
	BcJumpTrue                   // if regs[A] is truthy: pc = C
	BcJumpFalse                  // if regs[A] is falsy:  pc = C
	BcOpenCursor                 // open cursor A for table named consts[B].Text()
	BcRewind                     // rewind cursor A to first row; jump to C if empty
	BcNext                       // advance cursor A; jump to C if no more rows
	BcColumn                     // regs[C] = column B of cursor A
	BcRowid                      // regs[C] = rowid of cursor A
	BcResultRow                  // emit result row: regs[A..A+B-1]
	BcHalt                       // stop execution
	BcAggInit                    // init aggregate state for slot A
	BcAggStep                    // step aggregate A with value regs[B]
	BcAggFinal                   // regs[C] = final value of aggregate A
	BcCall                       // regs[C] = call func consts[A] with B args starting at regs[C-B]

	NumBcOpcodes // sentinel — must be last
)

// BcOpName maps opcode to its display name.
var BcOpName = map[BcOpCode]string{
	BcNoop:       "Noop",
	BcLoadConst:  "LoadConst",
	BcLoadReg:    "LoadReg",
	BcAdd:        "Add",
	BcAddInt:     "AddInt",
	BcSub:        "Sub",
	BcMul:        "Mul",
	BcDiv:        "Div",
	BcMod:        "Mod",
	BcNeg:        "Neg",
	BcConcat:     "Concat",
	BcEq:         "Eq",
	BcNe:         "Ne",
	BcLt:         "Lt",
	BcLe:         "Le",
	BcGt:         "Gt",
	BcGe:         "Ge",
	BcAnd:        "And",
	BcOr:         "Or",
	BcNot:        "Not",
	BcIsNull:     "IsNull",
	BcNotNull:    "NotNull",
	BcJump:       "Jump",
	BcJumpTrue:   "JumpTrue",
	BcJumpFalse:  "JumpFalse",
	BcOpenCursor: "OpenCursor",
	BcRewind:     "Rewind",
	BcNext:       "Next",
	BcColumn:     "Column",
	BcRowid:      "Rowid",
	BcResultRow:  "ResultRow",
	BcHalt:       "Halt",
	BcAggInit:    "AggInit",
	BcAggStep:    "AggStep",
	BcAggFinal:   "AggFinal",
	BcCall:       "Call",
}
