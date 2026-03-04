// Package VM - minimal Program type for C++ wrapper compatibility
package VM

// Program represents a VM program.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual program handling is done in C++.
type Program struct {
	NumRegs      int           // Number of registers
	NumCursors   int           // Number of cursors
	NumAgg       int           // Number of aggregations
	Instructions []Instruction // Instruction list
}

// NewProgram creates a new program.
func NewProgram() *Program {
	return &Program{}
}

// AddInstruction adds an instruction to the program.
func (p *Program) AddInstruction(inst Instruction) {
	p.Instructions = append(p.Instructions, inst)
}

// OpCode is an alias for uint16 opcode.
type OpCode = uint16
