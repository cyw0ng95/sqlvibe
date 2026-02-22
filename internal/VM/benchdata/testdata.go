// Package benchdata provides test data generators for VM benchmarks.
package benchdata

import (
	"fmt"

	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// GenerateArithProgram builds a program that performs n arithmetic operations
// (add, subtract, multiply) on two constant registers and stores the result.
func GenerateArithProgram(n int) *VM.Program {
	p := VM.NewProgram()
	p.EmitLoadConst(0, int64(42))
	p.EmitLoadConst(1, int64(7))
	for i := 0; i < n; i++ {
		dst := 2 + (i % 8)
		switch i % 3 {
		case 0:
			p.EmitAdd(dst, 0, 1)
		case 1:
			p.EmitSubtract(dst, 0, 1)
		case 2:
			p.EmitMultiply(dst, 0, 1)
		}
	}
	p.Emit(VM.OpHalt)
	return p
}

// GenerateResultRowProgram builds a program that emits n result rows, each with
// cols columns loaded from constants.
func GenerateResultRowProgram(n, cols int) *VM.Program {
	p := VM.NewProgram()
	regs := make([]int, cols)
	for c := 0; c < cols; c++ {
		regs[c] = c
		p.EmitLoadConst(c, int64(c+1))
	}
	for i := 0; i < n; i++ {
		p.EmitResultRow(regs)
	}
	p.Emit(VM.OpHalt)
	return p
}

// MakeTableRows generates n rows of data for the given column names.
func MakeTableRows(n int, cols []string) []map[string]interface{} {
	rows := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		row := make(map[string]interface{}, len(cols))
		for j, col := range cols {
			row[col] = fmt.Sprintf("val_%d_%d", i, j)
		}
		rows[i] = row
	}
	return rows
}

// MakeIntTableRows generates n rows with integer values.
func MakeIntTableRows(n int, cols []string) []map[string]interface{} {
	rows := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		row := make(map[string]interface{}, len(cols))
		for j, col := range cols {
			row[col] = int64(i*len(cols) + j)
		}
		rows[i] = row
	}
	return rows
}
