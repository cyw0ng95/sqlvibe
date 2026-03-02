package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "cg.h"
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// cgoBytecodeInstr mirrors the memory layout of VM.Instr (16 bytes):
//
//	uint16 Op, uint16 Fl, int32 A, int32 B, int32 C
//
// Used only to verify that the unsafe memory cast is safe.
var _ [16]byte = [16]byte{} // placeholder size assertion

// ── global compiler handle ────────────────────────────────────────────────

var (
	cgOnce     sync.Once
	cgCompiler *C.svdb_cg_compiler_t
)

func getCGCompiler() *C.svdb_cg_compiler_t {
	cgOnce.Do(func() {
		cgCompiler = C.svdb_cg_create()
	})
	return cgCompiler
}

// ── bytecode VM instruction optimiser ─────────────────────────────────────

// OptimizeBytecodeInstrs applies C++ dead-code elimination and peephole
// passes to a VM.BytecodeProg instruction slice.  The constant pool and
// other fields of the program are left unchanged; only the instruction
// slice may be shorter.
//
// A copy of the original slice is returned when the program is empty or the
// optimiser produces no improvement.
func OptimizeBytecodeInstrs(instrs []VM.Instr) []VM.Instr {
	if len(instrs) == 0 {
		return instrs
	}

	// VM.Instr is exactly 16 bytes: {uint16, uint16, int32, int32, int32}.
	const instrSize = 16
	inCount := C.size_t(len(instrs))
	outBuf := make([]VM.Instr, len(instrs))

	n := C.svdb_cg_optimize_bc_instrs(
		getCGCompiler(),
		(*C.uint8_t)(unsafe.Pointer(&instrs[0])),
		inCount,
		(*C.uint8_t)(unsafe.Pointer(&outBuf[0])),
		inCount,
	)
	_ = instrSize
	return outBuf[:n]
}

// ── plan-JSON wire format ─────────────────────────────────────────────────

// cgInstrJSON is the JSON representation of a VM.Instruction used to
// exchange programs with the C++ optimiser.
type cgInstrJSON struct {
	Op      int32  `json:"op"`
	P1      int32  `json:"p1"`
	P2      int32  `json:"p2"`
	P4Int   int64  `json:"p4_int"`
	P4Str   string `json:"p4_str"`
	P4Type  int32  `json:"p4_type"` // 0=none,1=int,2=str,3=regs
	P4Regs  []int  `json:"p4_regs,omitempty"`
}

type cgProgramJSON struct {
	Instructions []cgInstrJSON `json:"instructions"`
	ColumnNames  []string      `json:"column_names"`
	ResultReg    int32         `json:"result_reg"`
}

// programToJSON serialises a VM.Program to the CG wire format.
func programToJSON(prog *VM.Program, colNames []string) ([]byte, error) {
	if prog == nil {
		return nil, fmt.Errorf("nil program")
	}
	pj := cgProgramJSON{
		Instructions: make([]cgInstrJSON, len(prog.Instructions)),
		ColumnNames:  colNames,
		ResultReg:    -1,
	}
	for i, inst := range prog.Instructions {
		ij := cgInstrJSON{
			Op: int32(inst.Op),
			P1: inst.P1,
			P2: inst.P2,
		}
		switch v := inst.P4.(type) {
		case int:
			ij.P4Type = 1
			ij.P4Int = int64(v)
		case int64:
			ij.P4Type = 1
			ij.P4Int = v
		case string:
			ij.P4Type = 2
			ij.P4Str = v
		case []int:
			ij.P4Type = 3
			ij.P4Regs = v
		case map[string]int:
			// Named-column INSERT: extract register values as a register list
			// so the C++ optimizer can mark them as "read". Iteration order of Go
			// maps is non-deterministic, but that is fine — the optimizer only needs
			// the set of registers, not their order.
			regs := make([]int, 0, len(v))
			for _, reg := range v {
				regs = append(regs, reg)
			}
			ij.P4Type = 3
			ij.P4Regs = regs
		}
		pj.Instructions[i] = ij
	}
	return json.Marshal(&pj)
}

// jsonToProgram reconstructs a VM.Program from the CG wire JSON, using the
// original instruction slice to recover complex P4 values that the C++
// optimiser does not modify.
func jsonToProgram(data []byte, origInstrs []VM.Instruction) (*VM.Program, []string, error) {
	var pj cgProgramJSON
	if err := json.Unmarshal(data, &pj); err != nil {
		return nil, nil, fmt.Errorf("CG JSON decode: %w", err)
	}

	// Build a lookup map: (op,p1,p2) → original instruction for P4 recovery.
	type key struct{ op, p1, p2 int32 }
	origMap := make(map[key][]VM.Instruction, len(origInstrs))
	for _, oi := range origInstrs {
		k := key{int32(oi.Op), oi.P1, oi.P2}
		origMap[k] = append(origMap[k], oi)
	}
	usedCount := make(map[key]int)

	prog := VM.NewProgram()
	for _, ij := range pj.Instructions {
		k := key{ij.Op, ij.P1, ij.P2}
		idx := usedCount[k]
		var p4 interface{}
		if idx < len(origMap[k]) {
			p4 = origMap[k][idx].P4
			usedCount[k]++
		} else {
			switch ij.P4Type {
			case 1:
				p4 = int(ij.P4Int)
			case 2:
				p4 = ij.P4Str
			case 3:
				p4 = ij.P4Regs
			}
		}
		inst := VM.Instruction{
			Op:     VM.OpCode(ij.Op),
			P1:     ij.P1,
			P2:     ij.P2,
			P4:     p4,
			HasDst: p4 != nil,
		}
		if dst, ok := p4.(int); ok {
			inst.DstReg = dst
		}
		prog.AddInstruction(inst)
	}
	return prog, pj.ColumnNames, nil
}

// ── program optimiser (legacy VM.Program path) ────────────────────────────

// CGOptimizeProgram passes a compiled VM.Program through the C++ optimiser.
// It serialises the program to JSON, invokes the C++ optimisation passes,
// and reconstructs the optimised program.  The original program is returned
// unchanged if optimisation fails.
func CGOptimizeProgram(prog *VM.Program, colNames []string) *VM.Program {
	if prog == nil || len(prog.Instructions) == 0 {
		return prog
	}

	jsonData, err := programToJSON(prog, colNames)
	if err != nil {
		return prog
	}

	var errBuf [512]C.char
	cjson := C.CString(string(jsonData))
	defer C.free(unsafe.Pointer(cjson))

	cprog := C.svdb_cg_compile_select(
		getCGCompiler(),
		cjson,
		C.size_t(len(jsonData)),
		&errBuf[0],
		512,
	)
	if cprog == nil {
		return prog // C++ failed — keep original
	}
	defer C.svdb_cg_program_free(cprog)

	outJSON := C.GoString(C.svdb_cg_get_json(cprog))
	optimised, _, err := jsonToProgram([]byte(outJSON), prog.Instructions)
	if err != nil {
		return prog
	}
	optimised.NumRegs = prog.NumRegs
	optimised.NumCursors = prog.NumCursors
	optimised.NumAgg = prog.NumAgg
	return optimised
}

// ── C++ plan cache ────────────────────────────────────────────────────────

// CGPutPlan stores a serialised program in the C++ plan cache under sql.
func CGPutPlan(sql string, prog *VM.Program, colNames []string) {
	if prog == nil {
		return
	}
	data, err := programToJSON(prog, colNames)
	if err != nil {
		return
	}

	var errBuf [512]C.char
	cjson := C.CString(string(data))
	defer C.free(unsafe.Pointer(cjson))

	cprog := C.svdb_cg_compile_select(
		getCGCompiler(),
		cjson,
		C.size_t(len(data)),
		&errBuf[0],
		512,
	)
	if cprog == nil {
		return
	}
	defer C.svdb_cg_program_free(cprog)

	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	C.svdb_cg_cache_put(getCGCompiler(), csql, cprog)
}

// CGGetPlan retrieves a program from the C++ plan cache.
// Returns (nil, nil) on cache miss.
func CGGetPlan(sql string) (*VM.Program, []string) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	cprog := C.svdb_cg_cache_get(getCGCompiler(), csql)
	if cprog == nil {
		return nil, nil
	}
	defer C.svdb_cg_program_free(cprog)
	outJSON := C.GoString(C.svdb_cg_get_json(cprog))
	prog, cols, err := jsonToProgram([]byte(outJSON), nil)
	if err != nil {
		return nil, nil
	}
	return prog, cols
}

// CGClearPlanCache clears all entries in the C++ plan cache.
func CGClearPlanCache() {
	C.svdb_cg_cache_clear(getCGCompiler())
}

// CGPlanCacheSize returns the number of entries in the C++ plan cache.
func CGPlanCacheSize() int {
	return int(C.svdb_cg_cache_size(getCGCompiler()))
}

// ── optimisation level ────────────────────────────────────────────────────

// CGSetOptimizationLevel sets the C++ optimiser level (0=none, 1=basic, 2=aggressive).
func CGSetOptimizationLevel(level int) {
	C.svdb_cg_set_optimization_level(getCGCompiler(), C.int(level))
}
