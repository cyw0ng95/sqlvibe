package VM

import (
	"fmt"
	"math"
	"strings"
)

// bcOpNoop is a no-op.
func bcOpNoop(vm *BytecodeVM, inst Instr) (int, error) {
	return vm.pc + 1, nil
}

// bcOpLoadConst: regs[C] = consts[B]
func bcOpLoadConst(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = vm.constVal(inst.B)
	return vm.pc + 1, nil
}

// bcOpLoadReg: regs[C] = regs[A]
func bcOpLoadReg(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = *vm.reg(inst.A)
	return vm.pc + 1, nil
}

// bcOpAdd: regs[C] = regs[A] + regs[B]
func bcOpAdd(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = AddVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpAddInt: regs[C] = regs[A] + regs[B], integer fast path
func bcOpAddInt(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagInt && b.T == TagInt {
		*vm.reg(inst.C) = VmInt(a.N + b.N)
	} else {
		*vm.reg(inst.C) = AddVmVal(*a, *b)
	}
	return vm.pc + 1, nil
}

// bcOpSub: regs[C] = regs[A] - regs[B]
func bcOpSub(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = SubVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpMul: regs[C] = regs[A] * regs[B]
func bcOpMul(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = MulVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpDiv: regs[C] = regs[A] / regs[B]
func bcOpDiv(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = DivVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpMod: regs[C] = regs[A] % regs[B]
func bcOpMod(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = ModVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpNeg: regs[C] = -regs[A]
func bcOpNeg(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = NegVmVal(*vm.reg(inst.A))
	return vm.pc + 1, nil
}

// bcOpConcat: regs[C] = regs[A] || regs[B]
func bcOpConcat(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = ConcatVmVal(*vm.reg(inst.A), *vm.reg(inst.B))
	return vm.pc + 1, nil
}

// bcOpEq: regs[C] = (regs[A] == regs[B])
func bcOpEq(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) == 0)
	}
	return vm.pc + 1, nil
}

// bcOpNe: regs[C] = (regs[A] != regs[B])
func bcOpNe(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) != 0)
	}
	return vm.pc + 1, nil
}

// bcOpLt: regs[C] = (regs[A] < regs[B])
func bcOpLt(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) < 0)
	}
	return vm.pc + 1, nil
}

// bcOpLe: regs[C] = (regs[A] <= regs[B])
func bcOpLe(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) <= 0)
	}
	return vm.pc + 1, nil
}

// bcOpGt: regs[C] = (regs[A] > regs[B])
func bcOpGt(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) > 0)
	}
	return vm.pc + 1, nil
}

// bcOpGe: regs[C] = (regs[A] >= regs[B])
func bcOpGe(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	if a.T == TagNull || b.T == TagNull {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(CompareVmVal(*a, *b) >= 0)
	}
	return vm.pc + 1, nil
}

// bcOpAnd: regs[C] = regs[A] AND regs[B] (SQL three-valued logic)
func bcOpAnd(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	// FALSE AND anything = FALSE
	aFalse := !a.IsNull() && !bcTruthy(*a)
	bFalse := !b.IsNull() && !bcTruthy(*b)
	if aFalse || bFalse {
		*vm.reg(inst.C) = VmBool(false)
	} else if a.IsNull() || b.IsNull() {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(true)
	}
	return vm.pc + 1, nil
}

// bcOpOr: regs[C] = regs[A] OR regs[B] (SQL three-valued logic)
func bcOpOr(vm *BytecodeVM, inst Instr) (int, error) {
	a, b := vm.reg(inst.A), vm.reg(inst.B)
	// TRUE OR anything = TRUE
	aTrue := !a.IsNull() && bcTruthy(*a)
	bTrue := !b.IsNull() && bcTruthy(*b)
	if aTrue || bTrue {
		*vm.reg(inst.C) = VmBool(true)
	} else if a.IsNull() || b.IsNull() {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(false)
	}
	return vm.pc + 1, nil
}

// bcOpNot: regs[C] = NOT regs[A]
func bcOpNot(vm *BytecodeVM, inst Instr) (int, error) {
	a := vm.reg(inst.A)
	if a.IsNull() {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmBool(!bcTruthy(*a))
	}
	return vm.pc + 1, nil
}

// bcOpIsNull: regs[C] = (regs[A] IS NULL) — always boolean, never NULL.
func bcOpIsNull(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = VmBool(vm.reg(inst.A).IsNull())
	return vm.pc + 1, nil
}

// bcOpNotNull: regs[C] = (regs[A] IS NOT NULL)
func bcOpNotNull(vm *BytecodeVM, inst Instr) (int, error) {
	*vm.reg(inst.C) = VmBool(!vm.reg(inst.A).IsNull())
	return vm.pc + 1, nil
}

// bcOpJump: pc = C (unconditional)
func bcOpJump(vm *BytecodeVM, inst Instr) (int, error) {
	return int(inst.C), nil
}

// bcOpJumpTrue: if regs[A] is truthy: pc = C, else pc+1
func bcOpJumpTrue(vm *BytecodeVM, inst Instr) (int, error) {
	if bcTruthy(*vm.reg(inst.A)) {
		return int(inst.C), nil
	}
	return vm.pc + 1, nil
}

// bcOpJumpFalse: if regs[A] is falsy: pc = C, else pc+1
func bcOpJumpFalse(vm *BytecodeVM, inst Instr) (int, error) {
	if !bcTruthy(*vm.reg(inst.A)) {
		return int(inst.C), nil
	}
	return vm.pc + 1, nil
}

// bcOpOpenCursor: open cursor A for table named consts[B].Text()
func bcOpOpenCursor(vm *BytecodeVM, inst Instr) (int, error) {
	if vm.ctx == nil {
		return 0, fmt.Errorf("BcOpenCursor: no context")
	}
	tableName := vm.constVal(inst.B).Text()
	rows, colOrder, err := vm.ctx.GetTableRows(tableName)
	if err != nil {
		return 0, fmt.Errorf("BcOpenCursor: %w", err)
	}
	vm.cursors[inst.A] = &bcCursor{rows: rows, colOrder: colOrder, pos: -1}
	return vm.pc + 1, nil
}

// bcOpRewind: reset cursor A to first row; jump to C if empty.
func bcOpRewind(vm *BytecodeVM, inst Instr) (int, error) {
	cur := vm.cursors[inst.A]
	if cur == nil {
		return int(inst.C), nil
	}
	cur.pos = 0
	if len(cur.rows) == 0 {
		return int(inst.C), nil
	}
	return vm.pc + 1, nil
}

// bcOpNext: advance cursor A.
// If more rows remain, jump to C (loop-body target).
// Falls through (pc+1) when the cursor is exhausted.
func bcOpNext(vm *BytecodeVM, inst Instr) (int, error) {
	cur := vm.cursors[inst.A]
	if cur == nil {
		return vm.pc + 1, nil
	}
	cur.pos++
	if cur.pos < len(cur.rows) {
		return int(inst.C), nil // more rows → jump back to loop body
	}
	return vm.pc + 1, nil // exhausted → fall through (loop exit)
}

// bcOpColumn: regs[C] = column B of cursor A
func bcOpColumn(vm *BytecodeVM, inst Instr) (int, error) {
	cur := vm.cursors[inst.A]
	if cur == nil || cur.pos < 0 || cur.pos >= len(cur.rows) {
		*vm.reg(inst.C) = VmNull()
		return vm.pc + 1, nil
	}
	row := cur.rows[cur.pos]
	colIdx := int(inst.B)
	var val interface{}
	if colIdx < len(cur.colOrder) {
		val = row[cur.colOrder[colIdx]]
	} else {
		// fall back to sorted keys
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		if colIdx < len(keys) {
			val = row[keys[colIdx]]
		}
	}
	*vm.reg(inst.C) = FromInterface(val)
	return vm.pc + 1, nil
}

// bcOpRowid: regs[C] = current rowid of cursor A (1-based row index).
func bcOpRowid(vm *BytecodeVM, inst Instr) (int, error) {
	cur := vm.cursors[inst.A]
	if cur == nil || cur.pos < 0 {
		*vm.reg(inst.C) = VmNull()
	} else {
		*vm.reg(inst.C) = VmInt(int64(cur.pos + 1))
	}
	return vm.pc + 1, nil
}

// bcOpResultRow: emit result row from regs[A..A+B-1].
func bcOpResultRow(vm *BytecodeVM, inst Instr) (int, error) {
	n := int(inst.B)
	row := make([]VmVal, n)
	for i := 0; i < n; i++ {
		row[i] = *vm.reg(inst.A + int32(i))
	}
	vm.resultRows = append(vm.resultRows, row)
	return vm.pc + 1, nil
}

// bcOpHalt: stop execution.
func bcOpHalt(vm *BytecodeVM, inst Instr) (int, error) {
	return len(vm.prog.Instrs), nil // jump past end
}

// bcOpAggInit: initialise aggregate slot A with function name from consts[B].
func bcOpAggInit(vm *BytecodeVM, inst Instr) (int, error) {
	name := strings.ToLower(vm.constVal(inst.B).Text())
	vm.aggSlots[inst.A] = &aggState{name: name, allNull: true}
	return vm.pc + 1, nil
}

// bcOpAggStep: update aggregate A with value in regs[B].
func bcOpAggStep(vm *BytecodeVM, inst Instr) (int, error) {
	agg := vm.aggSlots[inst.A]
	if agg == nil {
		return 0, fmt.Errorf("BcAggStep: slot %d not initialised", inst.A)
	}
	v := *vm.reg(inst.B)
	if v.IsNull() {
		return vm.pc + 1, nil
	}
	agg.count++
	if agg.allNull {
		agg.allNull = false
		agg.sum = v
		agg.min = v
		agg.max = v
	} else {
		agg.sum = AddVmVal(agg.sum, v)
		if CompareVmVal(v, agg.min) < 0 {
			agg.min = v
		}
		if CompareVmVal(v, agg.max) > 0 {
			agg.max = v
		}
	}
	agg.vals = append(agg.vals, v)
	return vm.pc + 1, nil
}

// bcOpAggFinal: regs[C] = final aggregate value for slot A.
func bcOpAggFinal(vm *BytecodeVM, inst Instr) (int, error) {
	agg := vm.aggSlots[inst.A]
	if agg == nil {
		*vm.reg(inst.C) = VmNull()
		return vm.pc + 1, nil
	}
	if agg.allNull {
		switch agg.name {
		case "count", "count(*)":
			*vm.reg(inst.C) = VmInt(0)
		default:
			*vm.reg(inst.C) = VmNull()
		}
		return vm.pc + 1, nil
	}
	switch agg.name {
	case "count", "count(*)":
		*vm.reg(inst.C) = VmInt(agg.count)
	case "sum":
		*vm.reg(inst.C) = agg.sum
	case "min":
		*vm.reg(inst.C) = agg.min
	case "max":
		*vm.reg(inst.C) = agg.max
	case "avg":
		if agg.count == 0 {
			*vm.reg(inst.C) = VmNull()
		} else {
			*vm.reg(inst.C) = VmFloat(toFloat(agg.sum) / float64(agg.count))
		}
	default:
		*vm.reg(inst.C) = agg.sum
	}
	return vm.pc + 1, nil
}

// bcOpCall: regs[C] = call built-in function named consts[A] with B args starting at regs[C-B].
func bcOpCall(vm *BytecodeVM, inst Instr) (int, error) {
	name := strings.ToLower(vm.constVal(inst.A).Text())
	nArgs := int(inst.B)
	// args are in regs[C-nArgs .. C-1]
	args := make([]VmVal, nArgs)
	base := int(inst.C) - nArgs
	for i := 0; i < nArgs; i++ {
		args[i] = *vm.reg(int32(base + i))
	}
	result := bcCallBuiltin(name, args)
	*vm.reg(inst.C) = result
	return vm.pc + 1, nil
}

// bcTruthy determines the SQL truthiness of a VmVal.
func bcTruthy(v VmVal) bool {
	switch v.T {
	case TagNull:
		return false
	case TagBool:
		return v.N != 0
	case TagInt:
		return v.N != 0
	case TagFloat:
		return math.Float64frombits(uint64(v.N)) != 0
	case TagText:
		// SQL: non-empty strings that parse as non-zero are truthy
		if v.S == "" || v.S == "0" || strings.EqualFold(v.S, "false") {
			return false
		}
		return true
	case TagBlob:
		return len(v.S) > 0
	}
	return false
}

// bcCallBuiltin handles simple built-in scalar functions.
func bcCallBuiltin(name string, args []VmVal) VmVal {
	switch name {
	case "abs":
		if len(args) < 1 || args[0].IsNull() {
			return VmNull()
		}
		v := args[0]
		if v.T == TagFloat {
			f := v.Float()
			if f < 0 {
				return VmFloat(-f)
			}
			return v
		}
		n := v.Int()
		if n < 0 {
			return VmInt(-n)
		}
		return v
	case "length":
		if len(args) < 1 || args[0].IsNull() {
			return VmNull()
		}
		return VmInt(int64(len([]rune(args[0].Text()))))
	case "upper":
		if len(args) < 1 || args[0].IsNull() {
			return VmNull()
		}
		return VmText(strings.ToUpper(args[0].Text()))
	case "lower":
		if len(args) < 1 || args[0].IsNull() {
			return VmNull()
		}
		return VmText(strings.ToLower(args[0].Text()))
	case "coalesce":
		for _, a := range args {
			if !a.IsNull() {
				return a
			}
		}
		return VmNull()
	case "ifnull":
		if len(args) >= 1 && !args[0].IsNull() {
			return args[0]
		}
		if len(args) >= 2 {
			return args[1]
		}
		return VmNull()
	case "typeof":
		if len(args) < 1 {
			return VmText("null")
		}
		switch args[0].T {
		case TagNull:
			return VmText("null")
		case TagInt:
			return VmText("integer")
		case TagFloat:
			return VmText("real")
		case TagText:
			return VmText("text")
		case TagBlob:
			return VmText("blob")
		case TagBool:
			return VmText("integer")
		}
		return VmText("null")
	case "nullif":
		if len(args) >= 2 && !args[0].IsNull() && !args[1].IsNull() {
			if CompareVmVal(args[0], args[1]) == 0 {
				return VmNull()
			}
		}
		if len(args) >= 1 {
			return args[0]
		}
		return VmNull()
	}
	return VmNull()
}
