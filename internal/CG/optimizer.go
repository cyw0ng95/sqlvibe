package CG

import (
	"sort"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

type Optimizer struct{}

func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

// Optimize runs all optimization passes on the program.
func (o *Optimizer) Optimize(program *VM.Program) *VM.Program {
	if program == nil || len(program.Instructions) == 0 {
		return program
	}
	o.foldConstants(program)
	o.eliminateDeadCode(program)
	o.eliminateCommonSubexprs(program)
	o.reduceStrength(program)
	o.peepholeOptimize(program)
	return program
}

// foldConstants performs constant folding: replaces arithmetic/concat operations
// on compile-time constants with a single OpLoadConst instruction.
func (o *Optimizer) foldConstants(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)

	// Count how many times each register is read across the program.
	readCount := make(map[int32]int)
	for _, inst := range insts {
		switch inst.Op {
		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder, VM.OpConcat:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpMove, VM.OpCopy, VM.OpSCopy:
			readCount[inst.P1]++
		case VM.OpResultRow:
			if regs, ok := inst.P4.([]int); ok {
				for _, r := range regs {
					readCount[int32(r)]++
				}
			}
		case VM.OpEq, VM.OpNe, VM.OpLt, VM.OpLe, VM.OpGt, VM.OpGe, VM.OpIs, VM.OpIsNot:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpIsNull, VM.OpNotNull, VM.OpIfNull, VM.OpIfNull2:
			readCount[inst.P1]++
		case VM.OpIf, VM.OpIfNot:
			readCount[inst.P1]++
		}
	}

	// Track which registers hold compile-time constants and where they were defined.
	type constDef struct {
		value  interface{}
		defIdx int
	}
	constRegs := make(map[int32]constDef)

	for i := 0; i < n; i++ {
		inst := insts[i]

		switch inst.Op {
		case VM.OpLoadConst:
			constRegs[inst.P1] = constDef{inst.P4, i}

		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder:
			r1, r2 := inst.P1, inst.P2
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			c1, ok1 := constRegs[r1]
			c2, ok2 := constRegs[r2]
			if !ok1 || !ok2 {
				delete(constRegs, int32(dst))
				break
			}
			result := foldArith(inst.Op, c1.value, c2.value)
			if result == nil {
				delete(constRegs, int32(dst))
				break
			}
			// Replace arithmetic op with a constant load into dst.
			program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: result}
			constRegs[int32(dst)] = constDef{result, i}

			// NOP the source definitions if those registers are no longer read.
			readCount[r1]--
			if readCount[r1] <= 0 {
				program.Instructions[c1.defIdx].Op = VM.OpNoop
				delete(constRegs, r1)
			}
			readCount[r2]--
			if readCount[r2] <= 0 {
				program.Instructions[c2.defIdx].Op = VM.OpNoop
				delete(constRegs, r2)
			}

		case VM.OpConcat:
			r1, r2 := inst.P1, inst.P2
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			c1, ok1 := constRegs[r1]
			c2, ok2 := constRegs[r2]
			if !ok1 || !ok2 {
				delete(constRegs, int32(dst))
				break
			}
			s1, isStr1 := c1.value.(string)
			s2, isStr2 := c2.value.(string)
			if !isStr1 || !isStr2 {
				delete(constRegs, int32(dst))
				break
			}
			result := s1 + s2
			program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: result}
			constRegs[int32(dst)] = constDef{result, i}

			readCount[r1]--
			if readCount[r1] <= 0 {
				program.Instructions[c1.defIdx].Op = VM.OpNoop
				delete(constRegs, r1)
			}
			readCount[r2]--
			if readCount[r2] <= 0 {
				program.Instructions[c2.defIdx].Op = VM.OpNoop
				delete(constRegs, r2)
			}

		default:
			// If this instruction writes a register (as P1 or P4 int dest), invalidate it.
			delete(constRegs, inst.P1)
			if dst, ok := inst.P4.(int); ok {
				delete(constRegs, int32(dst))
			}
		}
	}
}

// foldArith evaluates a binary arithmetic operation on two constant values.
// Returns nil if the operation cannot be folded (e.g., division by zero, wrong types).
func foldArith(op VM.OpCode, v1, v2 interface{}) interface{} {
	// Convert both values to float64 for arithmetic.
	f1, ok1 := toFloat64(v1)
	f2, ok2 := toFloat64(v2)
	if !ok1 || !ok2 {
		return nil
	}

	switch op {
	case VM.OpAdd:
		result := f1 + f2
		return toSameType(v1, v2, result)
	case VM.OpSubtract:
		result := f1 - f2
		return toSameType(v1, v2, result)
	case VM.OpMultiply:
		result := f1 * f2
		return toSameType(v1, v2, result)
	case VM.OpDivide:
		if f2 == 0 {
			return nil // avoid division by zero
		}
		result := f1 / f2
		return toSameType(v1, v2, result)
	case VM.OpRemainder:
		i1, isInt1 := toInt64(v1)
		i2, isInt2 := toInt64(v2)
		if !isInt1 || !isInt2 || i2 == 0 {
			return nil
		}
		return i1 % i2
	}
	return nil
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float64:
		if val == float64(int64(val)) {
			return int64(val), true
		}
	}
	return 0, false
}

// toSameType preserves the type: if both inputs are integers, return int64; otherwise float64.
func toSameType(v1, v2 interface{}, result float64) interface{} {
	_, i1 := v1.(int64)
	_, i2 := v2.(int64)
	if !i1 {
		_, i1 = v1.(int)
	}
	if !i2 {
		_, i2 = v2.(int)
	}
	if i1 && i2 {
		return int64(result)
	}
	return result
}

// eliminateDeadCode removes unreachable instructions following unconditional
// control flow (OpHalt, OpGoto) by replacing them with OpNoop.
func (o *Optimizer) eliminateDeadCode(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)
	if n == 0 {
		return
	}

	// Collect all jump targets so we know which instructions are reachable entry points.
	jumpTargets := make(map[int]bool)
	for _, inst := range insts {
		// P2 is a jump target for most control-flow instructions.
		if inst.Op.IsJump() || inst.Op == VM.OpRewind || inst.Op == VM.OpNext ||
			inst.Op == VM.OpPrev || inst.Op == VM.OpInit {
			if inst.P2 > 0 {
				jumpTargets[int(inst.P2)] = true
			}
		}
		// P4 may be an integer jump target (WHERE fixups use P4 for jump addresses).
		if target, ok := inst.P4.(int); ok && target > 0 && target <= len(insts) {
			jumpTargets[target] = true
		}
	}

	dead := false
	for i := range insts {
		if jumpTargets[i] {
			dead = false
		}
		if dead {
			program.Instructions[i].Op = VM.OpNoop
			program.Instructions[i].P1 = 0
			program.Instructions[i].P2 = 0
			program.Instructions[i].P3 = ""
			program.Instructions[i].P4 = nil
			program.Instructions[i].DstReg = 0
			program.Instructions[i].HasDst = false
			continue
		}
		switch insts[i].Op {
		case VM.OpHalt:
			dead = true
		case VM.OpGoto:
			// OpGoto is unconditional; code after it is dead unless it's a jump target.
			dead = true
		}
	}
}

// csrExprKey is a key for common sub-expression elimination.
type csrExprKey struct {
	op VM.OpCode
	p1 int32
	p2 int32
}

// eliminateCommonSubexprs detects repeated arithmetic/concat computations in
// straight-line code segments (basic blocks) and replaces redundant re-
// computations with a cheap register copy (OpSCopy).
//
// The pass works at the VM-register level: if the same opcode is applied to the
// same pair of source registers, and neither source register has been modified
// since the first computation, the second instruction is replaced by an OpSCopy
// from the first destination register.
func (o *Optimizer) eliminateCommonSubexprs(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)
	if n == 0 {
		return
	}

	// Collect jump targets to detect basic-block boundaries.
	jumpTargets := make(map[int]bool)
	for _, inst := range insts {
		if inst.Op.IsJump() || inst.Op == VM.OpRewind || inst.Op == VM.OpNext ||
			inst.Op == VM.OpPrev || inst.Op == VM.OpInit {
			if inst.P2 > 0 {
				jumpTargets[int(inst.P2)] = true
			}
		}
		if target, ok := inst.P4.(int); ok && target > 0 && target <= n {
			jumpTargets[target] = true
		}
	}

	// computed maps an expression key to the destination register of its first
	// computation within the current basic-block segment.
	computed := make(map[csrExprKey]int32)
	// computedAt records the instruction index at which each CSE entry was created,
	// used to detect whether source registers have been redefined since.
	computedAt := make(map[csrExprKey]int)
	// lastWrite records the most recent instruction index that wrote each register.
	lastWrite := make(map[int32]int)

	resetCSE := func() {
		computed = make(map[csrExprKey]int32)
		computedAt = make(map[csrExprKey]int)
	}

	for i := 0; i < n; i++ {
		// At a basic-block boundary, discard accumulated CSE information.
		if jumpTargets[i] {
			resetCSE()
		}

		inst := insts[i]
		if inst.Op == VM.OpNoop {
			continue
		}

		switch inst.Op {
		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder, VM.OpConcat:
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			key := csrExprKey{inst.Op, inst.P1, inst.P2}
			if prevDst, exists := computed[key]; exists {
				prevIdx := computedAt[key]
				// Confirm that neither source register was redefined after the first computation.
				p1Stale := false
				p2Stale := false
				if w, ok2 := lastWrite[inst.P1]; ok2 && w > prevIdx {
					p1Stale = true
				}
				if w, ok2 := lastWrite[inst.P2]; ok2 && w > prevIdx {
					p2Stale = true
				}
				if !p1Stale && !p2Stale {
					// Replace with a shallow copy from the cached result register.
					program.Instructions[i] = VM.Instruction{
						Op: VM.OpSCopy,
						P1: prevDst,
						P2: int32(dst),
					}
					lastWrite[int32(dst)] = i
					continue
				}
			}
			// Record this computation for potential future CSE matches.
			computed[key] = int32(dst)
			computedAt[key] = i
			lastWrite[int32(dst)] = i

		default:
			// Determine which register this instruction writes to.
			writtenReg := int32(-1)
			switch inst.Op {
			case VM.OpLoadConst, VM.OpNull, VM.OpConstNull:
				writtenReg = inst.P1
			case VM.OpMove, VM.OpSCopy, VM.OpCopy, VM.OpIntCopy:
				writtenReg = inst.P2
			}
			if dst, ok := inst.P4.(int); ok {
				writtenReg = int32(dst)
			}
			if writtenReg >= 0 {
				lastWrite[writtenReg] = i
				// Invalidate CSE entries whose source operands include the overwritten register.
				for k := range computed {
					if k.p1 == writtenReg || k.p2 == writtenReg {
						delete(computed, k)
						delete(computedAt, k)
					}
				}
			}
			// Control-flow instructions end the current basic-block segment.
			if inst.Op.IsJump() || inst.Op == VM.OpGoto || inst.Op == VM.OpGosub ||
				inst.Op == VM.OpRewind || inst.Op == VM.OpNext {
				resetCSE()
			}
		}
	}
}

// reduceStrength replaces expensive arithmetic operations with cheaper
// equivalents when one operand is a known compile-time constant:
//
//   - x * 1  →  SCopy x, dst  (identity)
//   - 1 * x  →  SCopy x, dst
//   - x * 0  →  LoadConst dst, 0
//   - 0 * x  →  LoadConst dst, 0
//   - x * 2  →  Add x, x, dst  (addition is cheaper than multiply)
//   - x + 0  →  SCopy x, dst
//   - 0 + x  →  SCopy x, dst
//   - x - 0  →  SCopy x, dst
func (o *Optimizer) reduceStrength(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)
	if n == 0 {
		return
	}

	// Track registers that hold known constant values.
	constVals := make(map[int32]interface{})

	for i := 0; i < n; i++ {
		inst := insts[i]
		if inst.Op == VM.OpNoop {
			continue
		}

		switch inst.Op {
		case VM.OpLoadConst:
			constVals[inst.P1] = inst.P4

		case VM.OpMultiply:
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			// Check left operand
			if c, ok2 := constVals[inst.P1]; ok2 {
				if constIsZero(c) {
					program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: int64(0)}
					constVals[int32(dst)] = int64(0)
					continue
				}
				// Only reduce int(1) * x → SCopy x: float(1.0) * int(x) promotes x to float.
				if constIsIntOne(c) {
					program.Instructions[i] = VM.Instruction{Op: VM.OpSCopy, P1: inst.P2, P2: int32(dst)}
					delete(constVals, int32(dst))
					continue
				}
			}
			// Check right operand
			if c, ok2 := constVals[inst.P2]; ok2 {
				if constIsZero(c) {
					program.Instructions[i] = VM.Instruction{Op: VM.OpLoadConst, P1: int32(dst), P4: int64(0)}
					constVals[int32(dst)] = int64(0)
					continue
				}
				// Only reduce x * int(1) → SCopy x.
				if constIsIntOne(c) {
					program.Instructions[i] = VM.Instruction{Op: VM.OpSCopy, P1: inst.P1, P2: int32(dst)}
					delete(constVals, int32(dst))
					continue
				}
				if constIsIntTwo(c) {
					// x * 2 → x + x (avoids a more expensive multiply)
					program.Instructions[i] = VM.Instruction{
						Op: VM.OpAdd, P1: inst.P1, P2: inst.P1, P4: dst,
						DstReg: dst, HasDst: true,
					}
					delete(constVals, int32(dst))
					continue
				}
			}
			delete(constVals, int32(dst))

		case VM.OpAdd:
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			// Only reduce x + 0 (integer zero) → SCopy x.
			// Do NOT reduce x + 0.0 (float zero): the addition coerces x to float64,
			// which is observable when x is an integer column.
			if c, ok2 := constVals[inst.P1]; ok2 && constIsIntZero(c) {
				program.Instructions[i] = VM.Instruction{Op: VM.OpSCopy, P1: inst.P2, P2: int32(dst)}
				delete(constVals, int32(dst))
				continue
			}
			if c, ok2 := constVals[inst.P2]; ok2 && constIsIntZero(c) {
				program.Instructions[i] = VM.Instruction{Op: VM.OpSCopy, P1: inst.P1, P2: int32(dst)}
				delete(constVals, int32(dst))
				continue
			}
			delete(constVals, int32(dst))

		case VM.OpSubtract:
			dst, ok := inst.P4.(int)
			if !ok {
				break
			}
			// Only reduce x - 0 (integer zero) → SCopy x.
			// Do NOT reduce x - 0.0 (float zero): the subtraction coerces x to float64.
			if c, ok2 := constVals[inst.P2]; ok2 && constIsIntZero(c) {
				program.Instructions[i] = VM.Instruction{Op: VM.OpSCopy, P1: inst.P1, P2: int32(dst)}
				delete(constVals, int32(dst))
				continue
			}
			delete(constVals, int32(dst))

		default:
			// Invalidate any constant tracking for the written register.
			writtenReg := int32(-1)
			switch inst.Op {
			case VM.OpNull, VM.OpConstNull:
				writtenReg = inst.P1
			case VM.OpMove, VM.OpSCopy, VM.OpCopy, VM.OpIntCopy:
				writtenReg = inst.P2
			}
			if dst, ok := inst.P4.(int); ok {
				writtenReg = int32(dst)
			}
			if writtenReg >= 0 {
				delete(constVals, writtenReg)
			}
		}
	}
}

// constIsZero reports whether a constant value equals zero.
func constIsZero(v interface{}) bool {
	switch c := v.(type) {
	case int64:
		return c == 0
	case int:
		return c == 0
	case float64:
		return c == 0.0
	}
	return false
}

// constIsIntZero reports whether a constant value is an integer zero.
// This is used in strength reduction where type coercion must not be changed
// (e.g. x + 0.0 must remain as an add to coerce x to float64).
func constIsIntZero(v interface{}) bool {
	switch c := v.(type) {
	case int64:
		return c == 0
	case int:
		return c == 0
	}
	return false
}

// constIsOne reports whether a constant value equals one.
func constIsOne(v interface{}) bool {
	switch c := v.(type) {
	case int64:
		return c == 1
	case int:
		return c == 1
	case float64:
		return c == 1.0
	}
	return false
}

// constIsIntOne reports whether a constant value is an integer one.
// Used for x * 1 → SCopy only when the 1 is integral, to avoid suppressing
// int→float type promotion that would occur with 1.0 * int.
func constIsIntOne(v interface{}) bool {
	switch c := v.(type) {
	case int64:
		return c == 1
	case int:
		return c == 1
	}
	return false
}

// constIsIntTwo reports whether a constant value is the integer two.
// Used for x * 2 → x + x (addition is cheaper than multiply when both
// operands are the same). Only applied for integer 2, not float 2.0, to
// avoid changing the result type.
func constIsIntTwo(v interface{}) bool {
	switch c := v.(type) {
	case int64:
		return c == 2
	case int:
		return c == 2
	}
	return false
}

// peepholeOptimize applies small, local instruction-level transformations:
//
//  1. OpGoto targeting the immediately following instruction → OpNoop
//     (eliminates jumps that don't actually jump anywhere).
//
//  2. OpLoadConst(rx, v) immediately followed by OpMove/OpSCopy(rx→ry),
//     where rx is used only once → fold into OpLoadConst(ry, v) and Noop
//     the original load (reduces register pressure and copy overhead).
func (o *Optimizer) peepholeOptimize(program *VM.Program) {
	insts := program.Instructions
	n := len(insts)
	if n == 0 {
		return
	}

	// Pre-compute how many times each register is read by non-definition instructions.
	readCount := make(map[int32]int)
	for _, inst := range insts {
		if inst.Op == VM.OpNoop {
			continue
		}
		switch inst.Op {
		case VM.OpMove, VM.OpSCopy, VM.OpCopy, VM.OpIntCopy:
			readCount[inst.P1]++
		case VM.OpAdd, VM.OpSubtract, VM.OpMultiply, VM.OpDivide, VM.OpRemainder, VM.OpConcat:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpEq, VM.OpNe, VM.OpLt, VM.OpLe, VM.OpGt, VM.OpGe, VM.OpIs, VM.OpIsNot:
			readCount[inst.P1]++
			readCount[inst.P2]++
		case VM.OpIsNull, VM.OpNotNull, VM.OpIf, VM.OpIfNot, VM.OpIfNull:
			readCount[inst.P1]++
		case VM.OpIfNull2:
			readCount[inst.P1]++
			readCount[inst.P2]++
		}
		if regs, ok := inst.P4.([]int); ok {
			for _, r := range regs {
				readCount[int32(r)]++
			}
		}
	}

	for i := 0; i < n; i++ {
		inst := insts[i]
		if inst.Op == VM.OpNoop {
			continue
		}

		// Pattern 1: OpGoto to the next instruction → Noop.
		if inst.Op == VM.OpGoto && int(inst.P2) == i+1 {
			program.Instructions[i] = VM.Instruction{Op: VM.OpNoop}
			continue
		}

		// Pattern 2: LoadConst(rx, v); Move/SCopy(rx→ry) where rx read only once
		//            → LoadConst(ry, v); Noop
		if i+1 < n && inst.Op == VM.OpLoadConst {
			next := insts[i+1]
			if (next.Op == VM.OpMove || next.Op == VM.OpSCopy) &&
				next.P1 == inst.P1 && readCount[inst.P1] == 1 {
				program.Instructions[i+1] = VM.Instruction{
					Op: VM.OpLoadConst,
					P1: next.P2,
					P4: inst.P4,
				}
				program.Instructions[i] = VM.Instruction{Op: VM.OpNoop}
			}
		}
	}
}

// selectivity estimates how selective an expression is (higher = more selective = evaluate first).
// Equality > Range > LIKE > others
func selectivity(expr QP.Expr) int {
switch e := expr.(type) {
case *QP.BinaryExpr:
switch e.Op {
case QP.TokenEq:
return 100
case QP.TokenGt, QP.TokenLt, QP.TokenGe, QP.TokenLe:
return 60
case QP.TokenLike:
return 30
}
}
return 10
}

// ReorderPredicates sorts AND-connected predicates by descending selectivity.
func ReorderPredicates(expr QP.Expr) QP.Expr {
if expr == nil {
return nil
}
var preds []QP.Expr
collectAnds(expr, &preds)
if len(preds) <= 1 {
return expr
}
sort.Slice(preds, func(i, j int) bool {
return selectivity(preds[i]) > selectivity(preds[j])
})
result := preds[0]
for _, p := range preds[1:] {
result = &QP.BinaryExpr{Op: QP.TokenAnd, Left: result, Right: p}
}
return result
}

func collectAnds(expr QP.Expr, out *[]QP.Expr) {
if b, ok := expr.(*QP.BinaryExpr); ok && b.Op == QP.TokenAnd {
collectAnds(b.Left, out)
collectAnds(b.Right, out)
} else {
*out = append(*out, expr)
}
}
