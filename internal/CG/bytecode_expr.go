package CG

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// bcCompileExpr compiles a QP.Expr into bytecode, storing the result in a freshly
// allocated register (or an existing register for simple column/const cases).
// colIdxMap: column-name → 0-based index in the table's colOrder (may be nil).
// cursorID: pointer to the cursor index for column lookups (may be nil).
// Returns the register that holds the result value.
func bcCompileExpr(b *VM.BytecodeBuilder, expr QP.Expr, colIdxMap map[string]int, cursorID *int) (int32, error) {
	switch e := expr.(type) {
	case *QP.Literal:
		return bcCompileLiteral(b, e)

	case *QP.ColumnRef:
		return bcCompileColumnRef(b, e, colIdxMap, cursorID)

	case *QP.AliasExpr:
		return bcCompileExpr(b, e.Expr, colIdxMap, cursorID)

	case *QP.BinaryExpr:
		return bcCompileBinary(b, e, colIdxMap, cursorID)

	case *QP.UnaryExpr:
		return bcCompileUnary(b, e, colIdxMap, cursorID)

	case *QP.FuncCall:
		return bcCompileFuncCall(b, e, colIdxMap, cursorID)

	case *QP.CaseExpr:
		return bcCompileCaseExpr(b, e, colIdxMap, cursorID)

	case *QP.CastExpr:
		return bcCompileCastExpr(b, e, colIdxMap, cursorID)

	case *QP.CollateExpr:
		// Collation is ignored in bytecode path; just compile the inner expr.
		return bcCompileExpr(b, e.Expr, colIdxMap, cursorID)
	}

	return 0, fmt.Errorf("bcCompileExpr: unsupported expression type %T", expr)
}

// bcCompileLiteral emits a LoadConst for a literal value.
func bcCompileLiteral(b *VM.BytecodeBuilder, e *QP.Literal) (int32, error) {
	v := VM.FromInterface(e.Value)
	// Special handling for string "true"/"false" literals
	if e.Value != nil {
		switch sv := e.Value.(type) {
		case string:
			upper := strings.ToUpper(sv)
			if upper == "TRUE" {
				v = VM.VmBool(true)
			} else if upper == "FALSE" {
				v = VM.VmBool(false)
			}
		}
	}
	ci := b.AddConst(v)
	r := b.AllocReg()
	b.EmitABC(VM.BcLoadConst, 0, ci, r)
	return r, nil
}

// bcCompileColumnRef emits a BcColumn or BcRowid instruction.
func bcCompileColumnRef(b *VM.BytecodeBuilder, e *QP.ColumnRef, colIdxMap map[string]int, cursorID *int) (int32, error) {
	if strings.EqualFold(e.Name, "rowid") || strings.EqualFold(e.Name, "_rowid_") {
		r := b.AllocReg()
		cid := int32(0)
		if cursorID != nil {
			cid = int32(*cursorID)
		}
		b.EmitABC(VM.BcRowid, cid, 0, r)
		return r, nil
	}

	if colIdxMap == nil || cursorID == nil {
		// No table context — emit NULL
		r := b.AllocReg()
		ci := b.AddConst(VM.VmNull())
		b.EmitABC(VM.BcLoadConst, 0, ci, r)
		return r, nil
	}

	name := strings.ToLower(e.Name)
	colIdx, ok := colIdxMap[name]
	if !ok {
		for k, v := range colIdxMap {
			if strings.EqualFold(k, e.Name) {
				colIdx = v
				ok = true
				break
			}
		}
	}
	if !ok {
		colIdx = 0
	}
	r := b.AllocReg()
	b.EmitABC(VM.BcColumn, int32(*cursorID), int32(colIdx), r)
	return r, nil
}

// bcCompileBinary emits bytecode for a binary expression.
func bcCompileBinary(b *VM.BytecodeBuilder, e *QP.BinaryExpr, colIdxMap map[string]int, cursorID *int) (int32, error) {
	// Handle IS NULL / IS NOT NULL specially
	switch e.Op {
	case QP.TokenIs:
		if lit, ok := e.Right.(*QP.Literal); ok && lit.Value == nil {
			lReg, err := bcCompileExpr(b, e.Left, colIdxMap, cursorID)
			if err != nil {
				return 0, err
			}
			r := b.AllocReg()
			b.EmitABC(VM.BcIsNull, lReg, 0, r)
			return r, nil
		}
	case QP.TokenIsNot:
		if lit, ok := e.Right.(*QP.Literal); ok && lit.Value == nil {
			lReg, err := bcCompileExpr(b, e.Left, colIdxMap, cursorID)
			if err != nil {
				return 0, err
			}
			r := b.AllocReg()
			b.EmitABC(VM.BcNotNull, lReg, 0, r)
			return r, nil
		}
	}

	lReg, err := bcCompileExpr(b, e.Left, colIdxMap, cursorID)
	if err != nil {
		return 0, err
	}
	rReg, err := bcCompileExpr(b, e.Right, colIdxMap, cursorID)
	if err != nil {
		return 0, err
	}
	dst := b.AllocReg()

	switch e.Op {
	case QP.TokenPlus:
		b.EmitABC(VM.BcAdd, lReg, rReg, dst)
	case QP.TokenMinus:
		b.EmitABC(VM.BcSub, lReg, rReg, dst)
	case QP.TokenAsterisk:
		b.EmitABC(VM.BcMul, lReg, rReg, dst)
	case QP.TokenSlash:
		b.EmitABC(VM.BcDiv, lReg, rReg, dst)
	case QP.TokenPercent:
		b.EmitABC(VM.BcMod, lReg, rReg, dst)
	case QP.TokenConcat:
		b.EmitABC(VM.BcConcat, lReg, rReg, dst)
	case QP.TokenEq:
		b.EmitABC(VM.BcEq, lReg, rReg, dst)
	case QP.TokenNe:
		b.EmitABC(VM.BcNe, lReg, rReg, dst)
	case QP.TokenLt:
		b.EmitABC(VM.BcLt, lReg, rReg, dst)
	case QP.TokenLe:
		b.EmitABC(VM.BcLe, lReg, rReg, dst)
	case QP.TokenGt:
		b.EmitABC(VM.BcGt, lReg, rReg, dst)
	case QP.TokenGe:
		b.EmitABC(VM.BcGe, lReg, rReg, dst)
	case QP.TokenAnd:
		b.EmitABC(VM.BcAnd, lReg, rReg, dst)
	case QP.TokenOr:
		b.EmitABC(VM.BcOr, lReg, rReg, dst)
	default:
		return 0, fmt.Errorf("bcCompileBinary: unsupported op %v", e.Op)
	}
	return dst, nil
}

// bcCompileUnary emits bytecode for a unary expression.
func bcCompileUnary(b *VM.BytecodeBuilder, e *QP.UnaryExpr, colIdxMap map[string]int, cursorID *int) (int32, error) {
	inner, err := bcCompileExpr(b, e.Expr, colIdxMap, cursorID)
	if err != nil {
		return 0, err
	}
	dst := b.AllocReg()
	switch e.Op {
	case QP.TokenMinus:
		b.EmitABC(VM.BcNeg, inner, 0, dst)
	case QP.TokenNot:
		b.EmitABC(VM.BcNot, inner, 0, dst)
	default:
		// Unknown unary — copy through
		b.EmitABC(VM.BcLoadReg, inner, 0, dst)
	}
	return dst, nil
}

// bcCompileFuncCall emits bytecode for a scalar function call.
func bcCompileFuncCall(b *VM.BytecodeBuilder, e *QP.FuncCall, colIdxMap map[string]int, cursorID *int) (int32, error) {
	name := strings.ToLower(e.Name)

	// Compile args and place in consecutive registers ending at dst.
	nArgs := len(e.Args)
	argRegs := make([]int32, nArgs)
	for i, arg := range e.Args {
		r, err := bcCompileExpr(b, arg, colIdxMap, cursorID)
		if err != nil {
			return 0, err
		}
		argRegs[i] = r
	}
	dst := b.AllocReg()

	// Copy args to the consecutive registers just before dst, then emit BcCall.
	// BcCall semantics: args are regs[C-B .. C-1], result in regs[C].
	// We need to ensure they are consecutive.
	// Simplest approach: allocate B new registers, copy, then alloc dst.
	// For now, just emit a BcCall using func name in const pool.
	fnConst := b.AddConst(VM.VmText(name))

	// Allocate B consecutive argument registers.
	argBase := b.AllocReg() // first of B consecutive arg registers
	for i := 1; i < nArgs; i++ {
		b.AllocReg()
	}
	// Copy args into consecutive positions.
	for i, ar := range argRegs {
		tgt := argBase + int32(i)
		b.EmitABC(VM.BcLoadReg, ar, 0, tgt)
	}
	// dst must be argBase + nArgs
	dstReg := argBase + int32(nArgs)
	b.EmitABC(VM.BcCall, fnConst, int32(nArgs), dstReg)

	// Copy result to originally allocated dst if different.
	if dstReg != dst {
		b.EmitABC(VM.BcLoadReg, dstReg, 0, dst)
	}
	return dst, nil
}

// bcCompileCaseExpr emits bytecode for a CASE expression.
// Supports both CASE WHEN ... THEN ... END and CASE expr WHEN val THEN ... END.
func bcCompileCaseExpr(b *VM.BytecodeBuilder, e *QP.CaseExpr, colIdxMap map[string]int, cursorID *int) (int32, error) {
	dst := b.AllocReg()
	lblEnd := b.AllocLabel()

	var opReg int32 = -1
	if e.Operand != nil {
		r, err := bcCompileExpr(b, e.Operand, colIdxMap, cursorID)
		if err != nil {
			return 0, err
		}
		opReg = r
	}

	for _, when := range e.Whens {
		var condReg int32
		if opReg >= 0 {
			// CASE expr WHEN val THEN result: compare opReg == whenReg
			whenReg, err := bcCompileExpr(b, when.Condition, colIdxMap, cursorID)
			if err != nil {
				return 0, err
			}
			condReg = b.AllocReg()
			b.EmitABC(VM.BcEq, opReg, whenReg, condReg)
		} else {
			var err error
			condReg, err = bcCompileExpr(b, when.Condition, colIdxMap, cursorID)
			if err != nil {
				return 0, err
			}
		}
		// If condition is false, jump to next WHEN
		nextLbl := b.AllocLabel()
		b.EmitJump(VM.BcJumpFalse, condReg, nextLbl)

		// Compile result
		resReg, err := bcCompileExpr(b, when.Result, colIdxMap, cursorID)
		if err != nil {
			return 0, err
		}
		b.EmitABC(VM.BcLoadReg, resReg, 0, dst)
		b.EmitJump(VM.BcJump, 0, lblEnd)

		b.FixupLabel(nextLbl)
	}

	// ELSE clause
	if e.Else != nil {
		elseReg, err := bcCompileExpr(b, e.Else, colIdxMap, cursorID)
		if err != nil {
			return 0, err
		}
		b.EmitABC(VM.BcLoadReg, elseReg, 0, dst)
	} else {
		nullConst := b.AddConst(VM.VmNull())
		b.EmitABC(VM.BcLoadConst, 0, nullConst, dst)
	}

	b.FixupLabel(lblEnd)
	return dst, nil
}

// bcCompileCastExpr emits bytecode for CAST(expr AS type).
func bcCompileCastExpr(b *VM.BytecodeBuilder, e *QP.CastExpr, colIdxMap map[string]int, cursorID *int) (int32, error) {
	inner, err := bcCompileExpr(b, e.Expr, colIdxMap, cursorID)
	if err != nil {
		return 0, err
	}
	typeName := strings.ToUpper(e.TypeSpec.Name)
	fnName := "cast_" + strings.ToLower(typeName)
	fnConst := b.AddConst(VM.VmText(fnName))
	argBase := b.AllocReg()
	b.EmitABC(VM.BcLoadReg, inner, 0, argBase)
	dst := b.AllocReg()
	b.EmitABC(VM.BcCall, fnConst, 1, dst)
	return dst, nil
}
