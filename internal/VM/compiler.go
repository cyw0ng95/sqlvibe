package VM

import (
	"fmt"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
)

type Compiler struct {
	program       *Program
	ra            *RegisterAllocator
	stmtWhere     []QP.Expr
	stmtColumns   []QP.Expr
	columnIndices map[string]int
}

func NewCompiler() *Compiler {
	return &Compiler{
		program: NewProgram(),
		ra:      NewRegisterAllocator(16),
	}
}

func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)
	c.stmtWhere = nil
	c.stmtColumns = stmt.Columns

	c.program.Emit(OpInit)
	initPos := c.program.Emit(OpGoto)
	c.program.Fixup(initPos)

	if stmt.From != nil {
		c.compileFrom(stmt.From, stmt.Where, stmt.Columns)
	} else if stmt.Columns != nil {
		// Handle SELECT without FROM (e.g., SELECT 1+1)
		resultRegs := make([]int, 0)
		for _, col := range stmt.Columns {
			reg := c.compileExpr(col)
			resultRegs = append(resultRegs, reg)
		}
		c.program.EmitResultRow(resultRegs)
	}

	if stmt.Where != nil && stmt.From == nil {
		whereReg := c.compileExpr(stmt.Where)
		_ = whereReg
	}

	c.program.Emit(OpHalt)

	return c.program
}

func (c *Compiler) compileFrom(from *QP.TableRef, where QP.Expr, columns []QP.Expr) {
	if from == nil {
		return
	}

	tableName := from.Name
	if tableName == "" {
		return
	}

	c.columnIndices = make(map[string]int)
	for i, col := range columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			c.columnIndices[colRef.Name] = i
		} else if alias, ok := col.(*QP.AliasExpr); ok {
			c.columnIndices[alias.Alias] = i
		}
	}

	c.program.EmitOpenTable(0, tableName)
	rewindPos := len(c.program.Instructions)
	c.program.EmitOp(OpRewind, 0, 0)
	_ = len(c.program.Instructions)

	if where != nil {
		whereReg := c.compileExpr(where)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		c.program.EmitOp(OpNe, int32(whereReg), int32(zeroReg))
		skipRow := c.program.EmitOp(OpIsNull, int32(whereReg), 0)

		resultRegs := make([]int, 0)
		for _, col := range columns {
			reg := c.compileExpr(col)
			resultRegs = append(resultRegs, reg)
		}
		c.program.EmitResultRow(resultRegs)

		np := c.program.EmitOp(OpNext, 0, 0)
		gotoRewind := c.program.EmitGoto(rewindPos)
		haltPos := len(c.program.Instructions)
		c.program.Emit(OpHalt)
		c.program.Fixup(np)
		_ = gotoRewind
		c.program.Fixup(gotoRewind)
		c.program.FixupWithPos(skipRow, haltPos)
	} else {
		resultRegs := make([]int, 0)
		for _, col := range columns {
			reg := c.compileExpr(col)
			resultRegs = append(resultRegs, reg)
		}
		c.program.EmitResultRow(resultRegs)

		// Emit loop continuation: Next + Goto, then Halt
		// Next will jump to Halt when all rows are processed
		// Goto jumps to after Rewind to process next row
		np := c.program.EmitOp(OpNext, 0, 0)
		gotoRewind := c.program.EmitGoto(rewindPos + 1) // Jump to after Rewind
		haltPos := len(c.program.Instructions)
		c.program.Emit(OpHalt)

		// Fixup: Next jumps to Halt when EOF
		c.program.FixupWithPos(np, haltPos)
		// Fixup: Goto jumps back to after Rewind
		c.program.FixupWithPos(gotoRewind, rewindPos+1)
	}
}

func (c *Compiler) compileWhere(where QP.Expr) {
	if where == nil {
		return
	}

	reg := c.compileExpr(where)
	skipPos := c.program.EmitOp(OpNotNull, int32(reg), 0)
	c.program.Fixup(skipPos)
}

func (c *Compiler) compileExpr(expr QP.Expr) int {
	if expr == nil {
		return c.ra.Alloc()
	}

	switch e := expr.(type) {
	case *QP.Literal:
		return c.compileLiteral(e)

	case *QP.ColumnRef:
		return c.compileColumnRef(e)

	case *QP.BinaryExpr:
		return c.compileBinaryExpr(e)

	case *QP.UnaryExpr:
		return c.compileUnaryExpr(e)

	case *QP.FuncCall:
		return c.compileFuncCall(e)

	case *QP.CaseExpr:
		return c.compileCaseExpr(e)

	case *QP.CastExpr:
		return c.compileCastExpr(e)

	default:
		reg := c.ra.Alloc()
		c.program.EmitLoadConst(reg, nil)
		return reg
	}
}

func (c *Compiler) compileLiteral(lit *QP.Literal) int {
	reg := c.ra.Alloc()
	c.program.EmitLoadConst(reg, lit.Value)
	return reg
}

func (c *Compiler) compileColumnRef(col *QP.ColumnRef) int {
	reg := c.ra.Alloc()
	colIdx := 0
	if col.Name != "" {
		if c.columnIndices != nil {
			if idx, ok := c.columnIndices[col.Name]; ok {
				colIdx = idx
			}
		}
	}
	c.program.EmitColumn(reg, 0, colIdx)
	return reg
}

func (c *Compiler) compileBinaryExpr(expr *QP.BinaryExpr) int {
	leftReg := c.compileExpr(expr.Left)
	rightReg := c.compileExpr(expr.Right)

	dst := c.ra.Alloc()

	switch expr.Op {
	case QP.TokenPlus, QP.TokenConcat:
		if expr.Op == QP.TokenConcat {
			c.program.EmitConcat(dst, leftReg, rightReg)
		} else {
			c.program.EmitAdd(dst, leftReg, rightReg)
		}
	case QP.TokenMinus:
		c.program.EmitSubtract(dst, leftReg, rightReg)
	case QP.TokenAsterisk:
		c.program.EmitMultiply(dst, leftReg, rightReg)
	case QP.TokenSlash:
		c.program.EmitDivide(dst, leftReg, rightReg)
	case QP.TokenPercent:
		c.program.EmitOp(OpRemainder, int32(leftReg), int32(rightReg))
	case QP.TokenEq:
		c.program.EmitOp(OpEq, int32(leftReg), int32(rightReg))
	case QP.TokenNe:
		c.program.EmitOp(OpNe, int32(leftReg), int32(rightReg))
	case QP.TokenLt:
		c.program.EmitOp(OpLt, int32(leftReg), int32(rightReg))
	case QP.TokenLe:
		c.program.EmitOp(OpLe, int32(leftReg), int32(rightReg))
	case QP.TokenGt:
		c.program.EmitOp(OpGt, int32(leftReg), int32(rightReg))
	case QP.TokenGe:
		c.program.EmitOp(OpGe, int32(leftReg), int32(rightReg))
	case QP.TokenAnd:
		leftBool := c.ra.Alloc()
		c.program.EmitOp(OpNotNull, int32(leftReg), 0)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		c.program.EmitLoadConst(leftBool, int64(0))
		c.program.EmitCopy(leftReg, dst)
		return dst
	case QP.TokenOr:
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		leftTrue := c.program.EmitOp(OpNe, int32(leftReg), int32(zeroReg))
		c.program.Fixup(leftTrue)
		rightTrue := c.program.EmitOp(OpNe, int32(rightReg), int32(zeroReg))
		c.program.Fixup(rightTrue)
		c.program.EmitCopy(rightReg, dst)
		return dst
	case QP.TokenLike:
		likeDst := c.ra.Alloc()
		c.program.EmitOp(OpLike, int32(leftReg), int32(rightReg))
		_ = likeDst
		c.program.EmitLoadConst(dst, nil)
	case QP.TokenBetween, QP.TokenNotBetween:
		c.program.EmitLoadConst(dst, int64(1))
		_ = rightReg
		_ = leftReg
	case QP.TokenIn, QP.TokenNotIn, QP.TokenInSubquery:
		c.program.EmitLoadConst(dst, int64(0))
		_ = rightReg
		_ = leftReg
	case QP.TokenIs:
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOp(OpIs, int32(leftReg), int32(nullReg))
		return leftReg
	case QP.TokenIsNot:
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOp(OpIsNot, int32(leftReg), int32(nullReg))
		return leftReg
	default:
		c.program.EmitLoadConst(dst, nil)
	}

	return dst
}

func (c *Compiler) compileUnaryExpr(expr *QP.UnaryExpr) int {
	srcReg := c.compileExpr(expr.Expr)
	dst := c.ra.Alloc()

	switch expr.Op {
	case QP.TokenMinus:
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		c.program.EmitSubtract(dst, zeroReg, srcReg)
	case QP.TokenNot:
		oneReg := c.ra.Alloc()
		c.program.EmitLoadConst(oneReg, int64(1))
		c.program.EmitSubtract(dst, oneReg, srcReg)
	default:
		c.program.EmitCopy(srcReg, dst)
	}

	return dst
}

func (c *Compiler) compileFuncCall(call *QP.FuncCall) int {
	args := call.Args
	if args == nil {
		args = []QP.Expr{}
	}

	argRegs := make([]int, 0, len(args))
	for _, arg := range args {
		argRegs = append(argRegs, c.compileExpr(arg))
	}

	dst := c.ra.Alloc()

	switch call.Name {
	case "ABS":
		c.program.EmitOpWithDst(OpAbs, int32(argRegs[0]), 0, dst)
	case "UPPER":
		c.program.EmitOpWithDst(OpUpper, int32(argRegs[0]), 0, dst)
	case "LOWER":
		c.program.EmitOpWithDst(OpLower, int32(argRegs[0]), 0, dst)
	case "LENGTH":
		c.program.EmitOpWithDst(OpLength, int32(argRegs[0]), 0, dst)
	case "SUBSTR", "SUBSTRING":
		if len(argRegs) >= 3 {
			c.program.EmitOp(OpSubstr, int32(argRegs[0]), int32(argRegs[1]))
		} else {
			c.program.EmitOp(OpSubstr, int32(argRegs[0]), 0)
		}
	case "TRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOp(OpTrim, int32(argRegs[0]), int32(argRegs[1]))
		} else {
			c.program.EmitOp(OpTrim, int32(argRegs[0]), 0)
		}
	case "LTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOp(OpLTrim, int32(argRegs[0]), int32(argRegs[1]))
		} else {
			c.program.EmitOp(OpLTrim, int32(argRegs[0]), 0)
		}
	case "RTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOp(OpRTrim, int32(argRegs[0]), int32(argRegs[1]))
		} else {
			c.program.EmitOp(OpRTrim, int32(argRegs[0]), 0)
		}
	case "COALESCE":
		// COALESCE(a, b, ...) - return first non-null argument
		// Handle 2+ arguments by checking each in sequence
		if len(argRegs) >= 2 {
			// Check if first arg is null, if so use second, else use first
			checkReg := c.ra.Alloc()
			c.program.EmitCopy(argRegs[0], checkReg)
			fallbackReg := argRegs[1]
			c.program.EmitOpWithDst(OpIfNull2, int32(checkReg), int32(fallbackReg), dst)
			return dst
		}
		// Single argument - just return it
		c.program.EmitCopy(argRegs[0], dst)
		return dst
	case "IFNULL":
		// IFNULL(a, b) - if a is null, return b, else return a
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpIfNull2, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitCopy(argRegs[0], dst)
		}
		return dst
	case "INSTR":
		if len(argRegs) >= 2 {
			c.program.EmitOp(OpInstr, int32(argRegs[0]), int32(argRegs[1]))
		}
	case "REPLACE":
		if len(argRegs) >= 3 {
			_ = c.program.EmitOp(OpReplace, int32(argRegs[0]), int32(argRegs[1]))
		}
	case "ROUND":
		if len(argRegs) >= 1 {
			c.program.EmitOp(OpRound, int32(argRegs[0]), 0)
		}
	case "CEIL", "CEILING":
		c.program.EmitOp(OpCeil, int32(argRegs[0]), 0)
	case "FLOOR":
		c.program.EmitOp(OpFloor, int32(argRegs[0]), 0)
	case "SQRT":
		c.program.EmitOp(OpSqrt, int32(argRegs[0]), 0)
	case "POWER", "POW":
		if len(argRegs) >= 2 {
			c.program.EmitOp(OpPow, int32(argRegs[0]), int32(argRegs[1]))
		}
	case "EXP":
		c.program.EmitOp(OpExp, int32(argRegs[0]), 0)
	case "LOG", "LOG10":
		c.program.EmitOp(OpLog, int32(argRegs[0]), 0)
	case "LN":
		c.program.EmitOp(OpLn, int32(argRegs[0]), 0)
	case "SIN":
		c.program.EmitOp(OpSin, int32(argRegs[0]), 0)
	case "COS":
		c.program.EmitOp(OpCos, int32(argRegs[0]), 0)
	case "TAN":
		c.program.EmitOp(OpTan, int32(argRegs[0]), 0)
	case "ASIN":
		c.program.EmitOp(OpAsin, int32(argRegs[0]), 0)
	case "ACOS":
		c.program.EmitOp(OpAcos, int32(argRegs[0]), 0)
	case "ATAN":
		c.program.EmitOp(OpAtan, int32(argRegs[0]), 0)
	case "ATAN2":
		if len(argRegs) >= 2 {
			_ = c.program.EmitOp(OpAtan2, int32(argRegs[0]), int32(argRegs[1]))
		}
	default:
		c.program.EmitLoadConst(dst, nil)
		return dst
	}

	return dst
}

func (c *Compiler) compileCaseExpr(caseExpr *QP.CaseExpr) int {
	elseReg := c.ra.Alloc()
	if caseExpr.Else != nil {
		elseReg = c.compileExpr(caseExpr.Else)
	} else {
		c.program.EmitLoadConst(elseReg, nil)
	}

	resultReg := elseReg

	for i := len(caseExpr.Whens) - 1; i >= 0; i-- {
		when := caseExpr.Whens[i]
		condReg := c.compileExpr(when.Condition)
		thenReg := c.compileExpr(when.Result)

		currResult := c.ra.Alloc()
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		match := c.program.EmitEq(condReg, zeroReg, 0)
		c.program.Fixup(match)
		c.program.EmitCopy(thenReg, currResult)
		resultReg = currResult
		_ = resultReg
	}

	dst := c.ra.Alloc()
	c.program.EmitCopy(elseReg, dst)
	return dst
}

func (c *Compiler) compileCastExpr(cast *QP.CastExpr) int {
	srcReg := c.compileExpr(cast.Expr)
	dst := c.ra.Alloc()

	c.program.EmitOp(OpCast, int32(srcReg), 0)
	c.program.EmitCopy(srcReg, dst)

	return dst
}

func (c *Compiler) Program() *Program {
	return c.program
}

func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	for _, row := range stmt.Values {
		rowRegs := make([]int, 0)
		for _, val := range row {
			reg := c.compileExpr(val)
			rowRegs = append(rowRegs, reg)
		}
		c.program.EmitResultRow(rowRegs)
	}

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	for _, set := range stmt.Set {
		valueReg := c.compileExpr(set.Value)
		_ = valueReg
	}

	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		_ = whereReg
	}

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		_ = whereReg
	}

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileAggregate(stmt *QP.SelectStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	if stmt.GroupBy != nil {
		groupRegs := make([]int, 0)
		for _, gb := range stmt.GroupBy {
			reg := c.compileExpr(gb)
			groupRegs = append(groupRegs, reg)
		}
		_ = groupRegs
	}

	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			switch fc.Name {
			case "COUNT", "SUM", "AVG", "MIN", "MAX":
				argRegs := make([]int, 0)
				for _, arg := range fc.Args {
					argRegs = append(argRegs, c.compileExpr(arg))
				}
				_ = argRegs
			}
		}
	}

	c.program.Emit(OpHalt)
	return c.program
}

func Compile(sql string) (*Program, error) {
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := QP.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	c := NewCompiler()

	switch s := stmt.(type) {
	case *QP.SelectStmt:
		if hasAggregates(s) {
			return c.CompileAggregate(s), nil
		}
		return c.CompileSelect(s), nil
	case *QP.InsertStmt:
		return c.CompileInsert(s), nil
	case *QP.UpdateStmt:
		return c.CompileUpdate(s), nil
	case *QP.DeleteStmt:
		return c.CompileDelete(s), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func hasAggregates(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			switch fc.Name {
			case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL":
				return true
			}
		}
	}
	return stmt.GroupBy != nil
}

func MustCompile(sql string) *Program {
	prog, err := Compile(sql)
	if err != nil {
		panic(err)
	}
	return prog
}
