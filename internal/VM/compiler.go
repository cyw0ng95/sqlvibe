package VM

import (
	"fmt"
	"strings"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
)

type Compiler struct {
	program         *Program
	ra              *RegisterAllocator
	stmtWhere       []QP.Expr
	stmtColumns     []QP.Expr
	columnIndices   map[string]int // SELECT position: col name -> index in SELECT list
	TableColIndices map[string]int // TABLE position: col name -> index in table (exported for external use)
	TableColOrder   []string       // TABLE position: ordered list of column names (for SELECT * expansion)
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

	// Expand SELECT * to actual column names if needed
	columns := stmt.Columns
	if c.TableColIndices != nil && len(c.TableColIndices) > 0 {
		columns = c.expandStarColumns(stmt.Columns)
		c.stmtColumns = columns
	}

	c.program.Emit(OpInit)
	initPos := c.program.Emit(OpGoto)
	c.program.Fixup(initPos)

	if stmt.From != nil {
		c.compileFrom(stmt.From, stmt.Where, columns)
	} else if columns != nil {
		// Handle SELECT without FROM (e.g., SELECT 1+1)
		resultRegs := make([]int, 0)
		for _, col := range columns {
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

// expandStarColumns expands SELECT * to actual column references
func (c *Compiler) expandStarColumns(columns []QP.Expr) []QP.Expr {
	if columns == nil {
		return nil
	}

	// Check if there's a star column
	hasStar := false
	for _, col := range columns {
		if colRef, ok := col.(*QP.ColumnRef); ok && colRef.Name == "*" {
			hasStar = true
			break
		}
	}

	if !hasStar {
		return columns
	}

	// Build expanded columns list - use TableColOrder for deterministic ordering
	expanded := make([]QP.Expr, 0)

	// Use the ordered column list to ensure deterministic output
	if c.TableColOrder != nil && len(c.TableColOrder) > 0 {
		for _, colName := range c.TableColOrder {
			// Skip internal/placeholder columns
			if colName == "" || strings.HasPrefix(colName, "__") {
				continue
			}
			colRef := &QP.ColumnRef{
				Name: colName,
			}
			expanded = append(expanded, colRef)
		}
	} else {
		// Fallback: iterate over TableColIndices (may be non-deterministic)
		for colName, idx := range c.TableColIndices {
			// Skip internal/placeholder columns
			if colName == "" || strings.HasPrefix(colName, "__") {
				continue
			}
			colRef := &QP.ColumnRef{
				Name: colName,
			}
			_ = idx // idx is position in table
			expanded = append(expanded, colRef)
		}
	}

	// If no valid columns found, return original
	if len(expanded) == 0 {
		return columns
	}

	return expanded
}

func (c *Compiler) compileFrom(from *QP.TableRef, where QP.Expr, columns []QP.Expr) {
	if from == nil {
		return
	}

	// Handle JOINs - requires nested loop compilation
	if from.Join != nil {
		c.compileJoin(from, from.Join, where, columns)
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

	// Load columns first (needed for WHERE clause)
	colRegs := make([]int, 0)
	for _, col := range columns {
		reg := c.compileExpr(col)
		colRegs = append(colRegs, reg)
	}

	// WHERE clause: evaluate and skip row if false/null
	if where != nil {
		whereReg := c.compileExpr(where)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		skipPos := c.program.EmitEq(int(whereReg), int(zeroReg), 0)
		nullSkipIdx := c.program.EmitOp(OpIsNull, int32(whereReg), 0)
		c.program.Instructions[nullSkipIdx].P2 = 0
		c.program.MarkFixup(skipPos)
		c.program.MarkFixupP2(nullSkipIdx)
	}

	// Output result row
	c.program.EmitResultRow(colRegs)

	// Fixup: make WHERE skip instructions jump here (past ResultRow)
	c.program.ApplyWhereFixups()

	// Loop continuation: Next + Goto, then Halt
	np := c.program.EmitOp(OpNext, 0, 0)
	gotoRewind := c.program.EmitGoto(rewindPos + 1)
	haltPos := len(c.program.Instructions)
	c.program.Emit(OpHalt)

	// Fixup: Next jumps to Halt when EOF
	c.program.FixupWithPos(np, haltPos)
	// Fixup: Goto jumps back to after Rewind
	c.program.FixupWithPos(gotoRewind, rewindPos+1)
}

func (c *Compiler) compileJoin(leftTable *QP.TableRef, join *QP.Join, where QP.Expr, columns []QP.Expr) {
	leftTableName := leftTable.Name
	rightTableName := join.Right.Name

	if leftTableName == "" || rightTableName == "" {
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

	c.program.EmitOpenTable(0, leftTableName)
	c.program.EmitOpenTable(1, rightTableName)

	leftRewindPos := len(c.program.Instructions)
	c.program.EmitOp(OpRewind, 0, 0)

	rightRewindPos := len(c.program.Instructions)
	c.program.EmitOp(OpRewind, 1, 0)

	joinCondReg := -1
	if join.Cond != nil {
		joinCondReg = c.compileExpr(join.Cond)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		skipPos := c.program.EmitEq(joinCondReg, zeroReg, 0)
		nullSkip := c.program.EmitOp(OpIsNull, int32(joinCondReg), 0)
		_ = skipPos
		_ = nullSkip
	}

	colRegs := make([]int, 0)
	for _, col := range columns {
		reg := c.compileExpr(col)
		colRegs = append(colRegs, reg)
	}

	if where != nil {
		whereReg := c.compileExpr(where)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		c.program.EmitEq(whereReg, zeroReg, 0)
	}

	c.program.EmitResultRow(colRegs)

	rightNext := c.program.EmitOp(OpNext, 1, 0)
	gotoRightRewind := c.program.EmitGoto(rightRewindPos + 1)
	rightDonePos := len(c.program.Instructions)

	c.program.EmitOp(OpNext, 0, 0)
	gotoLeftRewind := c.program.EmitGoto(leftRewindPos + 1)
	_ = gotoLeftRewind
	c.program.Emit(OpHalt)

	c.program.FixupWithPos(rightNext, rightDonePos)
	c.program.FixupWithPos(gotoRightRewind, rightRewindPos+1)
	c.program.FixupWithPos(gotoLeftRewind, leftRewindPos+1)
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
	colIdx := -1 // Use -1 as sentinel for unknown columns
	if col.Name != "" {
		// First try table column order (for WHERE clause with schema)
		if c.TableColIndices != nil {
			if idx, ok := c.TableColIndices[col.Name]; ok {
				colIdx = idx
			}
		}
		// Fall back to SELECT position
		if colIdx == -1 && c.columnIndices != nil {
			if idx, ok := c.columnIndices[col.Name]; ok {
				colIdx = idx
			}
		}
	}
	// If column not found, emit NULL instead of silently reading column 0
	if colIdx == -1 {
		c.program.EmitLoadConst(reg, nil)
		return reg
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
		c.program.EmitOpWithDst(OpRemainder, int32(leftReg), int32(rightReg), dst)
	case QP.TokenEq:
		c.program.EmitOpWithDst(OpEq, int32(leftReg), int32(rightReg), dst)
	case QP.TokenNe:
		c.program.EmitOpWithDst(OpNe, int32(leftReg), int32(rightReg), dst)
	case QP.TokenLt:
		c.program.EmitOpWithDst(OpLt, int32(leftReg), int32(rightReg), dst)
	case QP.TokenLe:
		c.program.EmitOpWithDst(OpLe, int32(leftReg), int32(rightReg), dst)
	case QP.TokenGt:
		c.program.EmitOpWithDst(OpGt, int32(leftReg), int32(rightReg), dst)
	case QP.TokenGe:
		c.program.EmitOpWithDst(OpGe, int32(leftReg), int32(rightReg), dst)
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
			c.program.EmitOpWithDst(OpSubstr, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpSubstr, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(OpSubstr, int32(argRegs[0]), 0, dst)
		}
	case "TRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(OpTrim, int32(argRegs[0]), 0, dst)
		}
	case "LTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpLTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(OpLTrim, int32(argRegs[0]), 0, dst)
		}
	case "RTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpRTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(OpRTrim, int32(argRegs[0]), 0, dst)
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
			c.program.EmitOpWithDst(OpInstr, int32(argRegs[0]), int32(argRegs[1]), dst)
		}
	case "REPLACE":
		if len(argRegs) >= 3 {
			// REPLACE(str, from, to): copy input to dst, then apply replace
			c.program.EmitCopy(argRegs[0], dst)
			c.program.EmitOpWithDst(OpReplace, int32(argRegs[1]), int32(argRegs[2]), dst)
		}
	case "ROUND":
		if len(argRegs) >= 1 {
			c.program.EmitOpWithDst(OpRound, int32(argRegs[0]), 0, dst)
		}
	case "CEIL", "CEILING":
		c.program.EmitOpWithDst(OpCeil, int32(argRegs[0]), 0, dst)
	case "FLOOR":
		c.program.EmitOpWithDst(OpFloor, int32(argRegs[0]), 0, dst)
	case "SQRT":
		c.program.EmitOpWithDst(OpSqrt, int32(argRegs[0]), 0, dst)
	case "POWER", "POW":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpPow, int32(argRegs[0]), int32(argRegs[1]), dst)
		}
	case "EXP":
		c.program.EmitOpWithDst(OpExp, int32(argRegs[0]), 0, dst)
	case "LOG", "LOG10":
		c.program.EmitOpWithDst(OpLog, int32(argRegs[0]), 0, dst)
	case "LN":
		c.program.EmitOpWithDst(OpLn, int32(argRegs[0]), 0, dst)
	case "SIN":
		c.program.EmitOpWithDst(OpSin, int32(argRegs[0]), 0, dst)
	case "COS":
		c.program.EmitOpWithDst(OpCos, int32(argRegs[0]), 0, dst)
	case "TAN":
		c.program.EmitOpWithDst(OpTan, int32(argRegs[0]), 0, dst)
	case "ASIN":
		c.program.EmitOpWithDst(OpAsin, int32(argRegs[0]), 0, dst)
	case "ACOS":
		c.program.EmitOpWithDst(OpAcos, int32(argRegs[0]), 0, dst)
	case "ATAN":
		c.program.EmitOpWithDst(OpAtan, int32(argRegs[0]), 0, dst)
	case "ATAN2":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(OpAtan2, int32(argRegs[0]), int32(argRegs[1]), dst)
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
	return CompileWithSchema(sql, nil)
}

func CompileWithSchema(sql string, tableColumns []string) (*Program, error) {
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
	c.TableColIndices = make(map[string]int)
	c.TableColOrder = tableColumns
	for i, col := range tableColumns {
		c.TableColIndices[col] = i
	}

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
