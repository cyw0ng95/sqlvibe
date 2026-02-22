package CG

import (
	"fmt"
	"strings"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/SF/util"
	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

type Compiler struct {
	program         *VM.Program
	ra              *VM.RegisterAllocator
	optimizer       *Optimizer
	stmtWhere       []QP.Expr
	stmtColumns     []QP.Expr
	columnIndices   map[string]int
	TableColIndices map[string]int
	TableColOrder   []string
	TableColSources []string // parallel to TableColOrder: which table each column belongs to
	tableCursors    map[string]int
	TableSchemas    map[string]map[string]int
}

func NewCompiler() *Compiler {
	return &Compiler{
		program:   VM.NewProgram(),
		ra:        VM.NewRegisterAllocator(16),
		optimizer: NewOptimizer(),
	}
}

// finalize runs optimization passes and returns the compiled program.
func (c *Compiler) finalize() *VM.Program {
	return c.optimizer.Optimize(c.program)
}

func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *VM.Program {
	util.AssertNotNil(stmt, "SelectStmt")
	if hasAggregates(stmt) {
		return c.CompileAggregate(stmt)
	}

	if stmt.SetOp != "" && stmt.SetOpRight != nil {
		return c.compileSetOp(stmt)
	}

	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(16)
	c.stmtWhere = nil
	c.stmtColumns = stmt.Columns

	columns := stmt.Columns
	if (c.TableColIndices != nil && len(c.TableColIndices) > 0) || (c.TableSchemas != nil && len(c.TableSchemas) > 0) {
		columns = c.expandStarColumns(stmt.Columns)
		c.stmtColumns = columns
	}

	c.program.Emit(VM.OpInit)
	initPos := c.program.Emit(VM.OpGoto)
	c.program.Fixup(initPos)

	if stmt.From != nil {
		c.compileFrom(stmt.From, stmt.Where, columns)
	} else if columns != nil {
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

	c.program.Emit(VM.OpHalt)

	return c.finalize()
}

func (c *Compiler) compileFrom(from *QP.TableRef, where QP.Expr, columns []QP.Expr) {
	if from == nil {
		return
	}

	if from.Join != nil {
		c.compileJoin(from, from.Join, where, columns)
		return
	}

	tableName := from.Name
	if tableName == "" {
		return
	}

	c.tableCursors = make(map[string]int)
	c.tableCursors[tableName] = 0
	if from.Alias != "" {
		c.tableCursors[from.Alias] = 0
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
	c.program.EmitOp(VM.OpRewind, 0, 0)

	colRegs := make([]int, 0)
	for _, col := range columns {
		reg := c.compileExpr(col)
		colRegs = append(colRegs, reg)
	}

	if where != nil {
		whereReg := c.compileExpr(where)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		skipPos := c.program.EmitEq(int(whereReg), int(zeroReg), 0)
		nullSkipIdx := c.program.EmitOp(VM.OpIsNull, int32(whereReg), 0)
		c.program.Instructions[nullSkipIdx].P2 = 0
		c.program.MarkFixup(skipPos)
		c.program.MarkFixupP2(nullSkipIdx)
	}

	c.program.EmitResultRow(colRegs)
	c.program.ApplyWhereFixups()

	np := c.program.EmitOp(VM.OpNext, 0, 0)
	gotoRewind := c.program.EmitGoto(rewindPos + 1)
	haltPos := len(c.program.Instructions)
	c.program.Emit(VM.OpHalt)

	// Fix up: OpRewind jumps to halt when table is empty, OpNext jumps to halt at EOF
	c.program.FixupWithPos(rewindPos, haltPos)
	c.program.FixupWithPos(np, haltPos)
	c.program.FixupWithPos(gotoRewind, rewindPos+1)
}

// chainJoinStep represents one step in a multi-table join chain.
type chainJoinStep struct {
	table    *QP.TableRef
	joinType string
	cond     QP.Expr
}

// flattenJoinChain collects all tables and join steps from a chained join, converting
// USING clauses to equivalent ON conditions.
func flattenJoinChain(leftTable *QP.TableRef, firstJoin *QP.Join) ([]*QP.TableRef, []chainJoinStep) {
	tables := []*QP.TableRef{leftTable}
	steps := []chainJoinStep{}
	for j := firstJoin; j != nil; {
		jt := j.Type
		if jt == "" {
			jt = "INNER"
		}
		cond := j.Cond
		// Convert USING → ON condition
		if len(j.UsingColumns) > 0 && cond == nil {
			prevTable := tables[len(tables)-1]
			prevName := prevTable.Name
			if prevTable.Alias != "" {
				prevName = prevTable.Alias
			}
			rightName := j.Right.Name
			if j.Right.Alias != "" {
				rightName = j.Right.Alias
			}
			var useCond QP.Expr
			for i, col := range j.UsingColumns {
				eq := &QP.BinaryExpr{
					Op:    QP.TokenEq,
					Left:  &QP.ColumnRef{Table: prevName, Name: col},
					Right: &QP.ColumnRef{Table: rightName, Name: col},
				}
				if i == 0 {
					useCond = eq
				} else {
					useCond = &QP.BinaryExpr{Op: QP.TokenAnd, Left: useCond, Right: eq}
				}
			}
			cond = useCond
		}
		steps = append(steps, chainJoinStep{table: j.Right, joinType: jt, cond: cond})
		tables = append(tables, j.Right)
		j = j.Right.Join
	}
	return tables, steps
}

func (c *Compiler) compileJoin(leftTable *QP.TableRef, join *QP.Join, where QP.Expr, columns []QP.Expr) {
	util.AssertNotNil(leftTable, "leftTable")
	util.AssertNotNil(join, "join")
	util.Assert(join.Right != nil, "join.Right cannot be nil")

	// Flatten join chain and convert USING → ON
	tables, joinSteps := flattenJoinChain(leftTable, join)

	leftTableName := tables[0].Name
	rightTableName := tables[1].Name

	if leftTableName == "" || rightTableName == "" {
		return
	}

	joinType := joinSteps[0].joinType
	if joinType == "" || joinType == "INNER" || joinType == "CROSS" || joinType == "LEFT" {
		// Supported
	} else {
		return
	}

	// Setup cursors for all tables
	c.tableCursors = make(map[string]int)
	for i, t := range tables {
		c.tableCursors[t.Name] = i
		if t.Alias != "" {
			c.tableCursors[t.Alias] = i
		}
	}

	if c.TableSchemas == nil {
		c.TableSchemas = make(map[string]map[string]int)
		if c.TableColIndices != nil {
			leftSchema := make(map[string]int)
			for colName, colIdx := range c.TableColIndices {
				leftSchema[colName] = colIdx
			}
			c.TableSchemas[leftTableName] = leftSchema
			if leftTable.Alias != "" {
				c.TableSchemas[leftTable.Alias] = leftSchema
			}
		}
		rightSchema := make(map[string]int)
		c.TableSchemas[rightTableName] = rightSchema
		if join.Right.Alias != "" {
			c.TableSchemas[join.Right.Alias] = rightSchema
		}
	}

	// For chained JOINs (3+ tables), add schemas for extra tables
	for i := 2; i < len(tables); i++ {
		t := tables[i]
		if _, exists := c.TableSchemas[t.Name]; !exists {
			c.TableSchemas[t.Name] = make(map[string]int)
		}
		if t.Alias != "" {
			if _, exists := c.TableSchemas[t.Alias]; !exists {
				c.TableSchemas[t.Alias] = make(map[string]int)
			}
		}
	}

	c.columnIndices = make(map[string]int)
	for i, col := range columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			c.columnIndices[colRef.Name] = i
		} else if alias, ok := col.(*QP.AliasExpr); ok {
			c.columnIndices[alias.Alias] = i
		}
	}

	// For 3-table joins, dispatch to specialized handler
	if len(tables) >= 3 {
		for i, t := range tables {
			c.program.EmitOpenTable(i, t.Name)
		}
		c.compileJoinChain3(tables, joinSteps, where, columns)
		return
	}

	c.program.EmitOpenTable(0, leftTableName)
	c.program.EmitOpenTable(1, rightTableName)

	// Patch the first join's condition in case USING was converted to ON
	if joinSteps[0].cond != nil && join.Cond == nil {
		join.Cond = joinSteps[0].cond
	}

	isLeftJoin := joinType == "LEFT"

	leftRewind := c.program.EmitOp(VM.OpRewind, 0, 0)

	rightRewindPos := len(c.program.Instructions)
	c.program.EmitOp(VM.OpRewind, 1, 0)

	// For LEFT JOIN, allocate a "match found" register and reset it after each OpRewind(1)
	var matchReg int
	if isLeftJoin {
		matchReg = c.ra.Alloc()
		c.program.EmitLoadConst(matchReg, int64(0))
	}

	// innerLoopStart is the position of the first inner loop instruction
	// (after OpRewind and matchReg reset) - this is where we jump back to on each iteration
	innerLoopStart := len(c.program.Instructions)

	var skipPos int
	if join.Cond != nil {
		joinCondReg := c.compileExpr(join.Cond)
		// OpIfNot jumps to P2 if joinCondReg is false (0) or null
		skipPos = c.program.EmitOp(VM.OpIfNot, int32(joinCondReg), 0)
	}

	colRegs := make([]int, 0)
	for _, col := range columns {
		reg := c.compileExpr(col)
		colRegs = append(colRegs, reg)
	}

	// For LEFT JOIN, mark match found BEFORE WHERE check (ON condition is what determines match)
	if isLeftJoin {
		c.program.EmitLoadConst(matchReg, int64(1))
	}

	var whereSkipPos int
	hasWhere := false
	if where != nil {
		whereReg := c.compileExpr(where)
		// OpIfNot jumps to P2 if whereReg is false (0) or null - skip this row
		whereSkipPos = c.program.EmitOp(VM.OpIfNot, int32(whereReg), 0)
		hasWhere = true
	}

	c.program.EmitResultRow(colRegs)

	rightNextPos := len(c.program.Instructions)
	rightNext := c.program.EmitOp(VM.OpNext, 1, 0)

	if join.Cond != nil {
		c.program.Instructions[skipPos].P2 = int32(rightNextPos)
	}
	if hasWhere {
		c.program.Instructions[whereSkipPos].P2 = int32(rightNextPos)
	}

	// Jump back to inner loop start (after matchReg reset and after OpRewind)
	c.program.EmitGoto(innerLoopStart)

	rightDonePos := len(c.program.Instructions)

	// For LEFT JOIN: if no match was found, emit left row with NULLs for right columns
	var skipNullRowPos int
	if isLeftJoin {
		// OpIf(matchReg, skipNullRow) - skip null row emission if match was found
		skipNullRowPos = c.program.EmitOp(VM.OpIf, int32(matchReg), 0)
		// Re-read left columns and null out right columns
		for i, col := range columns {
			if c.exprCursorID(col) == 1 {
				c.program.EmitOp(VM.OpNull, int32(colRegs[i]), 0)
			} else {
				// Re-read from cursor 0 (left table)
				c.emitColumnForLeftJoinNullRow(col, colRegs[i])
			}
		}
		// Also apply WHERE filter on null row (right columns are NULL here)
		if where != nil {
			nullWhereReg := c.compileExpr(where)
			nullWhereSkipPos := c.program.EmitOp(VM.OpIfNot, int32(nullWhereReg), 0)
			c.program.EmitResultRow(colRegs)
			c.program.Instructions[nullWhereSkipPos].P2 = int32(len(c.program.Instructions))
		} else {
			c.program.EmitResultRow(colRegs)
		}
		c.program.Instructions[skipNullRowPos].P2 = int32(len(c.program.Instructions))
	}

	leftNext := c.program.EmitOp(VM.OpNext, 0, 0)
	// Go back to rightRewindPos to re-rewind right cursor and reset matchReg for next left row
	c.program.EmitGoto(rightRewindPos)

	leftDonePos := len(c.program.Instructions)
	c.program.Emit(VM.OpHalt)

	c.program.FixupWithPos(leftRewind, leftDonePos)
	c.program.FixupWithPos(rightNext, rightDonePos)
	c.program.FixupWithPos(leftNext, leftDonePos)
}

// compileJoinChain3 generates nested loop VM code for a 3-table JOIN.
// tables[0]=left(cursor 0), tables[1]=mid(cursor 1), tables[2]=right(cursor 2).
// joinSteps[0] connects tables[0]→tables[1], joinSteps[1] connects tables[1]→tables[2].
func (c *Compiler) compileJoinChain3(tables []*QP.TableRef, joinSteps []chainJoinStep, where QP.Expr, columns []QP.Expr) {
	isLeft0 := joinSteps[0].joinType == "LEFT"
	isLeft1 := len(joinSteps) > 1 && joinSteps[1].joinType == "LEFT"

	// Outer rewind (cursor 0 = t1)
	rewind0 := c.program.EmitOp(VM.OpRewind, 0, 0)

	// Per-left-row match tracking registers for LEFT JOINs
	var matchReg0, matchReg1 int

	// --- T2 rewind section (start of "for each t1 row" loop body) ---
	t2RewindPos := len(c.program.Instructions)
	rewind1 := c.program.EmitOp(VM.OpRewind, 1, 0)
	if isLeft0 {
		matchReg0 = c.ra.Alloc()
		c.program.EmitLoadConst(matchReg0, int64(0))
	}

	// --- T2 loop body ---
	t2LoopStart := len(c.program.Instructions)

	var skip12 int
	if joinSteps[0].cond != nil {
		cond12Reg := c.compileExpr(joinSteps[0].cond)
		skip12 = c.program.EmitOp(VM.OpIfNot, int32(cond12Reg), 0) // fixup → t2NextPos
	}
	if isLeft0 {
		// Mark t1-t2 match as found
		c.program.EmitLoadConst(matchReg0, int64(1))
	}

	// --- T3 rewind (for each t2 row that passed cond12) ---
	rewind2 := c.program.EmitOp(VM.OpRewind, 2, 0)
	if isLeft1 {
		matchReg1 = c.ra.Alloc()
		c.program.EmitLoadConst(matchReg1, int64(0))
	}

	// --- T3 loop body ---
	t3LoopStart := len(c.program.Instructions)

	var skip23 int
	if len(joinSteps) > 1 && joinSteps[1].cond != nil {
		cond23Reg := c.compileExpr(joinSteps[1].cond)
		skip23 = c.program.EmitOp(VM.OpIfNot, int32(cond23Reg), 0) // fixup → t3NextPos
	}
	if isLeft1 {
		c.program.EmitLoadConst(matchReg1, int64(1))
	}

	// Compile output columns
	colRegs := make([]int, 0, len(columns))
	for _, col := range columns {
		reg := c.compileExpr(col)
		colRegs = append(colRegs, reg)
	}

	var whereSkipPos int
	hasWhere := false
	if where != nil {
		whereReg := c.compileExpr(where)
		whereSkipPos = c.program.EmitOp(VM.OpIfNot, int32(whereReg), 0)
		hasWhere = true
	}

	c.program.EmitResultRow(colRegs)

	// --- T3 next ---
	t3NextPos := len(c.program.Instructions)
	next2 := c.program.EmitOp(VM.OpNext, 2, 0) // fixup → t2LeftCheckOrNextPos
	c.program.EmitGoto(t3LoopStart)

	if skip23 != 0 {
		c.program.Instructions[skip23].P2 = int32(t3NextPos)
	}
	if hasWhere {
		c.program.Instructions[whereSkipPos].P2 = int32(t3NextPos)
	}

	// --- After T3 loop: LEFT JOIN null row for t2-t3 level ---
	t2LeftCheckPos := len(c.program.Instructions)
	if isLeft1 {
		skipNullRow1 := c.program.EmitOp(VM.OpIf, int32(matchReg1), 0)
		// Re-read t1 and t2 columns; emit NULL for t3 columns
		for i, col := range columns {
			if c.exprCursorID(col) >= 2 {
				c.program.EmitOp(VM.OpNull, int32(colRegs[i]), 0)
			} else {
				c.emitColumnForLeftJoinNullRow(col, colRegs[i])
			}
		}
		if where != nil {
			nullWhere1Reg := c.compileExpr(where)
			nullWhere1Skip := c.program.EmitOp(VM.OpIfNot, int32(nullWhere1Reg), 0)
			c.program.EmitResultRow(colRegs)
			c.program.Instructions[nullWhere1Skip].P2 = int32(len(c.program.Instructions))
		} else {
			c.program.EmitResultRow(colRegs)
		}
		c.program.Instructions[skipNullRow1].P2 = int32(len(c.program.Instructions))
	}

	// --- T2 next ---
	t2NextPos := len(c.program.Instructions)
	next1 := c.program.EmitOp(VM.OpNext, 1, 0) // fixup → t1LeftCheckOrNextPos
	c.program.EmitGoto(t2LoopStart)

	if skip12 != 0 {
		c.program.Instructions[skip12].P2 = int32(t2NextPos)
	}
	// Fix rewind2: if t3 empty → go to t2NextPos (advance t2)
	// For LEFT JOIN, after empty t3, we still want to check the null row condition
	if isLeft1 {
		c.program.FixupWithPos(rewind2, t2LeftCheckPos)
		c.program.FixupWithPos(next2, t2LeftCheckPos)
	} else {
		c.program.FixupWithPos(rewind2, t2NextPos)
		c.program.FixupWithPos(next2, t2NextPos)
	}

	// --- After T2 loop: LEFT JOIN null row for t1-t2 level ---
	t1LeftCheckPos := len(c.program.Instructions)
	if isLeft0 {
		skipNullRow0 := c.program.EmitOp(VM.OpIf, int32(matchReg0), 0)
		if !isLeft1 && len(joinSteps) > 1 && joinSteps[1].cond != nil {
			// Mixed LEFT JOIN t2 + INNER JOIN t3: scan t3 for matches before emitting null-t2 row.
			// Only emit a row if at least one t3 row matches the INNER JOIN condition.
			nullRewind2 := c.program.EmitOp(VM.OpRewind, 2, 0)
			nullT3LoopStart := len(c.program.Instructions)
			// Re-read t1 columns; emit NULL for t2 columns (t3 columns will be filled from cursor 2)
			for i, col := range columns {
				if c.exprCursorID(col) == 1 {
					c.program.EmitOp(VM.OpNull, int32(colRegs[i]), 0)
				} else if c.exprCursorID(col) != 2 {
					c.emitColumnForLeftJoinNullRow(col, colRegs[i])
				} else {
					reg := c.compileExpr(col)
					c.program.EmitOp(VM.OpMove, int32(reg), int32(colRegs[i]))
				}
			}
			null23CondReg := c.compileExpr(joinSteps[1].cond)
			nullSkip23 := c.program.EmitOp(VM.OpIfNot, int32(null23CondReg), 0)
			if where != nil {
				nullWhereReg := c.compileExpr(where)
				nullWhereSkip := c.program.EmitOp(VM.OpIfNot, int32(nullWhereReg), 0)
				c.program.EmitResultRow(colRegs)
				c.program.Instructions[nullWhereSkip].P2 = int32(len(c.program.Instructions))
			} else {
				c.program.EmitResultRow(colRegs)
			}
			nullT3NextPos := len(c.program.Instructions)
			nullNext2 := c.program.EmitOp(VM.OpNext, 2, 0)
			c.program.EmitGoto(nullT3LoopStart)
			c.program.Instructions[nullSkip23].P2 = int32(nullT3NextPos)
			nullT3DonePos := len(c.program.Instructions)
			c.program.FixupWithPos(nullRewind2, nullT3DonePos)
			c.program.FixupWithPos(nullNext2, nullT3DonePos)
		} else {
			// Pure LEFT JOIN (no INNER JOIN after): emit null row for t2 and t3
			for i, col := range columns {
				if c.exprCursorID(col) >= 1 {
					c.program.EmitOp(VM.OpNull, int32(colRegs[i]), 0)
				} else {
					c.emitColumnForLeftJoinNullRow(col, colRegs[i])
				}
			}
			if where != nil {
				nullWhere0Reg := c.compileExpr(where)
				nullWhere0Skip := c.program.EmitOp(VM.OpIfNot, int32(nullWhere0Reg), 0)
				c.program.EmitResultRow(colRegs)
				c.program.Instructions[nullWhere0Skip].P2 = int32(len(c.program.Instructions))
			} else {
				c.program.EmitResultRow(colRegs)
			}
		}
		c.program.Instructions[skipNullRow0].P2 = int32(len(c.program.Instructions))
	}

	// --- T1 next ---
	t1NextPos := len(c.program.Instructions)
	next0 := c.program.EmitOp(VM.OpNext, 0, 0)
	c.program.EmitGoto(t2RewindPos) // rewind t2 for next t1 row

	haltPos := len(c.program.Instructions)
	c.program.Emit(VM.OpHalt)

	// Fixup rewind0
	c.program.FixupWithPos(rewind0, haltPos)

	// Fixup rewind1 and next1: when t2 is empty/exhausted → go to t1 left-check or next
	if isLeft0 {
		c.program.FixupWithPos(rewind1, t1LeftCheckPos)
		c.program.FixupWithPos(next1, t1LeftCheckPos)
	} else {
		c.program.FixupWithPos(rewind1, t1NextPos)
		c.program.FixupWithPos(next1, t1NextPos)
	}

	// Fixup next0
	c.program.FixupWithPos(next0, haltPos)
	_ = t1LeftCheckPos // used conditionally above
}

// exprCursorID returns which cursor (0=left, 1=right) the expression reads from.
func (c *Compiler) exprCursorID(col QP.Expr) int {
	switch e := col.(type) {
	case *QP.ColumnRef:
		if e.Table != "" && c.tableCursors != nil {
			if id, ok := c.tableCursors[e.Table]; ok {
				return id
			}
		}
		// For unqualified columns in JOIN context, look up in TableSchemas
		if e.Table == "" && c.TableSchemas != nil && c.tableCursors != nil {
			for tbl, schema := range c.TableSchemas {
				if _, ok := schema[e.Name]; ok {
					if cid, ok2 := c.tableCursors[tbl]; ok2 {
						return cid
					}
				}
			}
		}
		return 0
	case *QP.AliasExpr:
		return c.exprCursorID(e.Expr)
	default:
		return 0
	}
}

// emitColumnForLeftJoinNullRow re-reads a left-table column expression into reg.
func (c *Compiler) emitColumnForLeftJoinNullRow(col QP.Expr, reg int) {
	switch e := col.(type) {
	case *QP.ColumnRef:
		colIdx := -1
		cursorID := 0
		if e.Table != "" && c.TableSchemas != nil {
			if schema, ok := c.TableSchemas[e.Table]; ok {
				if idx, ok2 := schema[e.Name]; ok2 {
					colIdx = idx
					if c.tableCursors != nil {
						if cid, ok3 := c.tableCursors[e.Table]; ok3 {
							cursorID = cid
						}
					}
				}
			}
		} else if e.Table == "" && c.TableSchemas != nil && c.tableCursors != nil {
			// For unqualified columns in JOIN context, look in the left-side table (cursor 0)
			for tbl, schema := range c.TableSchemas {
				if cid, ok := c.tableCursors[tbl]; ok && cid == 0 {
					if idx, ok2 := schema[e.Name]; ok2 {
						colIdx = idx
						cursorID = 0
						break
					}
				}
			}
		} else if c.TableColIndices != nil {
			if idx, ok := c.TableColIndices[e.Name]; ok {
				colIdx = idx
			}
		}
		c.program.EmitColumnWithTable(reg, cursorID, colIdx, e.Table)
	case *QP.AliasExpr:
		c.emitColumnForLeftJoinNullRow(e.Expr, reg)
	default:
		// For complex expressions, just re-read (may be stale but better than nil)
	}
}

func (c *Compiler) Program() *VM.Program {
	return c.program
}

func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *VM.Program {
	util.AssertNotNil(stmt, "InsertStmt")
	util.Assert(stmt.Table != "", "InsertStmt.Table cannot be empty")
	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(16)

	c.program.Emit(VM.OpInit)
	c.program.EmitOpenTable(0, stmt.Table)

	if stmt.UseDefaults {
		c.program.Instructions = append(c.program.Instructions, VM.Instruction{
			Op: VM.OpInsert,
			P1: 0,
			P4: map[string]int{},
		})
	} else {
		for _, row := range stmt.Values {
			var insertInfo interface{}

			if len(stmt.Columns) > 0 {
				colMap := make(map[string]int)
				for i, val := range row {
					if i < len(stmt.Columns) {
						reg := c.compileExpr(val)
						colMap[stmt.Columns[i]] = reg
					}
				}
				insertInfo = colMap
			} else {
				rowRegs := make([]int, 0)
				for _, val := range row {
					reg := c.compileExpr(val)
					rowRegs = append(rowRegs, reg)
				}
				insertInfo = rowRegs
			}

			c.program.Instructions = append(c.program.Instructions, VM.Instruction{
				Op: VM.OpInsert,
				P1: 0,
				P4: insertInfo,
			})
		}
	}

	c.program.Emit(VM.OpHalt)
	return c.finalize()
}

func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *VM.Program {
	util.AssertNotNil(stmt, "UpdateStmt")
	util.Assert(stmt.Table != "", "UpdateStmt.Table cannot be empty")
	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(16)

	c.program.Emit(VM.OpInit)
	c.program.EmitOpenTable(0, stmt.Table)

	loopStartIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpRewind, P1: 0, P2: 0})

	loopBodyIdx := len(c.program.Instructions)

	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		skipTargetIdx := len(c.program.Instructions)
		c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpIfNot, P1: int32(whereReg), P2: 0})

		setInfo := make(map[string]int)
		for _, set := range stmt.Set {
			valueReg := c.compileExpr(set.Value)
			if colRef, ok := set.Column.(*QP.ColumnRef); ok {
				setInfo[colRef.Name] = valueReg
			}
		}

		c.program.Instructions = append(c.program.Instructions, VM.Instruction{
			Op: VM.OpUpdate,
			P1: 0,
			P4: setInfo,
		})

		c.program.Instructions[skipTargetIdx].P2 = int32(len(c.program.Instructions))
	} else {
		setInfo := make(map[string]int)
		for _, set := range stmt.Set {
			valueReg := c.compileExpr(set.Value)
			if colRef, ok := set.Column.(*QP.ColumnRef); ok {
				setInfo[colRef.Name] = valueReg
			}
		}

		c.program.Instructions = append(c.program.Instructions, VM.Instruction{
			Op: VM.OpUpdate,
			P1: 0,
			P4: setInfo,
		})
	}

	nextIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpNext, P1: 0, P2: 0})

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpGoto, P2: int32(loopBodyIdx)})

	afterLoopIdx := len(c.program.Instructions)
	c.program.Instructions[loopStartIdx].P2 = int32(afterLoopIdx)
	c.program.Instructions[nextIdx].P2 = int32(afterLoopIdx)

	c.program.Emit(VM.OpHalt)
	return c.finalize()
}

func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *VM.Program {
	util.AssertNotNil(stmt, "DeleteStmt")
	util.Assert(stmt.Table != "", "DeleteStmt.Table cannot be empty")
	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(16)

	c.program.Emit(VM.OpInit)
	c.program.EmitOpenTable(0, stmt.Table)

	loopStartIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpRewind, P1: 0, P2: 0})

	loopBodyIdx := len(c.program.Instructions)

	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		skipTargetIdx := len(c.program.Instructions)
		c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpIfNot, P1: int32(whereReg), P2: 0})

		c.program.Instructions = append(c.program.Instructions, VM.Instruction{
			Op: VM.OpDelete,
			P1: 0,
		})

		c.program.Instructions[skipTargetIdx].P2 = int32(len(c.program.Instructions))
	} else {
		c.program.Instructions = append(c.program.Instructions, VM.Instruction{
			Op: VM.OpDelete,
			P1: 0,
		})
	}

	nextIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpNext, P1: 0, P2: 0})

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{Op: VM.OpGoto, P2: int32(loopBodyIdx)})

	afterLoopIdx := len(c.program.Instructions)
	c.program.Instructions[loopStartIdx].P2 = int32(afterLoopIdx)
	c.program.Instructions[nextIdx].P2 = int32(afterLoopIdx)

	c.program.Emit(VM.OpHalt)
	return c.finalize()
}

func (c *Compiler) CompileAggregate(stmt *QP.SelectStmt) *VM.Program {
	util.AssertNotNil(stmt, "SelectStmt for aggregate")
	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(32)

	c.program.Emit(VM.OpInit)

	tableName := ""
	if stmt.From != nil {
		tableName = stmt.From.Name
	}
	if tableName != "" {
		c.program.EmitOpenTable(0, tableName)
	}

	aggInfo := &VM.AggregateInfo{
		GroupByExprs: make([]QP.Expr, 0),
		Aggregates:   make([]VM.AggregateDef, 0),
		NonAggCols:   make([]QP.Expr, 0),
		HavingExpr:   stmt.Having,
		WhereExpr:    stmt.Where,
	}

	if stmt.GroupBy != nil {
		aggInfo.GroupByExprs = stmt.GroupBy
	}

	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			switch strings.ToUpper(fc.Name) {
			case "COUNT", "SUM", "AVG", "MIN", "MAX":
				aggDef := VM.AggregateDef{
					Function: strings.ToUpper(fc.Name),
					Args:     fc.Args,
					Distinct: fc.Distinct,
				}
				aggInfo.Aggregates = append(aggInfo.Aggregates, aggDef)
			default:
				aggInfo.NonAggCols = append(aggInfo.NonAggCols, col)
			}
		} else if exprHasAggregate(col) {
			// Expression containing aggregates (e.g. MAX(id)+1) - extract embedded aggregates
			extractAggregatesFromExpr(col, aggInfo)
		} else {
			aggInfo.NonAggCols = append(aggInfo.NonAggCols, col)
		}
	}
	// Store original SELECT expressions for post-aggregate evaluation
	aggInfo.SelectExprs = stmt.Columns

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{
		Op: VM.OpAggregate,
		P1: 0,
		P4: aggInfo,
	})

	c.program.Emit(VM.OpHalt)
	return c.finalize()
}

func (c *Compiler) resolveColumnCount(columns []QP.Expr) int {
	numCols := len(columns)

	if numCols == 1 {
		if colRef, ok := columns[0].(*QP.ColumnRef); ok && colRef.Name == "*" {
			if c.TableColOrder != nil {
				return len(c.TableColOrder)
			} else if c.TableColIndices != nil {
				return len(c.TableColIndices)
			} else if c.TableSchemas != nil {
				totalCols := 0
				for _, schema := range c.TableSchemas {
					totalCols += len(schema)
				}
				return totalCols
			}
		}
	}

	return numCols
}

func (c *Compiler) compileSetOp(stmt *QP.SelectStmt) *VM.Program {
	c.program = VM.NewProgram()
	c.ra = VM.NewRegisterAllocator(32)

	c.program.Emit(VM.OpInit)

	numCols := c.resolveColumnCount(stmt.Columns)

	switch stmt.SetOp {
	case "UNION":
		if stmt.SetOpAll {
			c.compileSetOpUnionAll(stmt)
		} else {
			c.compileSetOpUnionDistinct(stmt, numCols)
		}
	case "EXCEPT":
		c.compileSetOpExcept(stmt, numCols)
	case "INTERSECT":
		c.compileSetOpIntersect(stmt, numCols)
	}

	c.program.Emit(VM.OpHalt)
	return c.finalize()
}

func (c *Compiler) compileSetOpUnionAll(stmt *QP.SelectStmt) {
	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(&leftStmt)

	baseAddr := len(c.program.Instructions)
	for i := 0; i < len(leftProg.Instructions); i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == VM.OpInit || inst.Op == VM.OpHalt {
			continue
		}
		if inst.Op.IsJump() && inst.P2 > 0 {
			inst.P2 = inst.P2 + int32(baseAddr)
		}
		c.program.Instructions = append(c.program.Instructions, inst)
	}

	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	baseAddr = len(c.program.Instructions)
	for i := 0; i < len(rightProg.Instructions); i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == VM.OpInit || inst.Op == VM.OpHalt {
			continue
		}
		if inst.Op.IsJump() && inst.P2 > 0 {
			inst.P2 = inst.P2 + int32(baseAddr)
		}
		c.program.Instructions = append(c.program.Instructions, inst)
	}
}

func (c *Compiler) compileSetOpUnionDistinct(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{
		Op: VM.OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(&leftStmt)

	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpUnionDistinct,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpUnionDistinct,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

func (c *Compiler) compileSetOpExcept(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{
		Op: VM.OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpEphemeralInsert,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(stmt)

	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				skipLabel := len(c.program.Instructions) + 2
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpExcept,
					P1: int32(ephemeralTableID),
					P2: int32(skipLabel),
					P4: regs,
				})
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpResultRow,
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

func (c *Compiler) compileSetOpIntersect(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{
		Op: VM.OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpEphemeralInsert,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(stmt)

	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == VM.OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				skipLabel := len(c.program.Instructions) + 2
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpIntersect,
					P1: int32(ephemeralTableID),
					P2: int32(skipLabel),
					P4: regs,
				})
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpResultRow,
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

func (c *Compiler) SetTableSchema(schema map[string]int, schemaOrder []string) {
	c.TableColIndices = schema
	c.TableColOrder = schemaOrder
}

func (c *Compiler) SetMultiTableSchema(schemas map[string]map[string]int, colOrder []string) {
	c.TableSchemas = schemas
	c.TableColOrder = colOrder
}

// HasAggregates reports whether stmt contains aggregate functions or a GROUP BY clause.
func HasAggregates(stmt *QP.SelectStmt) bool {
	return hasAggregates(stmt)
}

func Compile(sql string) (*VM.Program, error) {
	return CompileWithSchema(sql, nil)
}

func CompileWithSchema(sql string, tableColumns []string) (*VM.Program, error) {
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
	c.SetTableSchema(make(map[string]int), tableColumns)
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
		if exprHasAggregate(col) {
			return true
		}
	}
	return stmt.GroupBy != nil
}

// exprHasAggregate recursively checks if an expression contains an aggregate function.
func exprHasAggregate(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.FuncCall:
		switch strings.ToUpper(e.Name) {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL":
			return true
		}
	case *QP.BinaryExpr:
		return exprHasAggregate(e.Left) || exprHasAggregate(e.Right)
	case *QP.UnaryExpr:
		return exprHasAggregate(e.Expr)
	case *QP.AliasExpr:
		return exprHasAggregate(e.Expr)
	case *QP.CastExpr:
		return exprHasAggregate(e.Expr)
	case *QP.CaseExpr:
		if exprHasAggregate(e.Operand) {
			return true
		}
		for _, when := range e.Whens {
			if exprHasAggregate(when.Condition) || exprHasAggregate(when.Result) {
				return true
			}
		}
		return exprHasAggregate(e.Else)
	}
	return false
}

// extractAggregatesFromExpr extracts aggregate function calls embedded in an expression.
func extractAggregatesFromExpr(expr QP.Expr, aggInfo *VM.AggregateInfo) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *QP.FuncCall:
		upperName := strings.ToUpper(e.Name)
		switch upperName {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			// Check if already registered with the same args (not just same function name)
			// e.g., SUM(i) and SUM(r) are different aggregates
			argKey := fmt.Sprintf("%v", e.Args)
			for _, existing := range aggInfo.Aggregates {
				if existing.Function == upperName && fmt.Sprintf("%v", existing.Args) == argKey {
					return
				}
			}
			aggInfo.Aggregates = append(aggInfo.Aggregates, VM.AggregateDef{
				Function: upperName,
				Args:     e.Args,
				Distinct: e.Distinct,
			})
		}
	case *QP.BinaryExpr:
		extractAggregatesFromExpr(e.Left, aggInfo)
		extractAggregatesFromExpr(e.Right, aggInfo)
	case *QP.UnaryExpr:
		extractAggregatesFromExpr(e.Expr, aggInfo)
	case *QP.AliasExpr:
		extractAggregatesFromExpr(e.Expr, aggInfo)
	case *QP.CaseExpr:
		extractAggregatesFromExpr(e.Operand, aggInfo)
		for _, when := range e.Whens {
			extractAggregatesFromExpr(when.Condition, aggInfo)
			extractAggregatesFromExpr(when.Result, aggInfo)
		}
		extractAggregatesFromExpr(e.Else, aggInfo)
	case *QP.CastExpr:
		extractAggregatesFromExpr(e.Expr, aggInfo)
	}
}

func MustCompile(sql string) *VM.Program {
	prog, err := Compile(sql)
	if err != nil {
		panic(err)
	}
	return prog
}

func (c *Compiler) GetVMCompiler() *Compiler {
	return c
}

// shouldUseColumnar returns true when the query would benefit from columnar execution.
// Criteria: pure aggregates without joins, or full table scan without filter.
func shouldUseColumnar(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	// Joins are not supported by columnar path
	if stmt.From != nil && stmt.From.Join != nil {
		return false
	}
	if hasAggregates(stmt) {
		return true
	}
	// Full scan without filter
	if stmt.Where == nil {
		return true
	}
	return false
}
