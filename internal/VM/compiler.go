package VM

import (
	"fmt"
	"strings"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/util"
)

type Compiler struct {
	program         *Program
	ra              *RegisterAllocator
	stmtWhere       []QP.Expr
	stmtColumns     []QP.Expr
	columnIndices   map[string]int            // SELECT position: col name -> index in SELECT list
	TableColIndices map[string]int            // TABLE position: col name -> index in table (exported for external use)
	TableColOrder   []string                  // TABLE position: ordered list of column names (for SELECT * expansion)
	tableCursors    map[string]int            // Maps table name/alias to cursor ID (for JOINs)
	TableSchemas    map[string]map[string]int // Maps table name -> column name -> column index (for JOINs, exported for external use)
}

func NewCompiler() *Compiler {
	return &Compiler{
		program: NewProgram(),
		ra:      NewRegisterAllocator(16),
	}
}

func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *Program {
	// Handle SET operations (UNION, EXCEPT, INTERSECT)
	if stmt.SetOp != "" && stmt.SetOpRight != nil {
		return c.compileSetOp(stmt)
	}

	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)
	c.stmtWhere = nil
	c.stmtColumns = stmt.Columns

	// Expand SELECT * to actual column names if needed
	columns := stmt.Columns
	if (c.TableColIndices != nil && len(c.TableColIndices) > 0) || (c.TableSchemas != nil && len(c.TableSchemas) > 0) {
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

	// Check if there's any star column
	hasStar := false
	for _, col := range columns {
		if colRef, ok := col.(*QP.ColumnRef); ok {
			if colRef.Name == "*" {
				hasStar = true
				break
			}
		}
	}

	if !hasStar {
		return columns
	}

	// Build expanded columns list by processing each column
	expanded := make([]QP.Expr, 0)
	
	for _, col := range columns {
		colRef, isColRef := col.(*QP.ColumnRef)
		if !isColRef || colRef.Name != "*" {
			// Not a star column, keep as-is
			expanded = append(expanded, col)
			continue
		}
		
		// This is a star column - expand it
		starTable := colRef.Table
		
		// If star has table qualifier (e.g., t1.*), expand to that table's columns only
		if starTable != "" && c.TableSchemas != nil {
			if tableSchema, ok := c.TableSchemas[starTable]; ok {
				// Collect and sort columns by index
				type colInfo struct {
					name string
					idx  int
				}
				cols := make([]colInfo, 0, len(tableSchema))
				for colName, idx := range tableSchema {
					cols = append(cols, colInfo{name: colName, idx: idx})
				}
				// Sort by index
				for i := 0; i < len(cols); i++ {
					for j := i + 1; j < len(cols); j++ {
						if cols[i].idx > cols[j].idx {
							cols[i], cols[j] = cols[j], cols[i]
						}
					}
				}
				for _, c := range cols {
					expanded = append(expanded, &QP.ColumnRef{
						Name:  c.name,
						Table: starTable,
					})
				}
				continue
			}
		}
		
		// Unqualified * - expand to all columns from all tables
		// For multi-table queries (JOINs), use TableSchemas
		if c.TableSchemas != nil && len(c.TableSchemas) > 0 && c.TableColOrder != nil && len(c.TableColOrder) > 0 {
			for _, colName := range c.TableColOrder {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				// Find which table this column belongs to
				var tableName string
				for tbl, schema := range c.TableSchemas {
					if _, ok := schema[colName]; ok {
						tableName = tbl
						break
					}
				}
				expanded = append(expanded, &QP.ColumnRef{
					Name:  colName,
					Table: tableName,
				})
			}
			continue
		}
		
		// Single table case: use TableColOrder or TableColIndices
		if c.TableColOrder != nil && len(c.TableColOrder) > 0 {
			for _, colName := range c.TableColOrder {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				expanded = append(expanded, &QP.ColumnRef{Name: colName})
			}
		} else if c.TableColIndices != nil {
			// Fallback: iterate over TableColIndices (may be non-deterministic)
			for colName := range c.TableColIndices {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				expanded = append(expanded, &QP.ColumnRef{Name: colName})
			}
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

	// Assert: INNER, CROSS, and LEFT JOINs are currently supported
	// RIGHT and FULL OUTER JOINs require different logic
	joinType := strings.ToUpper(strings.TrimSpace(join.Type))
	if joinType == "" || joinType == "INNER" || joinType == "CROSS" || joinType == "LEFT" {
		// Supported - continue
	} else {
		util.Assert(false, "JOIN type '%s' is not yet implemented. RIGHT and FULL OUTER JOINs will be implemented in a future version.", joinType)
	}

	// Set up table-to-cursor mapping for JOIN
	c.tableCursors = make(map[string]int)
	// Map table name to cursor 0
	c.tableCursors[leftTableName] = 0
	// If there's an alias, map it as well
	if leftTable.Alias != "" {
		c.tableCursors[leftTable.Alias] = 0
	}
	// Map right table name to cursor 1
	c.tableCursors[rightTableName] = 1
	// If there's an alias, map it as well
	if join.Right.Alias != "" {
		c.tableCursors[join.Right.Alias] = 1
	}

	// Set up multi-table schemas for JOIN column resolution
	// Only initialize if not already set (database.go sets this for us)
	if c.TableSchemas == nil {
		c.TableSchemas = make(map[string]map[string]int)
		// Build schema for left table from TableColIndices (if it's for this table)
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
		// For right table, we need to build schema from scratch
		// We'll use the combined TableColIndices and figure out which columns belong to which table
		// This is a temporary solution - ideally we'd have proper schema info
		rightSchema := make(map[string]int)
		// For now, we'll populate rightSchema later when we have better schema info
		c.TableSchemas[rightTableName] = rightSchema
		if join.Right.Alias != "" {
			c.TableSchemas[join.Right.Alias] = rightSchema
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

	c.program.EmitOpenTable(0, leftTableName)
	c.program.EmitOpenTable(1, rightTableName)

	leftRewind := c.program.EmitOp(OpRewind, 0, 0)

	rightRewindPos := len(c.program.Instructions)
	c.program.EmitOp(OpRewind, 1, 0)

	// Evaluate JOIN condition if present
	var skipPos, nullSkip int
	if join.Cond != nil {
		joinCondReg := c.compileExpr(join.Cond)
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))
		// If condition is false (0), skip this row combination
		skipPos = c.program.EmitEq(joinCondReg, zeroReg, 0)
		// If condition is NULL, also skip
		nullSkip = c.program.EmitOp(OpIsNull, int32(joinCondReg), 0)
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

	// Advance right cursor, if EOF jump to rightDone
	rightNextPos := len(c.program.Instructions)
	rightNext := c.program.EmitOp(OpNext, 1, 0)

	// Fixup JOIN condition skips to jump here (to advance right cursor)
	if join.Cond != nil {
		c.program.FixupWithPos(skipPos, rightNextPos)
		c.program.FixupWithPos(nullSkip, rightNextPos)
	}

	// Loop back to process next right row with same left row
	c.program.EmitGoto(rightRewindPos + 1)

	// Right cursor exhausted, advance left cursor
	rightDonePos := len(c.program.Instructions)
	leftNext := c.program.EmitOp(OpNext, 0, 0)
	// Restart right cursor for next left row
	c.program.EmitGoto(rightRewindPos)

	// Left cursor exhausted, we're done
	leftDonePos := len(c.program.Instructions)
	c.program.Emit(OpHalt)

	// Fixup jump targets
	c.program.FixupWithPos(leftRewind, leftDonePos)
	c.program.FixupWithPos(rightNext, rightDonePos)
	c.program.FixupWithPos(leftNext, leftDonePos)
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

	case *QP.SubqueryExpr:
		return c.compileSubqueryExpr(e)

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
	colIdx := -1  // Use -1 as sentinel for unknown columns
	cursorID := 0 // Default cursor ID

	// Determine cursor ID from table qualifier (for JOINs)
	if col.Table != "" && c.tableCursors != nil {
		if cid, ok := c.tableCursors[col.Table]; ok {
			cursorID = cid
		}
	}

	if col.Name != "" {
		// For JOINs: use multi-table schema if available and table is specified
		if col.Table != "" && c.TableSchemas != nil {
			if tableSchema, ok := c.TableSchemas[col.Table]; ok {
				if idx, ok := tableSchema[col.Name]; ok {
					colIdx = idx
				}
			}
		}

		// Fall back to single table schema (for non-JOIN queries)
		if colIdx == -1 && c.TableColIndices != nil {
			if idx, ok := c.TableColIndices[col.Name]; ok {
				colIdx = idx
			}
		}

		// Fall back to SELECT position (for aliases)
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
	c.program.EmitColumn(reg, cursorID, colIdx)
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
		// AND: result = (left != 0) && (right != 0) ? 1 : 0
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))

		// Check if left is true (non-zero)
		leftCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(OpNe, int32(leftReg), int32(zeroReg), leftCheck)

		// Check if right is true (non-zero)
		rightCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(OpNe, int32(rightReg), int32(zeroReg), rightCheck)

		// AND them together: if both are true, result is true
		c.program.EmitOpWithDst(OpBitAnd, int32(leftCheck), int32(rightCheck), dst)
		return dst
	case QP.TokenOr:
		// OR: result = (left != 0) || (right != 0) ? 1 : 0
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))

		// Check if left is true (non-zero)
		leftCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(OpNe, int32(leftReg), int32(zeroReg), leftCheck)

		// Check if right is true (non-zero)
		rightCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(OpNe, int32(rightReg), int32(zeroReg), rightCheck)

		// OR them together: if either is true, result is true
		c.program.EmitOpWithDst(OpBitOr, int32(leftCheck), int32(rightCheck), dst)
		return dst
	case QP.TokenLike:
		// LIKE: use OpLike which stores result in dst (case-insensitive)
		c.program.EmitOpWithDst(OpLike, int32(leftReg), int32(rightReg), dst)
		return dst
	case QP.TokenGlob:
		// GLOB: use OpGlob which stores result in dst (case-sensitive)
		c.program.EmitOpWithDst(OpGlob, int32(leftReg), int32(rightReg), dst)
		return dst
	case QP.TokenNotLike:
		// NOT LIKE: compute LIKE then negate
		likeResult := c.ra.Alloc()
		c.program.EmitOpWithDst(OpLike, int32(leftReg), int32(rightReg), likeResult)
		// Negate: result = 1 - likeResult
		oneReg := c.ra.Alloc()
		c.program.EmitLoadConst(oneReg, int64(1))
		c.program.EmitOpWithDst(OpSubtract, int32(oneReg), int32(likeResult), dst)
		return dst
	case QP.TokenBetween:
		// BETWEEN: left >= lower AND left <= upper
		// Right side should be BinaryExpr with Op: TokenAnd containing lower and upper
		if binExpr, ok := expr.Right.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenAnd {
			lowerReg := c.compileExpr(binExpr.Left)
			upperReg := c.compileExpr(binExpr.Right)

			// left >= lower
			geResult := c.ra.Alloc()
			c.program.EmitOpWithDst(OpGe, int32(leftReg), int32(lowerReg), geResult)

			// left <= upper
			leResult := c.ra.Alloc()
			c.program.EmitOpWithDst(OpLe, int32(leftReg), int32(upperReg), leResult)

			// AND them together
			c.program.EmitOpWithDst(OpBitAnd, int32(geResult), int32(leResult), dst)
			return dst
		}
		// Fallback
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenNotBetween:
		// NOT BETWEEN: NOT (left >= lower AND left <= upper)
		if binExpr, ok := expr.Right.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenAnd {
			lowerReg := c.compileExpr(binExpr.Left)
			upperReg := c.compileExpr(binExpr.Right)

			// left >= lower
			geResult := c.ra.Alloc()
			c.program.EmitOpWithDst(OpGe, int32(leftReg), int32(lowerReg), geResult)

			// left <= upper
			leResult := c.ra.Alloc()
			c.program.EmitOpWithDst(OpLe, int32(leftReg), int32(upperReg), leResult)

			// AND them together
			betweenResult := c.ra.Alloc()
			c.program.EmitOpWithDst(OpBitAnd, int32(geResult), int32(leResult), betweenResult)

			// NOT the result
			zeroReg := c.ra.Alloc()
			c.program.EmitLoadConst(zeroReg, int64(0))
			c.program.EmitOpWithDst(OpEq, int32(betweenResult), int32(zeroReg), dst)
			return dst
		}
		// Fallback
		c.program.EmitLoadConst(dst, int64(1))
		return dst
	case QP.TokenIn:
		// IN (list): check if left matches any value in the list
		// Right side should be a Literal with a slice of values
		if lit, ok := expr.Right.(*QP.Literal); ok {
			if values, ok := lit.Value.([]interface{}); ok {
				// Load 0 into result (false by default)
				c.program.EmitLoadConst(dst, int64(0))

				// Compare against each value in the list
				for _, val := range values {
					valReg := c.ra.Alloc()
					c.program.EmitLoadConst(valReg, val)

					eqResult := c.ra.Alloc()
					c.program.EmitOpWithDst(OpEq, int32(leftReg), int32(valReg), eqResult)

					// OR with current result (dst = dst | eqResult)
					c.program.EmitOpWithDst(OpBitOr, int32(dst), int32(eqResult), dst)
				}
				return dst
			}
		}
		// Fallback
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenNotIn:
		// NOT IN (list): NOT (check if left matches any value in the list)
		if lit, ok := expr.Right.(*QP.Literal); ok {
			if values, ok := lit.Value.([]interface{}); ok {
				// Load 0 into temp result (false by default)
				inResult := c.ra.Alloc()
				c.program.EmitLoadConst(inResult, int64(0))

				// Compare against each value in the list
				for _, val := range values {
					valReg := c.ra.Alloc()
					c.program.EmitLoadConst(valReg, val)

					eqResult := c.ra.Alloc()
					c.program.EmitOpWithDst(OpEq, int32(leftReg), int32(valReg), eqResult)

					// OR with current result
					c.program.EmitOpWithDst(OpBitOr, int32(inResult), int32(eqResult), inResult)
				}

				// NOT the IN result
				zeroReg := c.ra.Alloc()
				c.program.EmitLoadConst(zeroReg, int64(0))
				c.program.EmitOpWithDst(OpEq, int32(inResult), int32(zeroReg), dst)
				return dst
			}
		}
		// Fallback
		c.program.EmitLoadConst(dst, int64(1))
		return dst
	case QP.TokenInSubquery:
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenIs:
		// IS NULL: compare with NULL
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOpWithDst(OpIs, int32(leftReg), int32(nullReg), dst)
		return dst
	case QP.TokenIsNot:
		// IS NOT NULL: compare with NULL
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOpWithDst(OpIsNot, int32(leftReg), int32(nullReg), dst)
		return dst
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
			// SUBSTR(string, start, length) - pass length register in P2's high bit
			// Use P2 for start register, encode length register in a way exec can decode
			idx := c.program.EmitOp(OpSubstr, int32(argRegs[0]), int32(argRegs[1]))
			// Store length register index in P3 as string
			c.program.Instructions[idx].P3 = fmt.Sprintf("len:%d", argRegs[2])
			c.program.Instructions[idx].P4 = dst
		} else if len(argRegs) >= 2 {
			// SUBSTR(string, start) - to end of string
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

func (c *Compiler) compileSubqueryExpr(subq *QP.SubqueryExpr) int {
	// Allocate a register for the subquery result
	resultReg := c.ra.Alloc()
	
	// Emit OpScalarSubquery instruction
	// P1 = destination register
	// P4 = the SELECT statement to execute
	c.program.Instructions = append(c.program.Instructions, Instruction{
		Op: OpScalarSubquery,
		P1: int32(resultReg),
		P4: subq.Select,
	})
	
	return resultReg
}

func (c *Compiler) Program() *Program {
	return c.program
}

func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	// Open cursor for the table (cursor 0)
	c.program.EmitOpenTable(0, stmt.Table)

	// Insert each row
	for _, row := range stmt.Values {
		// Compile each value expression into registers
		// If columns are specified in INSERT, map values to columns
		// Otherwise, values map to table column order
		var insertInfo interface{}

		if len(stmt.Columns) > 0 {
			// Columns specified: create map of column name to register
			colMap := make(map[string]int)
			for i, val := range row {
				if i < len(stmt.Columns) {
					reg := c.compileExpr(val)
					colMap[stmt.Columns[i]] = reg
				}
			}
			insertInfo = colMap
		} else {
			// No columns specified: use positional array
			rowRegs := make([]int, 0)
			for _, val := range row {
				reg := c.compileExpr(val)
				rowRegs = append(rowRegs, reg)
			}
			insertInfo = rowRegs
		}

		// Emit Insert opcode with column mapping or positional registers
		idx := len(c.program.Instructions)
		c.program.Instructions = append(c.program.Instructions, Instruction{
			Op: OpInsert,
			P1: 0, // cursor ID
			P4: insertInfo,
		})
		_ = idx
	}

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	// Open cursor for the table (cursor 0)
	c.program.EmitOpenTable(0, stmt.Table)

	// Rewind to start of table
	loopStartIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpRewind, P1: 0, P2: 0}) // P2 will be fixed up to jump past loop when empty

	// Loop body starts here
	loopBodyIdx := len(c.program.Instructions)

	// WHERE clause: skip row if condition is false
	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		// If whereReg is false (0), jump to Next
		skipTargetIdx := len(c.program.Instructions)
		c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpIfNot, P1: int32(whereReg), P2: 0}) // P2 will be fixed up

		// Compile SET expressions with column names
		// P4 will be a map[string]int mapping column name to register
		setInfo := make(map[string]int)
		for _, set := range stmt.Set {
			valueReg := c.compileExpr(set.Value)
			// Extract column name from SET clause
			if colRef, ok := set.Column.(*QP.ColumnRef); ok {
				setInfo[colRef.Name] = valueReg
			}
		}

		// Emit Update opcode with column mapping
		c.program.Instructions = append(c.program.Instructions, Instruction{
			Op: OpUpdate,
			P1: 0, // cursor ID
			P4: setInfo,
		})

		// Fix up skip target to jump here (to Next)
		c.program.Instructions[skipTargetIdx].P2 = int32(len(c.program.Instructions))
	} else {
		// No WHERE clause, update all rows
		// Compile SET expressions with column names
		setInfo := make(map[string]int)
		for _, set := range stmt.Set {
			valueReg := c.compileExpr(set.Value)
			// Extract column name from SET clause
			if colRef, ok := set.Column.(*QP.ColumnRef); ok {
				setInfo[colRef.Name] = valueReg
			}
		}

		// Emit Update opcode with column mapping
		c.program.Instructions = append(c.program.Instructions, Instruction{
			Op: OpUpdate,
			P1: 0, // cursor ID
			P4: setInfo,
		})
	}

	// Next: advance to next row, jump to after-loop if EOF
	nextIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpNext, P1: 0, P2: 0}) // P2 will be fixed up to after-loop

	// Jump back to loop body if not EOF
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpGoto, P2: int32(loopBodyIdx)})

	// After-loop: fix up Rewind and Next to jump here
	afterLoopIdx := len(c.program.Instructions)
	c.program.Instructions[loopStartIdx].P2 = int32(afterLoopIdx) // Rewind jumps here if empty
	c.program.Instructions[nextIdx].P2 = int32(afterLoopIdx)      // Next jumps here if EOF

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(16)

	c.program.Emit(OpInit)

	// Open cursor for the table (cursor 0)
	c.program.EmitOpenTable(0, stmt.Table)

	// Rewind to start of table
	loopStartIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpRewind, P1: 0, P2: 0}) // P2 will be fixed up to jump past loop when empty

	// Loop body starts here
	loopBodyIdx := len(c.program.Instructions)

	// WHERE clause: skip row if condition is false
	if stmt.Where != nil {
		whereReg := c.compileExpr(stmt.Where)
		// If whereReg is false (0), jump to Next
		skipTargetIdx := len(c.program.Instructions)
		c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpIfNot, P1: int32(whereReg), P2: 0}) // P2 will be fixed up

		// Emit Delete opcode
		c.program.Instructions = append(c.program.Instructions, Instruction{
			Op: OpDelete,
			P1: 0, // cursor ID
		})

		// Fix up skip target to jump here (to Next)
		c.program.Instructions[skipTargetIdx].P2 = int32(len(c.program.Instructions))
	} else {
		// No WHERE clause, delete all rows
		c.program.Instructions = append(c.program.Instructions, Instruction{
			Op: OpDelete,
			P1: 0, // cursor ID
		})
	}

	// Next: advance to next row, jump to after-loop if EOF
	nextIdx := len(c.program.Instructions)
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpNext, P1: 0, P2: 0}) // P2 will be fixed up to after-loop

	// Jump back to loop body if not EOF
	c.program.Instructions = append(c.program.Instructions, Instruction{Op: OpGoto, P2: int32(loopBodyIdx)})

	// After-loop: fix up Rewind and Next to jump here
	afterLoopIdx := len(c.program.Instructions)
	c.program.Instructions[loopStartIdx].P2 = int32(afterLoopIdx) // Rewind jumps here if empty
	c.program.Instructions[nextIdx].P2 = int32(afterLoopIdx)      // Next jumps here if EOF

	c.program.Emit(OpHalt)
	return c.program
}

func (c *Compiler) CompileAggregate(stmt *QP.SelectStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(32) // More registers for aggregates

	c.program.Emit(OpInit)

	// Open cursor for the table
	tableName := ""
	if stmt.From != nil {
		tableName = stmt.From.Name
	}
	if tableName != "" {
		c.program.EmitOpenTable(0, tableName)
	}

	// Strategy: Scan all rows, accumulate aggregates, emit results
	// P4 will contain aggregate information for the VM to execute
	
	// Build aggregate information structure
	aggInfo := &AggregateInfo{
		GroupByExprs: make([]QP.Expr, 0),
		Aggregates:   make([]AggregateDef, 0),
		NonAggCols:   make([]QP.Expr, 0),
		HavingExpr:   stmt.Having,
	}
	
	// Collect GROUP BY expressions
	if stmt.GroupBy != nil {
		aggInfo.GroupByExprs = stmt.GroupBy
	}
	
	// Analyze SELECT columns for aggregates and non-aggregate expressions
	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			switch fc.Name {
			case "COUNT", "SUM", "AVG", "MIN", "MAX":
				// This is an aggregate
				aggDef := AggregateDef{
					Function: fc.Name,
					Args:     fc.Args,
				}
				aggInfo.Aggregates = append(aggInfo.Aggregates, aggDef)
			default:
				// Non-aggregate function - treat as non-agg column
				aggInfo.NonAggCols = append(aggInfo.NonAggCols, col)
			}
		} else {
			// Non-aggregate column
			aggInfo.NonAggCols = append(aggInfo.NonAggCols, col)
		}
	}
	
	// Emit OpAggregate instruction with all the information
	// The VM will execute this by:
	// 1. Scanning all rows
	// 2. Grouping by GROUP BY expressions
	// 3. Accumulating aggregates per group
	// 4. Emitting result rows (one per group)
	// 5. Filtering by HAVING clause if present
	c.program.Instructions = append(c.program.Instructions, Instruction{
		Op: OpAggregate,
		P1: 0, // cursor ID
		P4: aggInfo,
	})

	c.program.Emit(OpHalt)
	return c.program
}

// AggregateInfo contains information about aggregate queries
type AggregateInfo struct {
	GroupByExprs []QP.Expr      // GROUP BY expressions
	Aggregates   []AggregateDef // Aggregate functions in SELECT
	NonAggCols   []QP.Expr      // Non-aggregate columns in SELECT
	HavingExpr   QP.Expr        // HAVING clause expression
}

// AggregateDef defines an aggregate function
type AggregateDef struct {
	Function string     // COUNT, SUM, AVG, MIN, MAX
	Args     []QP.Expr  // Arguments to the aggregate
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

// compileSetOp compiles SET operations (UNION, EXCEPT, INTERSECT)
func (c *Compiler) compileSetOp(stmt *QP.SelectStmt) *Program {
	c.program = NewProgram()
	c.ra = NewRegisterAllocator(32) // More registers for SetOp

	c.program.Emit(OpInit)

	// Determine number of columns from left SELECT
	numCols := c.resolveColumnCount(stmt.Columns)

	// Strategy depends on operation type
	switch stmt.SetOp {
	case "UNION":
		if stmt.SetOpAll {
			// UNION ALL: Execute left, then right, results combined automatically
			c.compileSetOpUnionAll(stmt)
		} else {
			// UNION DISTINCT: Use ephemeral table for deduplication
			c.compileSetOpUnionDistinct(stmt, numCols)
		}
	case "EXCEPT":
		c.compileSetOpExcept(stmt, numCols)
	case "INTERSECT":
		c.compileSetOpIntersect(stmt, numCols)
	}

	c.program.Emit(OpHalt)
	return c.program
}

// resolveColumnCount resolves the actual number of columns in a SELECT list
// If the list contains a star (*), it returns the actual column count from the table schema
func (c *Compiler) resolveColumnCount(columns []QP.Expr) int {
	numCols := len(columns)
	
	// Check if columns contains a star
	if numCols == 1 {
		if colRef, ok := columns[0].(*QP.ColumnRef); ok && colRef.Name == "*" {
			// Get actual column count from table schema
			if c.TableColOrder != nil {
				return len(c.TableColOrder)
			} else if c.TableColIndices != nil {
				return len(c.TableColIndices)
			} else if c.TableSchemas != nil {
				// For multi-table queries, count all columns
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

// compileSetOpUnionAll compiles UNION ALL (no deduplication)
func (c *Compiler) compileSetOpUnionAll(stmt *QP.SelectStmt) {
	// Create a temporary statement without SetOp for left side
	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	// Execute left SELECT - results go to vm.results
	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(&leftStmt)

	// Merge left program instructions, adjusting jump addresses
	baseAddr := len(c.program.Instructions)
	for i := 0; i < len(leftProg.Instructions); i++ {
		inst := leftProg.Instructions[i]
		// Skip Init and Halt
		if inst.Op == OpInit || inst.Op == OpHalt {
			continue
		}
		// Adjust jump addresses
		if inst.Op.IsJump() && inst.P2 > 0 {
			inst.P2 = inst.P2 + int32(baseAddr)
		}
		c.program.Instructions = append(c.program.Instructions, inst)
	}

	// Execute right SELECT - results also go to vm.results
	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	// Merge right program instructions, adjusting jump addresses
	baseAddr = len(c.program.Instructions)
	for i := 0; i < len(rightProg.Instructions); i++ {
		inst := rightProg.Instructions[i]
		// Skip Init and Halt
		if inst.Op == OpInit || inst.Op == OpHalt {
			continue
		}
		// Adjust jump addresses
		if inst.Op.IsJump() && inst.P2 > 0 {
			inst.P2 = inst.P2 + int32(baseAddr)
		}
		c.program.Instructions = append(c.program.Instructions, inst)
	}
}

// compileSetOpUnionDistinct compiles UNION (with deduplication)
func (c *Compiler) compileSetOpUnionDistinct(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	// Create ephemeral table for tracking seen rows
	c.program.Instructions = append(c.program.Instructions, Instruction{
		Op: OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	// Create a temporary statement without SetOp for left side
	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	// Compile and execute left SELECT
	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(&leftStmt)

	// For each row from left, check if seen, if not add to results and ephemeral table
	// Replace ResultRow with UnionDistinct logic
	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == OpResultRow {
			// Get register array
			if regs, ok := inst.P4.([]int); ok {
				// Use OpUnionDistinct to handle deduplication
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpUnionDistinct,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	// Compile and execute right SELECT
	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	// For each row from right, check if seen, if not add to results and ephemeral table
	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == OpResultRow {
			// Get register array
			if regs, ok := inst.P4.([]int); ok {
				// Use OpUnionDistinct to handle deduplication
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpUnionDistinct,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

// compileSetOpExcept compiles EXCEPT (left minus right)
func (c *Compiler) compileSetOpExcept(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	// Create ephemeral table for right-side rows
	c.program.Instructions = append(c.program.Instructions, Instruction{
		Op: OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	// Execute right SELECT and populate ephemeral table
	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	// Replace ResultRow with EphemeralInsert
	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == OpResultRow {
			// Insert into ephemeral table instead of results
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpEphemeralInsert,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	// Execute left SELECT, outputting only rows not in ephemeral table
	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(stmt)

	// For each row from left, check if exists in ephemeral table
	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				// Check if row exists in right side (ephemeral table)
				skipLabel := len(c.program.Instructions) + 2
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpExcept,
					P1: int32(ephemeralTableID),
					P2: int32(skipLabel), // Jump past ResultRow if in right
					P4: regs,
				})
				// If not skipped, output the row
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpResultRow,
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

// compileSetOpIntersect compiles INTERSECT (common rows)
func (c *Compiler) compileSetOpIntersect(stmt *QP.SelectStmt, numCols int) {
	ephemeralTableID := 1

	// Create ephemeral table for right-side rows
	c.program.Instructions = append(c.program.Instructions, Instruction{
		Op: OpEphemeralCreate,
		P1: int32(ephemeralTableID),
	})

	// Execute right SELECT and populate ephemeral table
	rightCompiler := NewCompiler()
	rightCompiler.TableColIndices = c.TableColIndices
	rightCompiler.TableColOrder = c.TableColOrder
	rightProg := rightCompiler.CompileSelect(stmt.SetOpRight)

	// Replace ResultRow with EphemeralInsert
	for i := 1; i < len(rightProg.Instructions)-1; i++ {
		inst := rightProg.Instructions[i]
		if inst.Op == OpResultRow {
			// Insert into ephemeral table instead of results
			if regs, ok := inst.P4.([]int); ok {
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpEphemeralInsert,
					P1: int32(ephemeralTableID),
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}

	// Execute left SELECT, outputting only rows that ARE in ephemeral table
	leftCompiler := NewCompiler()
	leftCompiler.TableColIndices = c.TableColIndices
	leftCompiler.TableColOrder = c.TableColOrder
	leftProg := leftCompiler.CompileSelect(stmt)

	// For each row from left, check if exists in ephemeral table
	for i := 1; i < len(leftProg.Instructions)-1; i++ {
		inst := leftProg.Instructions[i]
		if inst.Op == OpResultRow {
			if regs, ok := inst.P4.([]int); ok {
				// Check if row exists in right side (ephemeral table)
				skipLabel := len(c.program.Instructions) + 2
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpIntersect,
					P1: int32(ephemeralTableID),
					P2: int32(skipLabel), // Jump past ResultRow if NOT in right
					P4: regs,
				})
				// If not skipped, output the row
				c.program.Instructions = append(c.program.Instructions, Instruction{
					Op: OpResultRow,
					P4: regs,
				})
			}
		} else {
			c.program.Instructions = append(c.program.Instructions, inst)
		}
	}
}

func MustCompile(sql string) *Program {
	prog, err := Compile(sql)
	if err != nil {
		panic(err)
	}
	return prog
}
