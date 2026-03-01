package CG

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// BytecodeCompiler compiles QP AST nodes into VM.BytecodeProg.
type BytecodeCompiler struct {
	// TableSchemas maps table name → column name → type string.
	// Used for schema-aware column resolution (e.g. SELECT *).
	TableSchemas map[string]map[string]string
	// TableColOrder maps table name → ordered column names.
	TableColOrder map[string][]string

	b *VM.BytecodeBuilder
}

// NewBytecodeCompiler creates a new BytecodeCompiler.
func NewBytecodeCompiler() *BytecodeCompiler {
	return &BytecodeCompiler{
		TableSchemas:  make(map[string]map[string]string),
		TableColOrder: make(map[string][]string),
	}
}

// CompileSelect compiles a SELECT statement into a BytecodeProg.
// Falls back gracefully, returning an error for unsupported constructs.
func (bc *BytecodeCompiler) CompileSelect(stmt *QP.SelectStmt) (*VM.BytecodeProg, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil SelectStmt")
	}
	bc.b = VM.NewBytecodeBuilder()

	if stmt.From == nil {
		return bc.compileSelectNoFrom(stmt)
	}
	return bc.compileSelectFromTable(stmt)
}

// CompileInsert compiles an INSERT statement.
// v0.10.0: returns an error (not supported in bytecode path yet).
func (bc *BytecodeCompiler) CompileInsert(_ *QP.InsertStmt) (*VM.BytecodeProg, error) {
	return nil, fmt.Errorf("INSERT not supported in bytecode path (v0.10.0)")
}

// CompileUpdate compiles an UPDATE statement.
func (bc *BytecodeCompiler) CompileUpdate(_ *QP.UpdateStmt) (*VM.BytecodeProg, error) {
	return nil, fmt.Errorf("UPDATE not supported in bytecode path (v0.10.0)")
}

// CompileDelete compiles a DELETE statement.
func (bc *BytecodeCompiler) CompileDelete(_ *QP.DeleteStmt) (*VM.BytecodeProg, error) {
	return nil, fmt.Errorf("DELETE not supported in bytecode path (v0.10.0)")
}

// compileSelectNoFrom handles SELECT without FROM (e.g. SELECT 1+1, 'hi').
func (bc *BytecodeCompiler) compileSelectNoFrom(stmt *QP.SelectStmt) (*VM.BytecodeProg, error) {
	cols := stmt.Columns
	if len(cols) == 0 {
		return nil, fmt.Errorf("bytecode compiler: SELECT with no columns")
	}
	if stmt.SetOp != "" {
		return nil, fmt.Errorf("bytecode compiler: set operations (UNION/INTERSECT/EXCEPT) not supported yet")
	}
	colNames := make([]string, len(cols))
	regs := make([]int32, len(cols))

	for i, col := range cols {
		reg, err := bcCompileExpr(bc.b, col, nil, nil)
		if err != nil {
			return nil, err
		}
		regs[i] = reg
		colNames[i] = bcExprName(col, i)
	}
	bc.b.SetColNames(colNames)
	bc.b.EmitAB(VM.BcResultRow, regs[0], int32(len(regs)))
	bc.b.Emit(VM.BcHalt)
	return bc.b.Build(), nil
}

// compileSelectFromTable handles SELECT ... FROM table [WHERE ...].
func (bc *BytecodeCompiler) compileSelectFromTable(stmt *QP.SelectStmt) (*VM.BytecodeProg, error) {
	if stmt.From == nil {
		return nil, fmt.Errorf("expected FROM clause")
	}
	if stmt.From.Subquery != nil || stmt.From.Join != nil || stmt.From.TableFunc != nil {
		return nil, fmt.Errorf("bytecode compiler: JOINs / subqueries / table-funcs not supported yet")
	}
	if stmt.GroupBy != nil || stmt.SetOp != "" {
		return nil, fmt.Errorf("bytecode compiler: GROUP BY / set ops not supported yet")
	}
	if stmt.OrderBy != nil || stmt.Limit != nil {
		return nil, fmt.Errorf("bytecode compiler: ORDER BY / LIMIT not supported yet")
	}
	if stmt.Distinct || stmt.Having != nil {
		return nil, fmt.Errorf("bytecode compiler: DISTINCT / HAVING not supported yet")
	}

	// Reject if any projected column uses an aggregate function.
	for _, col := range stmt.Columns {
		if containsAggregateFunc(col) {
			return nil, fmt.Errorf("bytecode compiler: aggregate functions not supported yet")
		}
	}
	// Reject if WHERE contains a subquery expression.
	if stmt.Where != nil && containsSubquery(stmt.Where) {
		return nil, fmt.Errorf("bytecode compiler: subquery in WHERE not supported yet")
	}

	tableName := stmt.From.Name
	tableAlias := stmt.From.Alias
	if tableAlias == "" {
		tableAlias = tableName
	}
	colOrder := bc.TableColOrder[tableName]

	// Expand SELECT * into concrete column list.
	cols := expandStarCols(stmt.Columns, tableName, colOrder)

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = bcExprName(col, i)
	}
	bc.b.SetColNames(colNames)

	const cursorID = 0
	tblConst := bc.b.AddConst(VM.VmText(tableName))
	lblEnd := bc.b.AllocLabel()

	// OpenCursor, Rewind
	bc.b.EmitAB(VM.BcOpenCursor, cursorID, tblConst)
	bc.b.EmitJump(VM.BcRewind, cursorID, lblEnd)

	loopStart := bc.b.PC()

	// WHERE filter
	if stmt.Where != nil {
		colIdxMap := buildColIdxMap(colOrder)
		cursorIDVal := cursorID
		filterReg, err := bcCompileExpr(bc.b, stmt.Where, colIdxMap, &cursorIDVal)
		if err != nil {
			return nil, err
		}
		// JumpFalse → skip this row back to Next
		skipLbl := bc.b.AllocLabel()
		bc.b.EmitJump(VM.BcJumpFalse, filterReg, skipLbl)

		// Project columns
		if err := bc.emitProjection(cols, colOrder, cursorID); err != nil {
			return nil, err
		}

		bc.b.FixupLabel(skipLbl)
	} else {
		// Project columns (no filter)
		if err := bc.emitProjection(cols, colOrder, cursorID); err != nil {
			return nil, err
		}
	}

	// BcNext: loop back to loopStart while rows remain, fall through when done
	bc.b.EmitABC(VM.BcNext, cursorID, 0, int32(loopStart))

	bc.b.FixupLabel(lblEnd)
	bc.b.Emit(VM.BcHalt)
	return bc.b.Build(), nil
}

// emitProjection emits BcColumn instructions for each projected column and a BcResultRow.
func (bc *BytecodeCompiler) emitProjection(cols []QP.Expr, colOrder []string, cursorID int) error {
	colIdxMap := buildColIdxMap(colOrder)
	regs := make([]int32, len(cols))
	startReg := bc.b.AllocReg()
	regs[0] = startReg
	for i := 1; i < len(cols); i++ {
		regs[i] = bc.b.AllocReg()
	}

	for i, col := range cols {
		switch e := col.(type) {
		case *QP.ColumnRef:
			name := strings.ToLower(e.Name)
			if name == "*" {
				// Star should have been expanded before reaching here
				return fmt.Errorf("unexpected * in projection")
			}
			colIdx, ok := colIdxMap[name]
			if !ok {
				// Try case-insensitive fallback
				for k, v := range colIdxMap {
					if strings.EqualFold(k, name) {
						colIdx = v
						ok = true
						break
					}
				}
			}
			if !ok {
				colIdx = 0 // fallback
			}
			bc.b.EmitABC(VM.BcColumn, int32(cursorID), int32(colIdx), regs[i])
		case *QP.AliasExpr:
			// Recurse on the inner expression
			reg, err := bcCompileExpr(bc.b, e.Expr, colIdxMap, &cursorID)
			if err != nil {
				return err
			}
			if reg != regs[i] {
				bc.b.EmitABC(VM.BcLoadReg, reg, 0, regs[i])
			}
		default:
			reg, err := bcCompileExpr(bc.b, col, colIdxMap, &cursorID)
			if err != nil {
				return err
			}
			if reg != regs[i] {
				bc.b.EmitABC(VM.BcLoadReg, reg, 0, regs[i])
			}
		}
	}
	bc.b.EmitAB(VM.BcResultRow, startReg, int32(len(cols)))
	return nil
}

// expandStarCols replaces a single ColumnRef{Name:"*"} with concrete column refs.
func expandStarCols(cols []QP.Expr, _ string, colOrder []string) []QP.Expr {
	out := make([]QP.Expr, 0, len(cols))
	for _, c := range cols {
		if ref, ok := c.(*QP.ColumnRef); ok && ref.Name == "*" {
			for _, col := range colOrder {
				out = append(out, &QP.ColumnRef{Name: col})
			}
		} else {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		// no colOrder available, keep *
		return cols
	}
	return out
}

// buildColIdxMap builds column-name → 0-based-index map from ordered column list.
func buildColIdxMap(colOrder []string) map[string]int {
	m := make(map[string]int, len(colOrder))
	for i, c := range colOrder {
		m[strings.ToLower(c)] = i
	}
	return m
}

// bcExprName returns a display name for a column expression.
func bcExprName(expr QP.Expr, idx int) string {
	switch e := expr.(type) {
	case *QP.ColumnRef:
		return e.Name
	case *QP.AliasExpr:
		return e.Alias
	case *QP.Literal:
		return fmt.Sprintf("%v", e.Value)
	}
	return fmt.Sprintf("col%d", idx)
}

// aggregateFuncs is the set of SQL aggregate function names.
var aggregateFuncs = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"group_concat": true, "total": true,
}

// containsAggregateFunc reports whether expr contains any aggregate FuncCall.
func containsAggregateFunc(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.FuncCall:
		if aggregateFuncs[strings.ToLower(e.Name)] {
			return true
		}
		for _, a := range e.Args {
			if containsAggregateFunc(a) {
				return true
			}
		}
	case *QP.BinaryExpr:
		return containsAggregateFunc(e.Left) || containsAggregateFunc(e.Right)
	case *QP.UnaryExpr:
		return containsAggregateFunc(e.Expr)
	case *QP.AliasExpr:
		return containsAggregateFunc(e.Expr)
	case *QP.CaseExpr:
		for _, w := range e.Whens {
			if containsAggregateFunc(w.Condition) || containsAggregateFunc(w.Result) {
				return true
			}
		}
		return containsAggregateFunc(e.Else)
	case *QP.CastExpr:
		return containsAggregateFunc(e.Expr)
	}
	return false
}

// containsSubquery reports whether expr contains any SubqueryExpr node.
func containsSubquery(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.SubqueryExpr:
		return e != nil
	case *QP.BinaryExpr:
		return containsSubquery(e.Left) || containsSubquery(e.Right)
	case *QP.UnaryExpr:
		return containsSubquery(e.Expr)
	case *QP.AliasExpr:
		return containsSubquery(e.Expr)
	case *QP.FuncCall:
		for _, a := range e.Args {
			if containsSubquery(a) {
				return true
			}
		}
	case *QP.CaseExpr:
		for _, w := range e.Whens {
			if containsSubquery(w.Condition) || containsSubquery(w.Result) {
				return true
			}
		}
		return containsSubquery(e.Else)
	}
	return false
}
