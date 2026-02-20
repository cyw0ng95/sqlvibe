package CG

import (
	"fmt"
	"strings"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

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

	case *QP.AliasExpr:
		// Alias expression: just compile the inner expression (alias is handled by column naming)
		return c.compileExpr(e.Expr)

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
	colIdx := -1
	cursorID := 0

	if col.Table != "" && c.tableCursors != nil {
		if cid, ok := c.tableCursors[col.Table]; ok {
			cursorID = cid
		}
	}

	if col.Name != "" {
		if col.Table != "" && c.TableSchemas != nil {
			if tableSchema, ok := c.TableSchemas[col.Table]; ok {
				if idx, ok := tableSchema[col.Name]; ok {
					colIdx = idx
				}
			}
		}

		if colIdx == -1 && col.Table != "" && c.tableCursors != nil && c.TableColIndices != nil {
			if _, tableExists := c.tableCursors[col.Table]; tableExists {
				if idx, ok := c.TableColIndices[col.Name]; ok {
					colIdx = idx
				}
			}
		}

		if colIdx == -1 && c.TableColIndices != nil {
			if idx, ok := c.TableColIndices[col.Name]; ok {
				colIdx = idx
			}
		}

		if colIdx == -1 && c.columnIndices != nil {
			if idx, ok := c.columnIndices[col.Name]; ok {
				colIdx = idx
			}
		}
	}

	if colIdx == -1 && col.Table != "" {
		qualifiedName := col.Table + "." + col.Name
		c.program.EmitColumnWithTable(reg, cursorID, -1, qualifiedName)
		return reg
	}

	if colIdx == -1 {
		c.program.EmitLoadConst(reg, nil)
		return reg
	}

	c.program.EmitColumnWithTable(reg, cursorID, colIdx, col.Table)
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
		c.program.EmitOpWithDst(VM.OpRemainder, int32(leftReg), int32(rightReg), dst)
	case QP.TokenEq:
		c.program.EmitOpWithDst(VM.OpEq, int32(leftReg), int32(rightReg), dst)
	case QP.TokenNe:
		c.program.EmitOpWithDst(VM.OpNe, int32(leftReg), int32(rightReg), dst)
	case QP.TokenLt:
		c.program.EmitOpWithDst(VM.OpLt, int32(leftReg), int32(rightReg), dst)
	case QP.TokenLe:
		c.program.EmitOpWithDst(VM.OpLe, int32(leftReg), int32(rightReg), dst)
	case QP.TokenGt:
		c.program.EmitOpWithDst(VM.OpGt, int32(leftReg), int32(rightReg), dst)
	case QP.TokenGe:
		c.program.EmitOpWithDst(VM.OpGe, int32(leftReg), int32(rightReg), dst)
	case QP.TokenAnd:
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))

		leftCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(VM.OpNe, int32(leftReg), int32(zeroReg), leftCheck)

		rightCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(VM.OpNe, int32(rightReg), int32(zeroReg), rightCheck)

		c.program.EmitOpWithDst(VM.OpBitAnd, int32(leftCheck), int32(rightCheck), dst)
		return dst
	case QP.TokenOr:
		zeroReg := c.ra.Alloc()
		c.program.EmitLoadConst(zeroReg, int64(0))

		leftCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(VM.OpNe, int32(leftReg), int32(zeroReg), leftCheck)

		rightCheck := c.ra.Alloc()
		c.program.EmitOpWithDst(VM.OpNe, int32(rightReg), int32(zeroReg), rightCheck)

		c.program.EmitOpWithDst(VM.OpBitOr, int32(leftCheck), int32(rightCheck), dst)
		return dst
	case QP.TokenLike:
		c.program.EmitOpWithDst(VM.OpLike, int32(leftReg), int32(rightReg), dst)
		return dst
	case QP.TokenGlob:
		c.program.EmitOpWithDst(VM.OpGlob, int32(leftReg), int32(rightReg), dst)
		return dst
	case QP.TokenNotLike:
		likeResult := c.ra.Alloc()
		c.program.EmitOpWithDst(VM.OpLike, int32(leftReg), int32(rightReg), likeResult)
		oneReg := c.ra.Alloc()
		c.program.EmitLoadConst(oneReg, int64(1))
		c.program.EmitOpWithDst(VM.OpSubtract, int32(oneReg), int32(likeResult), dst)
		return dst
	case QP.TokenBetween:
		if binExpr, ok := expr.Right.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenAnd {
			lowerReg := c.compileExpr(binExpr.Left)
			upperReg := c.compileExpr(binExpr.Right)

			geResult := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpGe, int32(leftReg), int32(lowerReg), geResult)

			leResult := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpLe, int32(leftReg), int32(upperReg), leResult)

			c.program.EmitOpWithDst(VM.OpBitAnd, int32(geResult), int32(leResult), dst)
			return dst
		}
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenNotBetween:
		if binExpr, ok := expr.Right.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenAnd {
			lowerReg := c.compileExpr(binExpr.Left)
			upperReg := c.compileExpr(binExpr.Right)

			geResult := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpGe, int32(leftReg), int32(lowerReg), geResult)

			leResult := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpLe, int32(leftReg), int32(upperReg), leResult)

			betweenResult := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpBitAnd, int32(geResult), int32(leResult), betweenResult)

			zeroReg := c.ra.Alloc()
			c.program.EmitLoadConst(zeroReg, int64(0))
			c.program.EmitOpWithDst(VM.OpEq, int32(betweenResult), int32(zeroReg), dst)
			return dst
		}
		c.program.EmitLoadConst(dst, int64(1))
		return dst
	case QP.TokenIn:
		if lit, ok := expr.Right.(*QP.Literal); ok {
			if values, ok := lit.Value.([]interface{}); ok {
				c.program.EmitLoadConst(dst, int64(0))

				for _, val := range values {
					if val == nil {
						// NULL in IN list: skip (NULL never makes IN result TRUE)
						continue
					}
					valReg := c.ra.Alloc()
					c.program.EmitLoadConst(valReg, val)

					eqResult := c.ra.Alloc()
					c.program.EmitOpWithDst(VM.OpEq, int32(leftReg), int32(valReg), eqResult)

					c.program.EmitOpWithDst(VM.OpBitOr, int32(dst), int32(eqResult), dst)
				}
				return dst
			}
		}
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenNotIn:
		if subqExpr, ok := expr.Right.(*QP.SubqueryExpr); ok {
			c.program.Instructions = append(c.program.Instructions, VM.Instruction{
				Op: VM.OpNotInSubquery,
				P1: int32(dst),
				P2: int32(leftReg),
				P4: subqExpr.Select,
			})
			return dst
		}
		if lit, ok := expr.Right.(*QP.Literal); ok {
			if values, ok := lit.Value.([]interface{}); ok {
				inResult := c.ra.Alloc()
				c.program.EmitLoadConst(inResult, int64(0))

				for _, val := range values {
					valReg := c.ra.Alloc()
					c.program.EmitLoadConst(valReg, val)

					eqResult := c.ra.Alloc()
					c.program.EmitOpWithDst(VM.OpEq, int32(leftReg), int32(valReg), eqResult)

					c.program.EmitOpWithDst(VM.OpBitOr, int32(inResult), int32(eqResult), inResult)
				}

				zeroReg := c.ra.Alloc()
				c.program.EmitLoadConst(zeroReg, int64(0))
				c.program.EmitOpWithDst(VM.OpEq, int32(inResult), int32(zeroReg), dst)
				return dst
			}
		}
		c.program.EmitLoadConst(dst, int64(1))
		return dst
	case QP.TokenInSubquery:
		if subqExpr, ok := expr.Right.(*QP.SubqueryExpr); ok {
			c.program.Instructions = append(c.program.Instructions, VM.Instruction{
				Op: VM.OpInSubquery,
				P1: int32(dst),
				P2: int32(leftReg),
				P4: subqExpr.Select,
			})
			return dst
		}
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenExists:
		if subqExpr, ok := expr.Left.(*QP.SubqueryExpr); ok {
			c.program.Instructions = append(c.program.Instructions, VM.Instruction{
				Op: VM.OpExistsSubquery,
				P1: int32(dst),
				P4: subqExpr.Select,
			})
			return dst
		}
		c.program.EmitLoadConst(dst, int64(0))
		return dst
	case QP.TokenIs:
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOpWithDst(VM.OpIs, int32(leftReg), int32(nullReg), dst)
		return dst
	case QP.TokenIsNot:
		nullReg := c.ra.Alloc()
		c.program.EmitLoadConst(nullReg, nil)
		c.program.EmitOpWithDst(VM.OpIsNot, int32(leftReg), int32(nullReg), dst)
		return dst
	default:
		c.program.EmitLoadConst(dst, nil)
	}

	return dst
}

func (c *Compiler) compileUnaryExpr(expr *QP.UnaryExpr) int {
	dst := c.ra.Alloc()

	if expr.Op == QP.TokenNot {
		if binExpr, ok := expr.Expr.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenExists {
			if subqExpr, ok := binExpr.Left.(*QP.SubqueryExpr); ok {
				c.program.Instructions = append(c.program.Instructions, VM.Instruction{
					Op: VM.OpNotExistsSubquery,
					P1: int32(dst),
					P4: subqExpr.Select,
				})
				return dst
			}
		}
	}

	srcReg := c.compileExpr(expr.Expr)

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

	switch strings.ToUpper(call.Name) {
	case "ABS":
		c.program.EmitOpWithDst(VM.OpAbs, int32(argRegs[0]), 0, dst)
	case "UPPER":
		c.program.EmitOpWithDst(VM.OpUpper, int32(argRegs[0]), 0, dst)
	case "LOWER":
		c.program.EmitOpWithDst(VM.OpLower, int32(argRegs[0]), 0, dst)
	case "LENGTH":
		c.program.EmitOpWithDst(VM.OpLength, int32(argRegs[0]), 0, dst)
	case "SUBSTR", "SUBSTRING":
		if len(argRegs) >= 3 {
			idx := c.program.EmitOp(VM.OpSubstr, int32(argRegs[0]), int32(argRegs[1]))
			c.program.Instructions[idx].P3 = fmt.Sprintf("len:%d", argRegs[2])
			c.program.Instructions[idx].P4 = dst
		} else if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpSubstr, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(VM.OpSubstr, int32(argRegs[0]), 0, dst)
		}
	case "TRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(VM.OpTrim, int32(argRegs[0]), 0, dst)
		}
	case "LTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpLTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(VM.OpLTrim, int32(argRegs[0]), 0, dst)
		}
	case "RTRIM":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpRTrim, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitOpWithDst(VM.OpRTrim, int32(argRegs[0]), 0, dst)
		}
	case "COALESCE":
		if len(argRegs) >= 2 {
			checkReg := c.ra.Alloc()
			c.program.EmitCopy(argRegs[0], checkReg)
			fallbackReg := argRegs[1]
			c.program.EmitOpWithDst(VM.OpIfNull2, int32(checkReg), int32(fallbackReg), dst)
			return dst
		}
		c.program.EmitCopy(argRegs[0], dst)
		return dst
	case "IFNULL":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpIfNull2, int32(argRegs[0]), int32(argRegs[1]), dst)
		} else {
			c.program.EmitCopy(argRegs[0], dst)
		}
		return dst
	case "NULLIF":
		// NULLIF(a, b) = NULL if a == b, else a
		if len(argRegs) >= 2 {
			cmpReg := c.ra.Alloc()
			c.program.EmitOpWithDst(VM.OpEq, int32(argRegs[0]), int32(argRegs[1]), cmpReg)

			// If cmpReg is 0 or NULL (values are not equal), jump to copy section
			notEqJump := c.program.EmitOp(VM.OpIfNot, int32(cmpReg), 0)

			// Equal: result is NULL
			c.program.EmitLoadConst(dst, nil)

			// Jump to end
			endJump := c.program.EmitOp(VM.OpGoto, 0, 0)

			// Not equal: copy first arg
			c.program.Fixup(notEqJump)
			c.program.EmitCopy(argRegs[0], dst)

			// End
			c.program.Fixup(endJump)
		} else {
			c.program.EmitLoadConst(dst, nil)
		}
		return dst
	case "INSTR":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpInstr, int32(argRegs[0]), int32(argRegs[1]), dst)
		}
	case "REPLACE":
		if len(argRegs) >= 3 {
			c.program.EmitCopy(argRegs[0], dst)
			c.program.EmitOpWithDst(VM.OpReplace, int32(argRegs[1]), int32(argRegs[2]), dst)
		}
	case "ROUND":
		if len(argRegs) >= 1 {
			c.program.EmitOpWithDst(VM.OpRound, int32(argRegs[0]), 0, dst)
		}
	case "CEIL", "CEILING":
		c.program.EmitOpWithDst(VM.OpCeil, int32(argRegs[0]), 0, dst)
	case "FLOOR":
		c.program.EmitOpWithDst(VM.OpFloor, int32(argRegs[0]), 0, dst)
	case "SQRT":
		c.program.EmitOpWithDst(VM.OpSqrt, int32(argRegs[0]), 0, dst)
	case "POWER", "POW":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpPow, int32(argRegs[0]), int32(argRegs[1]), dst)
		}
	case "EXP":
		c.program.EmitOpWithDst(VM.OpExp, int32(argRegs[0]), 0, dst)
	case "LOG", "LOG10":
		c.program.EmitOpWithDst(VM.OpLog, int32(argRegs[0]), 0, dst)
	case "LN":
		c.program.EmitOpWithDst(VM.OpLn, int32(argRegs[0]), 0, dst)
	case "SIN":
		c.program.EmitOpWithDst(VM.OpSin, int32(argRegs[0]), 0, dst)
	case "COS":
		c.program.EmitOpWithDst(VM.OpCos, int32(argRegs[0]), 0, dst)
	case "TAN":
		c.program.EmitOpWithDst(VM.OpTan, int32(argRegs[0]), 0, dst)
	case "ASIN":
		c.program.EmitOpWithDst(VM.OpAsin, int32(argRegs[0]), 0, dst)
	case "ACOS":
		c.program.EmitOpWithDst(VM.OpAcos, int32(argRegs[0]), 0, dst)
	case "ATAN":
		c.program.EmitOpWithDst(VM.OpAtan, int32(argRegs[0]), 0, dst)
	case "ATAN2":
		if len(argRegs) >= 2 {
			c.program.EmitOpWithDst(VM.OpAtan2, int32(argRegs[0]), int32(argRegs[1]), dst)
		}
	default:
		c.program.EmitLoadConst(dst, nil)
		return dst
	}

	return dst
}

func (c *Compiler) compileCaseExpr(caseExpr *QP.CaseExpr) int {
	resultReg := c.ra.Alloc()

	endJumps := make([]int, 0)

	var operandReg int
	isSimpleCase := caseExpr.Operand != nil
	if isSimpleCase {
		operandReg = c.compileExpr(caseExpr.Operand)
	}

	for _, when := range caseExpr.Whens {
		var condReg int

		if isSimpleCase {
			whenValueReg := c.compileExpr(when.Condition)
			condReg = c.ra.Alloc()
			eqIdx := c.program.EmitOp(VM.OpEq, int32(operandReg), int32(whenValueReg))
			c.program.Instructions[eqIdx].P4 = condReg
		} else {
			condReg = c.compileExpr(when.Condition)
		}

		skipIdx := c.program.EmitOp(VM.OpIfNot, int32(condReg), 0)

		thenReg := c.compileExpr(when.Result)
		c.program.EmitCopy(thenReg, resultReg)

		jumpToEnd := c.program.EmitOp(VM.OpGoto, 0, 0)
		endJumps = append(endJumps, jumpToEnd)

		c.program.Fixup(skipIdx)
	}

	if caseExpr.Else != nil {
		elseReg := c.compileExpr(caseExpr.Else)
		c.program.EmitCopy(elseReg, resultReg)
	} else {
		c.program.EmitLoadConst(resultReg, nil)
	}

	for _, jumpIdx := range endJumps {
		c.program.Fixup(jumpIdx)
	}

	return resultReg
}

func (c *Compiler) compileCastExpr(cast *QP.CastExpr) int {
	srcReg := c.compileExpr(cast.Expr)
	dst := c.ra.Alloc()

	c.program.EmitOp(VM.OpCast, int32(srcReg), 0)
	c.program.Instructions[len(c.program.Instructions)-1].P4 = cast.TypeSpec
	c.program.EmitCopy(srcReg, dst)

	return dst
}

func (c *Compiler) compileSubqueryExpr(subq *QP.SubqueryExpr) int {
	resultReg := c.ra.Alloc()

	c.program.Instructions = append(c.program.Instructions, VM.Instruction{
		Op: VM.OpScalarSubquery,
		P1: int32(resultReg),
		P4: subq.Select,
	})

	return resultReg
}

func (c *Compiler) expandStarColumns(columns []QP.Expr) []QP.Expr {
	if columns == nil {
		return nil
	}

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

	expanded := make([]QP.Expr, 0)

	for _, col := range columns {
		colRef, isColRef := col.(*QP.ColumnRef)
		if !isColRef || colRef.Name != "*" {
			expanded = append(expanded, col)
			continue
		}

		starTable := colRef.Table

		if starTable != "" && c.TableSchemas != nil {
			if tableSchema, ok := c.TableSchemas[starTable]; ok {
				type colInfo struct {
					name string
					idx  int
				}
				cols := make([]colInfo, 0, len(tableSchema))
				for colName, idx := range tableSchema {
					cols = append(cols, colInfo{name: colName, idx: idx})
				}
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

		if c.TableSchemas != nil && len(c.TableSchemas) > 0 && c.TableColOrder != nil && len(c.TableColOrder) > 0 {
			for i, colName := range c.TableColOrder {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				var tableName string
				// Use positional source info if available (deterministic)
				if c.TableColSources != nil && i < len(c.TableColSources) {
					tableName = c.TableColSources[i]
				} else {
					// Fallback: iterate schemas (may be non-deterministic for shared cols)
					for tbl, schema := range c.TableSchemas {
						if _, ok := schema[colName]; ok {
							tableName = tbl
							break
						}
					}
				}
				expanded = append(expanded, &QP.ColumnRef{
					Name:  colName,
					Table: tableName,
				})
			}
			continue
		}

		if c.TableColOrder != nil && len(c.TableColOrder) > 0 {
			for _, colName := range c.TableColOrder {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				expanded = append(expanded, &QP.ColumnRef{Name: colName})
			}
		} else if c.TableColIndices != nil {
			for colName := range c.TableColIndices {
				if colName == "" || strings.HasPrefix(colName, "__") {
					continue
				}
				expanded = append(expanded, &QP.ColumnRef{Name: colName})
			}
		}
	}

	if len(expanded) == 0 {
		return columns
	}

	return expanded
}
