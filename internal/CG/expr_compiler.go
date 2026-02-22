package CG

import (
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// ExprCompiler compiles QP expression AST nodes into ExprBytecode.
type ExprCompiler struct {
	bytecode   *VM.ExprBytecode
	colIndices map[string]int // column name -> index in row
}

// CompileExpr compiles an expression into ExprBytecode.
// colIndices maps column names to their positions in a row.
func CompileExpr(expr QP.Expr, colIndices map[string]int) *VM.ExprBytecode {
	ec := &ExprCompiler{
		bytecode:   VM.NewExprBytecode(),
		colIndices: colIndices,
	}
	ec.compile(expr)
	return ec.bytecode
}

func (ec *ExprCompiler) compile(expr QP.Expr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		idx, ok := ec.colIndices[e.Name]
		if !ok {
			idx = -1
		}
		ec.bytecode.Emit(VM.EOpLoadColumn, int16(idx))

	case *QP.Literal:
		idx := ec.bytecode.AddConst(e.Value)
		ec.bytecode.Emit(VM.EOpLoadConst, idx)

	case *QP.BinaryExpr:
		ec.compile(e.Left)
		ec.compile(e.Right)
		switch e.Op {
		case QP.TokenPlus:
			ec.bytecode.Emit(VM.EOpAdd)
		case QP.TokenMinus:
			ec.bytecode.Emit(VM.EOpSub)
		case QP.TokenAsterisk:
			ec.bytecode.Emit(VM.EOpMul)
		case QP.TokenSlash:
			ec.bytecode.Emit(VM.EOpDiv)
		case QP.TokenPercent:
			ec.bytecode.Emit(VM.EOpMod)
		case QP.TokenEq:
			ec.bytecode.Emit(VM.EOpEq)
		case QP.TokenNe:
			ec.bytecode.Emit(VM.EOpNe)
		case QP.TokenLt:
			ec.bytecode.Emit(VM.EOpLt)
		case QP.TokenLe:
			ec.bytecode.Emit(VM.EOpLe)
		case QP.TokenGt:
			ec.bytecode.Emit(VM.EOpGt)
		case QP.TokenGe:
			ec.bytecode.Emit(VM.EOpGe)
		case QP.TokenAnd:
			ec.bytecode.Emit(VM.EOpAnd)
		case QP.TokenOr:
			ec.bytecode.Emit(VM.EOpOr)
		default:
			idx := ec.bytecode.AddConst(nil)
			ec.bytecode.Emit(VM.EOpLoadConst, idx)
		}

	case *QP.UnaryExpr:
		ec.compile(e.Expr)
		if e.Op == QP.TokenNot {
			ec.bytecode.Emit(VM.EOpNot)
		}
		// TokenMinus negation passes through the inner expression value unchanged.


	case *QP.AliasExpr:
		ec.compile(e.Expr)

	default:
		idx := ec.bytecode.AddConst(nil)
		ec.bytecode.Emit(VM.EOpLoadConst, idx)
	}
}
