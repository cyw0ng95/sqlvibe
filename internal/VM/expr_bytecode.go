package VM

// ExprOp is a single expression bytecode operation.
type ExprOp uint8

const (
	EOpNop ExprOp = iota
	EOpLoadColumn
	EOpLoadConst
	EOpLoadParam
	EOpAdd
	EOpSub
	EOpMul
	EOpDiv
	EOpMod
	EOpEq
	EOpNe
	EOpLt
	EOpLe
	EOpGt
	EOpGe
	EOpAnd
	EOpOr
	EOpNot
	EOpNeg // unary negation: negate top of stack
	EOpFunc1
	EOpFunc2
)

// ExprBytecode is a compact bytecode representation of a SQL expression.
// It can be evaluated against a row without building a full AST.
type ExprBytecode struct {
	ops    []ExprOp
	args   []int16
	consts []interface{}
}

// NewExprBytecode creates an empty ExprBytecode.
func NewExprBytecode() *ExprBytecode {
	return &ExprBytecode{
		ops:    make([]ExprOp, 0, 16),
		args:   make([]int16, 0, 32),
		consts: make([]interface{}, 0, 8),
	}
}

// Emit appends an operation with its arguments.
func (eb *ExprBytecode) Emit(op ExprOp, args ...int16) {
	eb.ops = append(eb.ops, op)
	eb.args = append(eb.args, args...)
}

// AddConst stores a constant and returns its index.
func (eb *ExprBytecode) AddConst(v interface{}) int16 {
	eb.consts = append(eb.consts, v)
	return int16(len(eb.consts) - 1)
}

// Ops returns the operation slice (for inspection/benchmarking).
func (eb *ExprBytecode) Ops() []ExprOp { return eb.ops }
