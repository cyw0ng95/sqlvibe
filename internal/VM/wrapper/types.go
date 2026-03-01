// Package wrapper provides Phase 4 invoke chain implementations.
package wrapper

// CompareOp represents a SQL comparison operator.
type CompareOp int

const (
	CmpEQ CompareOp = iota // =
	CmpNE                  // !=
	CmpLT                  // <
	CmpLE                  // <=
	CmpGT                  // >
	CmpGE                  // >=
)

// ArithOp represents an arithmetic operator for batch evaluation.
type ArithOp int

const (
	ArithAdd ArithOp = iota // a + b
	ArithSub                // a - b
	ArithMul                // a * b
)

// AggOp represents an aggregation operation.
type AggOp int

const (
	AggSum   AggOp = iota // SUM
	AggMin                // MIN
	AggMax                // MAX
	AggCount              // COUNT
)
