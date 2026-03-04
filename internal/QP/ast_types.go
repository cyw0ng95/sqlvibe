// Package QP - minimal AST types for C++ wrapper compatibility (profiling extension)
package QP

// ExplainStmt represents an EXPLAIN statement.
type ExplainStmt struct {
	SQL   string
	Query ASTNode
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Columns []Expr
	From    *TableRef
	Where   Expr
	GroupBy []Expr
	OrderBy []*OrderBy
}

// NodeType returns the node type for SelectStmt.
func (s *SelectStmt) NodeType() string { return "SelectStmt" }

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	Expr Expr
	Desc bool
}

// ASTNode is the base interface for all AST nodes.
type ASTNode interface {
	NodeType() string
}

// Expr is the base interface for all expressions.
type Expr interface {
	ASTNode
}

// TableRef represents a table reference.
type TableRef struct {
	Name     string
	Alias    string
	Schema   string
	IsCTE    bool
	Subquery *SelectStmt
	Join     *JoinClause
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Left  *TableRef
	Right *TableRef
	Type  string
	On    Expr
}

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Op  TokenType
	Left  Expr
	Right Expr
}

func (b *BinaryExpr) NodeType() string { return "BinaryExpr" }

// ColumnRef represents a column reference.
type ColumnRef struct {
	Table string
	Column string
	Name   string // Alias for Column
}

func (c *ColumnRef) NodeType() string { return "ColumnRef" }

// Literal represents a literal value.
type Literal struct {
	Value interface{}
}

func (l *Literal) NodeType() string { return "Literal" }
