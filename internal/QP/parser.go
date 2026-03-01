package QP

import (
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

type ASTNode interface {
	NodeType() string
}

type SelectStmt struct {
	Distinct   bool
	Columns    []Expr
	From       *TableRef
	Where      Expr
	GroupBy    []Expr
	Having     Expr
	OrderBy    []OrderBy
	Limit      Expr
	Offset     Expr
	SetOp      string
	SetOpAll   bool
	SetOpRight *SelectStmt
	CTEs       []CTEClause     // WITH ... AS (...) clauses
	IntoTable  string          // non-empty for SELECT ... INTO tablename
	Windows    []WindowDef     // WINDOW clause: name AS (window_spec)
}

// WindowDef represents a named window definition: name AS (window_spec)
type WindowDef struct {
	Name      string
	Partition []Expr
	OrderBy   []WindowOrderBy
	Frame     *WindowFrame
}

// CTEClause represents a single CTE definition: name AS (SELECT ...)
type CTEClause struct {
	Name      string
	Select    *SelectStmt
	Recursive bool
	Columns   []string
}

func (s *SelectStmt) NodeType() string { return "SelectStmt" }

type OrderBy struct {
	Expr  Expr
	Desc  bool
	Nulls string // "FIRST", "LAST", or ""
}

type TableRef struct {
	Schema        string
	Name          string
	Alias         string
	Join          *Join
	Subquery      *SelectStmt // derived table: FROM (SELECT ...) AS alias
	Values        [][]Expr    // non-nil for VALUES table constructor
	ValueCols     []string    // column names from AS t(col1, col2)
	TableFunc     *FuncCall   // non-nil for table-valued function: FROM json_each(...)
	TableFuncCols []string    // column names from AS t(col1, col2) for table func
}

func (t *TableRef) NodeType() string { return "TableRef" }

type Join struct {
	Type         string
	Left         *TableRef
	Right        *TableRef
	Cond         Expr
	UsingColumns []string // for JOIN ... USING (col1, col2)
	Natural      bool     // for NATURAL JOIN
}

// OnConflict represents the ON CONFLICT clause of an INSERT statement.
type OnConflict struct {
	Columns   []string    // conflict target columns, e.g. ON CONFLICT (id)
	DoNothing bool        // ON CONFLICT DO NOTHING
	Updates   []SetClause // ON CONFLICT DO UPDATE SET col = expr, ...
}

type InsertStmt struct {
	Table       string
	Columns     []string
	Values      [][]Expr
	UseDefaults bool        // True when using DEFAULT VALUES
	SelectQuery *SelectStmt // Non-nil for INSERT ... SELECT
	OnConflict  *OnConflict // nil if no ON CONFLICT clause
	OrAction    string      // "REPLACE", "IGNORE", "ABORT", "FAIL", "ROLLBACK" or ""
	Returning   []Expr      // nil if no RETURNING clause
}

func (i *InsertStmt) NodeType() string { return "InsertStmt" }

type UpdateStmt struct {
	Table     string
	Set       []SetClause
	Where     Expr
	From      *TableRef // nil if no FROM clause (UPDATE ... FROM t2)
	Returning []Expr    // nil if no RETURNING clause
}

func (u *UpdateStmt) NodeType() string { return "UpdateStmt" }

type SetClause struct {
	Column Expr
	Value  Expr
}

type DeleteStmt struct {
	Table     string
	Where     Expr
	Using     []string // nil if no USING clause
	Returning []Expr   // nil if no RETURNING clause
}

func (d *DeleteStmt) NodeType() string { return "DeleteStmt" }

// ReferenceAction represents ON DELETE / ON UPDATE actions for FK constraints.
type ReferenceAction int

const (
	ReferenceNoAction   ReferenceAction = iota // NO ACTION (default)
	ReferenceRestrict                          // RESTRICT
	ReferenceCascade                           // CASCADE
	ReferenceSetNull                           // SET NULL
	ReferenceSetDefault                        // SET DEFAULT
)

// ForeignKeyConstraint represents a FOREIGN KEY constraint.
type ForeignKeyConstraint struct {
	ChildColumns  []string        // columns in this table
	ParentTable   string          // referenced table
	ParentColumns []string        // referenced columns
	OnDelete      ReferenceAction // ON DELETE action
	OnUpdate      ReferenceAction // ON UPDATE action
}

type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	IfNotExists bool
	Temporary   bool
	AsSelect    *SelectStmt            // CREATE TABLE ... AS SELECT
	TableChecks []Expr                 // table-level CHECK constraints
	ForeignKeys []ForeignKeyConstraint // table-level FOREIGN KEY constraints
	UniqueKeys  [][]string             // table-level UNIQUE(col, ...) constraints
}

func (c *CreateTableStmt) NodeType() string { return "CreateTableStmt" }

// CreateVirtualTableStmt represents CREATE VIRTUAL TABLE ... USING module(args).
type CreateVirtualTableStmt struct {
	TableName   string
	IfNotExists bool
	ModuleName  string
	ModuleArgs  []string
}

func (c *CreateVirtualTableStmt) NodeType() string { return "CreateVirtualTableStmt" }

type ColumnDef struct {
	Name            string
	Type            string
	PrimaryKey      bool
	NotNull         bool
	Unique          bool // UNIQUE constraint on this column
	Default         Expr
	Check           Expr                  // CHECK constraint expression
	IsAutoincrement bool                  // AUTOINCREMENT
	ForeignKey      *ForeignKeyConstraint // inline REFERENCES clause
	Collation       string                // COLLATE name (e.g., NOCASE, RTRIM, BINARY)
	// Generated columns (v0.10.6)
	GeneratedExpr   Expr   // Expression for GENERATED ALWAYS AS
	GeneratedStored bool   // true for STORED, false for VIRTUAL
}

// CreateTriggerStmt represents CREATE TRIGGER
type CreateTriggerStmt struct {
	Name        string
	TableName   string
	Time        string    // "BEFORE", "AFTER", "INSTEAD OF"
	Event       string    // "INSERT", "UPDATE", "DELETE"
	Columns     []string  // for UPDATE OF col1, col2
	When        Expr      // WHEN condition (nil if absent)
	Body        []ASTNode // trigger body statements
	IfNotExists bool
}

func (c *CreateTriggerStmt) NodeType() string { return "CreateTriggerStmt" }

// DropTriggerStmt represents DROP TRIGGER
type DropTriggerStmt struct {
	Name     string
	IfExists bool
}

func (d *DropTriggerStmt) NodeType() string { return "DropTriggerStmt" }

// CreateViewStmt represents CREATE VIEW
type CreateViewStmt struct {
	Name        string
	Select      *SelectStmt
	IfNotExists bool
}

func (c *CreateViewStmt) NodeType() string { return "CreateViewStmt" }

// DropViewStmt represents DROP VIEW

type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (d *DropViewStmt) NodeType() string { return "DropViewStmt" }

// AlterTableStmt represents ALTER TABLE
type AlterTableStmt struct {
	Table          string
	Action         string // "ADD_COLUMN", "RENAME_TO", "DROP_COLUMN", "RENAME_COLUMN", "ADD_CONSTRAINT"
	Column         *ColumnDef
	NewName        string
	ConstraintName string   // for ADD CONSTRAINT
	CheckExpr      Expr     // for ADD CONSTRAINT CHECK
	UniqueColumns  []string // for ADD CONSTRAINT UNIQUE
}

func (a *AlterTableStmt) NodeType() string { return "AlterTableStmt" }

type DropTableStmt struct {
	Name     string
	IfExists bool
}

func (d *DropTableStmt) NodeType() string { return "DropTableStmt" }

type CreateIndexStmt struct {
	Name        string
	Table       string
	Columns     []string
	Exprs       []Expr // parallel to Columns; non-nil entry means expression index on that slot
	Unique      bool
	IfNotExists bool
	WhereExpr   Expr // nil if no WHERE clause (partial index)
}

func (c *CreateIndexStmt) NodeType() string { return "CreateIndexStmt" }

type DropIndexStmt struct {
	Name string
}

func (d *DropIndexStmt) NodeType() string { return "DropIndexStmt" }

type PragmaStmt struct {
	Name  string
	Value Expr
}

func (p *PragmaStmt) NodeType() string { return "PragmaStmt" }

type ExplainStmt struct {
	QueryPlan bool
	Analyze   bool    // EXPLAIN ANALYZE - runtime statistics
	Query     ASTNode
}

func (e *ExplainStmt) NodeType() string { return "ExplainStmt" }

type BeginStmt struct {
	Type string // "DEFERRED", "IMMEDIATE", "EXCLUSIVE", or ""
}

func (b *BeginStmt) NodeType() string { return "BeginStmt" }

type CommitStmt struct {
}

func (c *CommitStmt) NodeType() string { return "CommitStmt" }

type RollbackStmt struct {
	Savepoint string // if non-empty: ROLLBACK TO [SAVEPOINT] sp_name
}

func (r *RollbackStmt) NodeType() string { return "RollbackStmt" }

type SavepointStmt struct {
	Name string
}

func (s *SavepointStmt) NodeType() string { return "SavepointStmt" }

type ReleaseSavepointStmt struct {
	Name string
}

func (r *ReleaseSavepointStmt) NodeType() string { return "ReleaseSavepointStmt" }

// BACKUP DATABASE TO 'path'
// BACKUP INCREMENTAL TO 'path'
type BackupStmt struct {
	Incremental bool   // true for BACKUP INCREMENTAL, false for BACKUP DATABASE
	DestPath    string // destination file path
}

func (b *BackupStmt) NodeType() string { return "BackupStmt" }

// VacuumStmt represents VACUUM [INTO 'path']
type VacuumStmt struct {
	DestPath string // empty = in-place vacuum
}

func (v *VacuumStmt) NodeType() string { return "VacuumStmt" }

// AnalyzeStmt represents ANALYZE [table_or_index]
type AnalyzeStmt struct {
	Target string // empty = all tables
}

func (a *AnalyzeStmt) NodeType() string { return "AnalyzeStmt" }

// ReindexStmt represents REINDEX [table_or_index]
type ReindexStmt struct {
	Target string // empty = all indexes
}

func (r *ReindexStmt) NodeType() string { return "ReindexStmt" }

type Expr interface {
	exprNode()
}

type BinaryExpr struct {
	Op    TokenType
	Left  Expr
	Right Expr
}

func (e *BinaryExpr) exprNode() {}

type UnaryExpr struct {
	Op   TokenType
	Expr Expr
}

func (e *UnaryExpr) exprNode() {}

type Literal struct {
	Value interface{}
}

func (e *Literal) exprNode() {}

type ColumnRef struct {
	Table string // Optional table/alias qualifier (e.g., "e" in "e.dept_id")
	Name  string
}

func (e *ColumnRef) exprNode() {}

type FuncCall struct {
	Name     string
	Args     []Expr
	Distinct bool // true if DISTINCT keyword was used (e.g. COUNT(DISTINCT col))
}

func (e *FuncCall) exprNode() {}

type SubqueryExpr struct {
	Select     *SelectStmt
	OuterAlias string // Alias of the outer query's table for correlation resolution
}

func (e *SubqueryExpr) exprNode() {}

type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (e *AliasExpr) exprNode() {}

// WindowOrderBy represents an ORDER BY element in a window function spec
type WindowOrderBy struct {
	Expr Expr
	Desc bool
}

// WindowFrame represents ROWS/RANGE BETWEEN frame spec
type WindowFrame struct {
	Type  string // "ROWS" or "RANGE"
	Start FrameBound
	End   FrameBound
}

// FrameBound represents a window frame boundary
type FrameBound struct {
	Type  string // "UNBOUNDED", "CURRENT", "PRECEDING", "FOLLOWING"
	Value Expr   // for N PRECEDING/FOLLOWING (nil for UNBOUNDED/CURRENT)
}

// AnyAllExpr represents expr OP ANY/ALL (subquery)
type AnyAllExpr struct {
	Left       Expr
	Op         TokenType // e.g. TokenGt, TokenLt, TokenEq
	Quantifier string    // "ANY", "SOME", "ALL"
	Subquery   *SelectStmt
}

func (e *AnyAllExpr) exprNode() {}

// WindowFuncExpr represents a window function call: func(...) OVER ([PARTITION BY ...] [ORDER BY ...])
type WindowFuncExpr struct {
	Name      string          // Function name: COUNT, SUM, AVG, LAG, LEAD, FIRST_VALUE, LAST_VALUE, ROW_NUMBER, RANK
	Args      []Expr          // Function arguments
	IsStar    bool            // COUNT(*)
	Partition []Expr          // PARTITION BY expressions
	OrderBy   []WindowOrderBy // ORDER BY expressions
	Frame     *WindowFrame    // ROWS/RANGE frame spec (optional)
}

func (e *WindowFuncExpr) exprNode() {}

type CaseExpr struct {
	Operand Expr
	Whens   []CaseWhen
	Else    Expr
}

type CaseWhen struct {
	Condition Expr
	Result    Expr
}

func (e *CaseExpr) exprNode() {}

// TypeSpec represents a SQL type with optional precision and scale
type TypeSpec struct {
	Name      string // Type name (e.g., "INTEGER", "DECIMAL", "VARCHAR")
	Precision int    // For DECIMAL(p,s) or VARCHAR(n), this is p or n
	Scale     int    // For DECIMAL(p,s), this is s
}

type CastExpr struct {
	Expr     Expr
	TypeSpec TypeSpec // Changed from Type string to TypeSpec
}

func (e *CastExpr) exprNode() {}

// CollateExpr represents expr COLLATE collation_name
type CollateExpr struct {
	Expr      Expr
	Collation string
}

func (e *CollateExpr) exprNode() {}

// PlaceholderExpr represents a query parameter: '?' (positional) or ':name'/'@name' (named).
type PlaceholderExpr struct {
	Positional bool   // true for ?, false for :name / @name
	Name       string // ":foo" or "@foo"; empty for positional
	Index      int    // 0-based positional index (filled during binding)
}

func (e *PlaceholderExpr) exprNode() {}

type Parser struct {
	tokens     []Token
	pos        int
	outerAlias string // Track outer query's table alias for subquery correlation
	parseError error  // Deferred error from table ref parsing
}

func NewParser(tokens []Token) *Parser {
	util.AssertNotNil(tokens, "tokens")
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

func (p *Parser) Parse() (ASTNode, error) {
	if p.isEOF() {
		return nil, nil
	}

	node, err := p.parseInternal()
	if err != nil {
		return nil, err
	}
	if p.parseError != nil {
		return nil, p.parseError
	}
	return node, nil
}

func (p *Parser) parseInternal() (ASTNode, error) {
	switch p.current().Type {
	case TokenKeyword:
		switch p.current().Literal {
		case "SELECT":
			return p.parseSelect()
		case "INSERT":
			return p.parseInsert()
		case "UPDATE":
			return p.parseUpdate()
		case "DELETE":
			return p.parseDelete()
		case "CREATE":
			return p.parseCreate()
		case "DROP":
			return p.parseDrop()
		case "PRAGMA":
			return p.parsePragma()
		case "EXPLAIN":
			return p.parseExplain()
		case "BEGIN":
			return p.parseBegin()
		case "COMMIT":
			return p.parseCommit()
		case "ROLLBACK":
			return p.parseRollback()
		case "SAVEPOINT":
			return p.parseSavepoint()
		case "RELEASE":
			return p.parseRelease()
		case "ALTER":
			return p.parseAlterTable()
		case "WITH":
			return p.parseWithClause()
		case "BACKUP":
			return p.parseBackup()
		case "VACUUM":
			return p.parseVacuum()
		case "ANALYZE":
			return p.parseAnalyze()
		case "REINDEX":
			return p.parseReindex()
		case "VALUES":
			return p.parseStandaloneValues()
		}
	case TokenExplain:
		return p.parseExplain()
	}
	return nil, nil
}

func (p *Parser) current() Token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return Token{Type: TokenEOF}
}

func (p *Parser) peek() Token {
	if p.pos+1 < len(p.tokens) {
		return p.tokens[p.pos+1]
	}
	return Token{Type: TokenEOF}
}

func (p *Parser) advance() Token {
	tok := p.current()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) expect(typ TokenType) (Token, error) {
	tok := p.current()
	if tok.Type != typ {
		return tok, nil
	}
	return p.advance(), nil
}

func (p *Parser) isEOF() bool {
	return p.pos >= len(p.tokens) || p.current().Type == TokenEOF
}

