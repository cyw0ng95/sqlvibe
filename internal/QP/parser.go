package QP

import (
	"fmt"
	"math"
	"strconv"
	"strings"

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
	CTEs       []CTEClause // WITH ... AS (...) clauses
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
	Schema    string
	Name      string
	Alias     string
	Join      *Join
	Subquery  *SelectStmt // derived table: FROM (SELECT ...) AS alias
	Values    [][]Expr    // non-nil for VALUES table constructor
	ValueCols []string    // column names from AS t(col1, col2)
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
}

func (i *InsertStmt) NodeType() string { return "InsertStmt" }

type UpdateStmt struct {
	Table string
	Set   []SetClause
	Where Expr
}

func (u *UpdateStmt) NodeType() string { return "UpdateStmt" }

type SetClause struct {
	Column Expr
	Value  Expr
}

type DeleteStmt struct {
	Table string
	Where Expr
}

func (d *DeleteStmt) NodeType() string { return "DeleteStmt" }

// ReferenceAction represents ON DELETE / ON UPDATE actions for FK constraints.
type ReferenceAction int

const (
	ReferenceNoAction  ReferenceAction = iota // NO ACTION (default)
	ReferenceRestrict                         // RESTRICT
	ReferenceCascade                          // CASCADE
	ReferenceSetNull                          // SET NULL
	ReferenceSetDefault                       // SET DEFAULT
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
	AsSelect    *SelectStmt           // CREATE TABLE ... AS SELECT
	TableChecks []Expr                // table-level CHECK constraints
	ForeignKeys []ForeignKeyConstraint // table-level FOREIGN KEY constraints
}

func (c *CreateTableStmt) NodeType() string { return "CreateTableStmt" }

type ColumnDef struct {
	Name          string
	Type          string
	PrimaryKey    bool
	NotNull       bool
	Default       Expr
	Check         Expr                  // CHECK constraint expression
	IsAutoincrement bool               // AUTOINCREMENT
	ForeignKey    *ForeignKeyConstraint // inline REFERENCES clause
}

// CreateTriggerStmt represents CREATE TRIGGER
type CreateTriggerStmt struct {
	Name        string
	TableName   string
	Time        string   // "BEFORE", "AFTER", "INSTEAD OF"
	Event       string   // "INSERT", "UPDATE", "DELETE"
	Columns     []string // for UPDATE OF col1, col2
	When        Expr     // WHEN condition (nil if absent)
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
	Table   string
	Action  string // "ADD_COLUMN" or "RENAME_TO"
	Column  *ColumnDef
	NewName string
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
	Unique      bool
	IfNotExists bool
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
}

func (r *RollbackStmt) NodeType() string { return "RollbackStmt" }

// BackupStmt represents:
//
//	BACKUP DATABASE TO 'path'
//	BACKUP INCREMENTAL TO 'path'
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
	Type  string     // "ROWS" or "RANGE"
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

// parseTableRef parses a table reference which may be:
//   - a regular table: tablename [AS alias]
//   - a subquery: (SELECT ...) [AS alias]
func (p *Parser) parseTableRef() *TableRef {
	ref := &TableRef{}

	if p.current().Type == TokenLeftParen {
		// Derived table: (SELECT ...) or (VALUES ...)
		p.advance() // consume (
		if p.current().Type == TokenKeyword && p.current().Literal == "VALUES" {
			// Parse VALUES (row1), (row2), ... as a derived table
			p.advance() // consume VALUES
			var rows [][]Expr
			for {
				if p.current().Type != TokenLeftParen {
					break
				}
				p.advance() // consume (
				var row []Expr
				for !p.isEOF() && p.current().Type != TokenRightParen {
					expr, e := p.parseExpr()
					if e != nil {
						p.parseError = e
						return ref
					}
					row = append(row, expr)
					if p.current().Type == TokenComma {
						p.advance()
					}
				}
				if p.current().Type == TokenRightParen {
					p.advance() // consume )
				}
				rows = append(rows, row)
				if p.current().Type != TokenComma {
					break
				}
				p.advance() // consume comma between rows
			}
			ref.Values = rows
			if p.current().Type == TokenRightParen {
				p.advance() // consume outer )
			}
			// Parse AS alias(col1, col2)
			if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
				p.advance()
			}
			if p.current().Type == TokenIdentifier || p.current().Type == TokenString {
				ref.Alias = p.current().Literal
				p.advance()
			}
			// Parse column list: (col1, col2, ...)
			if p.current().Type == TokenLeftParen {
				p.advance() // consume (
				for !p.isEOF() && p.current().Type != TokenRightParen {
					if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
						ref.ValueCols = append(ref.ValueCols, p.current().Literal)
						p.advance()
					}
					if p.current().Type == TokenComma {
						p.advance()
					}
				}
				if p.current().Type == TokenRightParen {
					p.advance() // consume )
				}
			}
			return ref
		}
		subStmt, err := p.parseSelect()
		if err == nil {
			ref.Subquery = subStmt
		}
		if p.current().Type == TokenRightParen {
			p.advance() // consume )
		}
	} else {
		ref.Name = p.current().Literal
		p.advance()

		// Check for schema.table notation
		if p.current().Type == TokenDot {
			p.advance()
			ref.Schema = ref.Name
			ref.Name = p.current().Literal
			p.advance()
		}
	}

	// Check for alias (with or without AS keyword)
	if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
		p.advance() // consume AS
	}
	if p.current().Type == TokenIdentifier || p.current().Type == TokenString {
		ref.Alias = p.current().Literal
		p.advance()
	}

	// Skip INDEXED BY hint: INDEXED BY index_name or NOT INDEXED
	if p.current().Type == TokenKeyword && p.current().Literal == "INDEXED" {
		p.advance() // consume INDEXED
		if p.current().Type == TokenKeyword && p.current().Literal == "BY" {
			p.advance() // consume BY
			p.advance() // consume index_name
		}
	} else if p.current().Type == TokenKeyword && p.current().Literal == "NOT" {
		if p.peek().Type == TokenKeyword && strings.ToUpper(p.peek().Literal) == "INDEXED" {
			p.advance() // consume NOT
			p.advance() // consume INDEXED
		}
	}

	return ref
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	if p.current().Type != TokenKeyword || p.current().Literal != "SELECT" {
		return nil, nil
	}
	p.advance()

	// Handle DISTINCT
	if p.current().Type == TokenKeyword && p.current().Literal == "DISTINCT" {
		p.advance()
		stmt.Distinct = true
	} else if p.current().Type == TokenAll {
		// SELECT ALL is the default (non-distinct) - just consume the token
		p.advance()
	}

	if p.current().Type == TokenAsterisk {
		p.advance()
		stmt.Columns = []Expr{&ColumnRef{Name: "*"}}
		// Allow additional columns after *: SELECT *, col, expr
		if p.current().Type == TokenComma {
			p.advance()
			for {
				col, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if col == nil {
					break
				}
				if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
					p.advance()
					alias := p.current().Literal
					p.advance()
					col = &AliasExpr{Expr: col, Alias: alias}
				}
				stmt.Columns = append(stmt.Columns, col)
				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
		}
	} else {
		for {
			col, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if col == nil {
			}

			// Check for column alias: expr AS alias OR expr alias (implicit alias)
			if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
				p.advance()
				alias := p.current().Literal
				p.advance()
				col = &AliasExpr{Expr: col, Alias: alias}
			} else if p.current().Type == TokenIdentifier {
				// Implicit alias (without AS): detect by checking next token
				next := p.peek()
				if next.Type == TokenComma || next.Type == TokenEOF ||
					(next.Type == TokenKeyword && (strings.ToUpper(next.Literal) == "FROM" ||
						strings.ToUpper(next.Literal) == "WHERE" || strings.ToUpper(next.Literal) == "ORDER" ||
						strings.ToUpper(next.Literal) == "GROUP" || strings.ToUpper(next.Literal) == "HAVING" ||
						strings.ToUpper(next.Literal) == "LIMIT")) {
					alias := p.current().Literal
					p.advance()
					col = &AliasExpr{Expr: col, Alias: alias}
				}
			}

			stmt.Columns = append(stmt.Columns, col)

			if p.current().Type != TokenComma {
				// Check if we've hit a keyword that signals end of columns
				if p.current().Type == TokenKeyword {
					lit := p.current().Literal
					litUpper := strings.ToUpper(lit)
					if litUpper == "FROM" || litUpper == "WHERE" || litUpper == "ORDER" || litUpper == "GROUP" || litUpper == "HAVING" || litUpper == "LIMIT" {
						break
					}
				}
				break
			}
			p.advance()
		}
	}

	if p.current().Literal == "FROM" {
		p.advance()
		ref := p.parseTableRef()
		stmt.From = ref

		for p.current().Type == TokenKeyword && (p.current().Literal == "NATURAL" || p.current().Literal == "INNER" || p.current().Literal == "LEFT" || p.current().Literal == "RIGHT" || p.current().Literal == "CROSS" || p.current().Literal == "JOIN") {
			join := &Join{}

			// Handle NATURAL JOIN
			if p.current().Literal == "NATURAL" {
				join.Natural = true
				p.advance()
			}

			if p.current().Literal == "INNER" || p.current().Literal == "LEFT" || p.current().Literal == "RIGHT" || p.current().Literal == "CROSS" {
				join.Type = p.current().Literal
				p.advance()
			}

			// Consume OUTER if present (LEFT OUTER JOIN)
			if p.current().Type == TokenKeyword && p.current().Literal == "OUTER" {
				p.advance()
			}

			if p.current().Literal == "JOIN" {
				p.advance()
			}

			rightTable := p.parseTableRef()
			join.Right = rightTable

			if p.current().Literal == "ON" {
				p.advance()
				cond, err := p.parseExpr()
				if err == nil {
					join.Cond = cond
				}
			} else if p.current().Type == TokenKeyword && p.current().Literal == "USING" {
				p.advance() // consume USING
				if p.current().Type == TokenLeftParen {
					p.advance() // consume (
					for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
						join.UsingColumns = append(join.UsingColumns, p.current().Literal)
						p.advance()
						if p.current().Type == TokenComma {
							p.advance()
						}
					}
					if p.current().Type == TokenRightParen {
						p.advance() // consume )
					}
				}
			}

			join.Left = ref
			ref.Join = join
			ref = rightTable
		}

		// Handle comma-separated tables (implicit cross join): FROM t1 AS a, t2 AS b
		for p.current().Type == TokenComma {
			p.advance()
			// Check if next token is a table-level constraint keyword (end of FROM)
			if p.current().Type == TokenKeyword {
				kw := strings.ToUpper(p.current().Literal)
				if kw == "WHERE" || kw == "ORDER" || kw == "GROUP" || kw == "HAVING" || kw == "LIMIT" {
					break
				}
			}
			rightTable := p.parseTableRef()
			join := &Join{Type: "CROSS", Right: rightTable, Left: ref}
			ref.Join = join
			ref = rightTable
		}
	}

	if p.current().Literal == "WHERE" {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse GROUP BY
	if p.current().Literal == "GROUP" {
		p.advance()
		if p.current().Literal == "BY" {
			p.advance()
			for {
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				stmt.GroupBy = append(stmt.GroupBy, expr)
				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
		}
	}

	// Parse HAVING
	if p.current().Literal == "HAVING" {
		p.advance()
		having, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	if p.current().Literal == "ORDER" {
		p.advance()
		if p.current().Literal == "BY" {
			p.advance()
			for {
				// Detect invalid ORDER BY expressions starting with IS keyword
				if p.current().Type == TokenIs {
					p.parseError = fmt.Errorf("near \"IS\": syntax error")
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					break
				}
				if expr == nil {
					break
				}
				ob := OrderBy{Expr: expr, Desc: false, Nulls: ""}
				if strings.ToUpper(p.current().Literal) == "DESC" {
					ob.Desc = true
					p.advance()
				} else if strings.ToUpper(p.current().Literal) == "ASC" {
					p.advance()
				}
				// NULLS FIRST/LAST and bare FIRST/LAST are not supported by the SQLite version used in testing
				curLit := strings.ToUpper(p.current().Literal)
				if curLit == "NULLS" {
					p.advance() // consume NULLS
					curLit2 := strings.ToUpper(p.current().Literal)
					if curLit2 == "FIRST" {
						ob.Nulls = "FIRST"
						p.advance()
					} else if curLit2 == "LAST" {
						ob.Nulls = "LAST"
						p.advance()
					}
				} else if curLit == "FIRST" || curLit == "LAST" {
					// Bare FIRST/LAST after ORDER BY expression (e.g., "IS NULL FIRST") is not supported
					p.parseError = fmt.Errorf("near \"%s\": syntax error", p.current().Literal)
					p.advance()
				}
				stmt.OrderBy = append(stmt.OrderBy, ob)
				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
		}
	}

	if p.current().Literal == "LIMIT" {
		p.advance()
		limit, err := p.parseExpr()
		if err == nil {
			stmt.Limit = limit
		}
	}

	if p.current().Literal == "OFFSET" {
		p.advance()
		offset, err := p.parseExpr()
		if err == nil {
			stmt.Offset = offset
		}
	}

	if p.current().Literal == "UNION" || p.current().Literal == "EXCEPT" || p.current().Literal == "INTERSECT" {
		stmt.SetOp = p.current().Literal
		p.advance()
		if p.current().Literal == "ALL" || (p.current().Type == TokenAll) {
			stmt.SetOpAll = true
			p.advance()
		} else if p.current().Literal == "DISTINCT" {
			return nil, fmt.Errorf("near \"DISTINCT\": syntax error")
		}
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.SetOpRight = right
		// Hoist ORDER BY and LIMIT/OFFSET from right to outer (they apply to the full set op result)
		if right != nil {
			if right.OrderBy != nil && stmt.OrderBy == nil {
				stmt.OrderBy = right.OrderBy
				right.OrderBy = nil
			}
			if right.Limit != nil && stmt.Limit == nil {
				stmt.Limit = right.Limit
				stmt.Offset = right.Offset
				right.Limit = nil
				right.Offset = nil
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{Values: make([][]Expr, 0)}

	if p.current().Type != TokenKeyword || p.current().Literal != "INSERT" {
		return nil, nil
	}
	p.advance()

	// Handle INSERT OR <action> syntax (e.g. INSERT OR REPLACE, INSERT OR IGNORE)
	if p.current().Type == TokenOr {
		p.advance()
		action := strings.ToUpper(p.current().Literal)
		switch action {
		case "REPLACE", "IGNORE", "ABORT", "FAIL", "ROLLBACK":
			stmt.OrAction = action
			p.advance()
		default:
			return nil, fmt.Errorf("unknown INSERT OR action: %s", p.current().Literal)
		}
	}

	if p.current().Literal == "INTO" {
		p.advance()
	}
	stmt.Table = p.current().Literal
	p.advance()
	// Handle schema.table notation
	if p.current().Type == TokenDot {
		p.advance()
		stmt.Table = p.current().Literal
		p.advance()
	}

	if p.current().Type == TokenLeftParen {
		p.advance()
		for {
			col := p.current().Literal
			stmt.Columns = append(stmt.Columns, col)
			p.advance()
			if p.current().Type != TokenComma {
				break
			}
			p.advance()
		}
		p.expect(TokenRightParen)
	}

	// Check for DEFAULT VALUES
	if p.current().Literal == "DEFAULT" {
		p.advance()
		if p.current().Literal == "VALUES" {
			p.advance()
			stmt.UseDefaults = true
			return stmt, nil
		}
		// Not DEFAULT VALUES, backtrack would be needed but we'll error for now
		return nil, fmt.Errorf("expected VALUES after DEFAULT")
	}

	if p.current().Literal == "VALUES" {
		p.advance()
		for {
			if p.current().Type != TokenLeftParen {
				break
			}
			p.advance()

			row := make([]Expr, 0)
			// Don't allow empty VALUES () - that's not standard SQL
			// Use DEFAULT VALUES instead
			if p.current().Type == TokenRightParen {
				return nil, fmt.Errorf("empty VALUES () not supported, use DEFAULT VALUES")
			}

			for {
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				row = append(row, expr)

				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
			stmt.Values = append(stmt.Values, row)

			p.expect(TokenRightParen)

			if p.current().Type != TokenComma {
				break
			}
			p.advance()
		}
	}

	// Handle INSERT ... SELECT
	if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.SelectQuery = sel
	}

	// Parse ON CONFLICT clause
	if p.current().Type == TokenKeyword && p.current().Literal == "ON" {
		p.advance()
		if p.current().Type != TokenKeyword || p.current().Literal != "CONFLICT" {
			return nil, fmt.Errorf("expected CONFLICT after ON")
		}
		p.advance()

		oc := &OnConflict{}

		// Optional conflict target: (column_list)
		if p.current().Type == TokenLeftParen {
			p.advance()
			for {
				oc.Columns = append(oc.Columns, p.current().Literal)
				p.advance()
				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
			p.expect(TokenRightParen)
		}

		// DO NOTHING | DO UPDATE SET ...
		if p.current().Type != TokenKeyword || p.current().Literal != "DO" {
			return nil, fmt.Errorf("expected DO after ON CONFLICT target")
		}
		p.advance()

		if p.current().Literal == "NOTHING" {
			p.advance()
			oc.DoNothing = true
		} else if p.current().Literal == "UPDATE" {
			p.advance()
			if p.current().Literal != "SET" {
				return nil, fmt.Errorf("expected SET after ON CONFLICT DO UPDATE")
			}
			p.advance()
			for {
				col := &ColumnRef{Name: p.current().Literal}
				p.advance()
				p.expect(TokenEq)
				val, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				oc.Updates = append(oc.Updates, SetClause{Column: col, Value: val})
				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
		} else {
			return nil, fmt.Errorf("expected NOTHING or UPDATE after ON CONFLICT DO")
		}

		stmt.OnConflict = oc
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{Set: make([]SetClause, 0)}

	if p.current().Type != TokenKeyword || p.current().Literal != "UPDATE" {
		return nil, nil
	}
	p.advance()

	stmt.Table = p.current().Literal
	p.advance()

	if p.current().Literal == "SET" {
		p.advance()
		for {
			col := &ColumnRef{Name: p.current().Literal}
			p.advance()
			p.expect(TokenEq)
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Set = append(stmt.Set, SetClause{Column: col, Value: val})

			if p.current().Type != TokenComma {
				break
			}
			p.advance()
		}
	}

	if p.current().Literal == "WHERE" {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}

	if p.current().Type != TokenKeyword || p.current().Literal != "DELETE" {
		return nil, nil
	}
	p.advance()

	if p.current().Literal == "FROM" {
		p.advance()
	}
	stmt.Table = p.current().Literal
	p.advance()

	if p.current().Literal == "WHERE" {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (ASTNode, error) {
	p.advance()

	if p.current().Literal == "UNIQUE" {
		p.advance()
		if p.current().Literal == "INDEX" {
			return p.parseCreateIndex(true)
		}
		return nil, nil
	}

	if p.current().Literal == "INDEX" {
		return p.parseCreateIndex(false)
	}

	// Handle CREATE TRIGGER
	if strings.ToUpper(p.current().Literal) == "TRIGGER" {
		return p.parseCreateTrigger()
	}

	// Handle TEMPORARY/TEMP prefix
	temporary := false
	if strings.ToUpper(p.current().Literal) == "TEMPORARY" || strings.ToUpper(p.current().Literal) == "TEMP" {
		temporary = true
		p.advance()
	}

	// Handle VIEW
	if strings.ToUpper(p.current().Literal) == "VIEW" {
		p.advance()
		stmt := &CreateViewStmt{}
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
				if p.current().Type == TokenExists {
					stmt.IfNotExists = true
					p.advance()
				}
			}
		}
		stmt.Name = p.current().Literal
		p.advance()
		// Consume AS
		if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
			p.advance()
		}
		// Parse SELECT
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel
		return stmt, nil
	}

	if p.current().Literal == "TABLE" {
		p.advance()
		stmt := &CreateTableStmt{Temporary: temporary}
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
				if p.current().Type == TokenExists {
					stmt.IfNotExists = true
					p.advance()
				}
			}
		}
		// Handle TEMPORARY/TEMP prefix already consumed by caller
		stmt.Name = p.current().Literal
		p.advance()

		// Handle CREATE TABLE ... AS SELECT
		if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "AS" {
			p.advance()
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			stmt.AsSelect = sel
			return stmt, nil
		}

		if p.current().Type == TokenLeftParen {
			p.advance()
			// Check for empty table definition
			if p.current().Type == TokenRightParen {
				return nil, fmt.Errorf("near \")\": syntax error")
			}
			for {
				// Stop when we reach the end of column definitions
				if p.current().Type == TokenRightParen || p.current().Type == TokenEOF {
					break
				}
				// Stop at table-level constraint keywords
				if p.current().Type == TokenKeyword {
					kw := strings.ToUpper(p.current().Literal)
					if kw == "PRIMARY" || kw == "UNIQUE" || kw == "CHECK" || kw == "FOREIGN" || kw == "CONSTRAINT" {
						break
					}
				}
				col := ColumnDef{
					Name: p.current().Literal,
				}
				p.advance()
				if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword ||
					p.current().Type == TokenAny || p.current().Type == TokenAll {
					col.Type = p.current().Literal
					p.advance()
					if p.current().Type == TokenLeftParen {
						for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
							p.advance()
						}
						if p.current().Type == TokenRightParen {
							p.advance()
						}
					}
				}

				// Parse column constraints before appending
				for p.current().Type == TokenKeyword || p.current().Type == TokenNot {
					var keyword string
					if p.current().Type == TokenNot {
						keyword = "NOT"
					} else {
						keyword = p.current().Literal
					}
					if keyword == "PRIMARY" {
						col.PrimaryKey = true
						p.advance()
						if p.current().Type == TokenKeyword && p.current().Literal == "KEY" {
							p.advance()
						}
					} else if keyword == "NOT" {
						p.advance()
						if p.current().Literal == "NULL" {
							col.NotNull = true
							p.advance()
						}
					} else if keyword == "UNIQUE" {
						col.Type += " UNIQUE"
						p.advance()
					} else if keyword == "DEFAULT" {
						// Parse DEFAULT value
						p.advance()
						// Parse the default expression
						defaultExpr, err := p.parseExpr()
						if err != nil {
							return nil, err
						}
						col.Default = defaultExpr
					} else if keyword == "CHECK" {
						// Parse CHECK constraint
						p.advance()
						if p.current().Type == TokenLeftParen {
							p.advance()
							// Parse the check expression
							checkExpr, err := p.parseExpr()
							if err != nil {
								return nil, err
							}
							col.Check = checkExpr
							if p.current().Type == TokenRightParen {
								p.advance()
							}
						}
					} else if keyword == "REFERENCES" {
						// Inline REFERENCES: col TYPE REFERENCES parent(col) [ON DELETE ...] [ON UPDATE ...]
						fk := p.parseForeignKeyRef([]string{col.Name})
						col.ForeignKey = &fk
					} else if keyword == "AUTOINCREMENT" {
						col.IsAutoincrement = true
						p.advance()
					} else if keyword == "ASC" || keyword == "DESC" {
						// Column modifier keywords - skip
						p.advance()
					} else {
						// Stop at unknown keywords or table-level constraints
						break
					}
				}

				// Validate: column-level PKs must be singular
				if col.PrimaryKey {
					for _, existingCol := range stmt.Columns {
						if existingCol.PrimaryKey {
							return nil, fmt.Errorf("table %q has more than one primary key", stmt.Name)
						}
					}
				}

				stmt.Columns = append(stmt.Columns, col)

				if p.current().Type != TokenComma {
					break
				}
				p.advance()

				// Check for table-level constraints after comma
				for p.current().Type == TokenKeyword {
					kw := strings.ToUpper(p.current().Literal)
					if kw == "PRIMARY" {
						p.advance()
						if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "KEY" {
							p.advance()
						}
						if p.current().Type == TokenLeftParen {
							p.advance()
							for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
								pkColName := p.current().Literal
								for i := range stmt.Columns {
									if stmt.Columns[i].Name == pkColName {
										stmt.Columns[i].PrimaryKey = true
									}
								}
								p.advance()
								if p.current().Type == TokenComma {
									p.advance()
								}
							}
							if p.current().Type == TokenRightParen {
								p.advance()
							}
						}
					} else if kw == "UNIQUE" {
						p.advance()
						if p.current().Type == TokenLeftParen {
							for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
								p.advance()
							}
							if p.current().Type == TokenRightParen {
								p.advance()
							}
						}
					} else if kw == "CHECK" {
						p.advance()
						if p.current().Type == TokenLeftParen {
							p.advance()
							checkExpr, _ := p.parseExpr()
							if checkExpr != nil {
								stmt.TableChecks = append(stmt.TableChecks, checkExpr)
							}
							if p.current().Type == TokenRightParen {
								p.advance()
							}
						}
					} else if kw == "FOREIGN" {
						// Parse FOREIGN KEY (cols) REFERENCES parent(cols) [ON DELETE ...] [ON UPDATE ...]
						fk := p.parseTableForeignKey()
						stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
					} else if kw == "CONSTRAINT" {
						p.advance() // consume "CONSTRAINT"
						p.advance() // consume constraint name
						continue
					} else {
						break
					}
					if p.current().Type == TokenComma {
						p.advance()
					} else {
						break
					}
				}
			}
			p.expect(TokenRightParen)
		}

		return stmt, nil
	}

	return nil, nil
}

// parseRefAction parses ON DELETE / ON UPDATE action keywords.
func parseRefAction(s string) ReferenceAction {
	switch strings.ToUpper(s) {
	case "CASCADE":
		return ReferenceCascade
	case "RESTRICT":
		return ReferenceRestrict
	case "SET":
		return ReferenceSetNull // will be adjusted below
	case "NO":
		return ReferenceNoAction
	default:
		return ReferenceNoAction
	}
}

// parseForeignKeyRef parses REFERENCES parent(col) [ON DELETE ...] [ON UPDATE ...].
// childCols contains the already-parsed child column names.
func (p *Parser) parseForeignKeyRef(childCols []string) ForeignKeyConstraint {
	fk := ForeignKeyConstraint{
		ChildColumns: childCols,
		OnDelete:     ReferenceNoAction,
		OnUpdate:     ReferenceNoAction,
	}
	// consume REFERENCES
	p.advance()
	// parent table name
	fk.ParentTable = p.current().Literal
	p.advance()
	// optional (col, ...)
	if p.current().Type == TokenLeftParen {
		p.advance()
		for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
			fk.ParentColumns = append(fk.ParentColumns, p.current().Literal)
			p.advance()
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
	}
	// ON DELETE / ON UPDATE
	for p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "ON" {
		p.advance() // consume ON
		event := strings.ToUpper(p.current().Literal)
		p.advance() // consume DELETE or UPDATE
		// action: CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION
		action := ReferenceNoAction
		actionWord := strings.ToUpper(p.current().Literal)
		p.advance()
		switch actionWord {
		case "CASCADE":
			action = ReferenceCascade
		case "RESTRICT":
			action = ReferenceRestrict
		case "SET":
			next := strings.ToUpper(p.current().Literal)
			p.advance()
			if next == "NULL" {
				action = ReferenceSetNull
			} else {
				action = ReferenceSetDefault
			}
		case "NO":
			p.advance() // consume ACTION
			action = ReferenceNoAction
		}
		if event == "DELETE" {
			fk.OnDelete = action
		} else if event == "UPDATE" {
			fk.OnUpdate = action
		}
	}
	return fk
}

// parseTableForeignKey parses a table-level FOREIGN KEY clause.
func (p *Parser) parseTableForeignKey() ForeignKeyConstraint {
	fk := ForeignKeyConstraint{
		OnDelete: ReferenceNoAction,
		OnUpdate: ReferenceNoAction,
	}
	p.advance() // consume FOREIGN
	if strings.ToUpper(p.current().Literal) == "KEY" {
		p.advance()
	}
	// (child cols)
	if p.current().Type == TokenLeftParen {
		p.advance()
		for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
			fk.ChildColumns = append(fk.ChildColumns, p.current().Literal)
			p.advance()
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
	}
	// REFERENCES ...
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "REFERENCES" {
		updated := p.parseForeignKeyRef(fk.ChildColumns)
		fk.ParentTable = updated.ParentTable
		fk.ParentColumns = updated.ParentColumns
		fk.OnDelete = updated.OnDelete
		fk.OnUpdate = updated.OnUpdate
	}
	return fk
}

// parseCreateTrigger parses CREATE [TEMP] TRIGGER [IF NOT EXISTS] name
// BEFORE/AFTER/INSTEAD OF INSERT/UPDATE/DELETE ON table ...
func (p *Parser) parseCreateTrigger() (ASTNode, error) {
	p.advance() // consume TRIGGER
	stmt := &CreateTriggerStmt{}
	// IF NOT EXISTS
	if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
		p.advance()
		if p.current().Type == TokenNot {
			p.advance()
		}
		if p.current().Type == TokenExists {
			stmt.IfNotExists = true
			p.advance()
		}
	}
	stmt.Name = p.current().Literal
	p.advance()
	// BEFORE / AFTER / INSTEAD OF
	trigTime := strings.ToUpper(p.current().Literal)
	if trigTime == "BEFORE" || trigTime == "AFTER" {
		stmt.Time = trigTime
		p.advance()
	} else if trigTime == "INSTEAD" {
		stmt.Time = "INSTEAD OF"
		p.advance()
		if strings.ToUpper(p.current().Literal) == "OF" {
			p.advance()
		}
	}
	// INSERT / UPDATE / DELETE
	stmt.Event = strings.ToUpper(p.current().Literal)
	p.advance()
	// UPDATE OF col1, col2
	if stmt.Event == "UPDATE" && p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "OF" {
		p.advance()
		for {
			if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
				stmt.Columns = append(stmt.Columns, p.current().Literal)
				p.advance()
			}
			if p.current().Type == TokenComma {
				p.advance()
			} else {
				break
			}
		}
	}
	// ON tablename
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "ON" {
		p.advance()
		stmt.TableName = p.current().Literal
		p.advance()
	}
	// FOR EACH ROW (optional)
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "FOR" {
		p.advance() // FOR
		if p.current().Type == TokenKeyword {
			p.advance() // EACH
		}
		if p.current().Type == TokenKeyword {
			p.advance() // ROW
		}
	}
	// WHEN condition (optional)
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "WHEN" {
		p.advance()
		whenExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.When = whenExpr
	}
	// BEGIN ... END
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "BEGIN" {
		p.advance()
		for !p.isEOF() {
			if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "END" {
				p.advance()
				break
			}
			bodyStmt, err := p.parseInternal()
			if err != nil {
				return nil, err
			}
			if bodyStmt != nil {
				stmt.Body = append(stmt.Body, bodyStmt)
			}
			// consume optional semicolons between body statements
			for p.current().Type == TokenSemicolon {
				p.advance()
			}
		}
	}
	return stmt, nil
}

func (p *Parser) parseCreateIndex(unique bool) (ASTNode, error) {
	p.advance()
	stmt := &CreateIndexStmt{Unique: unique}

	if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
		p.advance()
		if p.current().Type == TokenNot {
			p.advance()
			if p.current().Type == TokenExists {
				stmt.IfNotExists = true
				p.advance()
			}
		}
	}

	stmt.Name = p.current().Literal
	p.advance()

	if p.current().Type == TokenKeyword && p.current().Literal == "ON" {
		p.advance()
		stmt.Table = p.current().Literal
		p.advance()

		if p.current().Type == TokenLeftParen {
			p.advance()
			for {
				if p.current().Type == TokenIdentifier {
					stmt.Columns = append(stmt.Columns, p.current().Literal)
					p.advance()
				}
				if p.current().Type == TokenComma {
					p.advance()
				} else {
					break
				}
			}
			p.expect(TokenRightParen)
		}
	}

	return stmt, nil
}

func (p *Parser) parseDrop() (ASTNode, error) {
	p.advance()

	if p.current().Literal == "TABLE" {
		p.advance()
		// Parse optional IF EXISTS
		ifExists := false
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
			}
			if p.current().Type == TokenExists {
				ifExists = true
				p.advance()
			}
		}
		// Skip RESTRICT/CASCADE keywords - but RESTRICT should error like SQLite
		name := p.current().Literal
		p.advance()
		for p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
			upper := strings.ToUpper(p.current().Literal)
			if upper == "RESTRICT" {
				return nil, fmt.Errorf("near \"RESTRICT\": syntax error")
			}
			if upper != "CASCADE" {
				break
			}
			p.advance()
		}
		return &DropTableStmt{Name: name, IfExists: ifExists}, nil
	}

	if strings.ToUpper(p.current().Literal) == "VIEW" {
		p.advance()
		ifExists := false
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
			}
			if p.current().Type == TokenExists {
				ifExists = true
				p.advance()
			}
		}
		name := p.current().Literal
		p.advance()
		for p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
			upper := strings.ToUpper(p.current().Literal)
			if upper == "RESTRICT" {
				return nil, fmt.Errorf("near \"RESTRICT\": syntax error")
			}
			if upper != "CASCADE" {
				break
			}
			p.advance()
		}
		return &DropViewStmt{Name: name, IfExists: ifExists}, nil
	}

	if p.current().Literal == "INDEX" {
		p.advance()
		// Handle IF EXISTS
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
			}
			if p.current().Type == TokenExists {
				p.advance() // consume EXISTS, index name is next
			}
		}
		return &DropIndexStmt{Name: p.current().Literal}, nil
	}

	if strings.ToUpper(p.current().Literal) == "TRIGGER" {
		p.advance()
		ifExists := false
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenNot {
				p.advance()
			}
			if p.current().Type == TokenExists {
				ifExists = true
				p.advance()
			}
		}
		name := p.current().Literal
		p.advance()
		return &DropTriggerStmt{Name: name, IfExists: ifExists}, nil
	}

	return nil, nil
}

func (p *Parser) parseAlterTable() (ASTNode, error) {
	p.advance() // consume "ALTER"
	if p.current().Type == TokenKeyword && p.current().Literal == "TABLE" {
		p.advance() // consume "TABLE"
	}
	stmt := &AlterTableStmt{
		Table: p.current().Literal,
	}
	p.advance() // consume table name

	kw := strings.ToUpper(p.current().Literal)
	switch kw {
	case "ADD":
		p.advance() // consume "ADD"
		// Optional COLUMN keyword
		if strings.ToUpper(p.current().Literal) == "COLUMN" {
			p.advance()
		}
		col := &ColumnDef{
			Name: p.current().Literal,
		}
		p.advance()
		if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
			col.Type = p.current().Literal
			p.advance()
			// Parse type modifiers
			if p.current().Type == TokenLeftParen {
				for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
					p.advance()
				}
				if p.current().Type == TokenRightParen {
					p.advance()
				}
			}
		}
		// Parse column constraints
		for p.current().Type == TokenKeyword || p.current().Type == TokenNot {
			keyword := strings.ToUpper(p.current().Literal)
			if p.current().Type == TokenNot {
				p.advance()
				if strings.ToUpper(p.current().Literal) == "NULL" {
					col.NotNull = true
					p.advance()
				}
			} else if keyword == "NOT" {
				p.advance()
				if strings.ToUpper(p.current().Literal) == "NULL" {
					col.NotNull = true
					p.advance()
				}
			} else if keyword == "DEFAULT" {
				p.advance()
				defExpr, err := p.parseExpr()
				if err != nil {
					break
				}
				col.Default = defExpr
			} else if keyword == "PRIMARY" {
				col.PrimaryKey = true
				p.advance()
				if strings.ToUpper(p.current().Literal) == "KEY" {
					p.advance()
				}
			} else if keyword == "UNIQUE" || keyword == "REFERENCES" || keyword == "CHECK" {
				// Skip for simplicity
				for p.current().Type != TokenEOF && p.current().Type != TokenComma {
					p.advance()
				}
				break
			} else {
				break
			}
		}
		stmt.Action = "ADD_COLUMN"
		stmt.Column = col
	case "RENAME":
		p.advance() // consume "RENAME"
		if strings.ToUpper(p.current().Literal) == "TO" {
			p.advance()
		}
		stmt.Action = "RENAME_TO"
		stmt.NewName = p.current().Literal
	}
	return stmt, nil
}

func (p *Parser) parsePragma() (ASTNode, error) {
	p.advance()
	stmt := &PragmaStmt{}

	if p.current().Type == TokenIdentifier {
		stmt.Name = p.current().Literal
		p.advance()

		if p.current().Type == TokenEq {
			p.advance()
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Value = val
		} else if p.current().Type == TokenLeftParen {
			p.advance()
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Value = val
			if p.current().Type == TokenRightParen {
				p.advance()
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenOr {
		p.advance()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: TokenOr, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpr() (Expr, error) {
	left, err := p.parseEqExpr()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenAnd {
		p.advance()
		right, err := p.parseEqExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: TokenAnd, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseEqExpr() (Expr, error) {
	left, err := p.parseCmpExpr()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenEq || p.current().Type == TokenNe ||
		p.current().Type == TokenLt || p.current().Type == TokenLe ||
		p.current().Type == TokenGt || p.current().Type == TokenGe {
		op := p.current().Type
		p.advance()
		right, err := p.parseCmpExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseCmpExpr() (Expr, error) {
	left, err := p.parseAddExpr()
	if err != nil {
		return nil, err
	}

	// MATCH operator: not supported in WHERE context (matches SQLite behavior)
	if p.current().Type == TokenIdentifier && strings.ToUpper(p.current().Literal) == "MATCH" {
		return nil, fmt.Errorf("unable to use function MATCH in the requested context")
	}
	if p.current().Type == TokenNot && p.peek().Type == TokenIdentifier && strings.ToUpper(p.peek().Literal) == "MATCH" {
		return nil, fmt.Errorf("unable to use function MATCH in the requested context")
	}

	if p.current().Type == TokenIs {
		p.advance()
		if p.current().Type == TokenNot {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "NULL" {
				p.advance()
				return &BinaryExpr{Op: TokenIsNot, Left: left, Right: &Literal{Value: nil}}, nil
			}
		}
		if p.current().Type == TokenKeyword && p.current().Literal == "NULL" {
			p.advance()
			return &BinaryExpr{Op: TokenIs, Left: left, Right: &Literal{Value: nil}}, nil
		}
	}

	if p.current().Type == TokenNot && p.peek().Type == TokenIn {
		p.advance()
		p.advance()
		if p.current().Type == TokenLeftParen {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
				sub, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if p.current().Type == TokenRightParen {
					p.advance()
				}
				return &BinaryExpr{Op: TokenNotIn, Left: left, Right: &SubqueryExpr{Select: sub}}, nil
			}
			var values []interface{}
			for {
				if p.current().Type == TokenRightParen {
					p.advance()
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			return &BinaryExpr{Op: TokenNotIn, Left: left, Right: &Literal{Value: values}}, nil
		}
	}

	if p.current().Type == TokenIn {
		p.advance()
		if p.current().Type == TokenLeftParen {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
				sub, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if p.current().Type == TokenRightParen {
					p.advance()
				}
				return &BinaryExpr{Op: TokenInSubquery, Left: left, Right: &SubqueryExpr{Select: sub}}, nil
			}
			var values []interface{}
			for {
				if p.current().Type == TokenRightParen {
					p.advance()
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			return &BinaryExpr{Op: TokenIn, Left: left, Right: &Literal{Value: values}}, nil
		}
	}

	if p.current().Type == TokenNot && p.peek().Type == TokenBetween {
		p.advance()
		p.advance()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		if p.current().Type == TokenAnd {
			p.advance()
			andExpr, err := p.parseAddExpr()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Op: TokenNotBetween, Left: left, Right: &BinaryExpr{Op: TokenAnd, Left: right, Right: andExpr}}, nil
		}
		return left, nil
	}

	if p.current().Type == TokenBetween {
		p.advance()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		if p.current().Type == TokenAnd {
			p.advance()
			andExpr, err := p.parseAddExpr()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Op: TokenBetween, Left: left, Right: &BinaryExpr{Op: TokenAnd, Left: right, Right: andExpr}}, nil
		}
		return left, nil
	}

	if p.current().Type == TokenExists {
		p.advance()
		if p.current().Type == TokenLeftParen {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
				sub, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if p.current().Type == TokenRightParen {
					p.advance()
				}
				return &BinaryExpr{Op: TokenExists, Left: &SubqueryExpr{Select: sub}, Right: nil}, nil
			}
		}
	}

	// UNIQUE predicate: WHERE UNIQUE (SELECT ...) - not supported by SQLite
	if p.current().Type == TokenKeyword && p.current().Literal == "UNIQUE" && p.peek().Type == TokenLeftParen {
		return nil, fmt.Errorf("near \"UNIQUE\": syntax error")
	}

	if p.current().Type == TokenIn {
		p.advance()
		if p.current().Type == TokenLeftParen {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
				sub, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if p.current().Type == TokenRightParen {
					p.advance()
				}
				return &BinaryExpr{Op: TokenInSubquery, Left: left, Right: &SubqueryExpr{Select: sub}}, nil
			}
			var values []interface{}
			for {
				if p.current().Type == TokenRightParen {
					p.advance()
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			return &BinaryExpr{Op: TokenIn, Left: left, Right: &Literal{Value: values}}, nil
		}
	}

	for (p.current().Type == TokenGt || p.current().Type == TokenGe ||
		p.current().Type == TokenLt || p.current().Type == TokenNe ||
		p.current().Type == TokenEq) &&
		((p.peek().Type == TokenKeyword && (p.peek().Literal == "ALL" || p.peek().Literal == "ANY" || p.peek().Literal == "SOME")) ||
			p.peek().Type == TokenAll || p.peek().Type == TokenAny) {
		op := p.current().Type
		p.advance() // consume comparison op
		quantifier := strings.ToUpper(p.current().Literal)
		p.advance() // consume ALL/ANY/SOME
		if p.current().Type != TokenLeftParen {
			return nil, fmt.Errorf("expected '(' after %s", quantifier)
		}
		p.advance() // consume (
		sub, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
		left = &AnyAllExpr{Left: left, Op: op, Quantifier: quantifier, Subquery: sub}
	}

	if p.current().Type == TokenLike {
		p.advance()
		pattern, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		// Check for ESCAPE clause
		if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) && strings.ToUpper(p.current().Literal) == "ESCAPE" {
			p.advance()
			escapeExpr, err := p.parseAddExpr()
			if err != nil {
				return nil, err
			}
			// Pre-process the pattern with the escape character at parse time
			if patLit, ok := pattern.(*Literal); ok {
				if escLit, ok := escapeExpr.(*Literal); ok {
					if patStr, ok := patLit.Value.(string); ok {
						if escStr, ok := escLit.Value.(string); ok && len(escStr) == 1 {
							pattern = &Literal{Value: applyLikeEscape(patStr, rune(escStr[0]))}
						}
					}
				}
			}
		}
		return &BinaryExpr{Op: TokenLike, Left: left, Right: pattern}, nil
	}

	if p.current().Type == TokenNot && p.peek().Type == TokenLike {
		p.advance()
		p.advance()
		pattern, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		// Check for ESCAPE clause
		if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) && strings.ToUpper(p.current().Literal) == "ESCAPE" {
			p.advance()
			escapeExpr, err := p.parseAddExpr()
			if err != nil {
				return nil, err
			}
			if patLit, ok := pattern.(*Literal); ok {
				if escLit, ok := escapeExpr.(*Literal); ok {
					if patStr, ok := patLit.Value.(string); ok {
						if escStr, ok := escLit.Value.(string); ok && len(escStr) == 1 {
							pattern = &Literal{Value: applyLikeEscape(patStr, rune(escStr[0]))}
						}
					}
				}
			}
		}
		return &BinaryExpr{Op: TokenNotLike, Left: left, Right: pattern}, nil
	}

	if p.current().Type == TokenGlob {
		p.advance()
		pattern, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: TokenGlob, Left: left, Right: pattern}, nil
	}

	for p.current().Type == TokenEq || p.current().Type == TokenNe ||
		p.current().Type == TokenLt || p.current().Type == TokenLe ||
		p.current().Type == TokenGt || p.current().Type == TokenGe {
		op := p.current().Type
		p.advance()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAddExpr() (Expr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenPlus || p.current().Type == TokenMinus || p.current().Type == TokenConcat {
		op := p.current().Type
		p.advance()
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseMulExpr() (Expr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenAsterisk || p.current().Type == TokenSlash || p.current().Type == TokenPercent {
		op := p.current().Type
		p.advance()
		right, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parseUnaryExpr() (Expr, error) {
	if p.current().Type == TokenMinus {
		op := p.current().Type
		p.advance()
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Expr: expr}, nil
	}
	if p.current().Type == TokenNot {
		op := p.current().Type
		p.advance()
		expr, err := p.parseEqExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Expr: expr}, nil
	}

	return p.parsePrimaryExpr()
}

func (p *Parser) parsePrimaryExpr() (Expr, error) {
	tok := p.current()

	switch tok.Type {
	case TokenLeftParen:
		p.advance()
		if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
			sub, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if p.current().Type == TokenRightParen {
				p.advance()
			}
			// Check for alias after subquery
			if p.current().Type == TokenKeyword && (p.current().Literal == "AS" || p.current().Literal == "as") {
				p.advance()
				if p.current().Type == TokenIdentifier {
					alias := p.current().Literal
					p.advance()
					return &AliasExpr{Expr: &SubqueryExpr{Select: sub}, Alias: alias}, nil
				}
			}
			return &SubqueryExpr{Select: sub}, nil
		}
		// VALUES as a table source is not supported
		if p.current().Type == TokenKeyword && p.current().Literal == "VALUES" {
			return nil, fmt.Errorf("near \"(\": syntax error")
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		// Row constructor (1, 2) is not supported by SQLite
		if p.current().Type == TokenComma {
			return nil, fmt.Errorf("row value misused")
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
		return expr, nil
	case TokenNumber:
		p.advance()
		if iv, err := strconv.ParseInt(tok.Literal, 10, 64); err == nil {
			return &Literal{Value: iv}, nil
		}
		if fv, err := strconv.ParseFloat(tok.Literal, 64); err == nil {
			if fv == math.MaxFloat64 && strings.Contains(tok.Literal, "e+") {
				return &Literal{Value: math.Inf(1)}, nil
			}
			return &Literal{Value: fv}, nil
		}
		return &Literal{Value: tok.Literal}, nil
	case TokenString:
		p.advance()
		// Check for qualified column reference with quoted identifier: "table".column
		if p.current().Type == TokenDot {
			p.advance()
			if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
				colName := p.current().Literal
				p.advance()
				return &ColumnRef{Table: tok.Literal, Name: colName}, nil
			}
		}
		return &Literal{Value: tok.Literal}, nil
	case TokenHexString:
		p.advance()
		return &Literal{Value: []byte(tok.Literal)}, nil
	case TokenCast:
		p.advance()
		if p.current().Type != TokenLeftParen {
			return nil, fmt.Errorf("expected '(' after CAST")
		}
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.current().Type != TokenKeyword || p.current().Literal != "AS" {
			return nil, fmt.Errorf("expected AS in CAST expression")
		}
		p.advance()

		// Parse type name
		typeSpec := TypeSpec{}
		if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
			typeSpec.Name = strings.ToUpper(p.current().Literal)
			p.advance()
		}

		// Check for precision/scale: TYPE(precision) or TYPE(precision, scale)
		if p.current().Type == TokenLeftParen {
			p.advance()

			// Parse precision (first number)
			if p.current().Type == TokenNumber {
				if precision, err := strconv.Atoi(p.current().Literal); err == nil {
					typeSpec.Precision = precision
				}
				p.advance()
			}

			// Check for scale (optional, after comma)
			if p.current().Type == TokenComma {
				p.advance()
				if p.current().Type == TokenNumber {
					if scale, err := strconv.Atoi(p.current().Literal); err == nil {
						typeSpec.Scale = scale
					}
					p.advance()
				}
			}

			// Expect closing paren for type parameters
			if p.current().Type != TokenRightParen {
				return nil, fmt.Errorf("expected ')' after type parameters")
			}
			p.advance()
		}

		// Expect closing paren for CAST expression
		if p.current().Type != TokenRightParen {
			return nil, fmt.Errorf("expected ')' after CAST type")
		}
		p.advance()
		return &CastExpr{Expr: expr, TypeSpec: typeSpec}, nil
	case TokenIdentifier:
		p.advance()

		if p.current().Type == TokenLeftParen {
			// ROW(...) is a row constructor not supported by SQLite
			if tok.Literal == "row" {
				return nil, fmt.Errorf("no such function: ROW")
			}
			p.advance()
			args := make([]Expr, 0)
			for !p.isEOF() && p.current().Type != TokenRightParen {
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			p.expect(TokenRightParen)
			// Check for OVER clause (window function) - handles identifiers like PERCENT_RANK, CUME_DIST
			if p.current().Type == TokenKeyword && p.current().Literal == "OVER" {
				p.advance() // consume OVER
				partition, orderBy, frame, err := p.parseWindowSpec()
				if err != nil {
					return nil, err
				}
				return &WindowFuncExpr{Name: strings.ToUpper(tok.Literal), Args: args, Partition: partition, OrderBy: orderBy, Frame: frame}, nil
			}
			return &FuncCall{Name: tok.Literal, Args: args}, nil
		}

		colName := tok.Literal
		var tableName string
		if p.current().Type == TokenDot {
			p.advance()
			if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
				tableName = tok.Literal
				colName = p.current().Literal
				p.advance()
			} else if p.current().Type == TokenAsterisk {
				// Handle table.* pattern
				tableName = tok.Literal
				colName = "*"
				p.advance()
			}
		}

		return &ColumnRef{Table: tableName, Name: colName}, nil
	case TokenAsterisk:
		p.advance()
		return &ColumnRef{Name: "*"}, nil
	case TokenKeyword:
		if tok.Literal == "AVG" || tok.Literal == "MIN" || tok.Literal == "MAX" || tok.Literal == "COUNT" || tok.Literal == "SUM" || tok.Literal == "COALESCE" || tok.Literal == "IFNULL" || tok.Literal == "NULLIF" || tok.Literal == "LAG" || tok.Literal == "LEAD" || tok.Literal == "FIRST_VALUE" || tok.Literal == "LAST_VALUE" || tok.Literal == "ROW_NUMBER" || tok.Literal == "RANK" || tok.Literal == "DENSE_RANK" || tok.Literal == "NTILE" || tok.Literal == "PERCENT_RANK" || tok.Literal == "CUME_DIST" {
			p.advance()
			if p.current().Type == TokenLeftParen {
				p.advance()

				// Handle DISTINCT or ALL keywords in aggregate functions
				distinct := false
				isStar := false
				if p.current().Type == TokenKeyword && p.current().Literal == "DISTINCT" {
					distinct = true
					p.advance()
				} else if p.current().Type == TokenAll {
					p.advance()
				} else if p.current().Type == TokenAsterisk {
					isStar = true
				}

				args := make([]Expr, 0)
				for !p.isEOF() && p.current().Type != TokenRightParen {
					arg, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if p.current().Type == TokenComma {
						p.advance()
					}
				}
				p.expect(TokenRightParen)
				// Check for OVER clause (window function)
				if p.current().Type == TokenKeyword && p.current().Literal == "OVER" {
					p.advance() // consume OVER
					partition, orderBy, frame, err := p.parseWindowSpec()
					if err != nil {
						return nil, err
					}
					return &WindowFuncExpr{Name: tok.Literal, Args: args, IsStar: isStar, Partition: partition, OrderBy: orderBy, Frame: frame}, nil
				}
				return &FuncCall{Name: tok.Literal, Args: args, Distinct: distinct}, nil
			}
			// Keyword used as column reference (e.g., alias 'count' in HAVING count > 1)
			return &ColumnRef{Name: tok.Literal}, nil
		}
		if tok.Literal == "NULL" {
			p.advance()
			return &Literal{Value: nil}, nil
		}
		if tok.Literal == "TRUE" {
			p.advance()
			return &Literal{Value: int64(1)}, nil
		}
		if tok.Literal == "FALSE" {
			p.advance()
			return &Literal{Value: int64(0)}, nil
		}
		if tok.Literal == "CURRENT_DATE" || tok.Literal == "CURRENT_TIME" || tok.Literal == "CURRENT_TIMESTAMP" || tok.Literal == "LOCALTIME" || tok.Literal == "LOCALTIMESTAMP" {
			p.advance()
			return &FuncCall{Name: tok.Literal, Args: []Expr{}}, nil
		}
		if tok.Literal == "CASE" {
			return p.parseCaseExpr()
		}
		// UNIQUE predicate: UNIQUE (SELECT ...) - not supported (matches SQLite behavior)
		if tok.Literal == "UNIQUE" && p.peek().Type == TokenLeftParen {
			return nil, fmt.Errorf("near \"UNIQUE\": syntax error")
		}
		p.advance()
		// If keyword is followed by (, treat as a generic function call (e.g., DATE(...), STRFTIME(...))
		if p.current().Type == TokenLeftParen {
			p.advance()
			args := make([]Expr, 0)
			for !p.isEOF() && p.current().Type != TokenRightParen {
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if arg != nil {
					args = append(args, arg)
				}
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			p.expect(TokenRightParen)
			return &FuncCall{Name: tok.Literal, Args: args}, nil
		}
		// Check for table.column format (e.g., e.dept_id)
		if p.current().Type == TokenDot {
			p.advance()
			if p.current().Type == TokenIdentifier {
				tableAlias := tok.Literal
				colName := p.current().Literal
				p.advance()
				return &ColumnRef{Table: tableAlias, Name: colName}, nil
			}
		}
		return &ColumnRef{Name: tok.Literal}, nil
	}

	return nil, nil
}

func (p *Parser) parseCaseExpr() (Expr, error) {
	p.advance()
	ce := &CaseExpr{}

	if p.current().Type == TokenKeyword && p.current().Literal == "WHEN" {
	} else if p.current().Type != TokenKeyword || p.current().Literal != "WHEN" {
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		ce.Operand = operand
	}

	for p.current().Type == TokenKeyword && p.current().Literal == "WHEN" {
		p.advance()
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.current().Type == TokenKeyword && p.current().Literal == "THEN" {
			p.advance()
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		ce.Whens = append(ce.Whens, CaseWhen{Condition: cond, Result: result})
	}

	if p.current().Type == TokenKeyword && p.current().Literal == "ELSE" {
		p.advance()
		elseExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		ce.Else = elseExpr
	}

	if p.current().Type == TokenKeyword && p.current().Literal == "END" {
		p.advance()
	}

	return ce, nil
}

func (p *Parser) parseExplain() (ASTNode, error) {
	p.advance()
	explain := &ExplainStmt{}

	isQueryPlan := false
	if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) && strings.EqualFold(p.current().Literal, "QUERY") {
		p.advance()
		if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) && strings.EqualFold(p.current().Literal, "PLAN") {
			p.advance()
			isQueryPlan = true
		}
	}

	if p.current().Type == TokenKeyword && p.current().Literal == "SELECT" {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		explain.Query = sel
	} else if p.current().Type == TokenKeyword && p.current().Literal == "INSERT" {
		ins, err := p.parseInsert()
		if err != nil {
			return nil, err
		}
		explain.Query = ins
	} else if p.current().Type == TokenKeyword && p.current().Literal == "UPDATE" {
		upd, err := p.parseUpdate()
		if err != nil {
			return nil, err
		}
		explain.Query = upd
	} else if p.current().Type == TokenKeyword && p.current().Literal == "DELETE" {
		del, err := p.parseDelete()
		if err != nil {
			return nil, err
		}
		explain.Query = del
	} else {
		return nil, fmt.Errorf("EXPLAIN not supported for this statement type: %v", p.current())
	}

	explain.QueryPlan = isQueryPlan
	return explain, nil
}

func (p *Parser) parseBegin() (ASTNode, error) {
	p.advance() // consume BEGIN

	stmt := &BeginStmt{}

	// Check for transaction type: DEFERRED, IMMEDIATE, or EXCLUSIVE
	if p.current().Type == TokenKeyword {
		switch p.current().Literal {
		case "DEFERRED", "IMMEDIATE", "EXCLUSIVE":
			stmt.Type = p.current().Literal
			p.advance()
		}
	}

	// Optional TRANSACTION keyword
	if p.current().Type == TokenKeyword && p.current().Literal == "TRANSACTION" {
		p.advance()
	}

	return stmt, nil
}

func (p *Parser) parseCommit() (ASTNode, error) {
	p.advance() // consume COMMIT

	// Optional TRANSACTION keyword
	if p.current().Type == TokenKeyword && p.current().Literal == "TRANSACTION" {
		p.advance()
	}

	return &CommitStmt{}, nil
}

func (p *Parser) parseRollback() (ASTNode, error) {
	p.advance() // consume ROLLBACK

	// Optional TRANSACTION keyword
	if p.current().Type == TokenKeyword && p.current().Literal == "TRANSACTION" {
		p.advance()
	}

	return &RollbackStmt{}, nil
}

// parseBackup parses:
//
//	BACKUP DATABASE TO 'path'
//	BACKUP INCREMENTAL TO 'path'
func (p *Parser) parseBackup() (ASTNode, error) {
	p.advance() // consume BACKUP

	stmt := &BackupStmt{}

	// Expect DATABASE or INCREMENTAL
	cur := p.current()
	if cur.Type == TokenKeyword && strings.ToUpper(cur.Literal) == "INCREMENTAL" {
		stmt.Incremental = true
		p.advance()
	} else if cur.Type == TokenKeyword && strings.ToUpper(cur.Literal) == "DATABASE" {
		stmt.Incremental = false
		p.advance()
	}
	// else: bare BACKUP TO 'path' is treated as BACKUP DATABASE

	// Expect TO
	cur = p.current()
	if cur.Type == TokenKeyword && strings.ToUpper(cur.Literal) == "TO" {
		p.advance()
	}

	// Expect destination path (string literal)
	cur = p.current()
	if cur.Type == TokenString {
		stmt.DestPath = cur.Literal
		p.advance()
	} else if cur.Type == TokenIdentifier {
		stmt.DestPath = cur.Literal
		p.advance()
	} else {
		return nil, fmt.Errorf("BACKUP: expected destination path, got %v", cur)
	}

	return stmt, nil
}

func (p *Parser) parseVacuum() (ASTNode, error) {
	p.advance() // consume VACUUM
	stmt := &VacuumStmt{}
	cur := p.current()
	if cur.Type == TokenKeyword && strings.ToUpper(cur.Literal) == "INTO" {
		p.advance()
		cur = p.current()
		if cur.Type == TokenString {
			stmt.DestPath = cur.Literal
			p.advance()
		} else if cur.Type == TokenIdentifier {
			stmt.DestPath = cur.Literal
			p.advance()
		}
	}
	return stmt, nil
}

func (p *Parser) parseAnalyze() (ASTNode, error) {
	p.advance() // consume ANALYZE
	stmt := &AnalyzeStmt{}
	cur := p.current()
	if cur.Type == TokenIdentifier || cur.Type == TokenKeyword {
		stmt.Target = cur.Literal
		p.advance()
	}
	return stmt, nil
}

// parseWithClause parses a WITH ... AS (...) SELECT statement (CTE)
func (p *Parser) parseWithClause() (ASTNode, error) {
	p.advance() // consume WITH

	// Optionally consume RECURSIVE
	recursive := false
	if p.current().Type == TokenKeyword && p.current().Literal == "RECURSIVE" {
		recursive = true
		p.advance()
	}

	var ctes []CTEClause
	for {
		if p.current().Type != TokenIdentifier && !(p.current().Type == TokenKeyword) {
			break
		}
		cteName := p.current().Literal
		p.advance()

		// Parse optional column list: cte(col1, col2) AS (...)
		var cteCols []string
		if p.current().Type == TokenLeftParen {
			p.advance() // consume (
			for !p.isEOF() && p.current().Type != TokenRightParen {
				if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
					cteCols = append(cteCols, p.current().Literal)
					p.advance()
				}
				if p.current().Type == TokenComma {
					p.advance()
				}
			}
			if p.current().Type == TokenRightParen {
				p.advance() // consume )
			}
		}

		// Expect AS
		if p.current().Type != TokenKeyword || p.current().Literal != "AS" {
			return nil, fmt.Errorf("expected AS in CTE definition, got %q", p.current().Literal)
		}
		p.advance()

		// Expect (SELECT ...)
		if p.current().Type != TokenLeftParen {
			return nil, fmt.Errorf("expected '(' in CTE definition")
		}
		p.advance()

		cteSelect, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		if p.current().Type == TokenRightParen {
			p.advance()
		}

		ctes = append(ctes, CTEClause{Name: cteName, Select: cteSelect, Recursive: recursive, Columns: cteCols})

		if p.current().Type != TokenComma {
			break
		}
		p.advance() // consume comma between CTEs
	}

	// Now expect SELECT (the main query)
	if p.current().Type != TokenKeyword || p.current().Literal != "SELECT" {
		return nil, fmt.Errorf("expected SELECT after WITH clause")
	}

	mainSelect, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	mainSelect.CTEs = ctes
	return mainSelect, nil
}

// parseWindowSpec parses the window specification after OVER: ([PARTITION BY ...] [ORDER BY ...])
func (p *Parser) parseWindowSpec() (partition []Expr, orderBy []WindowOrderBy, frame *WindowFrame, err error) {
	if p.current().Type != TokenLeftParen {
		return nil, nil, nil, nil // OVER without parens - treat as empty window
	}
	p.advance() // consume '('

	// Parse PARTITION BY
	if p.current().Type == TokenKeyword && p.current().Literal == "PARTITION" {
		p.advance() // consume PARTITION
		if p.current().Type == TokenKeyword && p.current().Literal == "BY" {
			p.advance() // consume BY
		}
		for !p.isEOF() && p.current().Type != TokenRightParen {
			if p.current().Type == TokenKeyword && p.current().Literal == "ORDER" {
				break
			}
			expr, e := p.parseExpr()
			if e != nil {
				return nil, nil, nil, e
			}
			partition = append(partition, expr)
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
	}

	// Parse ORDER BY
	if p.current().Type == TokenKeyword && p.current().Literal == "ORDER" {
		p.advance() // consume ORDER
		if p.current().Type == TokenKeyword && p.current().Literal == "BY" {
			p.advance() // consume BY
		}
		for !p.isEOF() && p.current().Type != TokenRightParen {
			if p.current().Type == TokenKeyword && (p.current().Literal == "ROWS" || p.current().Literal == "RANGE") {
				break
			}
			expr, e := p.parseExpr()
			if e != nil {
				return nil, nil, nil, e
			}
			desc := false
			if p.current().Type == TokenKeyword && (p.current().Literal == "ASC" || p.current().Literal == "DESC") {
				desc = p.current().Literal == "DESC"
				p.advance()
			}
			// Skip NULLS FIRST/LAST
			if p.current().Type == TokenKeyword && p.current().Literal == "NULLS" {
				p.advance()
				if p.current().Type == TokenKeyword && (p.current().Literal == "FIRST" || p.current().Literal == "LAST") {
					p.advance()
				}
			}
			orderBy = append(orderBy, WindowOrderBy{Expr: expr, Desc: desc})
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
	}

	// Parse ROWS/RANGE frame spec
	if p.current().Type == TokenKeyword && (p.current().Literal == "ROWS" || p.current().Literal == "RANGE") {
		frameType := p.current().Literal
		p.advance()
		wf := &WindowFrame{Type: frameType}
		// BETWEEN has its own token type (TokenBetween), check by type OR literal
		if p.current().Type == TokenBetween || (p.current().Type == TokenKeyword && p.current().Literal == "BETWEEN") {
			p.advance()
			wf.Start = p.parseFrameBound()
			if p.current().Type == TokenAnd || (p.current().Type == TokenKeyword && p.current().Literal == "AND") {
				p.advance()
			}
			wf.End = p.parseFrameBound()
		} else {
			wf.Start = p.parseFrameBound()
			wf.End = FrameBound{Type: "CURRENT"}
		}
		frame = wf
	}

	p.expect(TokenRightParen)
	return partition, orderBy, frame, nil
}

// parseFrameBound parses a single window frame boundary
func (p *Parser) parseFrameBound() FrameBound {
	if p.current().Type == TokenKeyword && p.current().Literal == "UNBOUNDED" {
		p.advance()
		if p.current().Type == TokenKeyword && (p.current().Literal == "PRECEDING" || p.current().Literal == "FOLLOWING") {
			direction := p.current().Literal
			p.advance()
			if direction == "PRECEDING" {
				return FrameBound{Type: "UNBOUNDED"}
			}
			return FrameBound{Type: "UNBOUNDED_FOLLOWING"}
		}
		return FrameBound{Type: "UNBOUNDED"}
	}
	if p.current().Type == TokenKeyword && p.current().Literal == "CURRENT" {
		p.advance()
		// Consume ROW - it may be a keyword or identifier depending on tokenizer version
		if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) &&
			strings.ToUpper(p.current().Literal) == "ROW" {
			p.advance()
		}
		return FrameBound{Type: "CURRENT"}
	}
	// N PRECEDING or N FOLLOWING
	expr, _ := p.parseExpr()
	if p.current().Type == TokenKeyword && p.current().Literal == "PRECEDING" {
		p.advance()
		return FrameBound{Type: "PRECEDING", Value: expr}
	}
	if p.current().Type == TokenKeyword && p.current().Literal == "FOLLOWING" {
		p.advance()
		return FrameBound{Type: "FOLLOWING", Value: expr}
	}
	return FrameBound{Type: "CURRENT"}
}

// evalConstExpr evaluates a constant expression (one with no column references)
// and returns the result as an interface{}. Used for IN list evaluation.
func evalConstExpr(expr Expr) interface{} {
	switch e := expr.(type) {
	case *Literal:
		return e.Value
	case *UnaryExpr:
		val := evalConstExpr(e.Expr)
		if e.Op == TokenMinus {
			switch v := val.(type) {
			case int64:
				return -v
			case float64:
				return -v
			}
		}
		return val
	case *BinaryExpr:
		left := evalConstExpr(e.Left)
		right := evalConstExpr(e.Right)
		if left == nil || right == nil {
			return nil
		}
		lf, lok := toFloat64Const(left)
		rf, rok := toFloat64Const(right)
		if !lok || !rok {
			return nil
		}
		switch e.Op {
		case TokenPlus:
			r := lf + rf
			if isIntVal(left) && isIntVal(right) {
				return int64(r)
			}
			return r
		case TokenMinus:
			r := lf - rf
			if isIntVal(left) && isIntVal(right) {
				return int64(r)
			}
			return r
		case TokenAsterisk:
			r := lf * rf
			if isIntVal(left) && isIntVal(right) {
				return int64(r)
			}
			return r
		case TokenSlash:
			if rf == 0 {
				return nil
			}
			r := lf / rf
			if isIntVal(left) && isIntVal(right) {
				return int64(r)
			}
			return r
		}
		return nil
	default:
		return nil
	}
}

func toFloat64Const(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

func isIntVal(v interface{}) bool {
	switch v.(type) {
	case int64, int:
		return true
	}
	return false
}

// applyLikeEscape pre-processes a LIKE pattern by converting escape sequences.
// It replaces escapeChar+X with \X (backslash escape used by likeMatchRecursive).
func applyLikeEscape(pattern string, escapeChar rune) string {
	runes := []rune(pattern)
	out := make([]rune, 0, len(runes))
	for i := 0; i < len(runes); i++ {
		if runes[i] == escapeChar {
			if i+1 < len(runes) {
				next := runes[i+1]
				// escape_char followed by any char: treat next char as literal
				out = append(out, '\\', next)
				i++ // skip next char
			} else {
				// Escape char at end of pattern - invalid, never matches
				return "\x00__INVALID_ESCAPE_PATTERN__"
			}
		} else {
			out = append(out, runes[i])
		}
	}
	return string(out)
}
