package QP

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/util"
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
}

func (s *SelectStmt) NodeType() string { return "SelectStmt" }

type OrderBy struct {
	Expr  Expr
	Desc  bool
	Nulls string // "FIRST", "LAST", or ""
}

type TableRef struct {
	Schema string
	Name   string
	Alias  string
	Join   *Join
}

func (t *TableRef) NodeType() string { return "TableRef" }

type Join struct {
	Type  string
	Left  *TableRef
	Right *TableRef
	Cond  Expr
}

type InsertStmt struct {
	Table       string
	Columns     []string
	Values      [][]Expr
	UseDefaults bool // True when using DEFAULT VALUES
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

type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	IfNotExists bool
}

func (c *CreateTableStmt) NodeType() string { return "CreateTableStmt" }

type ColumnDef struct {
	Name       string
	Type       string
	PrimaryKey bool
	NotNull    bool
	Default    Expr
	Check      Expr // CHECK constraint expression
}

type DropTableStmt struct {
	Name string
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
	Query ASTNode
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
	}

	if p.current().Type == TokenAsterisk {
		p.advance()
		stmt.Columns = []Expr{&ColumnRef{Name: "*"}}
	} else {
		for {
			col, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if col == nil {
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.current().Type != TokenComma {
				// Check if we've hit a keyword that signals end of columns
				if p.current().Type == TokenKeyword {
					lit := p.current().Literal
					litUpper := strings.ToUpper(lit)
					if litUpper == "FROM" || litUpper == "WHERE" || litUpper == "ORDER" || litUpper == "GROUP" || litUpper == "HAVING" || litUpper == "LIMIT" || litUpper == "AS" {
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
		ref := &TableRef{Name: p.current().Literal}
		p.advance()

		// Check for schema.table notation
		if p.current().Type == TokenDot {
			p.advance()
			// Previous name was actually the schema
			ref.Schema = ref.Name
			ref.Name = p.current().Literal
			p.advance()
		}

		// Check for table alias (with or without AS keyword)
		if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
			p.advance() // consume AS
		}
		if p.current().Type == TokenIdentifier {
			ref.Alias = p.current().Literal
			p.advance()
		}
		stmt.From = ref

		for p.current().Type == TokenKeyword && (p.current().Literal == "INNER" || p.current().Literal == "LEFT" || p.current().Literal == "CROSS" || p.current().Literal == "JOIN") {
			join := &Join{}

			if p.current().Literal == "INNER" || p.current().Literal == "LEFT" || p.current().Literal == "CROSS" {
				join.Type = p.current().Literal
				p.advance()
			}

			if p.current().Literal == "JOIN" {
				p.advance()
			}

			rightTable := &TableRef{Name: p.current().Literal}
			p.advance()

			// Check for schema.table notation in JOIN
			if p.current().Type == TokenDot {
				p.advance()
				rightTable.Schema = rightTable.Name
				rightTable.Name = p.current().Literal
				p.advance()
			}

			// Check for table alias in JOIN (with or without AS keyword)
			if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
				p.advance() // consume AS
			}
			if p.current().Type == TokenIdentifier {
				rightTable.Alias = p.current().Literal
				p.advance()
			}
			join.Right = rightTable

			if p.current().Literal == "ON" {
				p.advance()
				cond, err := p.parseExpr()
				if err == nil {
					join.Cond = cond
				}
			}

			join.Left = ref
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
				expr, err := p.parseExpr()
				if err != nil {
					break
				}
				ob := OrderBy{Expr: expr, Desc: false, Nulls: ""}
				if p.current().Literal == "DESC" {
					ob.Desc = true
					p.advance()
				} else if p.current().Literal == "ASC" {
					p.advance()
				}
				if p.current().Literal == "NULLS" {
					p.advance()
					if p.current().Literal == "FIRST" {
						ob.Nulls = "FIRST"
						p.advance()
					} else if p.current().Literal == "LAST" {
						ob.Nulls = "LAST"
						p.advance()
					}
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
		if p.current().Literal == "ALL" {
			stmt.SetOpAll = true
			p.advance()
		}
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.SetOpRight = right
	}

	return stmt, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{Values: make([][]Expr, 0)}

	if p.current().Type != TokenKeyword || p.current().Literal != "INSERT" {
		return nil, nil
	}
	p.advance()

	if p.current().Literal == "INTO" {
		p.advance()
	}
	stmt.Table = p.current().Literal
	p.advance()

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

	if p.current().Literal == "TABLE" {
		p.advance()
		stmt := &CreateTableStmt{}
		if p.current().Type == TokenKeyword && p.current().Literal == "IF" {
			p.advance()
			if p.current().Type == TokenKeyword && p.current().Literal == "NOT" {
				p.advance()
				if p.current().Type == TokenKeyword && p.current().Literal == "EXISTS" {
					stmt.IfNotExists = true
					p.advance()
				}
			}
		}
		stmt.Name = p.current().Literal
		p.advance()

		if p.current().Type == TokenLeftParen {
			p.advance()
			for {
				col := ColumnDef{
					Name: p.current().Literal,
				}
				p.advance()
				if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
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
						// Skip FOREIGN KEY reference for now
						p.advance()
						if p.current().Type == TokenIdentifier {
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
					} else {
						// Stop at unknown keywords or table-level constraints
						break
					}
				}

				stmt.Columns = append(stmt.Columns, col)

				if p.current().Type != TokenComma {
					break
				}
				p.advance()
			}
			p.expect(TokenRightParen)
		}

		return stmt, nil
	}

	return nil, nil
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
		return &DropTableStmt{Name: p.current().Literal}, nil
	}

	if p.current().Literal == "INDEX" {
		p.advance()
		return &DropIndexStmt{Name: p.current().Literal}, nil
	}

	return nil, nil
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
				if lit, ok := expr.(*Literal); ok {
					values = append(values, lit.Value)
				} else {
					values = append(values, nil)
				}
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
				if lit, ok := expr.(*Literal); ok {
					values = append(values, lit.Value)
				} else {
					values = append(values, nil)
				}
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
				if lit, ok := expr.(*Literal); ok {
					values = append(values, lit.Value)
				} else {
					values = append(values, nil)
				}
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
		(p.peek().Type == TokenKeyword && (p.peek().Literal == "ALL" || p.peek().Literal == "ANY" || p.peek().Literal == "SOME")) {
		op := p.current().Type
		p.advance()
		quantifier := p.current().Literal
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
				var quantOp TokenType
				if quantifier == "ALL" {
					quantOp = TokenAll
				} else {
					quantOp = TokenAny
				}
				return &BinaryExpr{Op: quantOp, Left: left, Right: &BinaryExpr{Op: op, Right: &SubqueryExpr{Select: sub}}}, nil
			}
		}
	}

	if p.current().Type == TokenLike {
		p.advance()
		pattern, err := p.parseAddExpr()
		if err != nil {
			return nil, err
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
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
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
		if tok.Literal == "AVG" || tok.Literal == "MIN" || tok.Literal == "MAX" || tok.Literal == "COUNT" || tok.Literal == "SUM" || tok.Literal == "COALESCE" || tok.Literal == "IFNULL" || tok.Literal == "NULLIF" {
			p.advance()
			if p.current().Type == TokenLeftParen {
				p.advance()

				// Handle DISTINCT or ALL keywords in aggregate functions
				distinct := false
				if p.current().Type == TokenKeyword && p.current().Literal == "DISTINCT" {
					distinct = true
					p.advance()
				} else if p.current().Type == TokenAll {
					p.advance()
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
				return &FuncCall{Name: tok.Literal, Args: args, Distinct: distinct}, nil
			}
		}
		if tok.Literal == "NULL" {
			p.advance()
			return &Literal{Value: nil}, nil
		}
		if tok.Literal == "CURRENT_DATE" || tok.Literal == "CURRENT_TIME" || tok.Literal == "CURRENT_TIMESTAMP" || tok.Literal == "LOCALTIME" || tok.Literal == "LOCALTIMESTAMP" {
			p.advance()
			return &FuncCall{Name: tok.Literal, Args: []Expr{}}, nil
		}
		if tok.Literal == "CASE" {
			return p.parseCaseExpr()
		}
		p.advance()
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
	if p.current().Type == TokenKeyword && p.current().Literal == "QUERY" {
		p.advance()
		if p.current().Type == TokenKeyword && p.current().Literal == "PLAN" {
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

	_ = isQueryPlan
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
