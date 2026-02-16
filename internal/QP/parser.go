package QP

import (
	"math"
	"strconv"
	"strings"
)

type ASTNode interface {
	NodeType() string
}

type SelectStmt struct {
	Distinct bool
	Columns  []Expr
	From     *TableRef
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderBy
	Limit    Expr
	Offset   Expr
}

func (s *SelectStmt) NodeType() string { return "SelectStmt" }

type OrderBy struct {
	Expr Expr
	Desc bool
}

type TableRef struct {
	Name  string
	Alias string
	Join  *Join
}

func (t *TableRef) NodeType() string { return "TableRef" }

type Join struct {
	Type  string
	Left  *TableRef
	Right *TableRef
	Cond  Expr
}

type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Expr
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
}

type DropTableStmt struct {
	Name string
}

func (d *DropTableStmt) NodeType() string { return "DropTableStmt" }

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
	Name string
	Args []Expr
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

type Parser struct {
	tokens     []Token
	pos        int
	outerAlias string // Track outer query's table alias for subquery correlation
}

func NewParser(tokens []Token) *Parser {
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
		}
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
				ob := OrderBy{Expr: expr, Desc: false}
				if p.current().Literal == "DESC" {
					ob.Desc = true
					p.advance()
				} else if p.current().Literal == "ASC" {
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

	if p.current().Literal == "VALUES" {
		p.advance()
		for {
			if p.current().Type != TokenLeftParen {
				break
			}
			p.advance()

			row := make([]Expr, 0)
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
				stmt.Columns = append(stmt.Columns, col)

				for p.current().Type == TokenKeyword && p.current().Literal != "PRIMARY" && p.current().Literal != "REFERENCES" {
					p.advance()
				}
				if p.current().Type == TokenKeyword && p.current().Literal == "PRIMARY" {
					col.PrimaryKey = true
					p.advance()
					if p.current().Type == TokenKeyword && p.current().Literal == "KEY" {
						p.advance()
					}
				}

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

func (p *Parser) parseDrop() (*DropTableStmt, error) {
	p.advance()

	if p.current().Literal == "TABLE" {
		p.advance()
		return &DropTableStmt{Name: p.current().Literal}, nil
	}

	return nil, nil
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

	for p.current().Type == TokenPlus || p.current().Type == TokenMinus {
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
		if p.current().Type == TokenDot {
			p.advance()
			if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
				colName = tok.Literal + "." + p.current().Literal
				p.advance()
			}
		}

		return &ColumnRef{Name: colName}, nil
	case TokenAsterisk:
		p.advance()
		return &ColumnRef{Name: "*"}, nil
	case TokenKeyword:
		if tok.Literal == "AVG" || tok.Literal == "MIN" || tok.Literal == "MAX" || tok.Literal == "COUNT" || tok.Literal == "SUM" || tok.Literal == "COALESCE" || tok.Literal == "IFNULL" {
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
		}
		if tok.Literal == "NULL" {
			p.advance()
			return &Literal{Value: nil}, nil
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
