package QP

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

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

	// MATCH operator: case-insensitive substring match
	if p.current().Type == TokenMatch {
		p.advance()
		pattern, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: TokenMatch, Left: left, Right: pattern}, nil
	}
	if p.current().Type == TokenNot && p.peek().Type == TokenMatch {
		return nil, fmt.Errorf("NOT MATCH is not supported")
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
				if p.current().Type == TokenEOF {
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if expr == nil {
					break
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				} else if p.current().Type != TokenRightParen {
					break
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
				if p.current().Type == TokenEOF {
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if expr == nil {
					break
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				} else if p.current().Type != TokenRightParen {
					// Unexpected token - try to recover by breaking
					break
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
				if p.current().Type == TokenEOF {
					break
				}
				expr, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if expr == nil {
					break
				}
				values = append(values, evalConstExpr(expr))
				if p.current().Type == TokenComma {
					p.advance()
				} else if p.current().Type != TokenRightParen {
					// Unexpected token - try to recover by breaking
					break
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

	for p.current().Type == TokenPlus || p.current().Type == TokenMinus || p.current().Type == TokenConcat ||
		p.current().Type == TokenArrow || p.current().Type == TokenArrowText {
		op := p.current().Type
		p.advance()
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		if op == TokenArrow || op == TokenArrowText {
			// a -> b  and  a ->> b  both map to json_extract(a, b)
			left = &FuncCall{Name: "json_extract", Args: []Expr{left, right}}
		} else {
			left = &BinaryExpr{Op: op, Left: left, Right: right}
		}
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

	return p.parsePrimaryExprWithCollate()
}

// parsePrimaryExprWithCollate parses a primary expression optionally followed by COLLATE name
func (p *Parser) parsePrimaryExprWithCollate() (Expr, error) {
	expr, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}
	if p.current().Type == TokenCollate {
		p.advance()
		collation := strings.ToUpper(p.current().Literal)
		p.advance()
		return &CollateExpr{Expr: expr, Collation: collation}, nil
	}
	return expr, nil
}

func (p *Parser) parsePrimaryExpr() (Expr, error) {
	tok := p.current()

	switch tok.Type {
	case TokenPlaceholderPos:
		p.advance()
		return &PlaceholderExpr{Positional: true}, nil
	case TokenPlaceholderNamed:
		name := tok.Literal
		p.advance()
		return &PlaceholderExpr{Positional: false, Name: name}, nil
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
				argPos := p.pos
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				// Safety: if parseExpr didn't advance, break to prevent infinite loop
				if p.pos == argPos {
					break
				}
				if arg == nil {
					break
				}
				args = append(args, arg)
				if p.current().Type == TokenComma {
					p.advance()
				} else if p.current().Type != TokenRightParen {
					// Unexpected token - try to recover by breaking
					break
				}
			}
			p.expect(TokenRightParen)
			// Check for OVER clause (window function) - handles identifiers like PERCENT_RANK, CUME_DIST
			if p.current().Type == TokenKeyword && p.current().Literal == "OVER" {
				p.advance() // consume OVER
				partition, orderBy, frame, err := p.parseWindowSpecOrName()
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
					argPos := p.pos
					arg, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					// Safety: if parseExpr didn't advance, break to prevent infinite loop
					if p.pos == argPos {
						break
					}
					if arg == nil {
						break
					}
					args = append(args, arg)
					if p.current().Type == TokenComma {
						p.advance()
					} else if p.current().Type != TokenRightParen {
						break
					}
				}
				p.expect(TokenRightParen)
				// Check for OVER clause (window function)
				if p.current().Type == TokenKeyword && p.current().Literal == "OVER" {
					p.advance() // consume OVER
					partition, orderBy, frame, err := p.parseWindowSpecOrName()
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
				argPos := p.pos
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				// Safety: if parseExpr didn't advance, break to prevent infinite loop
				if p.pos == argPos {
					break
				}
				if arg == nil {
					break
				}
				args = append(args, arg)
				if p.current().Type == TokenComma {
					p.advance()
				} else if p.current().Type != TokenRightParen {
					break
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

