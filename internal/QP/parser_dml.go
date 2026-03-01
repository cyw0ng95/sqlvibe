package QP

import (
	"fmt"
	"strings"
)

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

	// Parse optional RETURNING clause
	returning, err := p.parseReturning()
	if err != nil {
		return nil, err
	}
	stmt.Returning = returning

	return stmt, nil
}

// parseReturning parses an optional RETURNING clause.
func (p *Parser) parseReturning() ([]Expr, error) {
	if p.current().Type != TokenKeyword || strings.ToUpper(p.current().Literal) != "RETURNING" {
		return nil, nil
	}
	p.advance()
	// RETURNING * returns all columns (represented as a single star ColumnRef)
	if p.current().Type == TokenAsterisk {
		p.advance()
		return []Expr{&ColumnRef{Name: "*"}}, nil
	}
	var exprs []Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.current().Type != TokenComma {
			break
		}
		p.advance()
	}
	return exprs, nil
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

	// Parse optional FROM clause (PostgreSQL-style UPDATE ... FROM t2)
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "FROM" {
		p.advance()
		ref := p.parseTableRef()
		stmt.From = ref
	}

	if p.current().Literal == "WHERE" {
		p.advance()
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse optional RETURNING clause
	returning, err := p.parseReturning()
	if err != nil {
		return nil, err
	}
	stmt.Returning = returning

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

	// Parse optional USING clause (multi-table DELETE)
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "USING" {
		p.advance()
		for {
			stmt.Using = append(stmt.Using, p.current().Literal)
			p.advance()
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

	// Parse optional RETURNING clause
	returning, err := p.parseReturning()
	if err != nil {
		return nil, err
	}
	stmt.Returning = returning

	return stmt, nil
}
