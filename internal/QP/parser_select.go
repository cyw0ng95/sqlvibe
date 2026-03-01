package QP

import (
	"fmt"
	"strings"
)

func (p *Parser) parseFuncArgList() ([]Expr, error) {
	p.advance() // consume (
	var args []Expr
	for p.current().Type != TokenRightParen && !p.isEOF() {
		argPos := p.pos
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.pos == argPos {
			break
		}
		if arg != nil {
			args = append(args, arg)
		}
		if p.current().Type == TokenComma {
			p.advance()
		} else if p.current().Type != TokenRightParen {
			break
		}
	}
	if p.current().Type == TokenRightParen {
		p.advance()
	}
	return args, nil
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
					exprPos := p.pos
					expr, e := p.parseExpr()
					if e != nil {
						p.parseError = e
						return ref
					}
					// Safety: if parseExpr didn't advance, break to prevent infinite loop
					if p.pos == exprPos {
						break
					}
					if expr == nil {
						break
					}
					row = append(row, expr)
					if p.current().Type == TokenComma {
						p.advance()
					} else if p.current().Type != TokenRightParen {
						break
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
					colPos := p.pos
					if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
						ref.ValueCols = append(ref.ValueCols, p.current().Literal)
						p.advance()
					} else {
						// Safety: if didn't advance, break to prevent infinite loop
						if p.pos == colPos {
							break
						}
						break
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
		// Recovery: skip tokens until we find ) or EOF
		for p.current().Type != TokenRightParen && p.current().Type != TokenEOF && p.current().Type != TokenSemicolon {
			p.advance()
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
	} else {
		ref.Name = p.current().Literal
		p.advance()

		// Check for table-valued function: name(args...) [AS alias]
		if p.current().Type == TokenLeftParen {
			args, err := p.parseFuncArgList()
			if err != nil {
				p.parseError = err
				return ref
			}
			ref.TableFunc = &FuncCall{Name: ref.Name, Args: args}
			ref.Name = ""
			// Parse optional alias: either "AS name" or just a bare identifier.
			// Never treat unquoted SQL keywords (WHERE, ORDER, LIMIT, ...) as an
			// implicit alias unless they are preceded by an explicit AS keyword.
			hasExplicitAS := false
			if p.current().Type == TokenKeyword && p.current().Literal == "AS" {
				p.advance()
				hasExplicitAS = true
			}
			if p.current().Type == TokenIdentifier || (hasExplicitAS && p.current().Type == TokenKeyword) {
				ref.Alias = p.current().Literal
				p.advance()
				// Optional column list: (col1, col2, ...)
				if p.current().Type == TokenLeftParen {
					p.advance()
					for p.current().Type != TokenRightParen && !p.isEOF() {
						ref.TableFuncCols = append(ref.TableFuncCols, p.current().Literal)
						p.advance()
						if p.current().Type == TokenComma {
							p.advance()
						}
					}
					if p.current().Type == TokenRightParen {
						p.advance()
					}
				}
			}
			return ref
		}

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
				break
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
					if litUpper == "FROM" || litUpper == "INTO" || litUpper == "WHERE" || litUpper == "ORDER" || litUpper == "GROUP" || litUpper == "HAVING" || litUpper == "LIMIT" {
						break
					}
				}
				break
			}
			p.advance()
		}
	}

	// Handle SELECT ... INTO tablename (SELECT INTO creates a new table)
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "INTO" {
		p.advance() // consume INTO
		stmt.IntoTable = p.current().Literal
		p.advance() // consume table name
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

	// ORDER BY before UNION/EXCEPT/INTERSECT is invalid SQL
	if stmt.OrderBy != nil && (p.current().Literal == "UNION" || p.current().Literal == "EXCEPT" || p.current().Literal == "INTERSECT") {
		return nil, fmt.Errorf("ORDER BY clause should come after %s not before", p.current().Literal)
	}

	// Parse WINDOW clause: WINDOW name AS (window_spec), ...
	if strings.ToUpper(p.current().Literal) == "WINDOW" {
		p.advance() // consume WINDOW
		for {
			if p.current().Type != TokenIdentifier {
				break
			}
			name := p.current().Literal
			p.advance()

			// Expect AS
			if strings.ToUpper(p.current().Literal) != "AS" {
				break
			}
			p.advance() // consume AS

			// Parse window specification
			partition, orderBy, frame, err := p.parseWindowSpec()
			if err != nil {
				return nil, err
			}

			stmt.Windows = append(stmt.Windows, WindowDef{
				Name:      name,
				Partition: partition,
				OrderBy:   orderBy,
				Frame:     frame,
			})

			// Check for comma (multiple windows)
			if p.current().Type != TokenComma {
				break
			}
			p.advance()
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

	// SQL:2003 FETCH FIRST / FETCH NEXT syntax (equivalent to LIMIT)
	if strings.ToUpper(p.current().Literal) == "FETCH" {
		p.advance() // consume FETCH
		// accept FIRST or NEXT
		kw := strings.ToUpper(p.current().Literal)
		if kw == "FIRST" || kw == "NEXT" {
			p.advance()
		}
		if stmt.Limit == nil {
			limit, err := p.parseExpr()
			if err == nil {
				stmt.Limit = limit
			}
		} else {
			// skip the count expression
			p.parseExpr() //nolint
		}
		// accept ROW or ROWS
		kw = strings.ToUpper(p.current().Literal)
		if kw == "ROW" || kw == "ROWS" {
			p.advance()
		}
		// accept ONLY or WITH TIES (WITH TIES not yet fully supported — treat as ONLY)
		kw = strings.ToUpper(p.current().Literal)
		if kw == "ONLY" {
			p.advance()
		} else if kw == "WITH" {
			p.advance()
			if strings.ToUpper(p.current().Literal) == "TIES" {
				p.advance()
			}
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
				colPos := p.pos
				if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
					cteCols = append(cteCols, p.current().Literal)
					p.advance()
				} else {
					// Safety: if didn't advance, break to prevent infinite loop
					if p.pos == colPos {
						break
					}
					break
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

// parseWindowSpecOrName parses either a named window reference or a window specification.
// Named window: OVER window_name
// Window spec: OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
func (p *Parser) parseWindowSpecOrName() (partition []Expr, orderBy []WindowOrderBy, frame *WindowFrame, err error) {
	// Check if this is a named window reference (identifier without parentheses)
	if p.current().Type == TokenIdentifier {
		_ = p.current().Literal // window name - will be resolved later from WINDOW clause
		p.advance()
		// Return nil for partition/orderBy/frame - will be resolved later from the WINDOW clause
		// For now, we just note the window name by returning empty spec
		return nil, nil, nil, nil
	}
	// Otherwise, parse the inline window specification
	return p.parseWindowSpec()
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
			exprPos := p.pos
			expr, e := p.parseExpr()
			if e != nil {
				return nil, nil, nil, e
			}
			// Safety: if parseExpr didn't advance, break to prevent infinite loop
			if p.pos == exprPos {
				break
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
			exprPos := p.pos
			expr, e := p.parseExpr()
			if e != nil {
				return nil, nil, nil, e
			}
			// Safety: if parseExpr didn't advance, break to prevent infinite loop
			if p.pos == exprPos {
				break
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
