package QP

import (
	"fmt"
	"strings"
)

func (p *Parser) parseStandaloneValues() (ASTNode, error) {
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
				return nil, e
			}
			if expr == nil {
				break
			}
			row = append(row, expr)
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
		if p.current().Type == TokenRightParen {
			p.advance()
		}
		rows = append(rows, row)
		if p.current().Type != TokenComma {
			break
		}
		p.advance() // consume comma between rows
	}
	stmt := &SelectStmt{
		Columns: []Expr{&ColumnRef{Name: "*"}},
		From:    &TableRef{Values: rows, Alias: "__values__"},
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

func (p *Parser) parseExplain() (ASTNode, error) {
	p.advance()
	explain := &ExplainStmt{}

	isQueryPlan := false
	isAnalyze := false

	// Check for QUERY PLAN or ANALYZE keywords
	if p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier {
		lit := strings.ToUpper(p.current().Literal)
		if lit == "QUERY" {
			p.advance()
			if (p.current().Type == TokenKeyword || p.current().Type == TokenIdentifier) && strings.EqualFold(p.current().Literal, "PLAN") {
				p.advance()
				isQueryPlan = true
			}
		} else if lit == "ANALYZE" {
			p.advance()
			isAnalyze = true
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
	explain.Analyze = isAnalyze
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

	stmt := &RollbackStmt{}

	// Optional TRANSACTION keyword
	if p.current().Type == TokenKeyword && p.current().Literal == "TRANSACTION" {
		p.advance()
	}

	// ROLLBACK TO [SAVEPOINT] sp_name
	if p.current().Type == TokenKeyword && p.current().Literal == "TO" {
		p.advance() // consume TO
		// Optional SAVEPOINT keyword
		if p.current().Type == TokenKeyword && p.current().Literal == "SAVEPOINT" {
			p.advance()
		}
		if p.current().Type != TokenIdentifier && p.current().Type != TokenKeyword {
			return nil, fmt.Errorf("expected savepoint name after ROLLBACK TO")
		}
		stmt.Savepoint = p.current().Literal
		p.advance()
	}

	return stmt, nil
}

func (p *Parser) parseSavepoint() (ASTNode, error) {
	p.advance() // consume SAVEPOINT
	if p.current().Type != TokenIdentifier && p.current().Type != TokenKeyword {
		return nil, fmt.Errorf("expected savepoint name after SAVEPOINT")
	}
	name := p.current().Literal
	p.advance()
	return &SavepointStmt{Name: name}, nil
}

func (p *Parser) parseRelease() (ASTNode, error) {
	p.advance() // consume RELEASE
	// Optional SAVEPOINT keyword
	if p.current().Type == TokenKeyword && p.current().Literal == "SAVEPOINT" {
		p.advance()
	}
	if p.current().Type != TokenIdentifier && p.current().Type != TokenKeyword {
		return nil, fmt.Errorf("expected savepoint name after RELEASE")
	}
	name := p.current().Literal
	p.advance()
	return &ReleaseSavepointStmt{Name: name}, nil
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

func (p *Parser) parseReindex() (ASTNode, error) {
	p.advance() // consume REINDEX
	stmt := &ReindexStmt{}
	cur := p.current()
	if cur.Type == TokenIdentifier || cur.Type == TokenKeyword {
		stmt.Target = cur.Literal
		p.advance()
	}
	return stmt, nil
}

// parseWithClause parses a WITH ... AS (...) SELECT statement (CTE)
