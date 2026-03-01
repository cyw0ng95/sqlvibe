package QP

import (
	"fmt"
	"strings"
)

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
	case "RENAME":
		p.advance() // consume "RENAME"
		kw2 := strings.ToUpper(p.current().Literal)
		if kw2 == "TO" {
			p.advance()
			stmt.Action = "RENAME_TO"
			stmt.NewName = p.current().Literal
		} else if kw2 == "COLUMN" {
			p.advance() // consume optional COLUMN
			stmt.Column = &ColumnDef{Name: p.current().Literal}
			p.advance() // consume old column name
			if strings.ToUpper(p.current().Literal) == "TO" {
				p.advance()
			}
			stmt.NewName = p.current().Literal
			stmt.Action = "RENAME_COLUMN"
		} else {
			// RENAME <colname> TO <newname> (without COLUMN keyword)
			stmt.Column = &ColumnDef{Name: p.current().Literal}
			p.advance() // consume old column name
			if strings.ToUpper(p.current().Literal) == "TO" {
				p.advance()
			}
			stmt.NewName = p.current().Literal
			stmt.Action = "RENAME_COLUMN"
		}
	case "DROP":
		p.advance() // consume "DROP"
		if strings.ToUpper(p.current().Literal) == "COLUMN" {
			p.advance() // consume optional COLUMN keyword
		}
		stmt.Action = "DROP_COLUMN"
		stmt.Column = &ColumnDef{Name: p.current().Literal}
	case "ADD":
		p.advance() // consume "ADD"
		// Optional COLUMN keyword or CONSTRAINT keyword
		kw2 := strings.ToUpper(p.current().Literal)
		if kw2 == "CONSTRAINT" {
			p.advance() // consume CONSTRAINT
			stmt.ConstraintName = p.current().Literal
			p.advance() // consume constraint name
			kw3 := strings.ToUpper(p.current().Literal)
			if kw3 == "CHECK" {
				p.advance() // consume CHECK
				if p.current().Type == TokenLeftParen {
					p.advance()
					expr, err := p.parseExpr()
					if err == nil {
						stmt.CheckExpr = expr
					}
					if p.current().Type == TokenRightParen {
						p.advance()
					}
				}
				stmt.Action = "ADD_CONSTRAINT"
			} else if kw3 == "UNIQUE" {
				p.advance() // consume UNIQUE
				if p.current().Type == TokenLeftParen {
					p.advance()
					for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
						if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
							stmt.UniqueColumns = append(stmt.UniqueColumns, p.current().Literal)
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
				stmt.Action = "ADD_CONSTRAINT"
			}
		} else {
			if kw2 == "COLUMN" {
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
			for p.current().Type == TokenKeyword || p.current().Type == TokenNot || p.current().Type == TokenCollate {
				keyword := strings.ToUpper(p.current().Literal)
				if p.current().Type == TokenCollate {
					keyword = "COLLATE"
				}
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
				} else if keyword == "COLLATE" {
					p.advance()
					col.Collation = strings.ToUpper(p.current().Literal)
					p.advance()
				} else {
					break
				}
			}
			stmt.Action = "ADD_COLUMN"
			stmt.Column = col
		}
	}
	return stmt, nil
}

// parseStandaloneValues parses a standalone VALUES (v1,...), (v2,...) statement,
// converting it into a SelectStmt with a VALUES table in the FROM clause.
