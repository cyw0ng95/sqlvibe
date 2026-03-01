package QP

import (
	"fmt"
	"strings"
)

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

	// Handle CREATE VIRTUAL TABLE
	if strings.ToUpper(p.current().Literal) == "VIRTUAL" {
		return p.parseCreateVirtualTable()
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
				for p.current().Type == TokenKeyword || p.current().Type == TokenNot || p.current().Type == TokenCollate {
					var keyword string
					if p.current().Type == TokenNot {
						keyword = "NOT"
					} else if p.current().Type == TokenCollate {
						keyword = "COLLATE"
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
						col.Unique = true
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
					} else if keyword == "COLLATE" {
						// COLLATE collation_name
						p.advance()
						col.Collation = strings.ToUpper(p.current().Literal)
						p.advance()
					} else if keyword == "GENERATED" {
						// GENERATED ALWAYS AS (expr) [STORED|VIRTUAL]
						p.advance()
						if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "ALWAYS" {
							p.advance()
						}
						if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "AS" {
							p.advance()
						}
						if p.current().Type == TokenLeftParen {
							p.advance()
							genExpr, err := p.parseExpr()
							if err != nil {
								return nil, err
							}
							col.GeneratedExpr = genExpr
							if p.current().Type == TokenRightParen {
								p.advance()
							}
						}
						// Check for STORED or VIRTUAL
						if p.current().Type == TokenKeyword {
							kw := strings.ToUpper(p.current().Literal)
							if kw == "STORED" {
								col.GeneratedStored = true
								p.advance()
							} else if kw == "VIRTUAL" {
								col.GeneratedStored = false
								p.advance()
							}
						}
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
							p.advance()
							var ukCols []string
							for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
								if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
									ukCols = append(ukCols, p.current().Literal)
								}
								p.advance()
								if p.current().Type == TokenComma {
									p.advance()
								}
							}
							if p.current().Type == TokenRightParen {
								p.advance()
							}
							if len(ukCols) > 0 {
								stmt.UniqueKeys = append(stmt.UniqueKeys, ukCols)
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
			bodyPos := p.pos
			bodyStmt, err := p.parseInternal()
			if err != nil {
				return nil, err
			}
			// Safety: if parseInternal didn't advance, break to prevent infinite loop
			if p.pos == bodyPos {
				break
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

// parseCreateVirtualTable parses CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module[(arg, ...)].
func (p *Parser) parseCreateVirtualTable() (ASTNode, error) {
	// consume VIRTUAL
	p.advance()
	// expect TABLE
	if strings.ToUpper(p.current().Literal) != "TABLE" {
		return nil, fmt.Errorf("expected TABLE after VIRTUAL, got %q", p.current().Literal)
	}
	p.advance()

	stmt := &CreateVirtualTableStmt{}

	// optional IF NOT EXISTS
	if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "IF" {
		p.advance()
		if p.current().Type == TokenNot {
			p.advance()
			if p.current().Type == TokenExists {
				stmt.IfNotExists = true
				p.advance()
			}
		}
	}

	// table name
	stmt.TableName = p.current().Literal
	p.advance()

	// expect USING
	if strings.ToUpper(p.current().Literal) != "USING" {
		return nil, fmt.Errorf("expected USING after table name in CREATE VIRTUAL TABLE, got %q", p.current().Literal)
	}
	p.advance()

	// module name
	stmt.ModuleName = p.current().Literal
	p.advance()

	// optional (arg1, arg2, ...)
	if p.current().Type == TokenLeftParen {
		p.advance()
		for p.current().Type != TokenRightParen && p.current().Type != TokenEOF {
			stmt.ModuleArgs = append(stmt.ModuleArgs, p.current().Literal)
			p.advance()
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
		if p.current().Type == TokenRightParen {
			p.advance()
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
				// Check if this is an expression index (identifier followed by '(')
				if (p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword) &&
					p.peek().Type == TokenLeftParen {
					// Expression column (e.g., LOWER(col))
					expr, err := p.parsePrimaryExpr()
					if err != nil {
						return nil, err
					}
					stmt.Columns = append(stmt.Columns, "")
					stmt.Exprs = append(stmt.Exprs, expr)
				} else if p.current().Type == TokenIdentifier || p.current().Type == TokenKeyword {
					colName := p.current().Literal
					p.advance()
					// Skip ASC/DESC
					if p.current().Type == TokenKeyword && (strings.ToUpper(p.current().Literal) == "ASC" || strings.ToUpper(p.current().Literal) == "DESC") {
						p.advance()
					}
					stmt.Columns = append(stmt.Columns, colName)
					stmt.Exprs = append(stmt.Exprs, nil)
				}
				if p.current().Type == TokenComma {
					p.advance()
				} else {
					break
				}
			}
			p.expect(TokenRightParen)
		}

		// Parse optional WHERE clause (partial index)
		if p.current().Type == TokenKeyword && strings.ToUpper(p.current().Literal) == "WHERE" {
			p.advance()
			whereExpr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.WhereExpr = whereExpr
		}
	}

	return stmt, nil
}

