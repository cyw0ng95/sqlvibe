package PlainFuzzer

import (
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func FuzzSQL(f *testing.F) {
	f.Fuzz(func(t *testing.T, query string) {
		if len(query) == 0 || len(query) > 3000 {
			t.Skip()
		}

		db, err := sqlvibe.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to open sqlvibe: %v", err)
		}
		defer db.Close()

		statements := splitStatements(query)
		maxStmts := 10
		if len(statements) > maxStmts {
			statements = statements[:maxStmts]
		}

		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if len(stmt) == 0 {
				continue
			}
			executeSQL(db, stmt)
		}
	})
}

func splitStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	parenDepth := 0

	for _, ch := range sql {
		switch ch {
		case '(':
			parenDepth++
			current.WriteRune(ch)
		case ')':
			parenDepth--
			current.WriteRune(ch)
		case ';':
			if parenDepth == 0 {
				stmt := current.String()
				if strings.TrimSpace(stmt) != "" {
					statements = append(statements, stmt)
				}
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		stmt := current.String()
		if strings.TrimSpace(stmt) != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

func executeSQL(db *sqlvibe.Database, query string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	if len(upperQuery) >= 6 {
		prefix := upperQuery[:6]
		if prefix == "SELECT" || prefix == "PRAGMA" {
			db.Query(query)
			return nil
		}
	}
	db.Exec(query)
	return nil
}
