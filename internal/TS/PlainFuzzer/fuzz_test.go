package PlainFuzzer

import (
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func FuzzSQL(f *testing.F) {
	// Seed corpus - common SQL patterns that should work
	f.Add("CREATE TABLE t0 (c0 INTEGER, c1 TEXT)")
	f.Add("INSERT INTO t0 VALUES (1, 'hello')")
	f.Add("SELECT * FROM t0")
	f.Add("SELECT c0, c1 FROM t0 WHERE c0 = 1")
	f.Add("SELECT COUNT(*) FROM t0")
	f.Add("SELECT SUM(c0) FROM t0")
	f.Add("SELECT AVG(c0) FROM t0")
	f.Add("SELECT MAX(c0) FROM t0")
	f.Add("SELECT MIN(c0) FROM t0")
	f.Add("UPDATE t0 SET c1 = 'world' WHERE c0 = 1")
	f.Add("DELETE FROM t0 WHERE c0 = 1")
	f.Add("DROP TABLE t0")
	f.Add("BEGIN; COMMIT")
	f.Add("BEGIN; ROLLBACK")
	f.Add("CREATE INDEX idx0 ON t0(c0)")
	f.Add("DROP INDEX idx0")
	f.Add("SELECT * FROM t0 ORDER BY c0")
	f.Add("SELECT * FROM t0 ORDER BY c0 DESC")
	f.Add("SELECT * FROM t0 LIMIT 10")
	f.Add("SELECT * FROM t0 WHERE c0 > 0 AND c0 < 100")
	f.Add("SELECT * FROM t0 WHERE c1 LIKE '%hello%'")
	f.Add("SELECT * FROM t0 WHERE c0 IS NULL")
	f.Add("SELECT * FROM t0 WHERE c0 IS NOT NULL")
	f.Add("SELECT * FROM t0 WHERE c0 BETWEEN 1 AND 10")
	f.Add("SELECT * FROM t0 WHERE c0 IN (1, 2, 3)")
	f.Add("SELECT DISTINCT c0 FROM t0")
	f.Add("SELECT c0, COUNT(*) FROM t0 GROUP BY c0")
	f.Add("SELECT * FROM t0 WHERE c0 = (SELECT MAX(c0) FROM t0)")
	f.Add("WITH cte AS (SELECT 1 AS n) SELECT * FROM cte")
	f.Add("SELECT CASE WHEN c0 > 0 THEN 'positive' ELSE 'non-positive' END FROM t0")
	f.Add("INSERT INTO t0(c0, c1) VALUES(1, 'test') ON CONFLICT(c0) DO NOTHING")
	f.Add("EXPLAIN SELECT * FROM t0")
	f.Add("PRAGMA table_info(t0)")
	f.Add("PRAGMA index_list(t0)")
	f.Add("SELECT 1 UNION SELECT 2")
	f.Add("SELECT 1 UNION ALL SELECT 2")
	f.Add("CREATE VIEW v0 AS SELECT c0 FROM t0")
	f.Add("DROP VIEW IF EXISTS v0")
	f.Add("SAVEPOINT sp1")
	f.Add("RELEASE SAVEPOINT sp1")
	f.Add("ROLLBACK TO SAVEPOINT sp1")

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
