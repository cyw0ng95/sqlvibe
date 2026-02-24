package PlainFuzzer

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
)

const (
	defaultTimeout = 2 * time.Second
)

func FuzzSQL(f *testing.F) {
	// Seed corpus - comprehensive SQL patterns covering edge cases
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

	// ===== EDGE CASES: Empty/Minimal =====
	f.Add("")
	f.Add("SELECT")
	f.Add("SELECT 1")
	f.Add("SELECT NULL")
	f.Add("SELECT 1, 2, 3")
	f.Add("SELECT 'hello'")
	f.Add("SELECT -1")
	f.Add("SELECT -1.5")
	f.Add("SELECT 0")

	// ===== EDGE CASES: Malformed SQL (known bugs) =====
	f.Add("SELECT IN(c")   // IN clause infinite loop
	f.Add("SELECT MAX(0;") // function args infinite loop

	// ===== EDGE CASES: Operators =====
	f.Add("SELECT 1 + 2")
	f.Add("SELECT 1 - 2")
	f.Add("SELECT 1 * 2")
	f.Add("SELECT 1 / 2")
	f.Add("SELECT 1 % 2")
	f.Add("SELECT 1 || 2")
	f.Add("SELECT 1 = 1")
	f.Add("SELECT 1 != 1")
	f.Add("SELECT 1 < 1")
	f.Add("SELECT 1 > 1")
	f.Add("SELECT 1 <= 1")
	f.Add("SELECT 1 >= 1")
	f.Add("SELECT NOT 1")
	f.Add("SELECT 1 AND 0")
	f.Add("SELECT 1 OR 0")
	f.Add("SELECT 1 IS TRUE")
	f.Add("SELECT 0 IS FALSE")
	f.Add("SELECT NULL IS NULL")
	f.Add("SELECT 1 IS NOT NULL")

	// ===== EDGE CASES: Functions =====
	f.Add("SELECT ABS(-1)")
	f.Add("SELECT LENGTH('hello')")
	f.Add("SELECT UPPER('hello')")
	f.Add("SELECT LOWER('HELLO')")
	f.Add("SELECT SUBSTR('hello', 1, 2)")
	f.Add("SELECT TRIM('  hello  ')")
	f.Add("SELECT LTRIM('  hello')")
	f.Add("SELECT RTRIM('hello  ')")
	f.Add("SELECT REPLACE('hello', 'l', 'r')")
	f.Add("SELECT INSTR('hello', 'l')")
	f.Add("SELECT COALESCE(NULL, 'default')")
	f.Add("SELECT IFNULL(NULL, 'default')")
	f.Add("SELECT NULLIF(1, 1)")
	f.Add("SELECT IIF(1>0, 'yes', 'no')")
	f.Add("SELECT PRINTF('num: %d', 42)")
	f.Add("SELECT HEX('abc')")
	f.Add("SELECT QUOTE('hello')")
	f.Add("SELECT CHAR(72, 73)")
	f.Add("SELECT UNICODE('A')")
	f.Add("SELECT RANDOM()")
	f.Add("SELECT ZEROBLOB(10)")
	f.Add("SELECT UNHEX('48656c6c6f')")

	// ===== EDGE CASES: Aggregates =====
	f.Add("SELECT COUNT(*), SUM(c0), AVG(c0), MIN(c0), MAX(c0) FROM t0")
	f.Add("SELECT COUNT(DISTINCT c0) FROM t0")
	f.Add("SELECT GROUP_CONCAT(c1) FROM t0")
	f.Add("SELECT GROUP_CONCAT(c1, ',') FROM t0")
	f.Add("SELECT TOTAL(c0) FROM t0")

	// ===== EDGE CASES: Type Casts =====
	f.Add("SELECT CAST(1 AS TEXT)")
	f.Add("SELECT CAST('1' AS INTEGER)")
	f.Add("SELECT CAST(1.5 AS INTEGER)")
	f.Add("SELECT CAST(1 AS REAL)")
	f.Add("SELECT CAST('1' AS BLOB)")
	f.Add("SELECT CAST(X'0102' AS TEXT)")

	// ===== EDGE CASES: Null handling =====
	f.Add("SELECT NULL + 1")
	f.Add("SELECT NULL - 1")
	f.Add("SELECT NULL * 1")
	f.Add("SELECT NULL / 1")
	f.Add("SELECT NULL || 'a'")
	f.Add("SELECT NULL = NULL")
	f.Add("SELECT NULL != NULL")
	f.Add("SELECT NULL > 1")
	f.Add("SELECT COALESCE(NULL, NULL, 'a')")
	f.Add("SELECT IFNULL(NULL, 0)")
	f.Add("SELECT NULLIF(1, 2)")

	// ===== EDGE CASES: Subqueries =====
	f.Add("SELECT (SELECT 1)")
	f.Add("SELECT * FROM (SELECT 1 AS x)")
	f.Add("SELECT * FROM t0 WHERE c0 = (SELECT 1)")
	f.Add("SELECT * FROM t0 WHERE c0 IN (SELECT 1)")
	f.Add("SELECT * FROM t0 WHERE EXISTS (SELECT 1)")
	f.Add("SELECT * FROM t0 WHERE c0 > ALL (SELECT 1)")
	f.Add("SELECT * FROM t0 WHERE c0 > ANY (SELECT 1)")

	// ===== EDGE CASES: JOINs =====
	f.Add("SELECT * FROM t0, t1")
	f.Add("SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0")
	f.Add("SELECT * FROM t0 LEFT JOIN t1 ON t0.c0 = t1.c0")
	f.Add("SELECT * FROM t0 INNER JOIN t1 ON t0.c0 = t1.c0")
	f.Add("SELECT * FROM t0 CROSS JOIN t1")
	f.Add("SELECT * FROM t0 NATURAL JOIN t1")

	// ===== EDGE CASES: Complex WHERE =====
	f.Add("SELECT * FROM t0 WHERE c0 > 0 AND c1 = 'a'")
	f.Add("SELECT * FROM t0 WHERE c0 > 0 OR c1 = 'a'")
	f.Add("SELECT * FROM t0 WHERE NOT c0 = 1")
	f.Add("SELECT * FROM t0 WHERE (c0 = 1)")
	f.Add("SELECT * FROM t0 WHERE c0 = 1 AND (c1 = 'a' OR c1 = 'b')")
	f.Add("SELECT * FROM t0 WHERE c0 NOT IN (1, 2)")
	f.Add("SELECT * FROM t0 WHERE c0 NOT BETWEEN 1 AND 10")
	f.Add("SELECT * FROM t0 WHERE c0 GLOB 'test*'")
	f.Add("SELECT * FROM t0 WHERE c0 NOT GLOB 'test*'")
	f.Add("SELECT * FROM t0 WHERE c0 MATCH 'pattern'")
	f.Add("SELECT * FROM t0 WHERE c0 LIKE 'test%' ESCAPE '\\'")

	// ===== EDGE CASES: GROUP BY / HAVING =====
	f.Add("SELECT c0, COUNT(*) FROM t0 GROUP BY c0")
	f.Add("SELECT c0, SUM(c1) FROM t0 GROUP BY c0 HAVING SUM(c1) > 0")
	f.Add("SELECT c0, COUNT(*) FROM t0 GROUP BY c0 ORDER BY c0")
	f.Add("SELECT c0, COUNT(*) FROM t0 GROUP BY c0 LIMIT 10")
	f.Add("SELECT c0, c1, COUNT(*) FROM t0 GROUP BY c0, c1")

	// ===== EDGE CASES: ORDER BY / LIMIT =====
	f.Add("SELECT * FROM t0 ORDER BY c0 ASC")
	f.Add("SELECT * FROM t0 ORDER BY c0 DESC")
	f.Add("SELECT * FROM t0 ORDER BY c0, c1")
	f.Add("SELECT * FROM t0 ORDER BY c0 DESC, c1 ASC")
	f.Add("SELECT * FROM t0 LIMIT 0")
	f.Add("SELECT * FROM t0 LIMIT 100 OFFSET 10")
	f.Add("SELECT * FROM t0 ORDER BY c0 LIMIT 10")

	// ===== EDGE CASES: Window Functions =====
	f.Add("SELECT ROW_NUMBER() OVER () FROM t0")
	f.Add("SELECT RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT DENSE_RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT PERCENT_RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT CUME_DIST() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT NTILE(3) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LAG(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LEAD(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT FIRST_VALUE(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LAST_VALUE(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT NTH_VALUE(c0, 2) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT SUM(c0) OVER (PARTITION BY c1 ORDER BY c0) FROM t0")
	f.Add("SELECT SUM(c0) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t0")

	// ===== EDGE CASES: CTEs =====
	f.Add("WITH cte AS (SELECT 1) SELECT * FROM cte")
	f.Add("WITH cte AS (SELECT 1 AS x) SELECT x FROM cte")
	f.Add("WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2")
	f.Add("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<5) SELECT x FROM cnt")
	f.Add("WITH cte AS (SELECT * FROM t0) SELECT * FROM cte")

	// ===== EDGE CASES: VALUES =====
	f.Add("SELECT * FROM (VALUES(1), (2), (3))")
	f.Add("SELECT * FROM (VALUES(1, 'a'), (2, 'b'))")
	f.Add("INSERT INTO t0 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// ===== EDGE CASES: Set Operations =====
	f.Add("SELECT 1 UNION SELECT 2 UNION SELECT 3")
	f.Add("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
	f.Add("SELECT 1 INTERSECT SELECT 2")
	f.Add("SELECT 1 EXCEPT SELECT 2")

	// ===== EDGE CASES: Nested Functions =====
	f.Add("SELECT ABS(COALESCE(NULL, -1))")
	f.Add("SELECT UPPER(LOWER('Hello'))")
	f.Add("SELECT LENGTH(TRIM('  hello  '))")
	f.Add("SELECT SUBSTR(REPLACE('hello', 'l', 'r'), 1, 2)")
	f.Add("SELECT COALESCE(NULLIF(1, 1), 2)")
	f.Add("SELECT IIF(ABS(-1) > 0, 'positive', 'negative')")

	// ===== EDGE CASES: Transactions =====
	f.Add("BEGIN IMMEDIATE")
	f.Add("BEGIN EXCLUSIVE")
	f.Add("BEGIN; SELECT 1; COMMIT")
	f.Add("BEGIN; INSERT INTO t0 VALUES(1); ROLLBACK")
	f.Add("SAVEPOINT a; SAVEPOINT b; RELEASE SAVEPOINT a")
	f.Add("SAVEPOINT a; ROLLBACK TO SAVEPOINT a")

	// ===== EDGE CASES: DDL =====
	f.Add("CREATE TABLE t1 (c0 INTEGER PRIMARY KEY)")
	f.Add("CREATE TABLE t1 (c0 TEXT UNIQUE)")
	f.Add("CREATE TABLE t1 (c0 INTEGER NOT NULL)")
	f.Add("CREATE TABLE t1 (c0 INTEGER CHECK(c0 > 0))")
	f.Add("CREATE TABLE t1 (c0 INTEGER DEFAULT 0)")
	f.Add("CREATE TABLE t1 (c0 TEXT COLLATE NOCASE)")
	f.Add("CREATE TABLE t1 (c0 BLOB)")
	f.Add("CREATE TABLE t1 (c0, c1)")
	f.Add("CREATE TABLE t1 AS SELECT 1 AS c0")
	f.Add("ALTER TABLE t0 ADD COLUMN c2 TEXT")
	f.Add("ALTER TABLE t0 RENAME TO t1")
	f.Add("CREATE INDEX idx1 ON t0(c0 DESC)")
	f.Add("CREATE UNIQUE INDEX idx1 ON t0(c0)")
	f.Add("CREATE INDEX idx1 ON t0(c0) WHERE c0 > 0")
	f.Add("REINDEX")
	f.Add("REINDEX t0")
	f.Add("VACUUM")
	f.Add("ANALYZE")

	// ===== EDGE CASES: Foreign Keys =====
	f.Add("PRAGMA foreign_keys = ON")
	f.Add("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	f.Add("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id))")
	f.Add("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)")
	f.Add("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)")
	f.Add("CREATE TABLE child (id INTEGER, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)")

	// ===== EDGE CASES: Triggers =====
	f.Add("CREATE TRIGGER tr1 AFTER INSERT ON t0 BEGIN SELECT 1; END")
	f.Add("CREATE TRIGGER tr1 BEFORE DELETE ON t0 BEGIN SELECT 1; END")
	f.Add("CREATE TRIGGER tr1 INSTEAD OF UPDATE ON v0 BEGIN SELECT 1; END")
	f.Add("DROP TRIGGER IF EXISTS tr1")

	// ===== EDGE CASES: PRAGMAs =====
	f.Add("PRAGMA database_list")
	f.Add("PRAGMA table_info(t0)")
	f.Add("PRAGMA index_list(t0)")
	f.Add("PRAGMA index_info(idx0)")
	f.Add("PRAGMA foreign_key_list(t0)")
	f.Add("PRAGMA sqlite_sequence")
	f.Add("PRAGMA collation_list")
	f.Add("PRAGMA page_size")
	f.Add("PRAGMA cache_size")
	f.Add("PRAGMA synchronous")
	f.Add("PRAGMA journal_mode")
	f.Add("PRAGMA locking_mode")
	f.Add("PRAGMA temp_store")
	f.Add("PRAGMA read_uncommitted")
	f.Add("PRAGMA wal_autocheckpoint")
	f.Add("PRAGMA auto_vacuum")

	// ===== WAL Enhancement PRAGMAs =====
	f.Add("PRAGMA wal_autocheckpoint = 1000")
	f.Add("PRAGMA wal_autocheckpoint = 0")
	f.Add("PRAGMA wal_autocheckpoint = 100")
	f.Add("PRAGMA wal_checkpoint")
	f.Add("PRAGMA wal_checkpoint(passive)")
	f.Add("PRAGMA wal_checkpoint(full)")
	f.Add("PRAGMA wal_checkpoint(truncate)")
	f.Add("PRAGMA journal_size_limit")
	f.Add("PRAGMA journal_size_limit = 10000")
	f.Add("PRAGMA journal_size_limit = 0")

	// ===== Storage PRAGMAs =====
	f.Add("PRAGMA shrink_memory")
	f.Add("PRAGMA optimize")
	f.Add("PRAGMA integrity_check")
	f.Add("PRAGMA quick_check")
	f.Add("PRAGMA cache_size = 1000")
	f.Add("PRAGMA cache_size = -2000")
	f.Add("PRAGMA cache_grind")

	// ===== EDGE CASES: Numeric edge values =====
	f.Add("SELECT 9223372036854775807")  // INT64 max
	f.Add("SELECT -9223372036854775808") // INT64 min
	f.Add("SELECT 0.0")
	f.Add("SELECT 1e10")
	f.Add("SELECT 1e-10")
	f.Add("SELECT 3.14159265358979")

	// ===== EDGE CASES: String edge values =====
	f.Add("SELECT ''")
	f.Add("SELECT 'a'")
	f.Add("SELECT 'a'''")
	f.Add("SELECT 'a\"\"b'")
	f.Add("SELECT 'a\\nb'")
	f.Add("SELECT 'a\\tc'")
	f.Add("SELECT x'00'")
	f.Add("SELECT x'ff'")
	f.Add("SELECT x'0102030405060708090a0b0c0d0e0f'")

	// ===== EDGE CASES: Keywords as identifiers =====
	f.Add("SELECT [select] FROM [where]")
	f.Add("SELECT \"select\" FROM \"where\"")

	// ===== EDGE CASES: Comments (should be ignored) =====
	f.Add("SELECT 1 -- comment")
	f.Add("SELECT 1 /* comment */")
	f.Add("SELECT /* comment */ 1")
	f.Add("SELECT 1 + /* multi\nline\ncomment */ 2")

	// ===== EDGE CASES: Unicode =====
	f.Add("SELECT 'café'")
	f.Add("SELECT '日本語'")
	f.Add("SELECT '🎉'")
	f.Add("SELECT 'München'")
	f.Add("SELECT 'Αβγδ'")

	// ===== SQL1999: Recursive CTEs (R-series) =====
	f.Add("WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b<100) SELECT a FROM fib")
	f.Add("WITH RECURSIVE countdown(n) AS (SELECT 10 UNION ALL SELECT n-1 FROM countdown WHERE n>0) SELECT n FROM countdown")

	// ===== SQL1999: Extended Window Functions (E111/F441) =====
	f.Add("SELECT ROW_NUMBER() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT DENSE_RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LAG(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LEAD(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT FIRST_VALUE(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT LAST_VALUE(c0) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT NTILE(3) OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT PERCENT_RANK() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT CUME_DIST() OVER (ORDER BY c0) FROM t0")
	f.Add("SELECT SUM(c0) OVER (ORDER BY c0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t0")

	// ===== SQL1999: Type Cast (B2_TYPECONV/F201) =====
	f.Add("SELECT CAST('42' AS INTEGER)")
	f.Add("SELECT CAST('3.14' AS REAL)")
	f.Add("SELECT CAST(100 AS TEXT)")
	f.Add("SELECT CAST(NULL AS INTEGER)")
	f.Add("SELECT CAST('3' AS INTEGER) + 2")

	// ===== SQL1999: Subqueries (B7/F291) =====
	f.Add("SELECT * FROM t0 WHERE c0 = (SELECT MAX(c0) FROM t0)")
	f.Add("SELECT * FROM t0 WHERE c0 IN (SELECT c1 FROM t1)")
	f.Add("SELECT * FROM t0 WHERE EXISTS (SELECT 1 FROM t1)")
	f.Add("SELECT * FROM t0 WHERE c0 > (SELECT AVG(c0) FROM t0 t2 WHERE t2.c1 = t0.c1)")
	f.Add("SELECT * FROM t0 WHERE c0 > ANY (SELECT c1 FROM t1)")
	f.Add("SELECT * FROM t0 WHERE c0 > ALL (SELECT c1 FROM t1)")
	f.Add("SELECT * FROM (SELECT c0, COUNT(*) as cnt FROM t0 GROUP BY c0) AS subq")

	// ===== SQL1999: Transaction modes (E151-E153) =====
	f.Add("BEGIN DEFERRED")
	f.Add("BEGIN IMMEDIATE")
	f.Add("BEGIN EXCLUSIVE")
	f.Add("SAVEPOINT sp1")
	f.Add("RELEASE SAVEPOINT sp1")
	f.Add("ROLLBACK TO SAVEPOINT sp1")
	f.Add("BEGIN; SAVEPOINT sp1; INSERT INTO t0 VALUES(1); RELEASE SAVEPOINT sp1; COMMIT")

	// ===== SQL1999: JOINs (F041/F401) =====
	f.Add("SELECT * FROM t0 NATURAL JOIN t1")
	f.Add("SELECT * FROM t0 JOIN t1 USING(c0)")
	f.Add("SELECT * FROM t0 LEFT JOIN t1 USING(c0)")

	// ===== Deep Fuzzing: SQLSmith-style complex queries =====
	// Use DeepSQLGenerator to create seeds with multiple random seeds
	for seed := int64(12345); seed < 12400; seed += 15 {
		deepGen := NewDeepSQLGenerator(seed)
		for i := 0; i < 5; i++ {
			f.Add(deepGen.DeepGenerateComplexQuery())
		}
	}

	// Complex multi-feature queries
	f.Add("SELECT t.c0, t.rn FROM (SELECT c0, ROW_NUMBER() OVER (ORDER BY c0) AS rn FROM t0) t JOIN t1 ON t.c0 = t1.c0")
	f.Add("WITH cte AS (SELECT c0, SUM(c1) AS total FROM t0 GROUP BY c0) SELECT c0, total, ROW_NUMBER() OVER (ORDER BY total DESC) FROM cte")
	f.Add("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT x FROM cnt")
	f.Add("UPDATE t0 SET c1 = t1.c1 FROM t1 WHERE t0.c0 = t1.c0")
	f.Add("INSERT INTO t0(c0, c1) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	f.Add("SELECT t0.c0, (SELECT COUNT(*) FROM t1 WHERE t1.c0 = t0.c0) AS cnt FROM t0")

	// ===== WAL Multi-Statement Sequences =====
	// These test WAL operations with actual data changes
	f.Add("CREATE TABLE t1(a INT); INSERT INTO t1 VALUES(1); PRAGMA wal_checkpoint")
	f.Add("CREATE TABLE t2(a INT); INSERT INTO t2 VALUES(1),(2); PRAGMA wal_checkpoint(passive)")
	f.Add("CREATE TABLE t3(a INT); INSERT INTO t3 SELECT 1; PRAGMA wal_checkpoint(full)")
	f.Add("CREATE TABLE t4(a INT); INSERT INTO t4 VALUES(1); DELETE FROM t4; PRAGMA wal_checkpoint(truncate)")
	f.Add("CREATE TABLE t5(a INT); INSERT INTO t5 VALUES(1),(2),(3); UPDATE t5 SET a=10 WHERE a=1; PRAGMA wal_checkpoint")
	f.Add("CREATE TABLE t6(a INT PRIMARY KEY); INSERT OR REPLACE INTO t6 VALUES(1); PRAGMA wal_checkpoint")
	f.Add("PRAGMA wal_autocheckpoint = 500; CREATE TABLE t7(a INT); INSERT INTO t7 VALUES(1)")
	f.Add("PRAGMA journal_size_limit = 5000; CREATE TABLE t8(a INT); INSERT INTO t8 VALUES(1)")
	f.Add("PRAGMA shrink_memory; CREATE TABLE t9(a INT)")
	f.Add("PRAGMA optimize; CREATE TABLE t10(a INT)")
	f.Add("PRAGMA integrity_check; CREATE TABLE t11(a INT)")
	f.Add("PRAGMA quick_check; CREATE TABLE t12(a INT)")
	f.Add("PRAGMA cache_grind; CREATE TABLE t13(a INT)")

	// ===== Storage PRAGMA Edge Cases =====
	f.Add("PRAGMA cache_size = 0")
	f.Add("PRAGMA cache_size = -100")
	f.Add("PRAGMA cache_size = 10000")
	f.Add("PRAGMA journal_size_limit = -1")
	f.Add("PRAGMA journal_size_limit = 1000000")
	f.Add("PRAGMA wal_autocheckpoint = -1")
	f.Add("PRAGMA wal_autocheckpoint = 10000")

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
	type result struct {
		err error
	}
	resultCh := make(chan result, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, true)
				panicInfo := fmt.Sprintf("PANIC: %v\n\nStack trace:\n%s", r, string(buf[:n]))
				if e, ok := r.(error); ok {
					resultCh <- result{err: fmt.Errorf("%w\n%s", e, panicInfo)}
				} else {
					resultCh <- result{err: fmt.Errorf("%v\n%s", r, panicInfo)}
				}
			}
		}()

		upperQuery := strings.ToUpper(strings.TrimSpace(query))

		if len(upperQuery) >= 6 {
			prefix := upperQuery[:6]
			if prefix == "SELECT" || prefix == "PRAGMA" {
				_, err = db.Query(query)
				resultCh <- result{err: err}
				return
			}
		}
		_, err = db.Exec(query)
		if err != nil {
			resultCh <- result{err: err}
			return
		}
	}()

	select {
	case res := <-resultCh:
		return res.err
	case <-time.After(defaultTimeout):
		return fmt.Errorf("TIMEOUT: query hung after %v\nQuery: %q", defaultTimeout, query)
	}
}
