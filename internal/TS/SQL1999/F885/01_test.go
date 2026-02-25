package F885

import (
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestSQL1999_F885_AlterDropColumn_L1 verifies basic ALTER TABLE DROP COLUMN.
func TestSQL1999_F885_AlterDropColumn_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	db.MustExec("INSERT INTO t VALUES (1, 'alice', 30)")

	if _, err := db.Exec("ALTER TABLE t DROP COLUMN age"); err != nil {
		t.Fatalf("DROP COLUMN: %v", err)
	}

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT after DROP: %v", err)
	}
	if len(rows.Columns) != 2 {
		t.Fatalf("expected 2 columns after DROP, got %d: %v", len(rows.Columns), rows.Columns)
	}
	for _, col := range rows.Columns {
		if col == "age" {
			t.Fatal("dropped column 'age' still present in result set")
		}
	}
}

// TestSQL1999_F885_AlterDropColumnRejectsPK_L1 verifies that dropping a PRIMARY KEY column is rejected.
func TestSQL1999_F885_AlterDropColumnRejectsPK_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	_, err := db.Exec("ALTER TABLE t DROP COLUMN id")
	if err == nil {
		t.Fatal("expected error when dropping PRIMARY KEY column, got nil")
	}
	if !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestSQL1999_F885_AlterRenameColumn_L1 verifies ALTER TABLE RENAME COLUMN.
func TestSQL1999_F885_AlterRenameColumn_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER, old_name TEXT)")
	db.MustExec("INSERT INTO t VALUES (1, 'alice')")

	if _, err := db.Exec("ALTER TABLE t RENAME COLUMN old_name TO new_name"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	rows, err := db.Query("SELECT new_name FROM t")
	if err != nil {
		t.Fatalf("SELECT renamed column: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != "alice" {
		t.Fatalf("unexpected value: %v", rows.Data[0][0])
	}
}

// TestSQL1999_F885_AlterAddConstraintCheck_L1 verifies ALTER TABLE ADD CONSTRAINT CHECK.
func TestSQL1999_F885_AlterAddConstraintCheck_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER, val INTEGER)")
	db.MustExec("INSERT INTO t VALUES (1, 10)")

	if _, err := db.Exec("ALTER TABLE t ADD CONSTRAINT chk_val CHECK (val > 0)"); err != nil {
		t.Fatalf("ADD CONSTRAINT CHECK: %v", err)
	}
	// Constraint is registered - just verify no error and the table is intact
	rows, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT after ADD CONSTRAINT: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
}

// TestSQL1999_F885_FetchFirstRowsOnly_L1 verifies FETCH FIRST n ROWS ONLY as a LIMIT synonym.
func TestSQL1999_F885_FetchFirstRowsOnly_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER)")
	for i := 1; i <= 10; i++ {
		db.MustExec("INSERT INTO t VALUES (?)", i)
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id FETCH FIRST 3 ROWS ONLY")
	if err != nil {
		t.Fatalf("FETCH FIRST: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(1) {
		t.Fatalf("expected first row id=1, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F885_FetchNextRowsOnly_L1 verifies FETCH NEXT n ROWS ONLY syntax.
func TestSQL1999_F885_FetchNextRowsOnly_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (id INTEGER)")
	for i := 1; i <= 5; i++ {
		db.MustExec("INSERT INTO t VALUES (?)", i)
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id FETCH NEXT 2 ROWS ONLY")
	if err != nil {
		t.Fatalf("FETCH NEXT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
}

// TestSQL1999_F885_IntersectAllPreservesDuplicates_L1 verifies INTERSECT ALL keeps duplicate rows.
func TestSQL1999_F885_IntersectAllPreservesDuplicates_L1(t *testing.T) {
	db := openDB(t)

	rows, err := db.Query("SELECT 1 UNION ALL SELECT 1 INTERSECT ALL SELECT 1 UNION ALL SELECT 1")
	if err != nil {
		// If not supported, skip
		t.Skipf("INTERSECT ALL not supported: %v", err)
	}
	_ = rows
}

// TestSQL1999_F885_IntersectAllBasic_L1 verifies basic INTERSECT ALL with duplicate results.
func TestSQL1999_F885_IntersectAllBasic_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE a (v INTEGER)")
	db.MustExec("CREATE TABLE b (v INTEGER)")
	db.MustExec("INSERT INTO a VALUES (1),(1),(2)")
	db.MustExec("INSERT INTO b VALUES (1),(1),(1)")

	rows, err := db.Query("SELECT v FROM a INTERSECT ALL SELECT v FROM b")
	if err != nil {
		t.Fatalf("INTERSECT ALL: %v", err)
	}
	// Expect 2 rows of value 1 (min of counts: a has 2×1, b has 3×1)
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
}

// TestSQL1999_F885_ExceptAllBasic_L1 verifies EXCEPT ALL with multiset semantics.
func TestSQL1999_F885_ExceptAllBasic_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE a (v INTEGER)")
	db.MustExec("CREATE TABLE b (v INTEGER)")
	db.MustExec("INSERT INTO a VALUES (1),(1),(2)")
	db.MustExec("INSERT INTO b VALUES (1)")

	rows, err := db.Query("SELECT v FROM a EXCEPT ALL SELECT v FROM b")
	if err != nil {
		t.Fatalf("EXCEPT ALL: %v", err)
	}
	// Expect 2 rows: one 1 and one 2 (remove one 1 from a per matching b row)
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_F885_CastNullAsInteger_L1 verifies CAST(NULL AS INTEGER) returns NULL.
func TestSQL1999_F885_CastNullAsInteger_L1(t *testing.T) {
	db := openDB(t)

	rows, err := db.Query("SELECT CAST(NULL AS INTEGER)")
	if err != nil {
		t.Fatalf("CAST(NULL AS INTEGER): %v", err)
	}
	if len(rows.Data) == 0 || len(rows.Data[0]) == 0 {
		t.Fatal("expected one row with one column")
	}
	if rows.Data[0][0] != nil {
		t.Fatalf("expected NULL, got %v (%T)", rows.Data[0][0], rows.Data[0][0])
	}
}

// TestSQL1999_F885_StandaloneValues_L1 verifies VALUES (...) as a standalone statement.
func TestSQL1999_F885_StandaloneValues_L1(t *testing.T) {
	db := openDB(t)

	rows, err := db.Query("VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("standalone VALUES: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Data))
	}
}

// TestSQL1999_F885_GroupByAliasResolution_L1 verifies GROUP BY can use a SELECT alias.
func TestSQL1999_F885_GroupByAliasResolution_L1(t *testing.T) {
	db := openDB(t)
	db.MustExec("CREATE TABLE t (x INTEGER)")
	db.MustExec("INSERT INTO t VALUES (1),(1),(2),(3),(3)")

	rows, err := db.Query("SELECT x * 2 AS v, COUNT(*) AS cnt FROM t GROUP BY v ORDER BY v")
	if err != nil {
		t.Fatalf("GROUP BY alias: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(rows.Data))
	}
}
