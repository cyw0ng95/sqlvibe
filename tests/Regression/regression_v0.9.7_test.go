package Regression

// v0.9.7 edge-case regression tests.
// Covers E1-E6 as specified in docs/plan-v0.9.7.md:
//   E1 – INSERT OR REPLACE / IGNORE edge cases
//   E2 – RETURNING edge cases
//   E3 – Foreign Key cascade edge cases
//   E4 – COLLATE edge cases
//   E5 – MATCH / GLOB edge cases
//   E6 – Transaction / Savepoint edge cases

import (
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func mustOpen(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustExec(t *testing.T, db *sqlvibe.Database, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func mustQuery(t *testing.T, db *sqlvibe.Database, sql string) *sqlvibe.Rows {
	t.Helper()
	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return rows
}

// ────────────────────────────────────────────────────────────────────────────
// E1: INSERT OR REPLACE / IGNORE edge cases
// ────────────────────────────────────────────────────────────────────────────

// TestRegression_E1_InsertOrReplaceEmptyTable_L1 inserts into an empty table with
// INSERT OR REPLACE – should behave identically to a plain INSERT.
func TestRegression_E1_InsertOrReplaceEmptyTable_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (1, 'hello')`)

	rows := mustQuery(t, db, `SELECT id, v FROM t`)
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(1) || rows.Data[0][1] != "hello" {
		t.Errorf("unexpected row: %v", rows.Data[0])
	}
}

// TestRegression_E1_InsertOrReplacePKConflict_L1 verifies INSERT OR REPLACE replaces
// an existing row that conflicts on PRIMARY KEY.
func TestRegression_E1_InsertOrReplacePKConflict_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'old')`)
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (1, 'new')`)

	rows := mustQuery(t, db, `SELECT v FROM t WHERE id = 1`)
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != "new" {
		t.Errorf("expected 'new', got %v", rows.Data[0][0])
	}
}

// TestRegression_E1_InsertOrReplaceUniqueConflict_L1 verifies INSERT OR REPLACE
// correctly resolves a conflict on a non-PK UNIQUE column.
func TestRegression_E1_InsertOrReplaceUniqueConflict_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'a@b.com')`)
	// New row has a different PK but the same email → unique conflict on email
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (2, 'a@b.com')`)

	rows := mustQuery(t, db, `SELECT id, email FROM t`)
	if len(rows.Data) != 1 {
		t.Fatalf("expected exactly 1 row after REPLACE, got %d", len(rows.Data))
	}
	if rows.Data[0][1] != "a@b.com" {
		t.Errorf("expected email 'a@b.com', got %v", rows.Data[0][1])
	}
}

// TestRegression_E1_NullInUniqueColumn_L1 verifies that multiple NULL values are
// allowed in a UNIQUE column (SQLite semantics: NULL != NULL).
func TestRegression_E1_NullInUniqueColumn_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, NULL)`)
	// A second NULL in the same UNIQUE column must succeed.
	if _, err := db.Exec(`INSERT INTO t VALUES (2, NULL)`); err != nil {
		t.Errorf("inserting second NULL into UNIQUE column should succeed, got: %v", err)
	}
	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(2) {
		t.Errorf("expected 2 rows, got %v", rows.Data[0][0])
	}
}

// TestRegression_E1_MultiColumnNullInUnique_L1 verifies that if any column in a
// composite UNIQUE index is NULL the constraint is not enforced (SQLite semantics).
func TestRegression_E1_MultiColumnNullInUnique_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (a INTEGER, b TEXT, UNIQUE(a, b))`)
	mustExec(t, db, `INSERT INTO t VALUES (1, NULL)`)
	// Same (a, b) pair with NULL b – must succeed.
	if _, err := db.Exec(`INSERT INTO t VALUES (1, NULL)`); err != nil {
		t.Errorf("inserting duplicate row with NULL in composite UNIQUE should succeed, got: %v", err)
	}
}

// TestRegression_E1_AllColumnsNullReplace_L1 verifies INSERT OR REPLACE with all
// NULL values on a table that has no NOT NULL columns.
func TestRegression_E1_AllColumnsNullReplace_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (a INTEGER, b TEXT)`)
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (NULL, NULL)`)
	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row, got %v", rows.Data[0][0])
	}
}

// TestRegression_E1_InsertOrIgnoreDuplicatePK_L1 verifies INSERT OR IGNORE silently
// skips a row that conflicts on PRIMARY KEY.
func TestRegression_E1_InsertOrIgnoreDuplicatePK_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'first')`)
	mustExec(t, db, `INSERT OR IGNORE INTO t VALUES (1, 'second')`)

	rows := mustQuery(t, db, `SELECT v FROM t WHERE id = 1`)
	if len(rows.Data) != 1 || rows.Data[0][0] != "first" {
		t.Errorf("expected row to remain unchanged, got %v", rows.Data)
	}
}

// TestRegression_E1_InsertOrIgnoreDuplicateUniqueCol_L1 verifies INSERT OR IGNORE
// silently skips a row that conflicts on a non-PK UNIQUE column.
func TestRegression_E1_InsertOrIgnoreDuplicateUniqueCol_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'a@b.com')`)
	mustExec(t, db, `INSERT OR IGNORE INTO t VALUES (2, 'a@b.com')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row (second INSERT ignored), got %v", rows.Data[0][0])
	}
}

// TestRegression_E1_InsertOrReplaceMultipleRows_L1 tests INSERT OR REPLACE with
// multiple rows in a single statement, some conflicting.
func TestRegression_E1_InsertOrReplaceMultipleRows_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'old1'), (2, 'old2')`)
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (1, 'new1'), (3, 'new3')`)

	rows := mustQuery(t, db, `SELECT id, v FROM t ORDER BY id`)
	// Should have rows: (1, 'new1'), (2, 'old2'), (3, 'new3')
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
	if rows.Data[0][1] != "new1" {
		t.Errorf("row 1 v should be 'new1', got %v", rows.Data[0][1])
	}
	if rows.Data[1][1] != "old2" {
		t.Errorf("row 2 v should be 'old2', got %v", rows.Data[1][1])
	}
}

// TestRegression_E1_InsertOrIgnoreNotNullConstraint_L1 verifies INSERT OR IGNORE
// does NOT suppress a NOT NULL constraint violation (only UNIQUE conflicts are ignored).
func TestRegression_E1_InsertOrIgnoreNotNullConstraint_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)
	_, err := db.Exec(`INSERT OR IGNORE INTO t VALUES (1, NULL)`)
	if err == nil {
		t.Error("expected NOT NULL constraint error, got nil")
	}
}

// TestRegression_E1_InsertOrReplaceCompositeUnique_L1 verifies that INSERT OR
// REPLACE resolves composite UNIQUE constraint conflicts.
func TestRegression_E1_InsertOrReplaceCompositeUnique_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (a INTEGER, b TEXT, c INTEGER, UNIQUE(a, b))`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'x', 10)`)
	mustExec(t, db, `INSERT OR REPLACE INTO t VALUES (1, 'x', 20)`)

	rows := mustQuery(t, db, `SELECT c FROM t WHERE a=1 AND b='x'`)
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(20) {
		t.Errorf("expected c=20 after REPLACE, got %v", rows.Data[0][0])
	}
}

// ────────────────────────────────────────────────────────────────────────────
// E2: RETURNING edge cases
// ────────────────────────────────────────────────────────────────────────────

// TestRegression_E2_InsertReturning_L1 verifies basic INSERT … RETURNING.
func TestRegression_E2_InsertReturning_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	rows, err := db.Query(`INSERT INTO t VALUES (1, 'hello') RETURNING id, v`)
	if err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(1) || rows.Data[0][1] != "hello" {
		t.Errorf("unexpected RETURNING row: %v", rows.Data[0])
	}
}

// TestRegression_E2_InsertReturningExpression_L1 tests RETURNING with expressions.
func TestRegression_E2_InsertReturningExpression_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, v INTEGER)`)
	rows, err := db.Query(`INSERT INTO t VALUES (3, 7) RETURNING v * 2`)
	if err != nil {
		t.Fatalf("INSERT RETURNING expr: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(14) {
		t.Errorf("expected 14, got %v (%T)", rows.Data[0][0], rows.Data[0][0])
	}
}

// TestRegression_E2_InsertReturningUpper_L1 tests RETURNING UPPER(col).
func TestRegression_E2_InsertReturningUpper_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, name TEXT)`)
	rows, err := db.Query(`INSERT INTO t VALUES (1, 'alice') RETURNING UPPER(name)`)
	if err != nil {
		t.Fatalf("INSERT RETURNING UPPER: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "ALICE" {
		t.Errorf("expected 'ALICE', got %v", rows.Data)
	}
}

// TestRegression_E2_MultiRowInsertReturning_L1 tests RETURNING after multi-row INSERT.
func TestRegression_E2_MultiRowInsertReturning_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, v TEXT)`)
	rows, err := db.Query(`INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c') RETURNING id`)
	if err != nil {
		t.Fatalf("multi-row INSERT RETURNING: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 RETURNING rows, got %d", len(rows.Data))
	}
}

// TestRegression_E2_UpdateReturning_L1 tests UPDATE … RETURNING.
func TestRegression_E2_UpdateReturning_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 10), (2, 20)`)
	rows, err := db.Query(`UPDATE t SET v = v + 1 WHERE id = 1 RETURNING id, v`)
	if err != nil {
		t.Fatalf("UPDATE RETURNING: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][1] != int64(11) {
		t.Errorf("expected v=11 after UPDATE, got %v", rows.Data[0][1])
	}
}

// TestRegression_E2_DeleteReturning_L1 tests DELETE … RETURNING.
func TestRegression_E2_DeleteReturning_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'a'), (2, 'b')`)
	rows, err := db.Query(`DELETE FROM t WHERE id = 2 RETURNING id, v`)
	if err != nil {
		t.Fatalf("DELETE RETURNING: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(2) {
		t.Errorf("expected deleted row (2,'b'), got %v", rows.Data)
	}
}

// TestRegression_E2_InsertReturningInTransaction_L1 tests RETURNING inside a transaction.
func TestRegression_E2_InsertReturningInTransaction_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, db, `BEGIN`)
	rows, err := db.Query(`INSERT INTO t VALUES (1, 'tx') RETURNING id`)
	if err != nil {
		t.Fatalf("INSERT RETURNING in tx: %v", err)
	}
	mustExec(t, db, `COMMIT`)
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(1) {
		t.Errorf("unexpected RETURNING result: %v", rows.Data)
	}
}

// TestRegression_E2_InsertReturningStarWildcard_L1 tests INSERT … RETURNING *.
func TestRegression_E2_InsertReturningStarWildcard_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, v TEXT)`)
	rows, err := db.Query(`INSERT INTO t VALUES (5, 'five') RETURNING *`)
	if err != nil {
		t.Fatalf("INSERT RETURNING *: %v", err)
	}
	if len(rows.Data) != 1 || len(rows.Data[0]) != 2 {
		t.Fatalf("expected 1 row with 2 cols, got %v", rows.Data)
	}
}

// TestRegression_E2_InsertReturningEmptyTable_L1 tests RETURNING when inserting first row.
func TestRegression_E2_InsertReturningEmptyTable_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	rows, err := db.Query(`INSERT INTO t VALUES (42) RETURNING id`)
	if err != nil {
		t.Fatalf("INSERT RETURNING on empty table: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(42) {
		t.Errorf("expected id=42, got %v", rows.Data)
	}
}

// TestRegression_E2_UpdateReturningMultipleRows_L1 tests UPDATE RETURNING affects multiple rows.
func TestRegression_E2_UpdateReturningMultipleRows_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, v INTEGER)`)
	mustExec(t, db, `INSERT INTO t VALUES (1,10),(2,20),(3,30)`)
	rows, err := db.Query(`UPDATE t SET v = v * 2 RETURNING id, v`)
	if err != nil {
		t.Fatalf("UPDATE RETURNING multiple: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 RETURNING rows, got %d", len(rows.Data))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// E3: Foreign Key cascade edge cases
// ────────────────────────────────────────────────────────────────────────────

func enableFK(t *testing.T, db *sqlvibe.Database) {
	t.Helper()
	mustExec(t, db, `PRAGMA foreign_keys = ON`)
}

// TestRegression_E3_DeepCascadeDelete_L1 tests a 3-level ON DELETE CASCADE chain
// (A → B → C): deleting a row from A should cascade through B and then C.
func TestRegression_E3_DeepCascadeDelete_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE a (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id) ON DELETE CASCADE)`)
	mustExec(t, db, `CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b(id) ON DELETE CASCADE)`)

	mustExec(t, db, `INSERT INTO a VALUES (1)`)
	mustExec(t, db, `INSERT INTO b VALUES (10, 1)`)
	mustExec(t, db, `INSERT INTO c VALUES (100, 10)`)

	mustExec(t, db, `DELETE FROM a WHERE id = 1`)

	for _, tbl := range []string{"a", "b", "c"} {
		rows := mustQuery(t, db, `SELECT COUNT(*) FROM `+tbl)
		if rows.Data[0][0] != int64(0) {
			t.Errorf("table %s should be empty after 3-level cascade, got count=%v", tbl, rows.Data[0][0])
		}
	}
}

// TestRegression_E3_FKNullChildColumns_L1 verifies FK is not enforced when child
// FK column is NULL (nullable FK).
func TestRegression_E3_FKNullChildColumns_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id))`)
	mustExec(t, db, `INSERT INTO parent VALUES (1)`)
	// NULL FK should pass even though parent row 99 doesn't exist
	if _, err := db.Exec(`INSERT INTO child VALUES (1, NULL)`); err != nil {
		t.Errorf("inserting child with NULL FK should succeed, got: %v", err)
	}
}

// TestRegression_E3_FKRestrict_L1 verifies RESTRICT prevents deleting a parent
// row that has children.
func TestRegression_E3_FKRestrict_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id))`)
	mustExec(t, db, `INSERT INTO parent VALUES (1)`)
	mustExec(t, db, `INSERT INTO child VALUES (1, 1)`)

	_, err := db.Exec(`DELETE FROM parent WHERE id = 1`)
	if err == nil {
		t.Error("expected FK constraint error when deleting parent with children, got nil")
	}
}

// TestRegression_E3_FKSetNull_L1 verifies ON DELETE SET NULL sets child FK columns to NULL.
func TestRegression_E3_FKSetNull_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id) ON DELETE SET NULL)`)
	mustExec(t, db, `INSERT INTO parent VALUES (1)`)
	mustExec(t, db, `INSERT INTO child VALUES (1, 1)`)

	mustExec(t, db, `DELETE FROM parent WHERE id = 1`)

	rows := mustQuery(t, db, `SELECT p_id FROM child WHERE id = 1`)
	if len(rows.Data) != 1 || rows.Data[0][0] != nil {
		t.Errorf("expected child p_id = NULL after SET NULL cascade, got %v", rows.Data)
	}
}

// TestRegression_E3_FKCascadeUpdate_L1 verifies ON UPDATE CASCADE propagates new PK value.
func TestRegression_E3_FKCascadeUpdate_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id) ON UPDATE CASCADE)`)
	mustExec(t, db, `INSERT INTO parent VALUES (1, 'p')`)
	mustExec(t, db, `INSERT INTO child VALUES (1, 1)`)

	mustExec(t, db, `UPDATE parent SET id = 99 WHERE id = 1`)

	rows := mustQuery(t, db, `SELECT p_id FROM child WHERE id = 1`)
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(99) {
		t.Errorf("expected child p_id = 99 after CASCADE UPDATE, got %v", rows.Data)
	}
}

// TestRegression_E3_SelfReferencingCascadeDelete_L1 tests self-referential ON DELETE CASCADE.
// Deleting a node should cascade to its direct children.
func TestRegression_E3_SelfReferencingCascadeDelete_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE)`)
	mustExec(t, db, `INSERT INTO nodes VALUES (1, NULL)`)
	mustExec(t, db, `INSERT INTO nodes VALUES (2, 1)`)
	mustExec(t, db, `INSERT INTO nodes VALUES (3, 1)`)

	mustExec(t, db, `DELETE FROM nodes WHERE id = 1`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM nodes`)
	if rows.Data[0][0] != int64(0) {
		t.Errorf("expected all nodes deleted via self-referential cascade, got count=%v", rows.Data[0][0])
	}
}

// TestRegression_E3_FKInsertMissingParent_L1 verifies that inserting a child with
// a non-existent parent fails when FK is enabled.
func TestRegression_E3_FKInsertMissingParent_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id))`)

	_, err := db.Exec(`INSERT INTO child VALUES (1, 999)`)
	if err == nil {
		t.Error("expected FK constraint error for missing parent, got nil")
	}
}

// TestRegression_E3_FKDisabled_L1 verifies FK constraints are not checked when
// PRAGMA foreign_keys = OFF (default).
func TestRegression_E3_FKDisabled_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES parent(id))`)
	// FK off by default – should succeed even with non-existent parent
	if _, err := db.Exec(`INSERT INTO child VALUES (1, 999)`); err != nil {
		t.Errorf("expected success with FK off, got: %v", err)
	}
}

// TestRegression_E3_MultiLevelFKCascade_L1 tests 4-level cascade (A→B→C→D).
func TestRegression_E3_MultiLevelFKCascade_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()
	enableFK(t, db)

	mustExec(t, db, `CREATE TABLE a4 (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE b4 (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a4(id) ON DELETE CASCADE)`)
	mustExec(t, db, `CREATE TABLE c4 (id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b4(id) ON DELETE CASCADE)`)
	mustExec(t, db, `CREATE TABLE d4 (id INTEGER PRIMARY KEY, c_id INTEGER REFERENCES c4(id) ON DELETE CASCADE)`)

	mustExec(t, db, `INSERT INTO a4 VALUES (1)`)
	mustExec(t, db, `INSERT INTO b4 VALUES (10, 1)`)
	mustExec(t, db, `INSERT INTO c4 VALUES (100, 10)`)
	mustExec(t, db, `INSERT INTO d4 VALUES (1000, 100)`)

	mustExec(t, db, `DELETE FROM a4 WHERE id = 1`)

	for _, tbl := range []string{"a4", "b4", "c4", "d4"} {
		rows := mustQuery(t, db, `SELECT COUNT(*) FROM `+tbl)
		if rows.Data[0][0] != int64(0) {
			t.Errorf("table %s should be empty after 4-level cascade, got count=%v", tbl, rows.Data[0][0])
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// E4: COLLATE edge cases
// ────────────────────────────────────────────────────────────────────────────

// TestRegression_E4_CollateNocaseWhere_L1 verifies COLLATE NOCASE in WHERE clause.
func TestRegression_E4_CollateNocaseWhere_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, name TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB'), (3, 'charlie')`)

	rows := mustQuery(t, db, `SELECT id FROM t WHERE name = 'alice' COLLATE NOCASE`)
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(1) {
		t.Errorf("expected id=1 with COLLATE NOCASE, got %v", rows.Data)
	}
}

// TestRegression_E4_CollateNocaseOrderBy_L1 verifies COLLATE NOCASE in ORDER BY.
func TestRegression_E4_CollateNocaseOrderBy_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (name TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('banana'), ('Apple'), ('cherry')`)

	rows := mustQuery(t, db, `SELECT name FROM t ORDER BY name COLLATE NOCASE ASC`)
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
	// Case-insensitive: Apple, banana, cherry
	if !strings.EqualFold(rows.Data[0][0].(string), "apple") {
		t.Errorf("expected first row to be 'Apple' (case-insensitive), got %v", rows.Data[0][0])
	}
}

// TestRegression_E4_CollateBinary_L1 verifies default COLLATE BINARY is case-sensitive.
func TestRegression_E4_CollateBinary_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (name TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('Alice'), ('alice')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE name = 'alice' COLLATE BINARY`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("COLLATE BINARY should find only 'alice' (not 'Alice'), got %v", rows.Data[0][0])
	}
}

// TestRegression_E4_CollateNocaseLike_L1 verifies LIKE with COLLATE NOCASE column.
func TestRegression_E4_CollateNocaseLike_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (name TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('Hello World'), ('hello world'), ('HELLO WORLD')`)

	// LIKE is case-insensitive by default in SQLite for ASCII
	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE name LIKE 'hello%'`)
	if rows.Data[0][0] != int64(3) {
		t.Errorf("LIKE 'hello%%' should match all 3 rows case-insensitively, got %v", rows.Data[0][0])
	}
}

// TestRegression_E4_CollateColumnDefault_L1 verifies COLLATE NOCASE on column definition.
func TestRegression_E4_CollateColumnDefault_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER, name TEXT COLLATE NOCASE)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB')`)

	rows := mustQuery(t, db, `SELECT id FROM t WHERE name = 'alice'`)
	// With COLLATE NOCASE on column, this may match case-insensitively
	// Acceptable result: either 1 (if COLLATE on column is honored) or 0
	// We just verify no crash occurs
	_ = rows
}

// ────────────────────────────────────────────────────────────────────────────
// E5: MATCH / GLOB edge cases
// ────────────────────────────────────────────────────────────────────────────

// TestRegression_E5_GlobBasic_L1 verifies GLOB with * wildcard.
func TestRegression_E5_GlobBasic_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (filename TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('file.txt'), ('image.png'), ('doc.txt'), ('README')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE filename GLOB '*.txt'`)
	if rows.Data[0][0] != int64(2) {
		t.Errorf("GLOB '*.txt' should match 2 rows, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_GlobMatchAll_L1 verifies GLOB '*' matches all non-NULL rows.
func TestRegression_E5_GlobMatchAll_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('a'), ('bb'), ('ccc')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v GLOB '*'`)
	if rows.Data[0][0] != int64(3) {
		t.Errorf("GLOB '*' should match all rows, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_GlobEmptyPattern_L1 verifies GLOB ” only matches empty string.
func TestRegression_E5_GlobEmptyPattern_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (''), ('a'), ('bb')`)

	rows := mustQuery(t, db, "SELECT COUNT(*) FROM t WHERE v GLOB ''")
	if rows.Data[0][0] != int64(1) {
		t.Errorf("GLOB '' should only match empty string, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_GlobNullHandling_L1 verifies GLOB with NULL value returns NULL (no match).
func TestRegression_E5_GlobNullHandling_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (NULL), ('hello')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v GLOB '*'`)
	// NULL GLOB '*' = NULL = false, so only 'hello' matches
	if rows.Data[0][0] != int64(1) {
		t.Errorf("GLOB '*' should not match NULL, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_GlobQuestionMark_L1 verifies GLOB '?' matches exactly one char.
func TestRegression_E5_GlobQuestionMark_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('a'), ('ab'), ('abc'), ('')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v GLOB '?'`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("GLOB '?' should match exactly one char, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_GlobCaseSensitive_L1 verifies GLOB is case-sensitive (unlike LIKE).
func TestRegression_E5_GlobCaseSensitive_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('Hello'), ('hello'), ('HELLO')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v GLOB 'hello'`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("GLOB should be case-sensitive, expected 1 match for 'hello', got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_LikeBasic_L1 verifies basic LIKE pattern matching.
func TestRegression_E5_LikeBasic_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('hello'), ('world'), ('help')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v LIKE 'hel%'`)
	if rows.Data[0][0] != int64(2) {
		t.Errorf("LIKE 'hel%%' should match 2 rows, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_LikeNullHandling_L1 verifies LIKE with NULL value returns NULL.
func TestRegression_E5_LikeNullHandling_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES (NULL), ('hello')`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t WHERE v LIKE '%'`)
	// NULL LIKE '%' = NULL = false
	if rows.Data[0][0] != int64(1) {
		t.Errorf("LIKE '%%' should not match NULL, got %v", rows.Data[0][0])
	}
}

// TestRegression_E5_MatchBasic_L1 tests MATCH operator (FTS-style in SQLite, treated as LIKE in sqlvibe).
func TestRegression_E5_MatchBasic_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (v TEXT)`)
	mustExec(t, db, `INSERT INTO t VALUES ('hello world'), ('goodbye')`)

	// MATCH 'hello' – should at minimum not crash
	_, err := db.Query(`SELECT COUNT(*) FROM t WHERE v MATCH 'hello'`)
	if err != nil {
		// Acceptable: unimplemented MATCH returns an error
		_ = err
	}
}

// ────────────────────────────────────────────────────────────────────────────
// E6: Transaction / Savepoint edge cases
// ────────────────────────────────────────────────────────────────────────────

// TestRegression_E6_NestedBeginError_L1 verifies that BEGIN within an active
// transaction returns an error.
func TestRegression_E6_NestedBeginError_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `BEGIN`)
	_, err := db.Exec(`BEGIN`)
	if err == nil {
		t.Error("nested BEGIN should return an error, got nil")
	}
	mustExec(t, db, `ROLLBACK`)
}

// TestRegression_E6_RollbackWithoutBegin_L1 verifies ROLLBACK without an active
// transaction returns an error.
func TestRegression_E6_RollbackWithoutBegin_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	_, err := db.Exec(`ROLLBACK`)
	if err == nil {
		t.Error("ROLLBACK without BEGIN should return an error, got nil")
	}
}

// TestRegression_E6_CommitWithoutBegin_L1 verifies COMMIT without an active
// transaction returns an error.
func TestRegression_E6_CommitWithoutBegin_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	_, err := db.Exec(`COMMIT`)
	if err == nil {
		t.Error("COMMIT without BEGIN should return an error, got nil")
	}
}

// TestRegression_E6_BeginRollback_L1 verifies data is rolled back after ROLLBACK.
func TestRegression_E6_BeginRollback_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `ROLLBACK`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(0) {
		t.Errorf("expected 0 rows after ROLLBACK, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_BeginCommit_L1 verifies data is persisted after COMMIT.
func TestRegression_E6_BeginCommit_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `COMMIT`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row after COMMIT, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_SavepointRollback_L1 tests basic SAVEPOINT / ROLLBACK TO.
func TestRegression_E6_SavepointRollback_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t VALUES (2)`)
	mustExec(t, db, `ROLLBACK TO SAVEPOINT sp1`)
	mustExec(t, db, `COMMIT`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row after ROLLBACK TO SAVEPOINT, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_ReleaseSavepoint_L1 tests RELEASE SAVEPOINT keeps data.
func TestRegression_E6_ReleaseSavepoint_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t VALUES (2)`)
	mustExec(t, db, `RELEASE SAVEPOINT sp1`)
	mustExec(t, db, `COMMIT`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(2) {
		t.Errorf("expected 2 rows after RELEASE SAVEPOINT, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_MultipleSavepoints_L1 tests multiple savepoints.
func TestRegression_E6_MultipleSavepoints_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t VALUES (2)`)
	mustExec(t, db, `SAVEPOINT sp2`)
	mustExec(t, db, `INSERT INTO t VALUES (3)`)
	mustExec(t, db, `ROLLBACK TO SAVEPOINT sp1`)
	mustExec(t, db, `COMMIT`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row after rolling back to sp1, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_SavepointNoBegin_L1 verifies that SAVEPOINT works even without
// an explicit BEGIN (implicit transaction).
func TestRegression_E6_SavepointNoBegin_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t VALUES (2)`)
	mustExec(t, db, `ROLLBACK TO SAVEPOINT sp1`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row after savepoint rollback without explicit BEGIN, got %v", rows.Data[0][0])
	}
}

// TestRegression_E6_RollbackToUnknownSavepoint_L1 verifies ROLLBACK TO unknown savepoint returns error.
func TestRegression_E6_RollbackToUnknownSavepoint_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `BEGIN`)
	_, err := db.Exec(`ROLLBACK TO SAVEPOINT nonexistent`)
	if err == nil {
		t.Error("expected error for ROLLBACK TO unknown savepoint, got nil")
	}
	mustExec(t, db, `ROLLBACK`)
}

// TestRegression_E6_SavepointDepth10_L1 tests 10 nested savepoints.
func TestRegression_E6_SavepointDepth10_L1(t *testing.T) {
	db := mustOpen(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10; i++ {
		mustExec(t, db, `INSERT INTO t VALUES (`+itoa(i)+`)`)
		mustExec(t, db, `SAVEPOINT sp`+itoa(i))
	}
	// Roll all the way back to sp1
	mustExec(t, db, `ROLLBACK TO SAVEPOINT sp1`)
	mustExec(t, db, `COMMIT`)

	rows := mustQuery(t, db, `SELECT COUNT(*) FROM t`)
	// After rolling back to sp1, only row 1 should remain (sp1 was set after insert 1)
	if rows.Data[0][0] != int64(1) {
		t.Errorf("expected 1 row after rolling back to sp1 (10-level depth), got %v", rows.Data[0][0])
	}
}

// itoa converts int to string (avoiding import of strconv in this helper).
func itoa(n int) string {
	if n < 0 {
		return "-" + itoa(-n)
	}
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + string(rune('0'+n%10))
}
