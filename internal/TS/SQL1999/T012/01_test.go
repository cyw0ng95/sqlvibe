package T012

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// TestSQL1999_T012_SavepointCreateRelease_L1 tests basic SAVEPOINT creation and RELEASE.
func TestSQL1999_T012_SavepointCreateRelease_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'a')",
		"SAVEPOINT sp1",
		"INSERT INTO t VALUES (2, 'b')",
		"RELEASE SAVEPOINT sp1",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows after RELEASE SAVEPOINT + COMMIT, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T012_RollbackToSavepoint_L1 tests that ROLLBACK TO SAVEPOINT reverts to the savepoint state.
func TestSQL1999_T012_RollbackToSavepoint_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'a')",
		"SAVEPOINT sp1",
		"INSERT INTO t VALUES (2, 'b')",
		"ROLLBACK TO SAVEPOINT sp1",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 row after ROLLBACK TO SAVEPOINT, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T012_MultipleSavepointsStack_L1 tests stack behavior with nested savepoints.
func TestSQL1999_T012_MultipleSavepointsStack_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'base')",
		"SAVEPOINT sp1",
		"INSERT INTO t VALUES (2, 'sp1')",
		"SAVEPOINT sp2",
		"INSERT INTO t VALUES (3, 'sp2')",
		"ROLLBACK TO SAVEPOINT sp2",
		// row 3 gone, rows 1 and 2 remain
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T012_SavepointWithinTransaction_L1 tests savepoint inside a transaction commits cleanly.
func TestSQL1999_T012_SavepointWithinTransaction_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (10, 'x')",
		"SAVEPOINT inner",
		"INSERT INTO t VALUES (20, 'y')",
		"RELEASE SAVEPOINT inner",
		"INSERT INTO t VALUES (30, 'z')",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T012_ReleaseSavepointVerifyState_L1 tests that releasing a savepoint does not undo its changes.
func TestSQL1999_T012_ReleaseSavepointVerifyState_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"SAVEPOINT sp1",
		"INSERT INTO t VALUES (1, 'row1')",
		"INSERT INTO t VALUES (2, 'row2')",
		"RELEASE SAVEPOINT sp1",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows after RELEASE SAVEPOINT, got %d: %v", len(rows.Data), rows.Data)
	}
}
