package T011

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

// TestSQL1999_T011_BeginCommit_L1 tests that INSERT within BEGIN/COMMIT is visible after commit.
func TestSQL1999_T011_BeginCommit_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'hello')",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 row after COMMIT, got %d", len(rows.Data))
	}
}

// TestSQL1999_T011_RollbackDiscardsChanges_L1 tests that ROLLBACK removes uncommitted inserts.
func TestSQL1999_T011_RollbackDiscardsChanges_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'hello')",
		"ROLLBACK",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	rows, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Errorf("expected 0 rows after ROLLBACK, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T011_MultipleStatementsInTransaction_L1 tests multiple DML statements in one transaction.
func TestSQL1999_T011_MultipleStatementsInTransaction_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (1, 'a')",
		"INSERT INTO t VALUES (2, 'b')",
		"INSERT INTO t VALUES (3, 'c')",
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
		t.Errorf("expected 3 rows after COMMIT, got %d", len(rows.Data))
	}
}

// TestSQL1999_T011_CommitMakesDataPersistent_L1 tests committed data is visible on re-query.
func TestSQL1999_T011_CommitMakesDataPersistent_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (42, 'committed')",
		"COMMIT",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// Query multiple times to confirm persistence
	for i := 0; i < 3; i++ {
		rows, err := db.Query("SELECT val FROM t WHERE id = 42")
		if err != nil {
			t.Fatalf("SELECT (iter %d): %v", i, err)
		}
		if len(rows.Data) != 1 || rows.Data[0][0] != "committed" {
			t.Errorf("iter %d: expected 1 row with 'committed', got %v", i, rows.Data)
		}
	}
}

// TestSQL1999_T011_RollbackRevertsToPreTransactionState_L1 verifies pre-existing rows are unaffected by rollback.
func TestSQL1999_T011_RollbackRevertsToPreTransactionState_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert one row outside any transaction (autocommit)
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'before')"); err != nil {
		t.Fatalf("INSERT before transaction: %v", err)
	}

	// Start a transaction, insert more rows, then rollback
	for _, stmt := range []string{
		"BEGIN",
		"INSERT INTO t VALUES (2, 'during')",
		"INSERT INTO t VALUES (3, 'during')",
		"ROLLBACK",
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
		t.Errorf("expected 1 row after ROLLBACK, got %d: %v", len(rows.Data), rows.Data)
	}
}
