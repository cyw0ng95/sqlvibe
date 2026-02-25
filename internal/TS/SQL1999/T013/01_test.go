package T013

import (
	"fmt"
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

// TestSQL1999_T013_AutocommitBasic_L1 tests that an INSERT without BEGIN is immediately visible.
func TestSQL1999_T013_AutocommitBasic_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1, 'autocommit')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 row in autocommit mode, got %d", len(rows.Data))
	}
}

// TestSQL1999_T013_MultipleInsertsAutocommit_L1 tests that multiple inserts in autocommit are all visible.
func TestSQL1999_T013_MultipleInsertsAutocommit_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for i, stmt := range []string{
		"INSERT INTO t VALUES (1, 'one')",
		"INSERT INTO t VALUES (2, 'two')",
		"INSERT INTO t VALUES (3, 'three')",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows in autocommit mode, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_T013_ImmediateVisibility_L1 tests data is queryable immediately after insert without BEGIN/COMMIT.
func TestSQL1999_T013_ImmediateVisibility_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert and immediately query — no explicit transaction
	if _, err := db.Exec("INSERT INTO t VALUES (99, 'visible')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("SELECT val FROM t WHERE id = 99")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows.Data))
		return
	}
	if fmt.Sprintf("%v", rows.Data[0][0]) != "visible" {
		t.Errorf("expected val='visible', got %v", rows.Data[0][0])
	}
}
