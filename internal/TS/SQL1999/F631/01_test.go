package F631

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F631_AutoIncrement_L1 tests AUTOINCREMENT column behavior.
func TestSQL1999_F631_AutoIncrement_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	for _, stmt := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
	} {
		SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, stmt, "Setup")
	}

	for _, ins := range []string{
		"INSERT INTO users (name) VALUES ('Alice')",
		"INSERT INTO users (name) VALUES ('Bob')",
		"INSERT INTO users (name) VALUES ('Carol')",
	} {
		SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, ins, ins[:30])
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT id, name FROM users ORDER BY id", "SelectAll")
}

// TestSQL1999_F631_AutoIncrementSequential_L1 verifies IDs don't reuse after DELETE.
func TestSQL1999_F631_AutoIncrementSequential_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE seq_test (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
	db.Exec("INSERT INTO seq_test (v) VALUES ('a')")
	db.Exec("INSERT INTO seq_test (v) VALUES ('b')")
	db.Exec("INSERT INTO seq_test (v) VALUES ('c')")

	// Delete the second row
	db.Exec("DELETE FROM seq_test WHERE id = 2")

	// Insert a new row - should get id=4, not reuse id=2
	db.Exec("INSERT INTO seq_test (v) VALUES ('d')")

	rows, err := db.Query("SELECT id FROM seq_test ORDER BY id")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows.Data))
	}
	// IDs should be 1, 3, 4 (not 1, 3, 2)
	expectedIDs := []int64{1, 3, 4}
	for i, row := range rows.Data {
		id, ok := row[0].(int64)
		if !ok {
			t.Errorf("Row %d: expected int64 id, got %T", i, row[0])
			continue
		}
		if id != expectedIDs[i] {
			t.Errorf("Row %d: expected id=%d, got id=%d", i, expectedIDs[i], id)
		}
	}
}

// TestSQL1999_F631_PragmaSQLiteSequence_L1 tests PRAGMA sqlite_sequence.
func TestSQL1999_F631_PragmaSQLiteSequence_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE seq_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	db.Exec("INSERT INTO seq_tbl (name) VALUES ('x')")
	db.Exec("INSERT INTO seq_tbl (name) VALUES ('y')")
	db.Exec("INSERT INTO seq_tbl (name) VALUES ('z')")

	rows, err := db.Query("PRAGMA sqlite_sequence")
	if err != nil {
		t.Fatalf("PRAGMA sqlite_sequence error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Error("Expected at least 1 row from PRAGMA sqlite_sequence")
		return
	}
	// Find our table in the sequence
	found := false
	for _, row := range rows.Data {
		if row[0] == "seq_tbl" {
			found = true
			if row[1] != int64(3) {
				t.Errorf("Expected seq=3, got %v", row[1])
			}
		}
	}
	if !found {
		t.Error("seq_tbl not found in sqlite_sequence")
	}
}
