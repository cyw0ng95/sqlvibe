package sqlvibe

import (
	"testing"
)

func TestPragmaTableInfo(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

	rows, err := db.Query("PRAGMA table_info('users')")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(rows.Data) != 3 {
		t.Errorf("expected 3 columns, got %d", len(rows.Data))
	}

	expectedNames := []string{"id", "name", "email"}
	for i, row := range rows.Data {
		if row[1] != expectedNames[i] {
			t.Errorf("column %d: expected name %s, got %v", i, expectedNames[i], row[1])
		}
	}
}

func TestPragmaIndexList(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("CREATE INDEX idx_name ON users(name)")
	db.Exec("CREATE UNIQUE INDEX idx_name2 ON users(name)")

	rows, err := db.Query("PRAGMA index_list('users')")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(rows.Data) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(rows.Data))
	}
}

func TestPragmaDatabaseList(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	rows, err := db.Query("PRAGMA database_list")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(rows.Data) != 1 {
		t.Errorf("expected 1 database, got %d", len(rows.Data))
	}

	if rows.Data[0][1] != "main" {
		t.Errorf("expected database name 'main', got %v", rows.Data[0][1])
	}
}
