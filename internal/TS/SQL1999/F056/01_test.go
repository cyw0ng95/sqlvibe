package F056

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F056_BasicInsertUpdateDelete_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"},
		{"Insert1", "INSERT INTO t VALUES (1, 'a')"},
		{"Insert2", "INSERT INTO t VALUES (2, 'b')"},
		{"Insert3", "INSERT INTO t VALUES (3, 'c')"},
		{"Update1", "UPDATE t SET val = 'B' WHERE id = 2"},
		{"Delete1", "DELETE FROM t WHERE id = 3"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, val FROM t ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM t"},
		{"SelectUpdated", "SELECT val FROM t WHERE id = 2"},
		{"SelectDeleted", "SELECT COUNT(*) FROM t WHERE id = 3"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F056_MultiRowInsertAndUpdate_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)"},
		{"InsertMulti", "INSERT INTO items VALUES (1, 'apple', 10), (2, 'banana', 20), (3, 'cherry', 5)"},
		{"UpdateAll", "UPDATE items SET qty = qty + 1"},
		{"UpdateSpecific", "UPDATE items SET name = 'APPLE' WHERE id = 1"},
		{"DeleteLow", "DELETE FROM items WHERE qty < 10"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, name, qty FROM items ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM items"},
		{"TotalQty", "SELECT SUM(qty) FROM items"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
