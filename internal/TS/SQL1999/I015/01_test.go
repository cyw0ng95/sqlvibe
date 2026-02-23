package I015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_I015_I01501_L1(t *testing.T) {
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"CreatePKTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateAutoIncrement", "CREATE TABLE t2 (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)"},
		{"CreateCompositePK", "CREATE TABLE t3 (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Valid PK inserts
	validTests := []struct {
		name string
		sql  string
	}{
		{"InsertPK1", "INSERT INTO t1 VALUES (1, 'Alice')"},
		{"InsertPK2", "INSERT INTO t1 VALUES (2, 'Bob')"},
		{"InsertAutoInc1", "INSERT INTO t2 (val) VALUES ('first')"},
		{"InsertAutoInc2", "INSERT INTO t2 (val) VALUES ('second')"},
		{"InsertCompositePK1", "INSERT INTO t3 VALUES (1, 1, 'v11')"},
		{"InsertCompositePK2", "INSERT INTO t3 VALUES (1, 2, 'v12')"},
		{"InsertCompositePK3", "INSERT INTO t3 VALUES (2, 1, 'v21')"},
	}
	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Duplicate PK - both should fail
	dupTests := []struct {
		name string
		sql  string
	}{
		{"DuplicatePK", "INSERT INTO t1 VALUES (1, 'Duplicate')"},
		{"DuplicateCompositePK", "INSERT INTO t3 VALUES (1, 1, 'dup')"},
	}
	for _, tt := range dupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectPK", "SELECT * FROM t1 ORDER BY id"},
		{"SelectAutoInc", "SELECT * FROM t2 ORDER BY id"},
		{"SelectCompositePK", "SELECT * FROM t3 ORDER BY a, b"},
		{"SelectByPK", "SELECT * FROM t1 WHERE id = 1"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
