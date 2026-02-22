package E101

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E10110_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, name TEXT, value INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'a', 100), (2, 'b', 200)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertSingleRow", "INSERT INTO t1 (id, name, value) VALUES (3, 'c', 300)"},
		{"InsertMultipleRows", "INSERT INTO t1 VALUES (4, 'd', 400), (5, 'e', 500)"},
		{"InsertWithSelect", "INSERT INTO t1 SELECT 6, 'f', 600"},
		{"InsertDefault", "INSERT INTO t1 (id, name) VALUES (7, 'g')"},
		{"InsertNull", "INSERT INTO t1 VALUES (8, NULL, NULL)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "SelectAll")
}

func TestSQL1999_F301_E10111_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, name TEXT, value INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	updateTests := []struct {
		name string
		sql  string
	}{
		{"UpdateSingle", "UPDATE t1 SET value = 999 WHERE id = 1"},
		{"UpdateMultiple", "UPDATE t1 SET value = value * 2 WHERE id > 2"},
		{"UpdateMultipleColumns", "UPDATE t1 SET name = 'updated', value = 0 WHERE id = 3"},
		{"UpdateWithoutWhere", "UPDATE t1 SET value = -1"},
		{"UpdateWithExpression", "UPDATE t1 SET value = value + 100 WHERE value > 200"},
	}

	for _, tt := range updateTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY id", "SelectAfterUpdate")
}

func TestSQL1999_F301_E10112_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, name TEXT, value INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	deleteTests := []struct {
		name string
		sql  string
	}{
		{"DeleteSingle", "DELETE FROM t1 WHERE id = 1"},
		{"DeleteMultiple", "DELETE FROM t1 WHERE value > 250"},
		{"DeleteWithExpression", "DELETE FROM t1 WHERE id > 2 AND value < 400"},
		{"DeleteWithoutWhere", "DELETE FROM t1"},
	}

	for _, tt := range deleteTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "SelectAfterDelete")
}
