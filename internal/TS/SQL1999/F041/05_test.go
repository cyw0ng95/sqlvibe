package F041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F405_F04105_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES (1, 100)"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 200)"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (3, 300)"},
		{"InsertT1_4", "INSERT INTO t1 VALUES (4, 400)"},
		{"InsertT1_5", "INSERT INTO t1 VALUES (5, 500)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, name TEXT)"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (1, 'Alice')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (2, 'Bob')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (3, 'Charlie')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	deleteTests := []struct {
		name string
		sql  string
	}{
		{"DeleteSingle", "DELETE FROM t1 WHERE id = 1"},
		{"DeleteMultiple", "DELETE FROM t1 WHERE val > 300"},
		{"DeleteAll", "DELETE FROM t2"},
		{"DeleteWhereFalse", "DELETE FROM t1 WHERE id = 999"},
		{"DeleteZeroRows", "DELETE FROM t1 WHERE val > 1000"},
	}

	for _, tt := range deleteTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1AfterDelete", "SELECT * FROM t1 ORDER BY id"},
		{"SelectT2AfterDelete", "SELECT * FROM t2"},
		{"SelectCountT1", "SELECT COUNT(*) FROM t1"},
		{"SelectCountT2", "SELECT COUNT(*) FROM t2"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
