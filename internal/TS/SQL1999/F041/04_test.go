package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F404_F04104_L1(t *testing.T) {
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
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, name TEXT, age INTEGER)"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (1, 'Alice', 30)"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (2, 'Bob', 25)"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (3, 'Charlie', 35)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	updateTests := []struct {
		name string
		sql  string
	}{
		{"UpdateSingle", "UPDATE t1 SET val = 150 WHERE id = 1"},
		{"UpdateMultiple", "UPDATE t1 SET val = val + 50"},
		{"UpdateAll", "UPDATE t2 SET age = age + 1"},
		{"UpdateText", "UPDATE t2 SET name = 'Eve' WHERE id = 2"},
		{"UpdateMultipleCols", "UPDATE t2 SET name = 'Frank', age = 40 WHERE id = 3"},
		{"UpdateWhereFalse", "UPDATE t1 SET val = 999 WHERE id = 999"},
		{"UpdateZeroRows", "UPDATE t1 SET val = 0 WHERE val > 1000"},
	}

	for _, tt := range updateTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1AfterUpdate", "SELECT * FROM t1 ORDER BY id"},
		{"SelectT2AfterUpdate", "SELECT * FROM t2 ORDER BY id"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
