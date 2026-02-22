package E051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05104_L1(t *testing.T) {
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
		{"REAL", "CREATE TABLE t1 (a REAL)"},
		{"DOUBLE", "CREATE TABLE t2 (a DOUBLE)"},
		{"DOUBLEPRECISION", "CREATE TABLE t3 (a DOUBLE PRECISION)"},
		{"FLOAT", "CREATE TABLE t4 (a FLOAT)"},
		{"FLOATWithLength", "CREATE TABLE t5 (a FLOAT(10))"},
		{"FLOATWithPrecision", "CREATE TABLE t6 (a FLOAT(10, 2))"},
		{"NUMERIC", "CREATE TABLE t7 (a NUMERIC)"},
		{"DECIMAL", "CREATE TABLE t8 (a DECIMAL)"},
		{"DECIMALWithPrecision", "CREATE TABLE t9 (a DECIMAL(10, 2))"},
		{"NUMBER", "CREATE TABLE t10 (a NUMBER)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertReal", "INSERT INTO t1 VALUES (3.14)"},
		{"InsertDouble", "INSERT INTO t2 VALUES (6.28)"},
		{"InsertFloat", "INSERT INTO t4 VALUES (1.5)"},
		{"InsertNegative", "INSERT INTO t5 VALUES (-2.5)"},
		{"InsertZero", "INSERT INTO t6 VALUES (0.0)"},
		{"InsertLarge", "INSERT INTO t7 VALUES (123456.789)"},
		{"InsertScientific", "INSERT INTO t8 VALUES (1.23e5)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1", "SELECT * FROM t1"},
		{"SelectT2", "SELECT * FROM t2"},
		{"SelectT4", "SELECT * FROM t4"},
		{"SelectT7", "SELECT * FROM t7"},
		{"SelectT8", "SELECT * FROM t8"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
