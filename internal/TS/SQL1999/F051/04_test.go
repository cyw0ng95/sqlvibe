package F051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F504_F05104_L1(t *testing.T) {
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
		{"CreateNumeric", "CREATE TABLE t1 (a NUMERIC)"},
		{"CreateDecimal", "CREATE TABLE t2 (a DECIMAL)"},
		{"CreateNumeric10", "CREATE TABLE t3 (a NUMERIC(10))"},
		{"CreateNumeric52", "CREATE TABLE t4 (a NUMERIC(5,2))"},
		{"CreateDecimal102", "CREATE TABLE t5 (a DECIMAL(10,2))"},
		{"CreateReal", "CREATE TABLE t6 (a REAL)"},
		{"CreateDouble", "CREATE TABLE t7 (a DOUBLE)"},
		{"CreateFloat", "CREATE TABLE t8 (a FLOAT)"},
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
		{"InsertT1", "INSERT INTO t1 VALUES (123.456)"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (0)"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (-99.99)"},
		{"InsertT2", "INSERT INTO t2 VALUES (100)"},
		{"InsertT3", "INSERT INTO t3 VALUES (1234567890)"},
		{"InsertT4", "INSERT INTO t4 VALUES (123.45)"},
		{"InsertT4_2", "INSERT INTO t4 VALUES (999.99)"},
		{"InsertT5", "INSERT INTO t5 VALUES (12345678.90)"},
		{"InsertT6", "INSERT INTO t6 VALUES (3.14159)"},
		{"InsertT7", "INSERT INTO t7 VALUES (2.718281828)"},
		{"InsertT8", "INSERT INTO t8 VALUES (1.5e10)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1", "SELECT * FROM t1"},
		{"SelectT2", "SELECT * FROM t2"},
		{"SelectT3", "SELECT * FROM t3"},
		{"SelectT4", "SELECT * FROM t4"},
		{"SelectT5", "SELECT * FROM t5"},
		{"SelectT6", "SELECT * FROM t6"},
		{"SelectT7", "SELECT * FROM t7"},
		{"SelectT8", "SELECT * FROM t8"},
		{"SelectSum", "SELECT SUM(a) FROM t1"},
		{"SelectAvg", "SELECT AVG(a) FROM t4"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
