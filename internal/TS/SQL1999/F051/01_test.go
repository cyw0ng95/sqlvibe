package F051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F501_F05101_L1(t *testing.T) {
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
		{"CreateIntTable", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateIntAlias1", "CREATE TABLE t2 (a INT)"},
		{"CreateIntAlias2", "CREATE TABLE t3 (a SMALLINT)"},
		{"CreateIntAlias3", "CREATE TABLE t4 (a BIGINT)"},
		{"CreateMultipleInt", "CREATE TABLE t5 (a INTEGER, b INTEGER, c INTEGER)"},
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
		{"InsertPos", "INSERT INTO t1 VALUES (1)"},
		{"InsertNeg", "INSERT INTO t1 VALUES (-1)"},
		{"InsertZero", "INSERT INTO t1 VALUES (0)"},
		{"InsertLarge", "INSERT INTO t1 VALUES (9223372036854775807)"},
		{"InsertSmall", "INSERT INTO t1 VALUES (-9223372036854775808)"},
		{"InsertMultiple", "INSERT INTO t5 VALUES (1, 2, 3)"},
		{"InsertT2", "INSERT INTO t2 VALUES (100)"},
		{"InsertT3", "INSERT INTO t3 VALUES (32767)"},
		{"InsertT4", "INSERT INTO t4 VALUES (9223372036854775807)"},
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
		{"SelectSum", "SELECT SUM(a) FROM t1"},
		{"SelectAvg", "SELECT AVG(a) FROM t1"},
		{"SelectMax", "SELECT MAX(a) FROM t1"},
		{"SelectMin", "SELECT MIN(a) FROM t1"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
