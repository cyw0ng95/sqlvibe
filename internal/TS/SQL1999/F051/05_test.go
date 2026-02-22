package F051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F505_F05105_L1(t *testing.T) {
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
		{"CreateDate", "CREATE TABLE t1 (d DATE)"},
		{"CreateTime", "CREATE TABLE t2 (t TIME)"},
		{"CreateTimestamp", "CREATE TABLE t3 (ts TIMESTAMP)"},
		{"CreateDatetime", "CREATE TABLE t4 (dt DATETIME)"},
		{"CreateDateTime", "CREATE TABLE t5 (d DATE, t TIME, ts TIMESTAMP)"},
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
		{"InsertT1", "INSERT INTO t1 VALUES ('2024-01-15')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES ('2023-12-31')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES ('2000-02-29')"},
		{"InsertT2", "INSERT INTO t2 VALUES ('10:30:00')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES ('23:59:59')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES ('00:00:00')"},
		{"InsertT3", "INSERT INTO t3 VALUES ('2024-01-15 10:30:00')"},
		{"InsertT3_2", "INSERT INTO t3 VALUES ('2023-12-31 23:59:59')"},
		{"InsertT4", "INSERT INTO t4 VALUES ('2024-01-15 10:30:00')"},
		{"InsertT4_2", "INSERT INTO t4 VALUES ('2023-12-31 00:00:00')"},
		{"InsertT5", "INSERT INTO t5 VALUES ('2024-01-15', '10:30:00', '2024-01-15 10:30:00')"},
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
		{"SelectT1Order", "SELECT * FROM t1 ORDER BY d"},
		{"SelectT2Order", "SELECT * FROM t2 ORDER BY t"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
