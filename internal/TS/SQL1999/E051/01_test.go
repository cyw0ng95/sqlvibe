package E051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05101_L1(t *testing.T) {
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
		{"INTEGER", "CREATE TABLE t1 (a INTEGER)"},
		{"INT", "CREATE TABLE t2 (a INT)"},
		{"SMALLINT", "CREATE TABLE t3 (a SMALLINT)"},
		{"BIGINT", "CREATE TABLE t4 (a BIGINT)"},
		{"TINYINT", "CREATE TABLE t5 (a TINYINT)"},
		{"MEDIUMINT", "CREATE TABLE t6 (a MEDIUMINT)"},
		{"INT2", "CREATE TABLE t7 (a INT2)"},
		{"INT8", "CREATE TABLE t8 (a INT8)"},
		{"UNSIGNEDBIGINT", "CREATE TABLE t9 (a UNSIGNED BIG INT)"},
		{"INT1", "CREATE TABLE t10 (a INT1)"},
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
		{"InsertInteger", "INSERT INTO t1 VALUES (42)"},
		{"InsertNegativeInteger", "INSERT INTO t2 VALUES (-123)"},
		{"InsertZero", "INSERT INTO t3 VALUES (0)"},
		{"InsertLargeInteger", "INSERT INTO t4 VALUES (2147483647)"},
		{"InsertMinInteger", "INSERT INTO t5 VALUES (-2147483648)"},
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
		{"SelectT3", "SELECT * FROM t3"},
		{"SelectT4", "SELECT * FROM t4"},
		{"SelectT5", "SELECT * FROM t5"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
