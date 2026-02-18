package E051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05106_L1(t *testing.T) {
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
		{"MixedTypes", "CREATE TABLE t1 (a INTEGER, b REAL, c TEXT, d BLOB)"},
		{"AnyType", "CREATE TABLE t2 (a ANY)"},
		{"NoType", "CREATE TABLE t3 (a)"},
		{"MultipleAny", "CREATE TABLE t4 (a ANY, b ANY, c ANY)"},
		{"MixedWithAny", "CREATE TABLE t5 (a INTEGER, b ANY, c TEXT, d ANY)"},
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
		{"InsertMixedTypes", "INSERT INTO t1 VALUES (1, 3.14, 'hello', x'0102')"},
		{"InsertIntIntoAny", "INSERT INTO t2 VALUES (42)"},
		{"InsertTextIntoAny", "INSERT INTO t2 VALUES ('text')"},
		{"InsertRealIntoAny", "INSERT INTO t2 VALUES (2.5)"},
		{"InsertBlobIntoAny", "INSERT INTO t2 VALUES (x'AABB')"},
		{"InsertIntoNoType", "INSERT INTO t3 VALUES (123)"},
		{"InsertMultipleAny", "INSERT INTO t4 VALUES (1, 'test', 3.14)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	coercionTests := []struct {
		name string
		sql  string
	}{
		{"IntToReal", "INSERT INTO t1 VALUES (1, 2, 'hello', x'0102')"},
		{"TextToReal", "INSERT INTO t1 VALUES (2, '3.14', 'world', x'0304')"},
		{"RealToInt", "INSERT INTO t1 VALUES (3, 3.14, 'test', x'0506')"},
		{"IntToText", "INSERT INTO t3 VALUES ('text')"},
		{"TextToInt", "INSERT INTO t4 VALUES ('42', 'test', 3.14)"},
	}

	for _, tt := range coercionTests {
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
		{"SelectWithCoercion", "SELECT typeof(b) FROM t1"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
