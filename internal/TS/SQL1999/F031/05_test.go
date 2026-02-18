package F031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03113_L1(t *testing.T) {
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
		{"CreateSimpleTable", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateTableWithPK", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)"},
		{"CreateTableWithFK", "CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))"},
		{"MultipleTables", "CREATE TABLE t3 (id INTEGER); CREATE TABLE t4 (id INTEGER); CREATE TABLE t5 (id INTEGER)"},
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
		{"InsertData", "INSERT INTO t1 VALUES (1, 'test')"},
		{"InsertMultiple", "INSERT INTO t1 VALUES (2, 'data'), (3, 'more')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropSimple", "DROP TABLE t1"},
		{"DropIfNotExists", "DROP TABLE IF EXISTS t1"},
		{"DropWithConstraints", "DROP TABLE t2"},
		{"DropMultiple", "DROP TABLE t3; DROP TABLE t4; DROP TABLE t5"},
		{"DropWithRESTRICT", "DROP TABLE t5 RESTRICT"},
	}

	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectBeforeDrop", "SELECT * FROM t2"},
		{"VerifyTableExists", "SELECT name FROM sqlite_master WHERE type='table' AND name='t2'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
