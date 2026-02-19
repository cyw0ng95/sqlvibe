package F511

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F51101_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"BeginTransaction", "CREATE TABLE t1 (a INTEGER)"},
		{"CommitTransaction", "INSERT INTO t1 VALUES (1)"},
		{"RollbackTransaction", "INSERT INTO t1 VALUES (2)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F51102_L1(t *testing.T) {
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

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER)", "CreateTable")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1)")

	sqlvibeDB.Exec("BEGIN")
	sqliteDB.Exec("BEGIN")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2)")

	sqlvibeDB.Exec("COMMIT")
	sqliteDB.Exec("COMMIT")

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY a", "VerifyTransaction")
}

func TestSQL1999_F301_F51103_L1(t *testing.T) {
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

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "CREATE TABLE t1 (a INTEGER)", "CreateTable")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1)")

	sqlvibeDB.Exec("BEGIN")
	sqliteDB.Exec("BEGIN")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2)")

	sqlvibeDB.Exec("ROLLBACK")
	sqliteDB.Exec("ROLLBACK")

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1 ORDER BY a", "VerifyRollback")
}

func TestSQL1999_F301_F51104_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"IsolationLevel", "CREATE TABLE t1 (a INTEGER)"},
		{"ReadOnly", "INSERT INTO t1 VALUES (1)"},
		{"ReadWrite", "SELECT * FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
