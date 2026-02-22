package F057

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F057_BeginMultipleInsertCommit_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"Begin", "BEGIN"},
		{"Insert1", "INSERT INTO t1 VALUES (1, 'one')"},
		{"Insert2", "INSERT INTO t1 VALUES (2, 'two')"},
		{"Insert3", "INSERT INTO t1 VALUES (3, 'three')"},
		{"Commit", "COMMIT"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, val FROM t1 ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM t1"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F057_BeginMultipleInsertRollback_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertPersistent", "INSERT INTO t2 VALUES (1, 'persistent')"},
		{"Begin", "BEGIN"},
		{"InsertRollback1", "INSERT INTO t2 VALUES (2, 'rollback1')"},
		{"InsertRollback2", "INSERT INTO t2 VALUES (3, 'rollback2')"},
		{"InsertRollback3", "INSERT INTO t2 VALUES (4, 'rollback3')"},
		{"Rollback", "ROLLBACK"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, val FROM t2 ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM t2"},
		{"OnlyPersistent", "SELECT val FROM t2 WHERE id = 1"},
		{"RolledBackGone", "SELECT COUNT(*) FROM t2 WHERE id > 1"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F057_EmptyTransaction_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t3 (id INTEGER)"},
		{"Insert1", "INSERT INTO t3 VALUES (1)"},
		{"EmptyBegin", "BEGIN"},
		{"EmptyCommit", "COMMIT"},
		{"Insert2", "INSERT INTO t3 VALUES (2)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"Count", "SELECT COUNT(*) FROM t3"},
		{"SelectAll", "SELECT id FROM t3 ORDER BY id"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F057_TransactionWithUpdate_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE accounts (id INTEGER, balance INTEGER)"},
		{"InsertAcc1", "INSERT INTO accounts VALUES (1, 1000)"},
		{"InsertAcc2", "INSERT INTO accounts VALUES (2, 500)"},
		{"Begin", "BEGIN"},
		{"UpdateAcc1", "UPDATE accounts SET balance = balance - 200 WHERE id = 1"},
		{"UpdateAcc2", "UPDATE accounts SET balance = balance + 200 WHERE id = 2"},
		{"Commit", "COMMIT"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, balance FROM accounts ORDER BY id"},
		{"TotalBalance", "SELECT SUM(balance) FROM accounts"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F057_TransactionWithDelete_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE events (id INTEGER, type TEXT)"},
		{"Insert1", "INSERT INTO events VALUES (1, 'login')"},
		{"Insert2", "INSERT INTO events VALUES (2, 'logout')"},
		{"Insert3", "INSERT INTO events VALUES (3, 'error')"},
		{"Insert4", "INSERT INTO events VALUES (4, 'login')"},
		{"Begin", "BEGIN"},
		{"DeleteErrors", "DELETE FROM events WHERE type = 'error'"},
		{"Commit", "COMMIT"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, type FROM events ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM events"},
		{"NoErrors", "SELECT COUNT(*) FROM events WHERE type = 'error'"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
