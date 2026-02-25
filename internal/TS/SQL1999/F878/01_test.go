// Package F878 tests SQL:1999 F878 - Transaction & Integrity for v0.9.6.
// Features tested: SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT,
// nested savepoints, NOT NULL enforcement, UNIQUE enforcement, FK constraints.
package F878

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F878_Savepoint_Basic_L1 validates basic SAVEPOINT and ROLLBACK TO SAVEPOINT.
func TestSQL1999_F878_Savepoint_Basic_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	stmts := []struct {
		name string
		sql  string
	}{
		{"Create", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"Insert1", "INSERT INTO t1 VALUES (1, 'one')"},
		{"Begin", "BEGIN"},
		{"Insert2", "INSERT INTO t1 VALUES (2, 'two')"},
		{"Savepoint", "SAVEPOINT sp1"},
		{"Insert3", "INSERT INTO t1 VALUES (3, 'three')"},
		{"RollbackTo", "ROLLBACK TO SAVEPOINT sp1"},
		{"Insert4", "INSERT INTO t1 VALUES (4, 'four')"},
		{"Commit", "COMMIT"},
	}
	for _, tt := range stmts {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM t1"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F878_Savepoint_Release_L1 validates RELEASE SAVEPOINT.
func TestSQL1999_F878_Savepoint_Release_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	stmts := []struct {
		name string
		sql  string
	}{
		{"Create", "CREATE TABLE t2 (id INTEGER)"},
		{"Begin", "BEGIN"},
		{"Insert1", "INSERT INTO t2 VALUES (10)"},
		{"Savepoint", "SAVEPOINT sp1"},
		{"Insert2", "INSERT INTO t2 VALUES (20)"},
		{"Release", "RELEASE SAVEPOINT sp1"},
		{"Insert3", "INSERT INTO t2 VALUES (30)"},
		{"Commit", "COMMIT"},
	}
	for _, tt := range stmts {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t2 ORDER BY id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F878_Savepoint_Nested_L1 validates nested savepoints.
func TestSQL1999_F878_Savepoint_Nested_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	stmts := []struct {
		name string
		sql  string
	}{
		{"Create", "CREATE TABLE t3 (id INTEGER)"},
		{"Begin", "BEGIN"},
		{"Insert1", "INSERT INTO t3 VALUES (1)"},
		{"SP1", "SAVEPOINT outer"},
		{"Insert2", "INSERT INTO t3 VALUES (2)"},
		{"SP2", "SAVEPOINT inner"},
		{"Insert3", "INSERT INTO t3 VALUES (3)"},
		{"RollbackInner", "ROLLBACK TO SAVEPOINT inner"},
		{"Insert4", "INSERT INTO t3 VALUES (4)"},
		{"ReleaseInner", "RELEASE SAVEPOINT inner"},
		{"Commit", "COMMIT"},
	}
	for _, tt := range stmts {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t3 ORDER BY id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F878_NotNull_L1 validates NOT NULL constraint enforcement.
func TestSQL1999_F878_NotNull_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"Create", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"},
		{"InsertValid", "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"},
	}
	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Insert with NULL for NOT NULL column should error
	t.Run("InsertNullName", func(t *testing.T) {
		_, svErr := svDB.Exec("INSERT INTO users VALUES (2, NULL, 'b@example.com')")
		_, sqliteErr := sqliteDB.Exec("INSERT INTO users VALUES (2, NULL, 'b@example.com')")
		if (svErr == nil) != (sqliteErr == nil) {
			t.Errorf("NOT NULL enforcement mismatch: sqlvibe=%v sqlite=%v", svErr, sqliteErr)
		}
	})

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM users ORDER BY id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F878_UniqueConstraint_L1 validates UNIQUE constraint enforcement.
func TestSQL1999_F878_UniqueConstraint_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"Create", "CREATE TABLE emails (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"},
		{"Insert1", "INSERT INTO emails VALUES (1, 'a@b.com')"},
	}
	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Duplicate UNIQUE value should error
	t.Run("InsertDuplicate", func(t *testing.T) {
		_, svErr := svDB.Exec("INSERT INTO emails VALUES (2, 'a@b.com')")
		_, sqliteErr := sqliteDB.Exec("INSERT INTO emails VALUES (2, 'a@b.com')")
		if (svErr == nil) != (sqliteErr == nil) {
			t.Errorf("UNIQUE enforcement mismatch: sqlvibe=%v sqlite=%v", svErr, sqliteErr)
		}
	})

	queries := []struct {
		name string
		sql  string
	}{
		{"Count", "SELECT COUNT(*) FROM emails"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F878_FK_OnDelete_L1 validates FK ON DELETE CASCADE.
func TestSQL1999_F878_FK_OnDelete_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	stmts := []struct {
		name string
		sql  string
	}{
		{"PragmaOn", "PRAGMA foreign_keys = ON"},
		{"CreateParent", "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateChild", "CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE, name TEXT)"},
		{"InsertDept1", "INSERT INTO departments VALUES (1, 'Engineering')"},
		{"InsertDept2", "INSERT INTO departments VALUES (2, 'Marketing')"},
		{"InsertEmp1", "INSERT INTO employees VALUES (1, 1, 'Alice')"},
		{"InsertEmp2", "INSERT INTO employees VALUES (2, 1, 'Bob')"},
		{"InsertEmp3", "INSERT INTO employees VALUES (3, 2, 'Carol')"},
		{"DeleteDept1", "DELETE FROM departments WHERE id = 1"},
	}
	for _, tt := range stmts {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectDepts", "SELECT * FROM departments ORDER BY id"},
		{"SelectEmps", "SELECT * FROM employees ORDER BY id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
