package F031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03104_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (a INTEGER)"},
		{"AlterAddColumn", "ALTER TABLE t1 ADD COLUMN b TEXT"},
		{"AlterAddIntColumn", "ALTER TABLE t1 ADD COLUMN c INTEGER"},
		{"AlterAddRealColumn", "ALTER TABLE t1 ADD COLUMN d REAL"},
		{"AlterAddMultipleColumns", "ALTER TABLE t1 ADD COLUMN e BLOB"},
		{"AlterAddColumnWithDefault", "ALTER TABLE t1 ADD COLUMN f INTEGER DEFAULT 0"},
		{"AlterAddMultiple", "CREATE TABLE t2 (id INTEGER); ALTER TABLE t2 ADD COLUMN val TEXT; ALTER TABLE t2 ADD COLUMN num INTEGER"},
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
		{"InsertBeforeAlter", "INSERT INTO t2 (id) VALUES (1)"},
		{"InsertAfterAlter", "INSERT INTO t2 (id, val) VALUES (2, 'test')"},
		{"InsertWithAllColumns", "INSERT INTO t2 VALUES (3, 'data', 42)"},
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
		{"SelectFromAlteredTable", "SELECT * FROM t2"},
		{"SelectSpecificColumns", "SELECT id, val FROM t2"},
		{"CheckColumnAdded", "SELECT val FROM t2 WHERE id = 2"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
