package F261

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F261_F26101_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, status TEXT)"},
		{"Insert1", "INSERT INTO t1 VALUES (1, 'active')"},
		{"Insert2", "INSERT INTO t1 VALUES (2, 'inactive')"},
		{"Insert3", "INSERT INTO t1 VALUES (3, 'pending')"},
		{"Insert4", "INSERT INTO t1 VALUES (4, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	caseTests := []struct {
		name string
		sql  string
	}{
		{"CaseSimpleEquality", "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM t1"},
		{"CaseSearched", "SELECT CASE WHEN status = 'active' THEN 'Active User' WHEN status = 'inactive' THEN 'Inactive User' END FROM t1"},
		{"CaseWithElse", "SELECT CASE status WHEN 'active' THEN 'Active' ELSE 'Other' END FROM t1"},
		{"CaseNull", "SELECT CASE WHEN status IS NULL THEN 'No Status' ELSE status END FROM t1"},
		{"CaseInUpdate", "UPDATE t1 SET status = CASE WHEN status = 'pending' THEN 'active' ELSE status END"},
	}

	for _, tt := range caseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
