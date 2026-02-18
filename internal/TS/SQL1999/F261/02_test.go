package F261

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F261_F26102_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t2 (x INTEGER, y TEXT)"},
		{"Insert1", "INSERT INTO t2 VALUES (1, 'a')"},
		{"Insert2", "INSERT INTO t2 VALUES (2, 'b')"},
		{"Insert3", "INSERT INTO t2 VALUES (3, 'c')"},
		{"Insert4", "INSERT INTO t2 VALUES (4, 'd')"},
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
		{"CaseWithArithmetic", "SELECT CASE x WHEN 1 THEN x * 10 WHEN 2 THEN x * 20 ELSE x END FROM t2"},
		{"CaseWithString", "SELECT CASE x WHEN 1 THEN 'One' WHEN 2 THEN 'Two' ELSE 'Other' END FROM t2"},
		{"CaseInHaving", "SELECT x, CASE WHEN x > 2 THEN 'High' ELSE 'Low' END FROM t2 GROUP BY x"},
		{"CaseMultipleColumns", "SELECT CASE x WHEN 1 THEN x ELSE x * -1 END, CASE y WHEN 'a' THEN 1 ELSE 0 END FROM t2"},
		{"CaseInSubquery", "SELECT * FROM (SELECT CASE x WHEN 1 THEN 'One' ELSE 'Other' END as label FROM t2)"},
	}

	for _, tt := range caseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
