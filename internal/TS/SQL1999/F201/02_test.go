package F201

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F201_F26103_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t3 (score INTEGER, grade TEXT)"},
		{"Insert1", "INSERT INTO t3 VALUES (95, 'A')"},
		{"Insert2", "INSERT INTO t3 VALUES (85, 'B')"},
		{"Insert3", "INSERT INTO t3 VALUES (75, 'C')"},
		{"Insert4", "INSERT INTO t3 VALUES (65, 'D')"},
		{"Insert5", "INSERT INTO t3 VALUES (55, 'F')"},
		{"Insert6", "INSERT INTO t3 VALUES (100, 'A+')"},
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
		{"CaseSimple", "SELECT CASE score WHEN 95 THEN 'A' WHEN 85 THEN 'B' ELSE 'Other' END FROM t3"},
		{"CaseSearched", "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END FROM t3"},
		{"CaseWithElse", "SELECT CASE WHEN score > 100 THEN 'Invalid' ELSE 'Valid' END FROM t3"},
		{"CaseWithoutElse", "SELECT CASE WHEN score >= 90 THEN 'A' END FROM t3"},
		{"CaseInWhere", "SELECT * FROM t3 WHERE CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' END = 'A'"},
		{"CaseWithAggregate", "SELECT CASE WHEN COUNT(*) > 3 THEN 'Many' ELSE 'Few' END FROM t3"},
		{"CaseNested", "SELECT CASE WHEN score >= 90 THEN CASE WHEN score = 100 THEN 'Perfect' ELSE 'Excellent' END ELSE 'Other' END FROM t3"},
		{"CaseInOrderBy", "SELECT * FROM t3 ORDER BY CASE WHEN score >= 90 THEN 1 WHEN score >= 80 THEN 2 ELSE 3 END"},
	}

	for _, tt := range caseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
