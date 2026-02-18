package F261

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F262_F26103_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, status TEXT, amount REAL)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'pending', 50), (4, 'completed', 300), (5, NULL, 0)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	searchedCaseTests := []struct {
		name string
		sql  string
	}{
		{"SearchedCaseMultiple", "SELECT CASE WHEN status = 'active' THEN 'Active' WHEN status = 'inactive' THEN 'Inactive' WHEN status = 'pending' THEN 'Pending' ELSE 'Unknown' END FROM t1"},
		{"SearchedCaseComparison", "SELECT CASE WHEN amount > 100 THEN 'High' WHEN amount > 50 THEN 'Medium' ELSE 'Low' END FROM t1"},
		{"SearchedCaseWithAnd", "SELECT CASE WHEN status = 'active' AND amount > 50 THEN 'Active High' ELSE 'Other' END FROM t1"},
		{"SearchedCaseWithOr", "SELECT CASE WHEN status = 'active' OR status = 'pending' THEN 'In Progress' ELSE 'Done' END FROM t1"},
	}

	for _, tt := range searchedCaseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F263_F26104_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (10, 20), (5, 5), (NULL, 30)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullifTests := []struct {
		name string
		sql  string
	}{
		{"NullIfEqual", "SELECT NULLIF(a, b) FROM t1"},
		{"NullIfNotEqual", "SELECT NULLIF(a, 100) FROM t1"},
		{"NullIfWithNull", "SELECT NULLIF(NULL, 0)"},
		{"NullIfZero", "SELECT NULLIF(0, 0)"},
		{"NullIfExpression", "SELECT NULLIF(a + b, 30) FROM t1"},
	}

	for _, tt := range nullifTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F264_F26105_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'first', 'A'), (2, NULL, 'B'), (3, 'third', NULL), (4, NULL, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	coalesceTests := []struct {
		name string
		sql  string
	}{
		{"CoalesceTwo", "SELECT COALESCE(a, b) FROM t1"},
		{"CoalesceThree", "SELECT COALESCE(a, b, c) FROM t1"},
		{"CoalesceWithDefault", "SELECT COALESCE(b, 'default') FROM t1"},
		{"CoalesceAllNull", "SELECT COALESCE(a, b, c, 'all null') FROM t1"},
		{"CoalesceInExpression", "SELECT COALESCE(a, 0) + 10 FROM t1"},
		{"CoalesceNested", "SELECT COALESCE(b, COALESCE(c, 'nested default')) FROM t1"},
	}

	for _, tt := range coalesceTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F265_F26106_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, status TEXT, amount REAL)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'pending', 50)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	caseInClausesTests := []struct {
		name string
		sql  string
	}{
		{"CaseInWhere", "SELECT * FROM t1 WHERE CASE WHEN amount > 100 THEN 1 ELSE 0 END = 1"},
		{"CaseInOrderBy", "SELECT * FROM t1 ORDER BY CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END"},
		{"CaseInGroupBy", "SELECT CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END AS category, COUNT(*) FROM t1 GROUP BY CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END"},
		{"CaseWithoutElse", "SELECT CASE status WHEN 'active' THEN 'Active' END FROM t1"},
		{"CaseNested", "SELECT CASE WHEN amount > (CASE WHEN status = 'active' THEN 50 ELSE 100 END) THEN 'High' ELSE 'Low' END FROM t1"},
		{"CaseWithAggregation", "SELECT CASE WHEN COUNT(*) > 0 THEN 'has_data' ELSE 'empty' END FROM t1"},
	}

	for _, tt := range caseInClausesTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
