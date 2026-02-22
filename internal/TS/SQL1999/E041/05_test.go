package E041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04105_L1(t *testing.T) {
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
		{"SimpleCheck", "CREATE TABLE t1 (a INTEGER CHECK (a >= 0))"},
		{"CheckWithMultipleCols", "CREATE TABLE t2 (a INTEGER, b INTEGER, CHECK (a > b))"},
		{"MultipleChecks", "CREATE TABLE t3 (a INTEGER CHECK (a >= 0), b INTEGER CHECK (b <= 100))"},
		{"CheckWithText", "CREATE TABLE t4 (name TEXT CHECK (length(name) > 0))"},
		{"CheckWithExpression", "CREATE TABLE t5 (age INTEGER CHECK (age >= 18 AND age <= 120))"},
		{"CheckWithNotNull", "CREATE TABLE t6 (a INTEGER NOT NULL CHECK (a > 0))"},
		{"CheckWithDefault", "CREATE TABLE t7 (a INTEGER DEFAULT 0 CHECK (a >= 0))"},
		{"ComplexCheck", "CREATE TABLE t8 (x INTEGER, y INTEGER, z INTEGER, CHECK (x + y > z))"},
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
		{"InsertValidCheck", "INSERT INTO t1 VALUES (5)"},
		{"InsertValidMultiple", "INSERT INTO t2 VALUES (10, 5)"},
		{"InsertValidRange", "INSERT INTO t5 VALUES (25)"},
		{"InsertValidDefault", "INSERT INTO t7 VALUES ()"},
		{"InsertValidComplex", "INSERT INTO t8 VALUES (5, 10, 12)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertErrorTests := []struct {
		name string
		sql  string
	}{
		{"InsertNegativeCheck", "INSERT INTO t1 VALUES (-5)"},
		{"InsertFailingCheck", "INSERT INTO t2 VALUES (3, 10)"},
		{"InsertOutOfRange", "INSERT INTO t5 VALUES (15)"},
		{"InsertComplexFail", "INSERT INTO t8 VALUES (5, 5, 20)"},
	}

	for _, tt := range insertErrorTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT1", "SELECT * FROM t1"},
		{"SelectFromT5", "SELECT * FROM t5"},
		{"SelectFromT7", "SELECT * FROM t7"},
		{"SelectFromT8", "SELECT * FROM t8"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
