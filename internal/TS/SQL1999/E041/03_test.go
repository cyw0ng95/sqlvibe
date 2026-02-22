package E041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04103_L1(t *testing.T) {
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
		{"SinglePK", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)"},
		{"PKFirstCol", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"},
		{"PKMiddleCol", "CREATE TABLE t3 (name TEXT, id INTEGER PRIMARY KEY, age INTEGER)"},
		{"PKLastCol", "CREATE TABLE t4 (name TEXT, age INTEGER, id INTEGER PRIMARY KEY)"},
		{"CompositePK", "CREATE TABLE t5 (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))"},
		{"CompositePKAllCols", "CREATE TABLE t6 (a INTEGER, b INTEGER, PRIMARY KEY (a, b))"},
		{"CompositePKWithOtherCols", "CREATE TABLE t7 (a INTEGER, b INTEGER, c TEXT, d REAL, PRIMARY KEY (a, b))"},
		{"AutoIncrement", "CREATE TABLE t8 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"},
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
		{"InsertSinglePK", "INSERT INTO t1 VALUES (1, 'test')"},
		{"InsertMultipleSinglePK", "INSERT INTO t1 VALUES (2, 'test2'), (3, 'test3')"},
		{"InsertCompositePK", "INSERT INTO t5 VALUES (1, 2, 'test')"},
		{"InsertMultipleCompositePK", "INSERT INTO t5 VALUES (1, 3, 'test1'), (2, 1, 'test2')"},
		{"InsertAutoIncrement", "INSERT INTO t8 (name) VALUES ('test')"},
		{"InsertAutoIncrementExplicit", "INSERT INTO t8 VALUES (10, 'test')"},
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
		{"SelectFromT1", "SELECT * FROM t1 ORDER BY id"},
		{"SelectFromT5", "SELECT * FROM t5 ORDER BY a, b"},
		{"SelectFromT8", "SELECT * FROM t8 ORDER BY id"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
