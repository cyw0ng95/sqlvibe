package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02104_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE strings (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE strings (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO strings VALUES (1, 'hello')"},
		{"Empty", "INSERT INTO strings VALUES (2, '')"},
		{"Spaces", "INSERT INTO strings VALUES (3, '   ')"},
		{"Numbers", "INSERT INTO strings VALUES (4, '12345')"},
		{"Mixed", "INSERT INTO strings VALUES (5, 'Hello123')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	lenTests := []struct {
		name string
		sql  string
	}{
		{"LENGTH", "SELECT LENGTH(val) FROM strings WHERE id = 1"},
		{"LENGTH_Empty", "SELECT LENGTH(val) FROM strings WHERE id = 2"},
		{"LENGTH_Spaces", "SELECT LENGTH(val) FROM strings WHERE id = 3"},
		{"LENGTH_Numbers", "SELECT LENGTH(val) FROM strings WHERE id = 4"},
		{"LENGTH_Mixed", "SELECT LENGTH(val) FROM strings WHERE id = 5"},
	}

	for _, tt := range lenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E02105_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE strings2 (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE strings2 (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO strings2 VALUES (1, 'hello')"},
		{"Empty", "INSERT INTO strings2 VALUES (2, '')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	octetLenTests := []struct {
		name string
		sql  string
	}{
		{"LENGTH", "SELECT LENGTH(val) FROM strings2 WHERE id = 1"},
		{"LENGTH_Empty", "SELECT LENGTH(val) FROM strings2 WHERE id = 2"},
	}

	for _, tt := range octetLenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
