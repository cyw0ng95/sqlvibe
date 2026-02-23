package V013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_V013_ViewShowsData_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author TEXT)")
	sqlvibeDB.Exec("INSERT INTO books VALUES (1, 'Go Programming', 'Donovan')")
	sqlvibeDB.Exec("INSERT INTO books VALUES (2, 'Clean Code', 'Martin')")
	sqlvibeDB.Exec("CREATE VIEW book_titles AS SELECT id, title FROM books")
	sqliteDB.Exec("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author TEXT)")
	sqliteDB.Exec("INSERT INTO books VALUES (1, 'Go Programming', 'Donovan')")
	sqliteDB.Exec("INSERT INTO books VALUES (2, 'Clean Code', 'Martin')")
	sqliteDB.Exec("CREATE VIEW book_titles AS SELECT id, title FROM books")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectFromView", "SELECT * FROM book_titles ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V013_ViewReflectsInsert_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (1, 'Alice', 100)")
	sqlvibeDB.Exec("CREATE VIEW top_scores AS SELECT id, player, score FROM scores WHERE score >= 50")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (2, 'Bob', 80)")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (3, 'Carol', 30)")
	sqliteDB.Exec("CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)")
	sqliteDB.Exec("INSERT INTO scores VALUES (1, 'Alice', 100)")
	sqliteDB.Exec("CREATE VIEW top_scores AS SELECT id, player, score FROM scores WHERE score >= 50")
	sqliteDB.Exec("INSERT INTO scores VALUES (2, 'Bob', 80)")
	sqliteDB.Exec("INSERT INTO scores VALUES (3, 'Carol', 30)")

	tests := []struct {
		name string
		sql  string
	}{
		{"ViewReflectsNewData", "SELECT * FROM top_scores ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_V013_ViewMultipleColumns_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE person (id INTEGER PRIMARY KEY, first TEXT, last TEXT, age INTEGER, city TEXT)")
	sqlvibeDB.Exec("INSERT INTO person VALUES (1, 'Alice', 'Smith', 30, 'NYC')")
	sqlvibeDB.Exec("INSERT INTO person VALUES (2, 'Bob', 'Jones', 25, 'LA')")
	sqlvibeDB.Exec("INSERT INTO person VALUES (3, 'Carol', 'White', 35, 'NYC')")
	sqlvibeDB.Exec("CREATE VIEW nyc_residents AS SELECT id, first, last, age FROM person WHERE city = 'NYC'")
	sqliteDB.Exec("CREATE TABLE person (id INTEGER PRIMARY KEY, first TEXT, last TEXT, age INTEGER, city TEXT)")
	sqliteDB.Exec("INSERT INTO person VALUES (1, 'Alice', 'Smith', 30, 'NYC')")
	sqliteDB.Exec("INSERT INTO person VALUES (2, 'Bob', 'Jones', 25, 'LA')")
	sqliteDB.Exec("INSERT INTO person VALUES (3, 'Carol', 'White', 35, 'NYC')")
	sqliteDB.Exec("CREATE VIEW nyc_residents AS SELECT id, first, last, age FROM person WHERE city = 'NYC'")

	tests := []struct {
		name string
		sql  string
	}{
		{"SelectAllColumns", "SELECT * FROM nyc_residents ORDER BY id"},
		{"SelectSubsetColumns", "SELECT first, last FROM nyc_residents ORDER BY last"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
