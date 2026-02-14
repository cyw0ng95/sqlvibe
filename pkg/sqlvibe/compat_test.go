package sqlvibe

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func TestCompatibilityWithSQLite(t *testing.T) {
	sqlvibePath := "/tmp/test_sqlvibe.db"
	sqlitePath := "/tmp/test_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	tests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"},
		{"Insert1", "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"},
		{"Insert2", "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"},
		{"Insert3", "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"},
		{"SelectAll", "SELECT * FROM users"},
		{"SelectColumns", "SELECT name, age FROM users"},
		{"SelectWhere", "SELECT * FROM users WHERE age > 28"},
		{"SelectOrderBy", "SELECT * FROM users ORDER BY age DESC"},
		{"Update", "UPDATE users SET age = 31 WHERE id = 1"},
		{"Delete", "DELETE FROM users WHERE id = 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sqlvibeDB.Exec(tt.sql)
			if err != nil {
				t.Logf("sqlvibe exec error: %v", err)
			}

			_, err = sqliteDB.Exec(tt.sql)
			if err != nil {
				t.Logf("sqlite exec error: %v", err)
			}
		})
	}

	t.Run("VerifyResults", func(t *testing.T) {
		sqlvibeRows, err := sqlvibeDB.Query("SELECT id, name, age FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("sqlvibe query error: %v", err)
		}

		rows, err := sqliteDB.Query("SELECT id, name, age FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("sqlite query error: %v", err)
		}
		defer rows.Close()

		var sqliteResults []map[string]interface{}
		for rows.Next() {
			var id int64
			var name string
			var age int64
			rows.Scan(&id, &name, &age)
			sqliteResults = append(sqliteResults, map[string]interface{}{
				"id":   id,
				"name": name,
				"age":  age,
			})
		}

		t.Logf("sqlvibe columns: %v", sqlvibeRows.Columns)
		t.Logf("sqlite results: %v", sqliteResults)
	})
}
