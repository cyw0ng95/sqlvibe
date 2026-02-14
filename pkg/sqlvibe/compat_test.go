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

func TestPreparedStatements(t *testing.T) {
	sqlvibePath := "/tmp/test_prepared.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rows.Columns))
	}

	t.Logf("Columns: %v", rows.Columns)
}

func TestTransactionAPI(t *testing.T) {
	sqlvibePath := "/tmp/test_tx.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO accounts (id, balance) VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	rows, err := db.Query("SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows.Columns[0] != "balance" {
		t.Errorf("Expected balance column, got %s", rows.Columns[0])
	}
}

func TestMultipleTables(t *testing.T) {
	sqlvibePath := "/tmp/test_multi.db"
	sqlitePath := "/tmp/test_multi_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	tables := []string{
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)",
		"CREATE TABLE t2 (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
		"CREATE TABLE t3 (a TEXT, b REAL)",
	}

	for _, sql := range tables {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	sqlvibeRows, _ := sqlvibeDB.Query("SELECT name FROM sqlite_master WHERE type='table'")
	sqliteRows, _ := sqliteDB.Query("SELECT name FROM sqlite_master WHERE type='table'")

	t.Logf("sqlvibe tables query result: %v", sqlvibeRows)
	t.Logf("sqlite tables query result: %v", sqliteRows)
}
