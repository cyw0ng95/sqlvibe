package sqlvibe

import (
	"fmt"
	"testing"
)

func TestIndexUsage(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	db.Exec("CREATE INDEX idx_email ON users(email)")
	
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO users VALUES (%d, 'name%d', 'user%d@test.com')", i, i%10, i%10))
	}

	t.Run("IndexLookup", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM users WHERE email = 'user5@test.com'")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows.Data))
		}
	})

	t.Run("UniqueIndexLookup", func(t *testing.T) {
		db.Exec("CREATE UNIQUE INDEX idx_id ON users(id)")
		rows, err := db.Query("SELECT * FROM users WHERE id = 50")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows.Data))
		}
	})

	t.Run("NoIndexFallback", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM users WHERE name = 'name5'")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows.Data))
		}
	})
}
