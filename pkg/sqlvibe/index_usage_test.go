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

// TestIndexBetweenScan verifies that BETWEEN on an indexed column returns
// correct results (regression guard for Wave 3 index range scan).
func TestIndexBetweenScan(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
	db.Exec("CREATE INDEX idx_val ON nums(val)")
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO nums VALUES (%d, %d)", i, i))
	}

	rows, err := db.Query("SELECT id FROM nums WHERE val BETWEEN 10 AND 19")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) != 10 {
		t.Errorf("BETWEEN: expected 10 rows, got %d", len(rows.Data))
	}
}

// TestIndexInListScan verifies that IN list on an indexed column returns
// correct results (regression guard for Wave 3 IN-list index lookup).
func TestIndexInListScan(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE cats (id INTEGER PRIMARY KEY, cat TEXT)")
	db.Exec("CREATE INDEX idx_cat ON cats(cat)")
	for i := 0; i < 50; i++ {
		c := fmt.Sprintf("cat%d", i%5)
		db.Exec(fmt.Sprintf("INSERT INTO cats VALUES (%d, '%s')", i, c))
	}

	rows, err := db.Query("SELECT id FROM cats WHERE cat IN ('cat0', 'cat1')")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) != 20 {
		t.Errorf("IN list: expected 20 rows, got %d", len(rows.Data))
	}
}

// TestIndexLikePrefixScan verifies that LIKE 'prefix%' on an indexed TEXT
// column returns correct results (regression guard for Wave 3 prefix scan).
func TestIndexLikePrefixScan(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT)")
	db.Exec("CREATE INDEX idx_word ON words(word)")
	words := []string{"apple", "apricot", "banana", "avocado", "cherry"}
	for i, w := range words {
		db.Exec(fmt.Sprintf("INSERT INTO words VALUES (%d, '%s')", i, w))
	}

	rows, err := db.Query("SELECT word FROM words WHERE word LIKE 'a%'")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("LIKE prefix: expected 3 rows (apple, apricot, avocado), got %d", len(rows.Data))
	}
}
