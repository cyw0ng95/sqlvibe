package sqlvibe

import (
	"strings"
	"testing"
	"time"
)

func TestDateTimeFunctions(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	t.Run("CURRENT_DATE", func(t *testing.T) {
		rows, err := db.Query("SELECT CURRENT_DATE")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatal("no rows returned")
		}
		expected := time.Now().Format("2006-01-02")
		if rows.Data[0][0] != expected {
			t.Errorf("expected %v, got %v", expected, rows.Data[0][0])
		}
	})

	t.Run("CURRENT_TIME", func(t *testing.T) {
		rows, err := db.Query("SELECT CURRENT_TIME")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatal("no rows returned")
		}
		result := rows.Data[0][0].(string)
		if len(result) != 8 {
			t.Errorf("expected time format HH:MM:SS, got %v", result)
		}
	})

	t.Run("CURRENT_TIMESTAMP", func(t *testing.T) {
		rows, err := db.Query("SELECT CURRENT_TIMESTAMP")
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatal("no rows returned")
		}
		result := rows.Data[0][0].(string)
		if !strings.Contains(result, " ") {
			t.Errorf("expected datetime format, got %v", result)
		}
	})

	t.Run("DATE_table", func(t *testing.T) {
		db.Exec("CREATE TABLE events (id INTEGER, dt DATE)")
		db.Exec("INSERT INTO events VALUES (1, '2024-01-15')")
		rows, _ := db.Query("SELECT dt FROM events WHERE id = 1")
		if rows.Data[0][0] != "2024-01-15" {
			t.Errorf("expected 2024-01-15, got %v", rows.Data[0][0])
		}
	})
}
