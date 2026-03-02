package F871_MERGE

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// TestSQL1999_F871_UpsertReplace_L1 tests INSERT OR REPLACE as a merge-equivalent operation.
func TestSQL1999_F871_UpsertReplace_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)",
		"INSERT INTO config VALUES ('host', 'localhost')",
		"INSERT INTO config VALUES ('port', '5432')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Replace existing key
	if _, err := db.Exec("INSERT OR REPLACE INTO config VALUES ('host', '10.0.0.1')"); err != nil {
		t.Fatalf("INSERT OR REPLACE: %v", err)
	}
	// Insert new key
	if _, err := db.Exec("INSERT OR REPLACE INTO config VALUES ('timeout', '30')"); err != nil {
		t.Fatalf("INSERT OR REPLACE new: %v", err)
	}

	rows, err := db.Query("SELECT key, value FROM config ORDER BY key")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
	// host should be updated
	found := false
	for _, row := range rows.Data {
		if row[0] == "host" && row[1] == "10.0.0.1" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected host=10.0.0.1, got %v", rows.Data)
	}
}

// TestSQL1999_F871_UpsertIgnore_L1 tests INSERT OR IGNORE as a conditional insert (merge-like).
func TestSQL1999_F871_UpsertIgnore_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'Bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Attempt to insert duplicate (should be ignored)
	if _, err := db.Exec("INSERT OR IGNORE INTO users VALUES (1, 'Ignored')"); err != nil {
		t.Fatalf("INSERT OR IGNORE on conflict: %v", err)
	}
	// Insert new row (should succeed)
	if _, err := db.Exec("INSERT OR IGNORE INTO users VALUES (3, 'Carol')"); err != nil {
		t.Fatalf("INSERT OR IGNORE new: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows.Data))
	}
	// id=1 name should still be Alice (not overwritten)
	if rows.Data[0][1] != "Alice" {
		t.Errorf("expected Alice, got %v", rows.Data[0][1])
	}
}

// TestSQL1999_F871_OnConflictUpdate_L1 tests INSERT ... ON CONFLICT DO UPDATE (upsert).
func TestSQL1999_F871_OnConflictUpdate_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE inventory (product_id INTEGER PRIMARY KEY, qty INTEGER)",
		"INSERT INTO inventory VALUES (100, 50)",
		"INSERT INTO inventory VALUES (200, 30)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Update existing row on conflict
	if _, err := db.Exec("INSERT INTO inventory VALUES (100, 75) ON CONFLICT(product_id) DO UPDATE SET qty = excluded.qty"); err != nil {
		t.Fatalf("ON CONFLICT DO UPDATE: %v", err)
	}
	// Insert new row (no conflict)
	if _, err := db.Exec("INSERT INTO inventory VALUES (300, 20) ON CONFLICT(product_id) DO UPDATE SET qty = excluded.qty"); err != nil {
		t.Fatalf("ON CONFLICT DO UPDATE new: %v", err)
	}

	rows, err := db.Query("SELECT product_id, qty FROM inventory ORDER BY product_id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
	if rows.Data[0][1] != int64(75) {
		t.Errorf("expected qty=75 for product 100, got %v", rows.Data[0][1])
	}
}

// TestSQL1999_F871_OnConflictAccumulate_L1 tests upsert that updates on conflict via excluded values.
func TestSQL1999_F871_OnConflictAccumulate_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE counters (name TEXT PRIMARY KEY, cnt INTEGER)",
		"INSERT INTO counters VALUES ('hits', 10)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Update existing counter via ON CONFLICT DO UPDATE SET to the new value
	if _, err := db.Exec("INSERT INTO counters VALUES ('hits', 99) ON CONFLICT(name) DO UPDATE SET cnt = excluded.cnt"); err != nil {
		t.Fatalf("update on conflict: %v", err)
	}
	// Insert new counter (no conflict)
	if _, err := db.Exec("INSERT INTO counters VALUES ('errors', 3) ON CONFLICT(name) DO UPDATE SET cnt = excluded.cnt"); err != nil {
		t.Fatalf("new counter upsert: %v", err)
	}

	rows, err := db.Query("SELECT name, cnt FROM counters ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows.Data))
	}
	// hits should be 99 (updated via excluded.cnt)
	for _, row := range rows.Data {
		if row[0] == "hits" && row[1] != int64(99) {
			t.Errorf("expected hits=99, got %v", row[1])
		}
		if row[0] == "errors" && row[1] != int64(3) {
			t.Errorf("expected errors=3, got %v", row[1])
		}
	}
}

// TestSQL1999_F871_MultiRowUpsert_L1 tests multi-row upsert (batch merge-equivalent).
func TestSQL1999_F871_MultiRowUpsert_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE settings (k TEXT PRIMARY KEY, v TEXT)",
		"INSERT INTO settings VALUES ('a', 'old_a')",
		"INSERT INTO settings VALUES ('b', 'old_b')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Batch upsert: update a, b and insert c
	batchSQL := []string{
		"INSERT INTO settings VALUES ('a', 'new_a') ON CONFLICT(k) DO UPDATE SET v = excluded.v",
		"INSERT INTO settings VALUES ('b', 'new_b') ON CONFLICT(k) DO UPDATE SET v = excluded.v",
		"INSERT INTO settings VALUES ('c', 'new_c') ON CONFLICT(k) DO UPDATE SET v = excluded.v",
	}
	for _, sql := range batchSQL {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("batch upsert %q: %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT k, v FROM settings ORDER BY k")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
	expected := [][]interface{}{{"a", "new_a"}, {"b", "new_b"}, {"c", "new_c"}}
	for i, row := range rows.Data {
		if row[0] != expected[i][0] || row[1] != expected[i][1] {
			t.Errorf("row %d: expected %v, got %v", i, expected[i], row)
		}
	}
}
