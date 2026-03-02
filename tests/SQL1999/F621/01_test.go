package F621

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F621_TriggerBasic_L1 tests basic AFTER INSERT trigger functionality.
func TestSQL1999_F621_TriggerBasic_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)",
		"CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, order_id INTEGER)",
		`CREATE TRIGGER after_insert_order
		AFTER INSERT ON orders
		BEGIN
			INSERT INTO audit_log (id, action, order_id) VALUES (NEW.id, 'INSERT', NEW.id);
		END`,
	}
	for _, s := range setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("Setup error (%s): %v", s[:30], err)
		}
	}

	if _, err := db.Exec("INSERT INTO orders VALUES (1, 99.99)"); err != nil {
		t.Fatalf("INSERT error: %v", err)
	}
	if _, err := db.Exec("INSERT INTO orders VALUES (2, 50.00)"); err != nil {
		t.Fatalf("INSERT error: %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM audit_log")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(2) {
		t.Errorf("Expected 2 audit log entries, got %v", rows.Data)
	}
}

// TestSQL1999_F621_TriggerBeforeInsert_L1 tests BEFORE INSERT trigger.
func TestSQL1999_F621_TriggerBeforeInsert_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE counter (name TEXT PRIMARY KEY, val INTEGER)",
		"INSERT INTO counter VALUES ('inserts', 0)",
		`CREATE TRIGGER track_insert
		BEFORE INSERT ON counter
		WHEN NEW.name != 'inserts'
		BEGIN
			UPDATE counter SET val = val + 1 WHERE name = 'inserts';
		END`,
	}
	for _, s := range setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	db.Exec("INSERT INTO counter VALUES ('foo', 1)")
	db.Exec("INSERT INTO counter VALUES ('bar', 2)")

	rows, err := db.Query("SELECT val FROM counter WHERE name = 'inserts'")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Error("No rows returned")
	}
	// Should have counted 2 inserts
	if rows.Data[0][0] != int64(2) {
		t.Errorf("Expected 2 inserts counted, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F621_CreateDropTrigger_L1 tests CREATE and DROP TRIGGER.
func TestSQL1999_F621_CreateDropTrigger_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
		"CREATE TABLE log_ (msg TEXT)",
		`CREATE TRIGGER trig1
		AFTER INSERT ON t
		BEGIN
			INSERT INTO log_ VALUES ('inserted');
		END`,
	}
	for _, s := range setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	db.Exec("INSERT INTO t VALUES (1, 'a')")

	rows, _ := db.Query("SELECT COUNT(*) FROM log_")
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(1) {
		t.Errorf("Expected 1 log entry before drop, got %v", rows.Data)
	}

	// Drop trigger
	if _, err := db.Exec("DROP TRIGGER trig1"); err != nil {
		t.Fatalf("DROP TRIGGER error: %v", err)
	}

	db.Exec("INSERT INTO t VALUES (2, 'b')")

	rows, _ = db.Query("SELECT COUNT(*) FROM log_")
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(1) {
		t.Errorf("Expected still 1 log entry after drop, got %v", rows.Data)
	}
}

// TestSQL1999_F621_TriggerAfterDelete_L1 tests AFTER DELETE trigger.
func TestSQL1999_F621_TriggerAfterDelete_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE deleted_items (id INTEGER, name TEXT)",
		`CREATE TRIGGER backup_on_delete
		AFTER DELETE ON items
		BEGIN
			INSERT INTO deleted_items VALUES (OLD.id, OLD.name);
		END`,
		"INSERT INTO items VALUES (1, 'Alpha')",
		"INSERT INTO items VALUES (2, 'Beta')",
		"INSERT INTO items VALUES (3, 'Gamma')",
	}
	for _, s := range setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	db.Exec("DELETE FROM items WHERE id = 2")

	rows, err := db.Query("SELECT id, name FROM deleted_items")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("Expected 1 deleted item, got %d", len(rows.Data))
		return
	}
	if rows.Data[0][1] != "Beta" {
		t.Errorf("Expected 'Beta' in deleted_items, got %v", rows.Data[0][1])
	}
}

// TestSQL1999_F621_TriggerIfNotExists_L1 tests IF NOT EXISTS on CREATE TRIGGER.
func TestSQL1999_F621_TriggerIfNotExists_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("CREATE TRIGGER trig1 AFTER INSERT ON t BEGIN SELECT 1; END")

	// Duplicate without IF NOT EXISTS should error
	if _, err := db.Exec("CREATE TRIGGER trig1 AFTER INSERT ON t BEGIN SELECT 1; END"); err == nil {
		t.Error("Expected error for duplicate trigger, got nil")
	}

	// With IF NOT EXISTS should succeed
	if _, err := db.Exec("CREATE TRIGGER IF NOT EXISTS trig1 AFTER INSERT ON t BEGIN SELECT 1; END"); err != nil {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS error: %v", err)
	}

	// DROP TRIGGER IF EXISTS for non-existent trigger
	if _, err := db.Exec("DROP TRIGGER IF EXISTS nonexistent_trigger"); err != nil {
		t.Errorf("DROP TRIGGER IF EXISTS error: %v", err)
	}
}
