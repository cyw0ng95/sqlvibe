package vtab_test

import (
	"testing"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
	_ "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe" // ensure init() registers the series module
	sqlvibe "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestVTabRegistry_RegisterAndGet verifies that modules can be registered and retrieved.
func TestVTabRegistry_RegisterAndGet(t *testing.T) {
	// series is registered by pkg/sqlvibe init()
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found in registry")
	}
	if mod == nil {
		t.Fatal("series module is nil")
	}
}

// TestVTabCursorRowStore verifies that RowStoreCursor scans all live rows.
func TestVTabCursorRowStore(t *testing.T) {
	rs := DS.NewRowStore([]string{"id", "name"}, []DS.ValueType{DS.TypeInt, DS.TypeString})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(1), DS.StringValue("alice")}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(2), DS.StringValue("bob")}))

	cursor := DS.NewRowStoreCursor(rs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	var count int
	for !cursor.Eof() {
		v, err := cursor.Column(0)
		if err != nil {
			t.Fatalf("Column: %v", err)
		}
		if v == nil {
			t.Error("expected non-nil value for column 0")
		}
		count++
		if err := cursor.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	cursor.Close()
}

// TestSeriesVTab_BasicRange tests SELECT * FROM series(1,5) returns values 1..5.
func TestSeriesVTab_BasicRange(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM series(1, 5)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows.Data) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows.Data))
	}
	for i, row := range rows.Data {
		want := int64(i + 1)
		got, ok := row[0].(int64)
		if !ok {
			t.Errorf("row %d: expected int64, got %T (%v)", i, row[0], row[0])
			continue
		}
		if got != want {
			t.Errorf("row %d: expected %d, got %d", i, want, got)
		}
	}
}

// TestSeriesVTab_StepTwo tests SELECT * FROM series(1,10,2) returns odd numbers 1..9.
func TestSeriesVTab_StepTwo(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM series(1, 10, 2)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	expected := []int64{1, 3, 5, 7, 9}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows.Data))
	}
	for i, row := range rows.Data {
		got, ok := row[0].(int64)
		if !ok {
			t.Errorf("row %d: expected int64, got %T (%v)", i, row[0], row[0])
			continue
		}
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_CreateVirtualTable tests CREATE VIRTUAL TABLE then SELECT.
func TestSeriesVTab_CreateVirtualTable(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE VIRTUAL TABLE t1 USING series(1, 10)"); err != nil {
		t.Fatalf("CREATE VIRTUAL TABLE: %v", err)
	}

	rows, err := db.Query("SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows.Data) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_EmptyRange tests that start > stop returns 0 rows.
func TestSeriesVTab_EmptyRange(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM series(10, 1)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_IfNotExists tests CREATE VIRTUAL TABLE IF NOT EXISTS is idempotent.
func TestSeriesVTab_IfNotExists(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE VIRTUAL TABLE IF NOT EXISTS t2 USING series(1, 3)"); err != nil {
		t.Fatalf("first CREATE: %v", err)
	}
	// Second create with IF NOT EXISTS must not error.
	if _, err := db.Exec("CREATE VIRTUAL TABLE IF NOT EXISTS t2 USING series(1, 5)"); err != nil {
		t.Fatalf("second CREATE IF NOT EXISTS: %v", err)
	}

	rows, err := db.Query("SELECT * FROM t2")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Should still have the original 3 rows.
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows.Data))
	}
}
