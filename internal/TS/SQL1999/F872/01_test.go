package F872

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	return db
}

// TestSQL1999_F872_Unhex_L1 tests UNHEX function.
func TestSQL1999_F872_Unhex_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT UNHEX('48656C6C6F')")
	if err != nil {
		t.Fatalf("UNHEX error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("No rows returned")
	}
	result := rows.Data[0][0]
	b, ok := result.([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T: %v", result, result)
	}
	if string(b) != "Hello" {
		t.Errorf("Expected 'Hello', got %q", string(b))
	}
}

// TestSQL1999_F872_UnhexNull_L1 tests UNHEX with NULL returns NULL.
func TestSQL1999_F872_UnhexNull_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT UNHEX(NULL)")
	if err != nil {
		t.Fatalf("UNHEX(NULL) error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("No rows")
	}
	if rows.Data[0][0] != nil {
		t.Errorf("Expected NULL, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F872_Random_L1 tests RANDOM() returns an integer.
func TestSQL1999_F872_Random_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT RANDOM()")
	if err != nil {
		t.Fatalf("RANDOM() error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("No rows")
	}
	_, ok := rows.Data[0][0].(int64)
	if !ok {
		t.Errorf("Expected int64 from RANDOM(), got %T: %v", rows.Data[0][0], rows.Data[0][0])
	}
}

// TestSQL1999_F872_Randomblob_L1 tests RANDOMBLOB(n) returns n bytes.
func TestSQL1999_F872_Randomblob_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT RANDOMBLOB(8)")
	if err != nil {
		t.Fatalf("RANDOMBLOB error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("No rows")
	}
	b, ok := rows.Data[0][0].([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", rows.Data[0][0])
	}
	if len(b) != 8 {
		t.Errorf("Expected 8 bytes, got %d", len(b))
	}
}

// TestSQL1999_F872_Zeroblob_L1 tests ZEROBLOB(n) returns n zero bytes.
func TestSQL1999_F872_Zeroblob_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT ZEROBLOB(5)")
	if err != nil {
		t.Fatalf("ZEROBLOB error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("No rows")
	}
	b, ok := rows.Data[0][0].([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", rows.Data[0][0])
	}
	if len(b) != 5 {
		t.Errorf("Expected 5 bytes, got %d", len(b))
	}
	for i, v := range b {
		if v != 0 {
			t.Errorf("Expected zero byte at index %d, got %d", i, v)
		}
	}
}

// TestSQL1999_F872_IIF_L1 tests IIF function.
func TestSQL1999_F872_IIF_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	tests := []struct {
		sql      string
		expected interface{}
	}{
		{"SELECT IIF(1, 'yes', 'no')", "yes"},
		{"SELECT IIF(0, 'yes', 'no')", "no"},
		{"SELECT IIF(NULL, 'yes', 'no')", "no"},
		{"SELECT IIF(1 = 1, 42, 0)", int64(42)},
		{"SELECT IIF(1 = 2, 42, 99)", int64(99)},
	}

	for _, tt := range tests {
		rows, err := db.Query(tt.sql)
		if err != nil {
			t.Fatalf("IIF query %q error: %v", tt.sql, err)
		}
		if len(rows.Data) == 0 {
			t.Fatalf("No rows for %q", tt.sql)
		}
		got := rows.Data[0][0]
		if got != tt.expected {
			t.Errorf("IIF %q: expected %v (%T), got %v (%T)", tt.sql, tt.expected, tt.expected, got, got)
		}
	}
}
