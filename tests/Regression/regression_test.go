package Regression

import (
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_UnknownFunction_L1 verifies that calling an undefined function
// returns an error instead of silently returning NULL.
// Regression: evaluateFuncCallOnRow and evalFuncCall previously returned nil
// without setting any error when a function was not found.
func TestRegression_UnknownFunction_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (x INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A function that doesn't exist should produce an error, not NULL.
	_, err = db.Query(`SELECT totally_unknown_func(x) FROM t`)
	if err == nil {
		t.Error("expected error for unknown function in SELECT, got nil")
	} else if !strings.Contains(err.Error(), "no such function") {
		t.Errorf("expected 'no such function' error, got: %v", err)
	}
}

// TestRegression_UnknownFunction_Constant_L1 verifies that calling an undefined
// function in a constant SELECT (no FROM clause) returns an error.
func TestRegression_UnknownFunction_Constant_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Query(`SELECT totally_unknown_func(1)`)
	if err == nil {
		t.Error("expected error for unknown function in constant SELECT, got nil")
	} else if !strings.Contains(err.Error(), "no such function") {
		t.Errorf("expected 'no such function' error, got: %v", err)
	}
}

// TestRegression_JulianDayNULL_L1 verifies that JULIANDAY(NULL) returns NULL,
// not the current Julian day.
// Regression: parseDateTimeValue returned zero time for NULL, causing JULIANDAY
// to fall back to time.Now() instead of returning NULL.
func TestRegression_JulianDayNULL_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT julianday(NULL)`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows.Data) != 1 || len(rows.Data[0]) != 1 {
		t.Fatalf("expected 1 row with 1 col, got %v", rows.Data)
	}
	if rows.Data[0][0] != nil {
		t.Errorf("JULIANDAY(NULL) should return NULL, got: %v (%T)", rows.Data[0][0], rows.Data[0][0])
	}
}

// TestRegression_RoundFloat_L1 verifies that ROUND(x) returns a float64 value
// even when the decimal count is 0, matching SQLite behaviour.
// Regression: getRound returned int64 for 0 decimals, causing type mismatch in
// queries that expected a real result (e.g. ROUND(julianday('now'))).
func TestRegression_RoundFloat_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT ROUND(2.7)`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows.Data) != 1 || len(rows.Data[0]) != 1 {
		t.Fatalf("expected 1 row with 1 col, got %v", rows.Data)
	}
	val := rows.Data[0][0]
	if _, ok := val.(float64); !ok {
		t.Errorf("ROUND(2.7) should return float64, got %T (%v)", val, val)
	}
	if val.(float64) != 3.0 {
		t.Errorf("ROUND(2.7) should be 3.0, got %v", val)
	}
}

// TestRegression_RoundJulianDay_L1 verifies that ROUND(JULIANDAY('now'), 2)
// works correctly end-to-end.
func TestRegression_RoundJulianDay_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT ROUND(julianday('2024-01-01'), 5)`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows.Data) != 1 || len(rows.Data[0]) != 1 {
		t.Fatalf("expected 1 row with 1 col, got %v", rows.Data)
	}
	val := rows.Data[0][0]
	if _, ok := val.(float64); !ok {
		t.Errorf("ROUND(julianday(...), 5) should return float64, got %T (%v)", val, val)
	}
}
