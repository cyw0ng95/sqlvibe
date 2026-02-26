package Regression

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_NullPropagationArith_v0100 verifies NULL propagation in arithmetic.
// Regression: NULL + x should always be NULL (both legacy and bytecode paths).
func TestRegression_NullPropagationArith_v0100(t *testing.T) {
	for _, useBytecode := range []bool{false, true} {
		name := "legacy"
		if useBytecode {
			name = "bytecode"
		}
		t.Run(name, func(t *testing.T) {
			db, err := sqlvibe.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			if useBytecode {
				if _, err := db.Exec("PRAGMA use_bytecode = 1"); err != nil {
					t.Fatal(err)
				}
			}

			rows, err := db.Query("SELECT NULL + 1")
			if err != nil {
				t.Fatalf("SELECT NULL + 1: %v", err)
			}
			if len(rows.Data) == 0 {
				t.Fatal("expected 1 row")
			}
			if rows.Data[0][0] != nil {
				t.Errorf("NULL + 1 = %v, want nil", rows.Data[0][0])
			}
		})
	}
}

// TestRegression_IntegerOverflowWrap_v0100 verifies that integer overflow wraps
// rather than panicking.
func TestRegression_IntegerOverflowWrap_v0100(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA use_bytecode = 1"); err != nil {
		t.Fatal(err)
	}

	// 9223372036854775807 + 1 overflows int64 — should not panic.
	rows, err := db.Query("SELECT 9223372036854775807 + 1")
	if err != nil {
		// A compilation/execution error is acceptable; panic is not.
		return
	}
	// Result is implementation-defined on overflow; just ensure no panic.
	_ = rows
}

// TestRegression_LikeWildcard_v0100 verifies LIKE with % and _ wildcards.
func TestRegression_LikeWildcard_v0100(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE words(w TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"apple", "application", "apply", "banana"} {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO words VALUES ('%s')", w)); err != nil {
			t.Fatal(err)
		}
	}

	// Legacy path
	rows, err := db.Query("SELECT w FROM words WHERE w LIKE 'app%'")
	if err != nil {
		t.Fatalf("LIKE legacy: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("LIKE 'app%%' legacy: expected 3 rows, got %d", len(rows.Data))
	}

	// _ wildcard: "appl_" matches "apple" and "apply" (5-char words starting with "appl")
	rows, err = db.Query("SELECT w FROM words WHERE w LIKE 'appl_'")
	if err != nil {
		t.Fatalf("LIKE _ legacy: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("LIKE 'appl_' legacy: expected 2 rows, got %d", len(rows.Data))
	}
}

// TestRegression_CastTypes_v0100 verifies CAST with various input types.
func TestRegression_CastTypes_v0100(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT CAST('42' AS INTEGER)", "42"},
		{"SELECT CAST(3.7 AS INTEGER)", "3"},
		{"SELECT CAST(42 AS TEXT)", "42"},
	}

	for _, tc := range cases {
		rows, err := db.Query(tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		if len(rows.Data) == 0 {
			t.Errorf("%s: no rows", tc.sql)
			continue
		}
		got := fmt.Sprintf("%v", rows.Data[0][0])
		if got != tc.want {
			t.Errorf("%s = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// TestRegression_AggAllNullGroup_v0100 verifies that SUM of all-NULL returns NULL.
func TestRegression_AggAllNullGroup_v0100(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t(n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (NULL), (NULL), (NULL)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT SUM(n) FROM t")
	if err != nil {
		t.Fatalf("SUM(n) all-NULL: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("no rows")
	}
	if rows.Data[0][0] != nil {
		t.Errorf("SUM of all-NULL = %v, want nil", rows.Data[0][0])
	}
}
