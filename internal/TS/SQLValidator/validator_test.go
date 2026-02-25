package SQLValidator

import (
	"testing"
)

// TestSQLValidator_TPC_C runs the validator for 1000 randomly-generated SQL
// statements (LCG seed 42) against the TPC-C starter schema.
// Any result mismatch between SQLite and sqlvibe is reported as a test failure.
func TestSQLValidator_TPC_C(t *testing.T) {
	v, err := NewValidator(42)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	mismatches, err := v.Run(1000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for i, m := range mismatches {
		t.Errorf("mismatch[%d]:\n  SQL:    %s\n  SQLite: %s\n  SVibe:  %s\n  Reason: %s",
			i,
			m.Query,
			fmtResult(m.SQLiteResult),
			fmtResult(m.SQLVibeResult),
			m.Reason,
		)
	}
	if len(mismatches) == 0 {
		t.Logf("All 1000 statements matched between SQLite and sqlvibe (seed=42)")
	}
}

// TestSQLValidator_Regression replays specific LCG seeds that have triggered
// mismatches in the past. Add new entries here whenever a bug is fixed.
func TestSQLValidator_Regression(t *testing.T) {
	// Each entry describes a regression case.
	type regCase struct {
		desc  string
		seed  uint64
		count int
	}

	// Regression cases — populated as bugs are found.
	// Format: {description, seed, statement_count}
	cases := []regCase{
		// Example (disabled until a real regression is found):
		// {"GroupBy aggregate mismatch seed 1234", 1234, 50},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			v, err := NewValidator(tc.seed)
			if err != nil {
				t.Fatalf("NewValidator(seed=%d): %v", tc.seed, err)
			}
			defer v.Close()

			mismatches, err := v.Run(tc.count)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			for i, m := range mismatches {
				t.Errorf("mismatch[%d]:\n  SQL:    %s\n  Reason: %s",
					i, m.Query, m.Reason)
			}
		})
	}
}
