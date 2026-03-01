package QP

import "testing"

// TestPhase18_FastTokenCount tests Phase 18: QP Parser fast token count.
func TestPhase18_FastTokenCount(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"empty", ""},
		{"simple_select", "SELECT 1"},
		{"full_query", "SELECT id, name FROM users WHERE id > 0 ORDER BY name LIMIT 10"},
		{"insert", "INSERT INTO t VALUES (1, 'hello', 3.14)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := FastTokenCount(tt.sql)
			if n < 1 {
				t.Errorf("FastTokenCount(%q) = %d, want >= 1", tt.sql, n)
			}
		})
	}
}
