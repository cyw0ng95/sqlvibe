package sqlvibe

import (
	"testing"
)

func TestE021StringFunctions(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE t (s TEXT)")
	db.Exec("INSERT INTO t VALUES ('hello')")

	tests := []struct {
		name     string
		sql      string
		expected interface{}
	}{
		{"UPPER", "SELECT UPPER(s) FROM t", "HELLO"},
		{"LOWER", "SELECT LOWER(s) FROM t", "hello"},
		{"LENGTH", "SELECT LENGTH(s) FROM t", int64(5)},
		{"TRIM", "SELECT TRIM('  hello  ')", "hello"},
		{"SUBSTRING", "SELECT SUBSTRING('hello', 2, 3)", "ell"},
		{"Concat", "SELECT 'a' || 'b'", "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.sql)
			if err != nil {
				t.Fatalf("Query error: %v", err)
			}
			if len(rows.Data) == 0 {
				t.Fatal("no rows returned")
			}
			if rows.Data[0][0] != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, rows.Data[0][0])
			}
		})
	}
}
