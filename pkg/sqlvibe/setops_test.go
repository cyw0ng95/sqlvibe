package sqlvibe

import (
	"testing"
)

func TestSetOperations(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE TABLE t2 (a INTEGER)")
	db.Exec("INSERT INTO t1 VALUES (1), (2), (3)")
	db.Exec("INSERT INTO t2 VALUES (2), (3), (4)")

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"Union", "SELECT a FROM t1 UNION SELECT a FROM t2", 4},
		{"UnionAll", "SELECT a FROM t1 UNION ALL SELECT a FROM t2", 6},
		{"Except", "SELECT a FROM t1 EXCEPT SELECT a FROM t2", 1},
		{"Intersect", "SELECT a FROM t1 INTERSECT SELECT a FROM t2", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.sql)
			if err != nil {
				t.Fatalf("Query error: %v", err)
			}
			if len(rows.Data) != tt.expected {
				t.Errorf("expected %d rows, got %d: %v", tt.expected, len(rows.Data), rows.Data)
			}
		})
	}
}
