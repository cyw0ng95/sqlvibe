package B6_JOIN

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

func createJoinTables(sv *sqlvibe.Database, sl *sql.DB) {
	sv.Exec("CREATE TABLE a (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE a (id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE b (a_id INTEGER, val TEXT)")
	sl.Exec("CREATE TABLE b (a_id INTEGER, val TEXT)")

	sv.Exec("INSERT INTO a VALUES (1, 'Alice')")
	sl.Exec("INSERT INTO a VALUES (1, 'Alice')")
	sv.Exec("INSERT INTO a VALUES (2, 'Bob')")
	sl.Exec("INSERT INTO a VALUES (2, 'Bob')")
	sv.Exec("INSERT INTO a VALUES (3, 'Carol')")
	sl.Exec("INSERT INTO a VALUES (3, 'Carol')")

	sv.Exec("INSERT INTO b VALUES (1, 'x')")
	sl.Exec("INSERT INTO b VALUES (1, 'x')")
	sv.Exec("INSERT INTO b VALUES (1, 'y')")
	sl.Exec("INSERT INTO b VALUES (1, 'y')")
	sv.Exec("INSERT INTO b VALUES (NULL, 'z')")
	sl.Exec("INSERT INTO b VALUES (NULL, 'z')")
}

// TestSQL1999_B6_LeftJoinNullKey_L1 tests LEFT JOIN with NULL keys.
func TestSQL1999_B6_LeftJoinNullKey_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createJoinTables(sv, sl)

	tests := []struct{ name, sql string }{
		{"LeftJoinBasic", "SELECT a.id, a.name, b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id, b.val"},
		{"LeftJoinNullKey", "SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.a_id IS NULL ORDER BY a.id"},
		{"LeftJoinNoMatch", "SELECT a.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.val IS NULL ORDER BY a.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B6_InnerJoin_L1 tests INNER JOIN basics.
func TestSQL1999_B6_InnerJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createJoinTables(sv, sl)

	tests := []struct{ name, sql string }{
		{"InnerJoin", "SELECT a.name, b.val FROM a INNER JOIN b ON a.id = b.a_id ORDER BY a.name, b.val"},
		{"InnerJoinCount", "SELECT COUNT(*) FROM a INNER JOIN b ON a.id = b.a_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B6_CrossJoinEmpty_L1 tests CROSS JOIN behavior.
func TestSQL1999_B6_CrossJoinEmpty_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t1 (a INTEGER)")
	sl.Exec("CREATE TABLE t1 (a INTEGER)")
	sv.Exec("CREATE TABLE t2 (b INTEGER)")
	sl.Exec("CREATE TABLE t2 (b INTEGER)")

	sv.Exec("INSERT INTO t1 VALUES (1)")
	sl.Exec("INSERT INTO t1 VALUES (1)")
	sv.Exec("INSERT INTO t2 VALUES (10)")
	sl.Exec("INSERT INTO t2 VALUES (10)")

	tests := []struct{ name, sql string }{
		{"CrossJoinBasic", "SELECT t1.a, t2.b FROM t1 CROSS JOIN t2 ORDER BY t1.a, t2.b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B6_SelfJoin_L1 tests self-join queries.
func TestSQL1999_B6_SelfJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE emp (id INTEGER, name TEXT, mgr_id INTEGER)")
	sl.Exec("CREATE TABLE emp (id INTEGER, name TEXT, mgr_id INTEGER)")
	sv.Exec("INSERT INTO emp VALUES (1, 'Alice', NULL)")
	sl.Exec("INSERT INTO emp VALUES (1, 'Alice', NULL)")
	sv.Exec("INSERT INTO emp VALUES (2, 'Bob', 1)")
	sl.Exec("INSERT INTO emp VALUES (2, 'Bob', 1)")
	sv.Exec("INSERT INTO emp VALUES (3, 'Carol', 1)")
	sl.Exec("INSERT INTO emp VALUES (3, 'Carol', 1)")

	tests := []struct{ name, sql string }{
		{"SelfJoin", "SELECT e.name, m.name AS manager FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id ORDER BY e.name"},
		{"SelfJoinCount", "SELECT COUNT(*) FROM emp e INNER JOIN emp m ON e.mgr_id = m.id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B6_MultiTableJoin_L1 tests three-table joins.
func TestSQL1999_B6_MultiTableJoin_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER)")
	sl.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER)")
	sv.Exec("CREATE TABLE items (ord_id INTEGER, product TEXT)")
	sl.Exec("CREATE TABLE items (ord_id INTEGER, product TEXT)")

	sv.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sl.Exec("INSERT INTO customers VALUES (1, 'Alice')")
	sv.Exec("INSERT INTO orders VALUES (1, 1)")
	sl.Exec("INSERT INTO orders VALUES (1, 1)")
	sv.Exec("INSERT INTO items VALUES (1, 'Widget')")
	sl.Exec("INSERT INTO items VALUES (1, 'Widget')")

	tests := []struct{ name, sql string }{
		{"ThreeTableJoin", "SELECT c.name, i.product FROM customers c JOIN orders o ON c.id = o.cust_id JOIN items i ON o.id = i.ord_id ORDER BY c.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B6_JoinUsing_L1 tests JOIN ... USING syntax.
func TestSQL1999_B6_JoinUsing_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE p (id INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE p (id INTEGER, name TEXT)")
	sv.Exec("CREATE TABLE q (id INTEGER, score INTEGER)")
	sl.Exec("CREATE TABLE q (id INTEGER, score INTEGER)")

	sv.Exec("INSERT INTO p VALUES (1, 'Alice')")
	sl.Exec("INSERT INTO p VALUES (1, 'Alice')")
	sv.Exec("INSERT INTO p VALUES (2, 'Bob')")
	sl.Exec("INSERT INTO p VALUES (2, 'Bob')")
	sv.Exec("INSERT INTO q VALUES (1, 90)")
	sl.Exec("INSERT INTO q VALUES (1, 90)")
	sv.Exec("INSERT INTO q VALUES (3, 80)")
	sl.Exec("INSERT INTO q VALUES (3, 80)")

	tests := []struct{ name, sql string }{
		{"JoinUsing", "SELECT p.name, q.score FROM p JOIN q USING (id) ORDER BY p.name"},
		{"LeftJoinUsing", "SELECT p.name, q.score FROM p LEFT JOIN q USING (id) ORDER BY p.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
