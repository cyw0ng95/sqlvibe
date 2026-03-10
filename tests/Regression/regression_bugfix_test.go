package Regression

import (
	"database/sql"
	_ "github.com/cyw0ng95/sqlvibe/driver"
	"testing"

)

// TestRegression_DerivedTableWhere_L1 tests that WHERE clause is applied correctly
// on derived table (subquery in FROM) queries.
// Bug: vectorized filter used wrong type (TEXT) for derived table columns, causing
// integer comparisons like a > 1 to match all rows due to type ordering.
func TestRegression_DerivedTableWhere_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

	r := qDB(t, db, "SELECT a FROM (SELECT a, b FROM t) WHERE a > 1")
	if len(r.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(r.Data), r.Data)
	}
}

// TestRegression_DerivedTableDoubleNested_L1 tests WHERE on doubly-nested derived table.
func TestRegression_DerivedTableDoubleNested_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

	r := qDB(t, db, "SELECT a FROM (SELECT a FROM (SELECT a, b FROM t) WHERE a > 1) AS sub ORDER BY a")
	if len(r.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(r.Data), r.Data)
	}
}

// TestRegression_GroupByTableAlias_L1 tests that GROUP BY with a table alias
// resolves columns correctly.
// Bug: evaluateExprOnRow did not fall back to unqualified lookup after failing
// qualified lookup, so GROUP BY e.col returned nil for all rows.
func TestRegression_GroupByTableAlias_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE employees (name TEXT, department TEXT)")
	db.Exec("INSERT INTO employees VALUES ('Alice', 'Engineering')")
	db.Exec("INSERT INTO employees VALUES ('Bob', 'Engineering')")
	db.Exec("INSERT INTO employees VALUES ('Charlie', 'HR')")

	r := qDB(t, db, "SELECT e.department, COUNT(*) FROM employees AS e GROUP BY e.department ORDER BY e.department")
	if len(r.Data) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(r.Data), r.Data)
	}
	if r.Data[0][0] != "Engineering" {
		t.Fatalf("expected Engineering, got %v", r.Data[0][0])
	}
}

// TestRegression_LeftJoinGroupBy_L1 tests LEFT JOIN combined with GROUP BY.
// Bug: execJoinAggregate only handled INNER JOINs; LEFT JOINs fell through to
// a path that ignored the join entirely.
func TestRegression_LeftJoinGroupBy_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE t1 (cat TEXT, id INTEGER)")
	db.Exec("CREATE TABLE t2 (id INTEGER, val INTEGER)")
	db.Exec("INSERT INTO t1 VALUES ('A', 1), ('A', 2), ('B', 3)")
	db.Exec("INSERT INTO t2 VALUES (1, 10), (3, 30)")

	r := qDB(t, db, "SELECT t1.cat, COUNT(*) FROM t1 LEFT JOIN t2 ON t1.id = t2.id GROUP BY t1.cat ORDER BY t1.cat")
	if len(r.Data) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(r.Data), r.Data)
	}
}

// TestRegression_CorrelatedSubqueryInSelect_L1 tests correlated scalar subquery
// in the SELECT list.
// Bug: evaluateExprOnRow fell back to unqualified lookup before outer context,
// causing self-correlated subqueries to always return the wrong value.
func TestRegression_CorrelatedSubqueryInSelect_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	db.Exec("CREATE TABLE orders (id INTEGER, customer_id INTEGER, total INTEGER)")
	db.Exec("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")

	r := qDB(t, db, "SELECT name, (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) FROM customers ORDER BY id")
	if len(r.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Data))
	}
	// Alice: 2 orders
	if r.Data[0][1] != int64(2) {
		t.Fatalf("Alice should have 2 orders, got %v", r.Data[0][1])
	}
	// Bob: 1 order
	if r.Data[1][1] != int64(1) {
		t.Fatalf("Bob should have 1 order, got %v", r.Data[1][1])
	}
	// Charlie: 0 orders
	if r.Data[2][1] != int64(0) {
		t.Fatalf("Charlie should have 0 orders, got %v", r.Data[2][1])
	}
}

// TestRegression_SubqueryGroupByColumn_L1 tests GROUP BY on a materialized subquery.
// Bug: subquery temp table registered with TEXT types caused HybridStore
// to store values as strings, breaking integer GROUP BY comparisons.
func TestRegression_SubqueryGroupByColumn_L1(t *testing.T) {
	db, _ := sql.Open("sqlvibe", ":memory:")
	defer db.Close()
	db.Exec("CREATE TABLE t1 (a INTEGER, b INTEGER)")
	db.Exec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")

	r := qDB(t, db, "SELECT subq.a, COUNT(*) FROM (SELECT a, b FROM t1) AS subq GROUP BY subq.a ORDER BY subq.a")
	if len(r.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(r.Data), r.Data)
	}
}

// TestRegression_ExistsWhereTableNameKeyword_L1 tests EXISTS subquery where the
// FROM table name starts with a SQL keyword (e.g. "order_line" starts with "order").
// Bug: parser's read_where_clause treated "order" from "order_line" as the ORDER BY
// keyword, truncating the WHERE clause. Fixed by checking word boundaries.
func TestRegression_ExistsWhereTableNameKeyword_L1(t *testing.T) {
db, _ := sql.Open("sqlvibe", ":memory:")
defer db.Close()
db.Exec("CREATE TABLE stock (s_i_id INT, s_w_id INT)")
db.Exec("INSERT INTO stock VALUES (1, 1), (2, 1), (3, 1)")
db.Exec("CREATE TABLE order_line (ol_o_id INT, ol_d_id INT)")
db.Exec("INSERT INTO order_line VALUES (1, 1), (2, 1)")

// WHERE clause contains "order_line": parser should NOT stop at "order"
r := qDB(t, db, "SELECT s_i_id FROM stock WHERE EXISTS (SELECT 1 FROM order_line WHERE s_i_id = order_line.ol_o_id) ORDER BY s_i_id")
if len(r.Data) != 2 {
t.Fatalf("expected 2 rows (s_i_id=1,2), got %d: %v", len(r.Data), r.Data)
}
if r.Data[0][0] != int64(1) || r.Data[1][0] != int64(2) {
t.Fatalf("expected rows [1,2], got %v", r.Data)
}
}

// TestRegression_ExistsSelfJoin_L1 tests EXISTS subquery where the inner and outer
// queries use the same table. In SQL, qualified references like table.col in the
// subquery refer to the inner table when the inner FROM uses that same table name.
// Bug: subst_outer incorrectly substituted qualified outer row values for inner
// table references when outer and inner tables shared the same name.
func TestRegression_ExistsSelfJoin_L1(t *testing.T) {
db, _ := sql.Open("sqlvibe", ":memory:")
defer db.Close()
db.Exec("CREATE TABLE orders (o_id INT, o_d_id INT, o_c_id INT)")
db.Exec("INSERT INTO orders VALUES (1, 1, 1), (2, 1, 1), (3, 2, 2), (4, 3, 1)")

// Self-join EXISTS: find all orders where there exists some order with o_d_id = o_id
// "orders.o_id" in subquery refers to inner "orders" table (non-correlated)
r := qDB(t, db, "SELECT o_id FROM orders WHERE EXISTS (SELECT 1 FROM orders WHERE o_d_id = orders.o_id) ORDER BY o_id")
// o_d_id values are {1,1,2,3}; o_id values in inner = {1,2,3,4}
// exists any row where o_d_id=o_id: yes (row 1: o_d_id=1=o_id=1, row 3: o_d_id=2=o_id=2)
// So EXISTS is true for ALL outer rows (since there exists inner rows with o_d_id=1 and o_d_id=2)
if len(r.Data) != 4 {
t.Fatalf("expected 4 rows (exists non-correlated), got %d: %v", len(r.Data), r.Data)
}
}
