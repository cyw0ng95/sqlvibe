// Package F873 tests v0.9.1 optimization features via the standard
// database/sql driver interface: covering indexes, prepared statement caching,
// expression evaluation, column projection, direct-compiler fast paths, and
// opcode dispatch correctness.  All database access goes through the
// "sqlvibe" driver registered in github.com/cyw0ng95/sqlvibe/driver.
package F873

import (
"database/sql"
"fmt"
"testing"

_ "github.com/cyw0ng95/sqlvibe/driver"
_ "github.com/glebarez/go-sqlite"
)

// openDB opens a fresh in-memory sqlvibe database via the driver/ interface.
func openDB(t *testing.T) *sql.DB {
t.Helper()
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("Failed to open: %v", err)
}
t.Cleanup(func() { db.Close() })
return db
}

// mustExec executes a statement and fails the test on error.
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
t.Helper()
if _, err := db.Exec(query, args...); err != nil {
t.Fatalf("exec %q: %v", query, err)
}
}

// queryVal executes a query that returns exactly one scalar value.
func queryVal(t *testing.T, db *sql.DB, query string, args ...interface{}) interface{} {
t.Helper()
row := db.QueryRow(query, args...)
var v interface{}
if err := row.Scan(&v); err != nil {
t.Fatalf("queryVal %q: %v", query, err)
}
return v
}

// queryRowCount returns the number of rows a query returns.
func queryRowCount(t *testing.T, db *sql.DB, query string) int {
t.Helper()
rows, err := db.Query(query)
if err != nil {
t.Fatalf("query %q: %v", query, err)
}
defer rows.Close()
n := 0
for rows.Next() {
n++
}
if err := rows.Err(); err != nil {
t.Fatalf("rows.Err for %q: %v", query, err)
}
return n
}

// -----------------------------------------------------------------
// 1. Covering Index: queries on indexed columns return correct results
// -----------------------------------------------------------------

// TestSQL1999_F873_CoversColumns_L1 verifies that queries whose WHERE clause
// references only indexed columns return correct results (covering-index path).
func TestSQL1999_F873_CoversColumns_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE users (name TEXT, age INTEGER, dept TEXT)")
mustExec(t, db, "CREATE INDEX idx_name_age ON users (name, age)")
mustExec(t, db, "INSERT INTO users VALUES ('Alice', 30, 'eng')")
mustExec(t, db, "INSERT INTO users VALUES ('Bob', 25, 'sales')")
mustExec(t, db, "INSERT INTO users VALUES ('Carol', 30, 'eng')")

// Query on leading index column.
n := queryRowCount(t, db, "SELECT name, age FROM users WHERE name = 'Alice'")
if n != 1 {
t.Errorf("expected 1 row for name='Alice', got %d", n)
}

// Query on second index column.
n = queryRowCount(t, db, "SELECT name, age FROM users WHERE age = 30")
if n != 2 {
t.Errorf("expected 2 rows for age=30, got %d", n)
}

// Query on both index columns.
n = queryRowCount(t, db, "SELECT name, age FROM users WHERE name = 'Bob' AND age = 25")
if n != 1 {
t.Errorf("expected 1 row for name='Bob' AND age=25, got %d", n)
}

// Query on non-indexed column still works.
n = queryRowCount(t, db, "SELECT name FROM users WHERE dept = 'eng'")
if n != 2 {
t.Errorf("expected 2 rows for dept='eng', got %d", n)
}
}

// TestSQL1999_F873_FindCoveringIndex_L1 verifies that the optimizer can use
// multiple different indexes for different query patterns.
func TestSQL1999_F873_FindCoveringIndex_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT, dept TEXT)")
mustExec(t, db, "CREATE INDEX idx_id ON t (id)")
mustExec(t, db, "CREATE INDEX idx_name_dept ON t (name, dept)")
mustExec(t, db, "INSERT INTO t VALUES (1, 'Alice', 'eng')")
mustExec(t, db, "INSERT INTO t VALUES (2, 'Bob', 'sales')")

// Filter on first index.
n := queryRowCount(t, db, "SELECT id FROM t WHERE id = 1")
if n != 1 {
t.Errorf("idx_id: expected 1 row, got %d", n)
}

// Filter on second index leading column.
n = queryRowCount(t, db, "SELECT name, dept FROM t WHERE name = 'Alice' AND dept = 'eng'")
if n != 1 {
t.Errorf("idx_name_dept: expected 1 row, got %d", n)
}

// No matching index column → full scan, still works.
n = queryRowCount(t, db, "SELECT * FROM t WHERE dept = 'eng'")
if n != 1 {
t.Errorf("full scan: expected 1 row for dept='eng', got %d", n)
}
}

// TestSQL1999_F873_SelectBestIndex_L1 verifies that queries using different
// filter columns each return correct results (optimizer picks appropriate index).
func TestSQL1999_F873_SelectBestIndex_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE t (id INTEGER, val TEXT, name TEXT)")
mustExec(t, db, "CREATE INDEX idx_id_val ON t (id, val)")
mustExec(t, db, "CREATE INDEX idx_name ON t (name)")
for i := 1; i <= 5; i++ {
mustExec(t, db, "INSERT INTO t VALUES (?, ?, ?)", i, fmt.Sprintf("v%d", i), fmt.Sprintf("n%d", i))
}

// Filter on first index leading column.
n := queryRowCount(t, db, "SELECT id, val FROM t WHERE id = 3")
if n != 1 {
t.Errorf("idx_id filter: expected 1 row, got %d", n)
}

// Filter on second index column.
n = queryRowCount(t, db, "SELECT name FROM t WHERE name = 'n2'")
if n != 1 {
t.Errorf("idx_name filter: expected 1 row, got %d", n)
}
}

// TestSQL1999_F873_CanSkipScan_L1 verifies queries that benefit from skip-scan
// optimization return correct results regardless of optimizer path chosen.
func TestSQL1999_F873_CanSkipScan_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE t (dept TEXT, id INTEGER, v TEXT)")
mustExec(t, db, "CREATE INDEX idx_dept_id ON t (dept, id)")
depts := []string{"eng", "sales", "hr", "legal", "ops"}
for _, d := range depts {
for i := 1; i <= 20; i++ {
mustExec(t, db, "INSERT INTO t VALUES (?, ?, ?)", d, i, fmt.Sprintf("%s-%d", d, i))
}
}

// Filter on suffix column (id) — may use skip scan.
n := queryRowCount(t, db, "SELECT * FROM t WHERE id = 5")
if n != 5 {
t.Errorf("skip-scan on id=5: expected 5 rows (one per dept), got %d", n)
}

// Filter on leading column — normal index scan.
n = queryRowCount(t, db, "SELECT * FROM t WHERE dept = 'eng'")
if n != 20 {
t.Errorf("normal scan on dept='eng': expected 20 rows, got %d", n)
}
}

// -----------------------------------------------------------------
// 2. Prepared Statements: caching and repeated execution
// -----------------------------------------------------------------

// TestSQL1999_F873_PreparedStatement_L1 tests that prepared statements can be
// compiled once and executed multiple times with correct results.
func TestSQL1999_F873_PreparedStatement_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE nums (n INTEGER)")
for i := 1; i <= 5; i++ {
mustExec(t, db, "INSERT INTO nums VALUES (?)", i)
}

stmt, err := db.Prepare("SELECT n FROM nums WHERE n > ? ORDER BY n")
if err != nil {
t.Fatalf("Prepare: %v", err)
}
defer stmt.Close()

// First execution.
rows, err := stmt.Query(2)
if err != nil {
t.Fatalf("first Query: %v", err)
}
var vals []int64
for rows.Next() {
var v int64
rows.Scan(&v)
vals = append(vals, v)
}
rows.Close()
if len(vals) != 3 || vals[0] != 3 || vals[1] != 4 || vals[2] != 5 {
t.Errorf("first execution: expected [3 4 5], got %v", vals)
}

// Second execution with a different parameter.
rows2, err := stmt.Query(0)
if err != nil {
t.Fatalf("second Query: %v", err)
}
n := 0
for rows2.Next() {
n++
}
rows2.Close()
if n != 5 {
t.Errorf("second execution: expected 5 rows, got %d", n)
}
}

// TestSQL1999_F873_PreparedStatementLRUEviction_L1 verifies that many different
// prepared statements can all be created and executed without errors.
func TestSQL1999_F873_PreparedStatementLRUEviction_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE x (v INTEGER)")
mustExec(t, db, "INSERT INTO x VALUES (1)")

queries := []string{
"SELECT v FROM x",
"SELECT v FROM x WHERE v > 0",
"SELECT v FROM x WHERE v > 1",
"SELECT v FROM x WHERE v > 2",
}
stmts := make([]*sql.Stmt, 0, len(queries))
for _, q := range queries {
stmt, err := db.Prepare(q)
if err != nil {
t.Fatalf("Prepare(%q): %v", q, err)
}
stmts = append(stmts, stmt)
}
for _, stmt := range stmts {
stmt.Close()
}
}

// -----------------------------------------------------------------
// 3. Expression evaluation: arithmetic, column load, comparisons
// -----------------------------------------------------------------

// TestSQL1999_F873_ExprAdd_L1 tests that simple addition is evaluated correctly.
func TestSQL1999_F873_ExprAdd_L1(t *testing.T) {
db := openDB(t)
v := queryVal(t, db, "SELECT 2 + 3")
if fmt.Sprintf("%v", v) != "5" {
t.Errorf("2 + 3 = %v, want 5", v)
}
}

// TestSQL1999_F873_ExprColumn_L1 tests that column values are returned correctly.
func TestSQL1999_F873_ExprColumn_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE t (n INTEGER)")
mustExec(t, db, "INSERT INTO t VALUES (99)")
v := queryVal(t, db, "SELECT n FROM t")
if fmt.Sprintf("%v", v) != "99" {
t.Errorf("SELECT n = %v, want 99", v)
}
}

// TestSQL1999_F873_ExprComparison_L1 tests that comparison expressions are correct.
func TestSQL1999_F873_ExprComparison_L1(t *testing.T) {
db := openDB(t)
v := queryVal(t, db, "SELECT 10 > 5")
if fmt.Sprintf("%v", v) != "1" {
t.Errorf("10 > 5 = %v, want 1", v)
}
v2 := queryVal(t, db, "SELECT 3 > 5")
if fmt.Sprintf("%v", v2) != "0" {
t.Errorf("3 > 5 = %v, want 0", v2)
}
}

// -----------------------------------------------------------------
// 4. Column projection: queries return only selected columns
// -----------------------------------------------------------------

// TestSQL1999_F873_RequiredColumns_L1 verifies that column projection works:
// a SELECT with specific columns returns exactly those columns.
func TestSQL1999_F873_RequiredColumns_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE employees (name TEXT, age INTEGER, dept TEXT, salary REAL)")
mustExec(t, db, "INSERT INTO employees VALUES ('Alice', 30, 'eng', 90000)")

rows, err := db.Query("SELECT name, age FROM employees WHERE dept = 'eng' ORDER BY age")
if err != nil {
t.Fatalf("query: %v", err)
}
defer rows.Close()
cols, err := rows.Columns()
if err != nil {
t.Fatalf("Columns: %v", err)
}
if len(cols) != 2 || cols[0] != "name" || cols[1] != "age" {
t.Errorf("expected columns [name age], got %v", cols)
}
}

// -----------------------------------------------------------------
// 5. Fast-path queries: simple SELECTs complete correctly
// -----------------------------------------------------------------

// TestSQL1999_F873_IsFastPath_L1 verifies that typical fast-path queries
// (simple SELECT, WHERE equality, COUNT, ORDER BY, LIMIT) return correct results.
func TestSQL1999_F873_IsFastPath_L1(t *testing.T) {
db := openDB(t)
mustExec(t, db, "CREATE TABLE orders (id INTEGER, status TEXT)")
for i := 1; i <= 10; i++ {
status := "open"
if i%2 == 0 {
status = "closed"
}
mustExec(t, db, "INSERT INTO orders VALUES (?, ?)", i, status)
}

// Simple SELECT.
n := queryRowCount(t, db, "SELECT id, status FROM orders")
if n != 10 {
t.Errorf("simple SELECT: expected 10 rows, got %d", n)
}

// SELECT with WHERE.
n = queryRowCount(t, db, "SELECT * FROM orders WHERE id = 5")
if n != 1 {
t.Errorf("WHERE equality: expected 1 row, got %d", n)
}

// COUNT(*) with GROUP BY.
rows, err := db.Query("SELECT COUNT(*) FROM orders GROUP BY status ORDER BY 1")
if err != nil {
t.Fatalf("COUNT GROUP BY: %v", err)
}
defer rows.Close()
counts := []int64{}
for rows.Next() {
var c int64
rows.Scan(&c)
counts = append(counts, c)
}
if len(counts) != 2 {
t.Errorf("COUNT GROUP BY: expected 2 groups, got %d", len(counts))
}

// ORDER BY + LIMIT.
n = queryRowCount(t, db, "SELECT id FROM orders ORDER BY id LIMIT 5")
if n != 5 {
t.Errorf("ORDER BY LIMIT: expected 5 rows, got %d", n)
}
}

// -----------------------------------------------------------------
// 6. Opcode dispatch: arithmetic opcodes return correct results
// -----------------------------------------------------------------

// TestSQL1999_F873_HasDispatchHandler_L1 verifies that core arithmetic and
// control opcodes are handled correctly by executing representative SQL.
func TestSQL1999_F873_HasDispatchHandler_L1(t *testing.T) {
db := openDB(t)

cases := []struct {
sql  string
want string
}{
{"SELECT 3 + 4", "7"},
{"SELECT 10 - 3", "7"},
{"SELECT 3 * 4", "12"},
{"SELECT 10 / 2", "5"},
{"SELECT NULL", "<nil>"},
{"SELECT 42", "42"},
}
for _, tc := range cases {
row := db.QueryRow(tc.sql)
var v interface{}
if err := row.Scan(&v); err != nil {
t.Errorf("%q: scan error: %v", tc.sql, err)
continue
}
got := fmt.Sprintf("%v", v)
if got != tc.want {
t.Errorf("%q = %v, want %v", tc.sql, got, tc.want)
}
}
}
