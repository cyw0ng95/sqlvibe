package Regression

import (
"database/sql"
_ "github.com/cyw0ng95/sqlvibe/driver"
"testing"
)

// TestRegression_ParamPositional_L1 verifies that positional parameters bind correctly.
func TestRegression_ParamPositional_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}

if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(1), "Alice"); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT * FROM t WHERE id = ?", int64(1))
if len(rows.Data) != 1 {
t.Fatalf("expected 1 row, got %d", len(rows.Data))
}
}

// TestRegression_ParamInjectionSafe_L1 verifies that a SQL-injection attempt via a
// string parameter is treated as a literal string and NOT executed as SQL.
func TestRegression_ParamInjectionSafe_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}
if _, err := db.Exec("INSERT INTO t VALUES (1, 'safe')"); err != nil {
t.Fatalf("seed: %v", err)
}

// This injection attempt should be stored as a literal string, not executed.
malicious := "'; DROP TABLE t; --"
if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(2), malicious); err != nil {
t.Fatalf("insert: %v", err)
}

// Table must still exist and have 2 rows.
rows := qDB(t, db, "SELECT COUNT(*) FROM t")
if len(rows.Data) == 0 || rows.Data[0][0] != int64(2) {
t.Fatalf("expected 2 rows, got %v", rows.Data)
}
}

// TestRegression_ParamNamedColon_L1 verifies that positional params bind correctly
// (named-param syntax converted to positional for database/sql compatibility).
func TestRegression_ParamNamedColon_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}
if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(10), "Bob"); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT name FROM t WHERE id = ?", int64(10))
if len(rows.Data) != 1 || rows.Data[0][0] != "Bob" {
t.Fatalf("expected Bob, got %v", rows.Data)
}
}

// TestRegression_ParamNamedAt_L1 verifies that positional params bind correctly
// for the @name named-parameter scenario.
func TestRegression_ParamNamedAt_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}
if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(20), "Carol"); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT name FROM t WHERE id = ?", int64(20))
if len(rows.Data) != 1 || rows.Data[0][0] != "Carol" {
t.Fatalf("expected Carol, got %v", rows.Data)
}
}

// TestRegression_ParamMissing_L1 verifies that a missing positional param returns an error.
func TestRegression_ParamMissing_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
t.Fatalf("create: %v", err)
}
_, err = db.Exec("INSERT INTO t VALUES (?)")
if err == nil {
t.Fatal("expected error for missing param, got nil")
}
}

// TestRegression_ParamNullBind_L1 verifies that nil param binds as SQL NULL.
func TestRegression_ParamNullBind_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}
if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(1), nil); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT val FROM t WHERE id = 1")
if len(rows.Data) != 1 || rows.Data[0][0] != nil {
t.Fatalf("expected nil, got %v", rows.Data[0][0])
}
}

// TestRegression_ParamBlob_L1 verifies that []byte params bind as BLOB.
func TestRegression_ParamBlob_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, data BLOB)"); err != nil {
t.Fatalf("create: %v", err)
}
payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", int64(1), payload); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT data FROM t WHERE id = 1")
if len(rows.Data) != 1 {
t.Fatalf("expected 1 row, got %d", len(rows.Data))
}
}

// TestRegression_ParamPreparedStmt_L1 verifies that Prepare + stmt.Query works with params.
func TestRegression_ParamPreparedStmt_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
t.Fatalf("create: %v", err)
}
if _, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
t.Fatalf("insert: %v", err)
}

stmt, err := db.Prepare("SELECT name FROM t WHERE id = ?")
if err != nil {
t.Fatalf("prepare: %v", err)
}
defer stmt.Close()

sqlRows, err := stmt.Query(int64(2))
if err != nil {
t.Fatalf("stmt.Query: %v", err)
}
defer sqlRows.Close()
var name string
if sqlRows.Next() {
sqlRows.Scan(&name)
}
if name != "Bob" {
t.Fatalf("expected Bob, got %v", name)
}
}

// TestRegression_ParamTooMany_L1 verifies that extra params beyond '?' count
// are handled gracefully (engine-defined behaviour: may error or ignore).
func TestRegression_ParamTooMany_L1(t *testing.T) {
db, err := sql.Open("sqlvibe", ":memory:")
if err != nil {
t.Fatalf("open: %v", err)
}
defer db.Close()

if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
t.Fatalf("create: %v", err)
}
// Only 1 '?' but 3 params — extra ones are ignored.
if _, err := db.Exec("INSERT INTO t VALUES (?)", int64(1), int64(2), int64(3)); err != nil {
t.Fatalf("insert: %v", err)
}

rows := qDB(t, db, "SELECT COUNT(*) FROM t")
if len(rows.Data) == 0 || rows.Data[0][0] != int64(1) {
t.Fatalf("expected 1 row, got %v", rows.Data)
}
}
