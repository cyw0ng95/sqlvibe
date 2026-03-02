package F882

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F882_SelectPositionalParam_L1 tests SELECT ? + 1 with positional param.
func TestSQL1999_F882_SelectPositionalParam_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryWithParams("SELECT ? + 1", []interface{}{int64(10)})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(11) {
		t.Fatalf("expected 11, got %v", rows.Data[0][0])
	}
}

// TestSQL1999_F882_InsertWithParams_L1 tests INSERT INTO t VALUES (?, ?) with positional params.
func TestSQL1999_F882_InsertWithParams_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := db.ExecWithParams("INSERT INTO t VALUES (?, ?)", []interface{}{int64(1), "Alice"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.ExecWithParams("INSERT INTO t VALUES (?, ?)", []interface{}{int64(2), "Bob"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(2) {
		t.Fatalf("expected 2 rows, got %v", rows.Data)
	}
}

// TestSQL1999_F882_SelectWherePositionalParam_L1 tests SELECT * FROM t WHERE id = ?
func TestSQL1999_F882_SelectWherePositionalParam_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.QueryWithParams("SELECT name FROM t WHERE id = ?", []interface{}{int64(2)})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "Bob" {
		t.Fatalf("expected Bob, got %v", rows.Data)
	}
}

// TestSQL1999_F882_SelectWhereNamedParam_L1 tests SELECT * FROM t WHERE name = :name
func TestSQL1999_F882_SelectWhereNamedParam_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.QueryNamed("SELECT id FROM t WHERE name = :name", map[string]interface{}{"name": "Alice"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(1) {
		t.Fatalf("expected id=1, got %v", rows.Data)
	}
}

// TestSQL1999_F882_PrepareQueryRoundTrip_L1 tests Prepare + stmt.Query with a param.
func TestSQL1999_F882_PrepareQueryRoundTrip_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, score INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, row := range [][]int64{{1, 90}, {2, 75}, {3, 88}} {
		if _, err := db.ExecWithParams("INSERT INTO t VALUES (?, ?)", []interface{}{row[0], row[1]}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	stmt, err := db.Prepare("SELECT score FROM t WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(int64(3))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(88) {
		t.Fatalf("expected score=88, got %v", rows.Data)
	}
}

// TestSQL1999_F882_MultiRowInsertRepeatedParams_L1 tests repeated parameterized INSERT.
func TestSQL1999_F882_MultiRowInsertRepeatedParams_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 5; i++ {
		if _, err := stmt.Exec(int64(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("exec row %d: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(5) {
		t.Fatalf("expected 5 rows, got %v", rows.Data)
	}
}
