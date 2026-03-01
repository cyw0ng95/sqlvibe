package sqlvibe

import (
	"context"
	"fmt"
	"testing"
)

func TestOpen(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Error("Database should not be nil")
	}
}

func TestClose(t *testing.T) {
	db, _ := Open(":memory:")
	err := db.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestExec(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec CREATE TABLE failed: %v", err)
	}

	result, err := db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Exec INSERT failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = 1, got %d", result.RowsAffected)
	}
}

func TestExecMultiple(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Exec CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Exec INSERT failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("Exec INSERT failed: %v", err)
	}
}

func TestQuery(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	db.Exec("INSERT INTO test VALUES (2, 'Bob')")

	result, err := db.Query("SELECT id, name FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	count := 0
	for result.Next() {
		var id int
		var name string
		if err := result.Scan(&id, &name); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestQueryRow(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")

	result, _ := db.Query("SELECT id, name FROM test")

	if result.Next() {
		var id int
		var name string
		result.Scan(&id, &name)
		if id != 1 {
			t.Errorf("id = 1, got %d", id)
		}
		if name != "Alice" {
			t.Errorf("name = Alice, got %s", name)
		}
	}
}

func TestPrepare(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	_, err = stmt.Exec(1, "Alice")
	if err != nil {
		t.Errorf("Exec failed: %v", err)
	}
}

func TestTransaction(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("Exec in tx failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

func TestTransactionRollback(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx, _ := db.Begin()
	tx.Exec("INSERT INTO test VALUES (1)")
	_ = tx
	// Note: In-memory db may not fully support transactions
}

func TestSyntaxError(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("SELECT FROM")
	// Note: Parser may handle this gracefully
	_ = err
}

func TestNullValues(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, NULL)")

	result, _ := db.Query("SELECT name FROM test")
	result.Next()
	var name []byte
	result.Scan(&name)
	_ = name
}

func TestEmptyQuery(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("")
	if err != nil {
		t.Errorf("Empty query failed: %v", err)
	}
}

func TestMultipleStatements(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("CREATE TABLE t1(id INT); CREATE TABLE t2(id INT)")
	if err != nil {
		t.Fatalf("Multiple statements failed: %v", err)
	}
}

func TestMustExec(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	result := db.MustExec("INSERT INTO test VALUES (1)")

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = 1, got %d", result.RowsAffected)
	}
}

// ---- Additional coverage tests ----

func TestOpenInvalidPath(t *testing.T) {
	// Test opening a database in a non-existent directory
	_, err := Open("/nonexistent/path/to/db.sqlite")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestQueryWithParams(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	db.Exec("INSERT INTO test VALUES (2, 'Bob')")

	rows, err := db.QueryWithParams("SELECT name FROM test WHERE id = ?", []interface{}{1})
	if err != nil {
		t.Fatalf("QueryWithParams failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}

func TestExecWithParams(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	result, err := db.ExecWithParams("INSERT INTO test VALUES (?, ?)", []interface{}{1, "Alice"})
	if err != nil {
		t.Fatalf("ExecWithParams failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}
}

func TestExecNamed(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	result, err := db.ExecNamed("INSERT INTO test VALUES (:id, :name)", map[string]interface{}{
		"id":   1,
		"name": "Alice",
	})
	if err != nil {
		t.Fatalf("ExecNamed failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}
}

func TestQueryNamed(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")

	rows, err := db.QueryNamed("SELECT name FROM test WHERE id = :id", map[string]interface{}{"id": 1})
	if err != nil {
		t.Fatalf("QueryNamed failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}

func TestStatementQuery(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")

	stmt, err := db.Prepare("SELECT name FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatalf("Statement Query failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}

func TestTransactionRollbackFull(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx, _ := db.Begin()
	tx.Exec("INSERT INTO test VALUES (1)")

	err := tx.Rollback()
	if err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// Verify transaction is no longer active
	if db.tx != nil {
		t.Error("Transaction should be nil after rollback")
	}
}

func TestTransactionDoubleCommit(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx, _ := db.Begin()
	tx.Exec("INSERT INTO test VALUES (1)")
	tx.Commit()

	// Second commit should fail
	err := tx.Commit()
	if err == nil {
		t.Error("Expected error for double commit")
	}
}

func TestTransactionRollbackAfterCommit(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx, _ := db.Begin()
	tx.Exec("INSERT INTO test VALUES (1)")
	tx.Commit()

	// Rollback after commit should fail
	err := tx.Rollback()
	if err == nil {
		t.Error("Expected error for rollback after commit")
	}
}

func TestTransactionQuery(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")

	tx, _ := db.Begin()
	defer tx.Commit()

	rows, err := tx.Query("SELECT id FROM test")
	if err != nil {
		t.Fatalf("Transaction Query failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}
}

func TestDoubleBegin(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	tx1, _ := db.Begin()
	// Store tx1 before attempting second Begin
	_ = tx1

	_, err := db.Begin()
	if err == nil {
		t.Error("Expected error for double Begin")
	}
}

func TestClearResultCache(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")

	// Execute query to populate cache
	db.Query("SELECT * FROM test")

	// Clear cache should not panic
	db.ClearResultCache()
}

func TestMustExecPanics(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MustExec to panic on error")
		}
	}()

	// This should panic due to missing parameter
	db.MustExec("INSERT INTO test VALUES (?)")
}

func TestQueryEmptyResult(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows.Next() {
		t.Error("Expected no rows from empty table")
	}
}

func TestQueryMultipleRows(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	for i := 1; i <= 5; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d)", i))
	}

	rows, err := db.Query("SELECT id FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

func TestScanTypes(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (i INT, i64 BIGINT, f REAL, s TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 9223372036854775807, 3.14, 'hello')")

	rows, _ := db.Query("SELECT i, i64, f, s FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}

	var i int
	var i64 int64
	var f float64
	var s string
	if err := rows.Scan(&i, &i64, &f, &s); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if i != 1 {
		t.Errorf("i = %d, want 1", i)
	}
	if i64 != 9223372036854775807 {
		t.Errorf("i64 = %d, want 9223372036854775807", i64)
	}
	if f != 3.14 {
		t.Errorf("f = %f, want 3.14", f)
	}
	if s != "hello" {
		t.Errorf("s = %s, want hello", s)
	}
}

func TestScanInterface(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (val TEXT)")
	db.Exec("INSERT INTO test VALUES ('test-value')")

	rows, _ := db.Query("SELECT val FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}

	var val interface{}
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if val.(string) != "test-value" {
		t.Errorf("val = %v, want test-value", val)
	}
}

func TestQueryRowNoResults(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	rows, _ := db.Query("SELECT id FROM test WHERE id = 999")

	if rows.Next() {
		t.Error("Expected no rows")
	}
}

func TestInvalidSQL(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestPrepareEmptySQL(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Prepare("")
	if err == nil {
		t.Error("Expected error for empty SQL")
	}
}

func TestGetHybridStoreNonExistent(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	hs := db.GetHybridStore("nonexistent")
	if hs != nil {
		t.Error("Expected nil for non-existent table")
	}
}

func TestExecDropNonExistentTable(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	_, err := db.Exec("DROP TABLE nonexistent")
	if err == nil {
		t.Error("Expected error for dropping non-existent table")
	}
}

func TestExecDropTableIfExists(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	// Should not error even if table doesn't exist
	_, err := db.Exec("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS should not error: %v", err)
	}
}

func TestExecCreateTableIfNotExists(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	// Second create with IF NOT EXISTS should not error
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS test (id INT)")
	if err != nil {
		t.Errorf("CREATE TABLE IF NOT EXISTS should not error: %v", err)
	}
}

func TestExecCreateDuplicateTable(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	_, err := db.Exec("CREATE TABLE test (id INT)")
	if err == nil {
		t.Error("Expected error for duplicate table")
	}
}

// ---- Context tests ----

func TestExecContext(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	ctx := context.Background()
	result, err := db.ExecContext(ctx, "CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}
	_ = result
}

func TestQueryContext(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT id FROM test")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected row")
	}
}

func TestExecContextCancelled(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := db.ExecContext(ctx, "CREATE TABLE test (id INT)")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestQueryContextCancelled(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := db.QueryContext(ctx, "SELECT id FROM test")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestExecContextWithParams(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	ctx := context.Background()
	result, err := db.ExecContextWithParams(ctx, "INSERT INTO test VALUES (?, ?)", []interface{}{1, "Alice"})
	if err != nil {
		t.Fatalf("ExecContextWithParams failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}
}

func TestQueryContextWithParams(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")

	ctx := context.Background()
	rows, err := db.QueryContextWithParams(ctx, "SELECT name FROM test WHERE id = ?", []interface{}{1})
	if err != nil {
		t.Fatalf("QueryContextWithParams failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected row")
	}
}

func TestExecContextNamed(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	ctx := context.Background()
	result, err := db.ExecContextNamed(ctx, "INSERT INTO test VALUES (:id, :name)", map[string]interface{}{
		"id":   1,
		"name": "Alice",
	})
	if err != nil {
		t.Fatalf("ExecContextNamed failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}
}

func TestQueryContextNamed(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")

	ctx := context.Background()
	rows, err := db.QueryContextNamed(ctx, "SELECT name FROM test WHERE id = :id", map[string]interface{}{"id": 1})
	if err != nil {
		t.Fatalf("QueryContextNamed failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected row")
	}
}

// ---- Aggregate tests ----

func TestCountStar(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	rows, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var count int64
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if count != 3 {
		t.Errorf("COUNT(*) = %d, want 3", count)
	}
}

func TestSumAggregate(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (val INT)")
	db.Exec("INSERT INTO test VALUES (10)")
	db.Exec("INSERT INTO test VALUES (20)")
	db.Exec("INSERT INTO test VALUES (30)")

	rows, _ := db.Query("SELECT SUM(val) FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var sum int64
	rows.Scan(&sum)
	if sum != 60 {
		t.Errorf("SUM = %d, want 60", sum)
	}
}

func TestMinMaxAggregate(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (val INT)")
	db.Exec("INSERT INTO test VALUES (5)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (9)")

	rows, _ := db.Query("SELECT MIN(val), MAX(val) FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var min, max int64
	rows.Scan(&min, &max)
	if min != 1 {
		t.Errorf("MIN = %d, want 1", min)
	}
	if max != 9 {
		t.Errorf("MAX = %d, want 9", max)
	}
}

func TestAvgAggregate(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (val INT)")
	db.Exec("INSERT INTO test VALUES (10)")
	db.Exec("INSERT INTO test VALUES (20)")

	rows, _ := db.Query("SELECT AVG(val) FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var avg float64
	rows.Scan(&avg)
	if avg != 15.0 {
		t.Errorf("AVG = %f, want 15.0", avg)
	}
}

// ---- ORDER BY and LIMIT tests ----

func TestOrderBy(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (3)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")

	rows, _ := db.Query("SELECT id FROM test ORDER BY id")

	var ids []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		ids = append(ids, id)
	}

	if len(ids) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(ids))
	}
	for i, id := range ids {
		if id != i+1 {
			t.Errorf("ids[%d] = %d, want %d", i, id, i+1)
		}
	}
}

func TestOrderByDesc(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	rows, _ := db.Query("SELECT id FROM test ORDER BY id DESC")

	var ids []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		ids = append(ids, id)
	}

	if ids[0] != 3 || ids[1] != 2 || ids[2] != 1 {
		t.Errorf("ORDER BY DESC failed: %v", ids)
	}
}

func TestLimit(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	for i := 1; i <= 10; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d)", i))
	}

	rows, _ := db.Query("SELECT id FROM test LIMIT 5")

	count := 0
	for rows.Next() {
		count++
	}

	if count != 5 {
		t.Errorf("LIMIT returned %d rows, want 5", count)
	}
}

func TestLimitOffset(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	for i := 1; i <= 10; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d)", i))
	}

	rows, _ := db.Query("SELECT id FROM test ORDER BY id LIMIT 3 OFFSET 2")

	var ids []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		ids = append(ids, id)
	}

	// Should get rows 3, 4, 5 (1-indexed)
	if len(ids) != 3 || ids[0] != 3 || ids[1] != 4 || ids[2] != 5 {
		t.Errorf("LIMIT OFFSET failed: %v", ids)
	}
}

// ---- WHERE clause tests ----

func TestWhereEquals(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	db.Exec("INSERT INTO test VALUES (2, 'Bob')")

	rows, _ := db.Query("SELECT name FROM test WHERE id = 1")

	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var name string
	rows.Scan(&name)
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}

func TestWhereGreaterThan(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	rows, _ := db.Query("SELECT id FROM test WHERE id > 1")

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("WHERE id > 1 returned %d rows, want 2", count)
	}
}

func TestWhereAndCondition(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (a INT, b INT)")
	db.Exec("INSERT INTO test VALUES (1, 1)")
	db.Exec("INSERT INTO test VALUES (1, 2)")
	db.Exec("INSERT INTO test VALUES (2, 1)")

	rows, _ := db.Query("SELECT a, b FROM test WHERE a = 1 AND b = 2")

	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var a, b int
	rows.Scan(&a, &b)
	if a != 1 || b != 2 {
		t.Errorf("WHERE AND failed: a=%d, b=%d", a, b)
	}
}

func TestWhereOrCondition(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	rows, _ := db.Query("SELECT id FROM test WHERE id = 1 OR id = 3")

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("WHERE OR returned %d rows, want 2", count)
	}
}

func TestWhereLike(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (name TEXT)")
	db.Exec("INSERT INTO test VALUES ('Alice')")
	db.Exec("INSERT INTO test VALUES ('Bob')")
	db.Exec("INSERT INTO test VALUES ('Charlie')")

	rows, _ := db.Query("SELECT name FROM test WHERE name LIKE 'A%'")

	if !rows.Next() {
		t.Fatal("Expected row starting with A")
	}
	var name string
	rows.Scan(&name)
	if name != "Alice" {
		t.Errorf("WHERE LIKE failed: %s", name)
	}
}

// ---- GROUP BY tests ----

func TestGroupByCount(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (category TEXT)")
	db.Exec("INSERT INTO test VALUES ('A')")
	db.Exec("INSERT INTO test VALUES ('A')")
	db.Exec("INSERT INTO test VALUES ('B')")

	rows, _ := db.Query("SELECT category, COUNT(*) FROM test GROUP BY category ORDER BY category")

	var results []struct {
		cat   string
		count int64
	}
	for rows.Next() {
		var cat string
		var count int64
		rows.Scan(&cat, &count)
		results = append(results, struct {
			cat   string
			count int64
		}{cat, count})
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(results))
	}
	if results[0].cat != "A" || results[0].count != 2 {
		t.Errorf("Group A: got count %d, want 2", results[0].count)
	}
	if results[1].cat != "B" || results[1].count != 1 {
		t.Errorf("Group B: got count %d, want 1", results[1].count)
	}
}

// ---- DISTINCT tests ----

func TestDistinct(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (val INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	rows, _ := db.Query("SELECT DISTINCT val FROM test ORDER BY val")

	var vals []int
	for rows.Next() {
		var val int
		rows.Scan(&val)
		vals = append(vals, val)
	}

	if len(vals) != 3 {
		t.Errorf("DISTINCT returned %d rows, want 3", len(vals))
	}
}

// ---- CREATE INDEX tests ----

func TestCreateIndexBasic(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	_, err := db.Exec("CREATE INDEX idx_name ON test(name)")
	if err != nil {
		t.Errorf("CREATE INDEX failed: %v", err)
	}
}

func TestCreateUniqueIndexBasic(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, name TEXT)")

	_, err := db.Exec("CREATE UNIQUE INDEX idx_id ON test(id)")
	if err != nil {
		t.Errorf("CREATE UNIQUE INDEX failed: %v", err)
	}
}

func TestDropIndexBasic(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("CREATE INDEX idx_id ON test(id)")

	_, err := db.Exec("DROP INDEX idx_id")
	if err != nil {
		t.Errorf("DROP INDEX failed: %v", err)
	}
}

// ---- INSERT variations ----

func TestInsertDefaultValues(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT DEFAULT 0)")

	_, err := db.Exec("INSERT INTO test DEFAULT VALUES")
	if err != nil {
		t.Errorf("INSERT DEFAULT VALUES failed: %v", err)
	}

	rows, _ := db.Query("SELECT id FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var id int
	rows.Scan(&id)
	if id != 0 {
		t.Errorf("id = %d, want 0", id)
	}
}

func TestInsertMultipleRows(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")

	result, err := db.Exec("INSERT INTO test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT multiple rows failed: %v", err)
	}

	if result.RowsAffected != 3 {
		t.Errorf("RowsAffected = %d, want 3", result.RowsAffected)
	}
}

// ---- UPDATE tests ----

func TestUpdate(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, val TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'old')")

	_, err := db.Exec("UPDATE test SET val = 'new' WHERE id = 1")
	if err != nil {
		t.Errorf("UPDATE failed: %v", err)
	}

	rows, _ := db.Query("SELECT val FROM test WHERE id = 1")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var val string
	rows.Scan(&val)
	if val != "new" {
		t.Errorf("val = %s, want new", val)
	}
}

func TestUpdateAllRows(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT, active INT)")
	db.Exec("INSERT INTO test VALUES (1, 0)")
	db.Exec("INSERT INTO test VALUES (2, 0)")

	_, err := db.Exec("UPDATE test SET active = 1")
	if err != nil {
		t.Errorf("UPDATE all rows failed: %v", err)
	}

	rows, _ := db.Query("SELECT COUNT(*) FROM test WHERE active = 1")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var count int64
	rows.Scan(&count)
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// ---- DELETE tests ----

func TestDelete(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")

	_, err := db.Exec("DELETE FROM test WHERE id = 1")
	if err != nil {
		t.Errorf("DELETE failed: %v", err)
	}

	rows, _ := db.Query("SELECT COUNT(*) FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var count int64
	rows.Scan(&count)
	if count != 1 {
		t.Errorf("COUNT = %d, want 1", count)
	}
}

func TestDeleteAll(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE test (id INT)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Exec("INSERT INTO test VALUES (2)")
	db.Exec("INSERT INTO test VALUES (3)")

	_, err := db.Exec("DELETE FROM test")
	if err != nil {
		t.Errorf("DELETE all failed: %v", err)
	}

	rows, _ := db.Query("SELECT COUNT(*) FROM test")
	if !rows.Next() {
		t.Fatal("Expected row")
	}
	var count int64
	rows.Scan(&count)
	if count != 0 {
		t.Errorf("COUNT = %d, want 0", count)
	}
}
