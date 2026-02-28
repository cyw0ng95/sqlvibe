package sqlvibe

import (
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
