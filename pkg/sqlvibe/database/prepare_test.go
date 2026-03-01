package database_test

import (
	"testing"
)

func TestPrepare_Statement(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE prep (id INTEGER, v TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	stmt, err := d.Prepare().Statement("INSERT INTO prep VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	r, err := stmt.Exec(int64(1), "hello")
	if err != nil {
		t.Fatalf("exec prepared: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("want 1 row affected, got %d", r.RowsAffected)
	}

	rows, err := d.Query().Select("SELECT v FROM prep WHERE id = 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "hello" {
		t.Fatalf("want 'hello', got %v", rows.Data)
	}
}

func TestPrepare_StatementReuse(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE prep2 (id INTEGER, v INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	stmt, err := d.Prepare().Statement("INSERT INTO prep2 VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = stmt.Exec(int64(i), int64(i*10))
		if err != nil {
			t.Fatalf("exec %d: %v", i, err)
		}
	}

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM prep2")
	if rows.Data[0][0] != int64(5) {
		t.Fatalf("want 5 rows, got %v", rows.Data[0][0])
	}
}

func TestPrepare_SelectStatement(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE prep3 (id INTEGER, v TEXT)")
	_, _ = d.DML().Insert("INSERT INTO prep3 VALUES (1,'a'),(2,'b'),(3,'c')")

	stmt, err := d.Prepare().Statement("SELECT v FROM prep3 WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	rows, err := stmt.Query(int64(2))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "b" {
		t.Fatalf("want 'b', got %v", rows.Data)
	}
}
