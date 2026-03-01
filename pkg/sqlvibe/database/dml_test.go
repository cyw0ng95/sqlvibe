package database_test

import (
	"testing"
)

func TestDML_Insert(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	r, err := d.DML().Insert("INSERT INTO emp VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("want 1 row affected, got %d", r.RowsAffected)
	}
}

func TestDML_InsertMultiRow(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE emp2 (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DML().Insert("INSERT INTO emp2 VALUES (1,'A'),(2,'B'),(3,'C')")
	if err != nil {
		t.Fatalf("insert multi: %v", err)
	}
	rows, err := d.Query().Select("SELECT COUNT(*) FROM emp2")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(3) {
		t.Fatalf("want 3 rows, got %v", rows.Data)
	}
}

func TestDML_InsertWithParams(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE ep (id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DML().InsertWithParams("INSERT INTO ep VALUES (?, ?)", []interface{}{int64(10), "hello"})
	if err != nil {
		t.Fatalf("insert params: %v", err)
	}
}

func TestDML_Update(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE upd (id INTEGER, v INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO upd VALUES (1, 10)")
	r, err := d.DML().Update("UPDATE upd SET v = 99 WHERE id = 1")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("want 1 row affected, got %d", r.RowsAffected)
	}
}

func TestDML_UpdateWithParams(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE upd2 (id INTEGER, v INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO upd2 VALUES (1, 10)")
	_, err = d.DML().UpdateWithParams("UPDATE upd2 SET v = ? WHERE id = ?", []interface{}{int64(42), int64(1)})
	if err != nil {
		t.Fatalf("update params: %v", err)
	}
}

func TestDML_Delete(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE del (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO del VALUES (1)")
	r, err := d.DML().Delete("DELETE FROM del WHERE id = 1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("want 1 row affected, got %d", r.RowsAffected)
	}
}

func TestDML_DeleteWithParams(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE del2 (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO del2 VALUES (5)")
	_, err = d.DML().DeleteWithParams("DELETE FROM del2 WHERE id = ?", []interface{}{int64(5)})
	if err != nil {
		t.Fatalf("delete params: %v", err)
	}
}

func TestDML_InsertOnConflictDoNothing(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE oc (id INTEGER PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO oc VALUES (1, 'original')")
	_, err = d.DML().Insert("INSERT INTO oc VALUES (1, 'conflict') ON CONFLICT(id) DO NOTHING")
	if err != nil {
		t.Fatalf("on conflict do nothing: %v", err)
	}
	rows, _ := d.Query().Select("SELECT v FROM oc WHERE id = 1")
	if len(rows.Data) == 0 || rows.Data[0][0] != "original" {
		t.Fatalf("expected original, got %v", rows.Data)
	}
}

func TestDML_InsertOnConflictDoUpdate(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE oc2 (id INTEGER PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO oc2 VALUES (1, 'original')")
	_, err = d.DML().Insert("INSERT INTO oc2 VALUES (1, 'new') ON CONFLICT(id) DO UPDATE SET v = excluded.v")
	if err != nil {
		t.Fatalf("on conflict do update: %v", err)
	}
	rows, _ := d.Query().Select("SELECT v FROM oc2 WHERE id = 1")
	if len(rows.Data) == 0 || rows.Data[0][0] != "new" {
		t.Fatalf("expected 'new', got %v", rows.Data)
	}
}
