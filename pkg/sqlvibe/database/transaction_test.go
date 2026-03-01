package database_test

import (
	"testing"
)

func TestTxn_BeginCommit(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE txboth (id INTEGER)")

	tx, err := d.Txn().Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = d.DML().Insert("INSERT INTO txboth VALUES (1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM txboth")
	if rows.Data[0][0] != int64(1) {
		t.Fatalf("want 1 row after commit, got %v", rows.Data[0][0])
	}
}

func TestTxn_BeginRollback(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE txrb (id INTEGER)")

	_, err := d.Txn().Exec("BEGIN")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO txrb VALUES (99)")
	_, err = d.Txn().Exec("ROLLBACK")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM txrb")
	if rows.Data[0][0] != int64(0) {
		t.Fatalf("want 0 rows after rollback, got %v", rows.Data[0][0])
	}
}

func TestTxn_Savepoint(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE sptest (id INTEGER)")

	_, _ = d.Txn().Exec("BEGIN")
	_, _ = d.DML().Insert("INSERT INTO sptest VALUES (1)")
	_, err := d.Txn().Savepoint("sp1")
	if err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO sptest VALUES (2)")

	_, err = d.Txn().RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM sptest")
	if rows.Data[0][0] != int64(1) {
		t.Fatalf("want 1 row after rollback to savepoint, got %v", rows.Data[0][0])
	}

	_, _ = d.Txn().Exec("COMMIT")
}

func TestTxn_ReleaseSavepoint(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE rsp (id INTEGER)")

	_, _ = d.Txn().Exec("BEGIN")
	_, _ = d.DML().Insert("INSERT INTO rsp VALUES (10)")
	_, _ = d.Txn().Savepoint("sp2")
	_, _ = d.DML().Insert("INSERT INTO rsp VALUES (20)")

	_, err := d.Txn().ReleaseSavepoint("sp2")
	if err != nil {
		t.Fatalf("release savepoint: %v", err)
	}
	_, _ = d.Txn().Exec("COMMIT")

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM rsp")
	if rows.Data[0][0] != int64(2) {
		t.Fatalf("want 2 rows after release savepoint, got %v", rows.Data[0][0])
	}
}

func TestTxn_NestedSavepoints(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE nsp (id INTEGER)")

	_, _ = d.Txn().Exec("BEGIN")
	_, _ = d.DML().Insert("INSERT INTO nsp VALUES (1)")
	_, _ = d.Txn().Savepoint("outer")
	_, _ = d.DML().Insert("INSERT INTO nsp VALUES (2)")
	_, _ = d.Txn().Savepoint("inner")
	_, _ = d.DML().Insert("INSERT INTO nsp VALUES (3)")

	_, err := d.Txn().RollbackToSavepoint("inner")
	if err != nil {
		t.Fatalf("rollback to inner: %v", err)
	}

	rows, _ := d.Query().Select("SELECT COUNT(*) FROM nsp")
	if rows.Data[0][0] != int64(2) {
		t.Fatalf("want 2 rows after rollback to inner, got %v", rows.Data[0][0])
	}

	_, err = d.Txn().RollbackToSavepoint("outer")
	if err != nil {
		t.Fatalf("rollback to outer: %v", err)
	}

	rows, _ = d.Query().Select("SELECT COUNT(*) FROM nsp")
	if rows.Data[0][0] != int64(1) {
		t.Fatalf("want 1 row after rollback to outer, got %v", rows.Data[0][0])
	}
	_, _ = d.Txn().Exec("COMMIT")
}

func TestTxn_ExplicitBeginCommit(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ebc (id INTEGER)")

	_, _ = d.Txn().Exec("BEGIN TRANSACTION")
	_, _ = d.DML().Insert("INSERT INTO ebc VALUES (42)")
	_, err := d.Txn().Exec("COMMIT")
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	rows, _ := d.Query().Select("SELECT id FROM ebc")
	if len(rows.Data) != 1 || rows.Data[0][0] != int64(42) {
		t.Fatalf("want 42, got %v", rows.Data)
	}
}
