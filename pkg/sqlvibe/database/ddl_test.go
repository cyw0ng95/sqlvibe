package database_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/database"
)

func openTestDB(t *testing.T) *database.DB {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return database.New(db)
}

func TestDDL_CreateTable(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

func TestDDL_CreateTableIfNotExists(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err = d.DDL().CreateTable("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
}

func TestDDL_DropTable(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE tmp (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().DropTable("DROP TABLE tmp")
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
}

func TestDDL_DropTableIfExists(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().DropTable("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
}

func TestDDL_CreateIndex(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE items (id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().CreateIndex("CREATE INDEX idx_val ON items(val)")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
}

func TestDDL_DropIndex(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE t2 (id INTEGER, x INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().CreateIndex("CREATE INDEX idx_x ON t2(x)")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	_, err = d.DDL().DropIndex("DROP INDEX idx_x")
	if err != nil {
		t.Fatalf("drop index: %v", err)
	}
}

func TestDDL_DropIndexIfExists(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().DropIndex("DROP INDEX IF EXISTS missing_idx")
	if err != nil {
		t.Fatalf("drop index if exists: %v", err)
	}
}

func TestDDL_CreateView(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE src (id INTEGER, v TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = d.DDL().CreateView("CREATE VIEW v_src AS SELECT id, v FROM src")
	if err != nil {
		t.Fatalf("create view: %v", err)
	}
}

func TestDDL_DropView(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE s2 (id INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = d.DDL().CreateView("CREATE VIEW v2 AS SELECT id FROM s2")
	if err != nil {
		t.Fatalf("create view: %v", err)
	}
	_, err = d.DDL().DropView("DROP VIEW v2")
	if err != nil {
		t.Fatalf("drop view: %v", err)
	}
}

func TestDDL_AlterTableAddColumn(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE t3 (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE t3 ADD COLUMN email TEXT")
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
}
