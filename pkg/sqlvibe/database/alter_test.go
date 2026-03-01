package database_test

import (
	"testing"
)

func TestAlter_AddColumn(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE alt (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE alt ADD COLUMN name TEXT")
	if err != nil {
		t.Fatalf("add column: %v", err)
	}
	rows, _ := d.Meta().TableInfo("alt")
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 columns, got %d", len(rows.Data))
	}
}

func TestAlter_RenameTable(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE old_name (id INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE old_name RENAME TO new_name")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	rows, _ := d.Query().Select("SELECT COUNT(*) FROM new_name")
	if len(rows.Data) == 0 {
		t.Fatal("expected new_name table to exist")
	}
}

func TestAlter_RenameColumn(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE rcol (id INTEGER, old_col TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO rcol VALUES (1, 'val')")
	_, err = d.DDL().AlterTable("ALTER TABLE rcol RENAME COLUMN old_col TO new_col")
	if err != nil {
		t.Fatalf("rename column: %v", err)
	}
	rows, err := d.Query().Select("SELECT new_col FROM rcol")
	if err != nil {
		t.Fatalf("select renamed col: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "val" {
		t.Fatalf("want 'val', got %v", rows.Data)
	}
}

func TestAlter_DropColumn(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE dcol (id INTEGER, extra TEXT, keep INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE dcol DROP COLUMN extra")
	if err != nil {
		t.Fatalf("drop column: %v", err)
	}
	rows, _ := d.Meta().TableInfo("dcol")
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 columns after drop, got %d", len(rows.Data))
	}
}

func TestAlter_AddConstraintCheck(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE achk (id INTEGER, score INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE achk ADD CONSTRAINT c_score CHECK(score >= 0)")
	if err != nil {
		t.Fatalf("add constraint check: %v", err)
	}
	// Existing rows are not re-validated
	_, err = d.DML().Insert("INSERT INTO achk VALUES (1, -1)")
	if err == nil {
		t.Fatal("expected CHECK constraint violation on insert")
	}
}

func TestAlter_AddConstraintUnique(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE auniq (id INTEGER, code TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE auniq ADD CONSTRAINT u_code UNIQUE (code)")
	if err != nil {
		t.Fatalf("add constraint unique: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO auniq VALUES (1, 'ABC')")
	_, err = d.DML().Insert("INSERT INTO auniq VALUES (2, 'ABC')")
	if err == nil {
		t.Fatal("expected unique constraint violation")
	}
}

func TestAlter_RenameIndex(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ri (id INTEGER, v TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_old ON ri(v)")

	_, err := d.DDL().AlterTable("ALTER TABLE ri RENAME INDEX idx_old TO idx_new")
	if err != nil {
		t.Fatalf("rename index: %v", err)
	}
	// The old name should no longer exist; the new name should appear
	rows, _ := d.Meta().IndexList("ri")
	found := false
	for _, row := range rows.Data {
		if row[1] == "idx_new" {
			found = true
		}
		if row[1] == "idx_old" {
			t.Fatal("old index name still present after rename")
		}
	}
	if !found {
		t.Fatal("new index name not found after rename")
	}
}

func TestAlter_RenameColumnNoKeyword(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE rck (id INTEGER, foo TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DDL().AlterTable("ALTER TABLE rck RENAME foo TO bar")
	if err != nil {
		t.Fatalf("rename column without COLUMN keyword: %v", err)
	}
	rows, err := d.Query().Select("SELECT bar FROM rck")
	if err != nil {
		t.Fatalf("select after rename: %v", err)
	}
	_ = rows
}
