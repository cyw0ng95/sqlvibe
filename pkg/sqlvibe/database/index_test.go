package database_test

import (
	"testing"
)

func TestIndex_CreateAndUse(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE idxt (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO idxt VALUES (1,'Alice',30),(2,'Bob',25),(3,'Carol',30)")

	_, err = d.DDL().CreateIndex("CREATE INDEX idx_age ON idxt(age)")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	rows, err := d.Query().Select("SELECT name FROM idxt WHERE age = 30 ORDER BY name")
	if err != nil {
		t.Fatalf("query with index: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows.Data))
	}
}

func TestIndex_UniqueConstraint(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE uidx (id INTEGER, email TEXT)")
	_, err := d.DDL().CreateIndex("CREATE UNIQUE INDEX idx_email ON uidx(email)")
	if err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO uidx VALUES (1, 'a@b.com')")
	_, err = d.DML().Insert("INSERT INTO uidx VALUES (2, 'a@b.com')")
	if err == nil {
		t.Fatal("expected unique index violation")
	}
}

func TestIndex_DropIfExists(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE dif (id INTEGER)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_dif ON dif(id)")
	_, err := d.DDL().DropIndex("DROP INDEX IF EXISTS idx_dif")
	if err != nil {
		t.Fatalf("drop if exists: %v", err)
	}
	// Second drop should also succeed
	_, err = d.DDL().DropIndex("DROP INDEX IF EXISTS idx_dif")
	if err != nil {
		t.Fatalf("second drop if exists: %v", err)
	}
}

func TestIndex_MultiColumn(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE mcidx (id INTEGER, a TEXT, b INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO mcidx VALUES (1,'x',10),(2,'x',20),(3,'y',10)")
	_, err := d.DDL().CreateIndex("CREATE INDEX idx_ab ON mcidx(a, b)")
	if err != nil {
		t.Fatalf("multi-col index: %v", err)
	}
	rows, err := d.Query().Select("SELECT id FROM mcidx WHERE a = 'x' AND b = 10")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows.Data))
	}
}

func TestIndex_PragmaIndexList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pil2 (id INTEGER, v TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX i1 ON pil2(v)")
	_, _ = d.DDL().CreateIndex("CREATE UNIQUE INDEX i2 ON pil2(id)")

	rows, err := d.Meta().IndexList("pil2")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	if len(rows.Data) < 2 {
		t.Fatalf("want at least 2, got %d", len(rows.Data))
	}
}

func TestIndex_RenameViaAlter(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ria (id INTEGER, k TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX old_k ON ria(k)")

	_, err := d.DDL().AlterTable("ALTER TABLE ria RENAME INDEX old_k TO new_k")
	if err != nil {
		t.Fatalf("rename index: %v", err)
	}

	rows, _ := d.Meta().IndexList("ria")
	found := false
	for _, r := range rows.Data {
		if r[1] == "new_k" {
			found = true
		}
	}
	if !found {
		t.Fatal("renamed index not found")
	}
}
