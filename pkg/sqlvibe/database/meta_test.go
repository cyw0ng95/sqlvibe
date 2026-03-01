package database_test

import (
	"testing"
)

func TestMeta_TableInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ti (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")

	rows, err := d.Meta().TableInfo("ti")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("want 3 columns, got %d", len(rows.Data))
	}
	// Check columns
	wantCols := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	if len(rows.Columns) != len(wantCols) {
		t.Fatalf("want columns %v, got %v", wantCols, rows.Columns)
	}
}

func TestMeta_TableList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE tl1 (id INTEGER)")
	_, _ = d.DDL().CreateTable("CREATE TABLE tl2 (id INTEGER)")
	_, _ = d.DDL().CreateView("CREATE VIEW v_tl AS SELECT id FROM tl1")

	rows, err := d.Meta().TableList()
	if err != nil {
		t.Fatalf("table_list: %v", err)
	}
	// Expect at least 2 tables + 1 view
	if len(rows.Data) < 3 {
		t.Fatalf("want at least 3 entries, got %d", len(rows.Data))
	}
	wantCols := []string{"schema", "name", "type", "ncol", "wr", "strict"}
	if len(rows.Columns) != len(wantCols) {
		t.Fatalf("want columns %v, got %v", wantCols, rows.Columns)
	}
}

func TestMeta_IndexList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE il (id INTEGER, v TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_v ON il(v)")
	_, _ = d.DDL().CreateIndex("CREATE UNIQUE INDEX idx_id ON il(id)")

	rows, err := d.Meta().IndexList("il")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	if len(rows.Data) < 2 {
		t.Fatalf("want at least 2 indexes, got %d", len(rows.Data))
	}
}

func TestMeta_IndexInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ii (id INTEGER, v TEXT, w INTEGER)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_vw ON ii(v, w)")

	rows, err := d.Meta().IndexInfo("idx_vw")
	if err != nil {
		t.Fatalf("index_info: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 columns in index, got %d", len(rows.Data))
	}
}

func TestMeta_IndexXInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ixi (id INTEGER, name TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_name ON ixi(name)")

	rows, err := d.Meta().IndexXInfo("idx_name")
	if err != nil {
		t.Fatalf("index_xinfo: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("want 1 column in index, got %d", len(rows.Data))
	}
	wantCols := []string{"seqno", "cid", "name", "desc", "coll", "key"}
	if len(rows.Columns) != len(wantCols) {
		t.Fatalf("want columns %v, got %v", wantCols, rows.Columns)
	}
}

func TestMeta_ForeignKeyList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE fkp (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE fkc (id INTEGER, pid INTEGER REFERENCES fkp(id))")

	rows, err := d.Meta().ForeignKeyList("fkc")
	if err != nil {
		t.Fatalf("foreign_key_list: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("want FK info, got none")
	}
}

func TestMeta_Schema(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE sch (id INTEGER PRIMARY KEY, name TEXT)")

	schema, err := d.Meta().Schema("sch")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if schema == "" {
		t.Fatal("expected schema DDL string")
	}
}
