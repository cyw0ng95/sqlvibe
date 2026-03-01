package database_test

import (
	"testing"
)

func TestPragma_TableInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pti (id INTEGER PRIMARY KEY, name TEXT)")

	rows, err := d.Query().Pragma("PRAGMA table_info(pti)")
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows.Data))
	}
}

func TestPragma_TableList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ptl1 (id INTEGER)")
	_, _ = d.DDL().CreateTable("CREATE TABLE ptl2 (id INTEGER)")

	rows, err := d.Query().Pragma("PRAGMA table_list")
	if err != nil {
		t.Fatalf("table_list: %v", err)
	}
	if len(rows.Data) < 2 {
		t.Fatalf("want at least 2 tables, got %d", len(rows.Data))
	}
	// Check columns: schema, name, type, ncol, wr, strict
	wantCols := []string{"schema", "name", "type", "ncol", "wr", "strict"}
	for i, wc := range wantCols {
		if i >= len(rows.Columns) || rows.Columns[i] != wc {
			t.Fatalf("want column %q at position %d, got %q", wc, i, rows.Columns[i])
		}
	}
}

func TestPragma_IndexList(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pil (id INTEGER, v TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_pv ON pil(v)")

	rows, err := d.Query().Pragma("PRAGMA index_list(pil)")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	if len(rows.Data) < 1 {
		t.Fatalf("want at least 1 index, got %d", len(rows.Data))
	}
}

func TestPragma_IndexInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pii (id INTEGER, a TEXT, b INTEGER)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_ab ON pii(a, b)")

	rows, err := d.Query().Pragma("PRAGMA index_info(idx_ab)")
	if err != nil {
		t.Fatalf("index_info: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 index columns, got %d", len(rows.Data))
	}
}

func TestPragma_IndexXInfo(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pixi (id INTEGER, nm TEXT)")
	_, _ = d.DDL().CreateIndex("CREATE INDEX idx_nm ON pixi(nm)")

	rows, err := d.Query().Pragma("PRAGMA index_xinfo(idx_nm)")
	if err != nil {
		t.Fatalf("index_xinfo: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows.Data))
	}
	wantCols := []string{"seqno", "cid", "name", "desc", "coll", "key"}
	for i, wc := range wantCols {
		if i >= len(rows.Columns) || rows.Columns[i] != wc {
			t.Fatalf("want column %q at pos %d, got %q", wc, i, rows.Columns[i])
		}
	}
}

func TestPragma_ForeignKeyCheck(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pfkp (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE pfkc (id INTEGER, pid INTEGER REFERENCES pfkp(id))")
	_, _ = d.DML().Insert("INSERT INTO pfkp VALUES (1)")
	_, _ = d.DML().Insert("INSERT INTO pfkc VALUES (1, 1)")

	rows, err := d.Query().Pragma("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Fatalf("want no violations, got %v", rows.Data)
	}
}

func TestPragma_ForeignKeyCheckWithViolation(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE pfkp2 (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE pfkc2 (id INTEGER, pid INTEGER REFERENCES pfkp2(id))")
	_, _ = d.DML().Insert("INSERT INTO pfkp2 VALUES (1)")
	// Insert orphan directly
	_, _ = d.Unwrap().Exec("INSERT INTO pfkc2 VALUES (1, 999)")

	rows, err := d.Query().Pragma("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected FK violations")
	}
}

func TestPragma_Encoding(t *testing.T) {
	d := openTestDB(t)
	rows, err := d.Query().Pragma("PRAGMA encoding")
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != "UTF-8" {
		t.Fatalf("want UTF-8, got %v", rows.Data)
	}
}
