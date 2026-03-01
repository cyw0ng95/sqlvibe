package database_test

import (
	"testing"
)

func TestQuery_Select(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE qsel (id INTEGER, v TEXT)")
	_, _ = d.DML().Insert("INSERT INTO qsel VALUES (1, 'a'), (2, 'b')")

	rows, err := d.Query().Select("SELECT * FROM qsel ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows.Data))
	}
}

func TestQuery_SelectWithParams(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE qp (id INTEGER, v TEXT)")
	_, _ = d.DML().Insert("INSERT INTO qp VALUES (1, 'x'), (2, 'y')")

	rows, err := d.Query().SelectWithParams("SELECT v FROM qp WHERE id = ?", []interface{}{int64(2)})
	if err != nil {
		t.Fatalf("select params: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "y" {
		t.Fatalf("want 'y', got %v", rows.Data)
	}
}

func TestQuery_SelectNamed(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE qn (id INTEGER, v TEXT)")
	_, _ = d.DML().Insert("INSERT INTO qn VALUES (1, 'foo'), (2, 'bar')")

	rows, err := d.Query().SelectNamed("SELECT v FROM qn WHERE id = :id", map[string]interface{}{"id": int64(1)})
	if err != nil {
		t.Fatalf("select named: %v", err)
	}
	if len(rows.Data) != 1 || rows.Data[0][0] != "foo" {
		t.Fatalf("want 'foo', got %v", rows.Data)
	}
}

func TestQuery_SetOperations(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE qa (v INTEGER)")
	_, _ = d.DDL().CreateTable("CREATE TABLE qb (v INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO qa VALUES (1),(2),(3)")
	_, _ = d.DML().Insert("INSERT INTO qb VALUES (2),(3),(4)")

	// UNION
	rows, err := d.Query().Select("SELECT v FROM qa UNION SELECT v FROM qb ORDER BY v")
	if err != nil {
		t.Fatalf("union: %v", err)
	}
	if len(rows.Data) != 4 {
		t.Fatalf("want 4 rows for UNION, got %d", len(rows.Data))
	}

	// UNION ALL
	rows, err = d.Query().Select("SELECT v FROM qa UNION ALL SELECT v FROM qb")
	if err != nil {
		t.Fatalf("union all: %v", err)
	}
	if len(rows.Data) != 6 {
		t.Fatalf("want 6 rows for UNION ALL, got %d", len(rows.Data))
	}

	// INTERSECT
	rows, err = d.Query().Select("SELECT v FROM qa INTERSECT SELECT v FROM qb ORDER BY v")
	if err != nil {
		t.Fatalf("intersect: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 rows for INTERSECT, got %d", len(rows.Data))
	}

	// EXCEPT
	rows, err = d.Query().Select("SELECT v FROM qa EXCEPT SELECT v FROM qb ORDER BY v")
	if err != nil {
		t.Fatalf("except: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("want 1 row for EXCEPT, got %d", len(rows.Data))
	}
}

func TestQuery_Pragma(t *testing.T) {
	d := openTestDB(t)
	rows, err := d.Query().Pragma("PRAGMA encoding")
	if err != nil {
		t.Fatalf("pragma encoding: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected encoding result")
	}
}

func TestQuery_Aggregate(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE nums (n INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO nums VALUES (10),(20),(30)")

	rows, err := d.Query().Select("SELECT SUM(n), AVG(n), MIN(n), MAX(n) FROM nums")
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(60) {
		t.Fatalf("sum want 60, got %v", rows.Data[0][0])
	}
}

func TestQuery_GroupBy(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE grp (cat TEXT, val INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO grp VALUES ('a',1),('b',2),('a',3),('b',4)")

	rows, err := d.Query().Select("SELECT cat, SUM(val) FROM grp GROUP BY cat ORDER BY cat")
	if err != nil {
		t.Fatalf("group by: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Fatalf("want 2 groups, got %d", len(rows.Data))
	}
}

func TestQuery_OrderByLimit(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ord (n INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO ord VALUES (3),(1),(4),(1),(5),(9),(2),(6)")

	rows, err := d.Query().Select("SELECT n FROM ord ORDER BY n DESC LIMIT 3")
	if err != nil {
		t.Fatalf("order by limit: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows.Data))
	}
	if rows.Data[0][0] != int64(9) {
		t.Fatalf("want 9 first, got %v", rows.Data[0][0])
	}
}
