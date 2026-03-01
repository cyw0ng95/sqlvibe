package database_test

import (
	"testing"
)

func TestConstraint_ForeignKeyCheck_NoViolations(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE parent_c (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE child_c (id INTEGER, pid INTEGER REFERENCES parent_c(id))")
	_, _ = d.Unwrap().Exec("PRAGMA foreign_keys = ON")
	_, _ = d.DML().Insert("INSERT INTO parent_c VALUES (1),(2)")
	_, _ = d.DML().Insert("INSERT INTO child_c VALUES (1, 1),(2, 2)")

	rows, err := d.Constraint().ForeignKeyCheck()
	if err != nil {
		t.Fatalf("fk_check: %v", err)
	}
	if len(rows.Data) != 0 {
		t.Fatalf("want no violations, got %v", rows.Data)
	}
}

func TestConstraint_ForeignKeyCheck_WithViolation(t *testing.T) {
	d := openTestDB(t)
	// Insert child with non-existent parent directly (FK enforcement off)
	_, _ = d.DDL().CreateTable("CREATE TABLE par2 (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE chi2 (id INTEGER, pid INTEGER REFERENCES par2(id))")
	_, _ = d.DML().Insert("INSERT INTO par2 VALUES (1)")
	// Insert without FK check - directly via Exec to bypass FK enforcement
	_, _ = d.Unwrap().Exec("INSERT INTO chi2 VALUES (1, 999)")

	rows, err := d.Constraint().ForeignKeyCheck()
	if err != nil {
		t.Fatalf("fk check: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("want FK violations, got none")
	}
}

func TestConstraint_ForeignKeyCheckTable(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE par3 (id INTEGER PRIMARY KEY)")
	_, _ = d.DDL().CreateTable("CREATE TABLE chi3 (id INTEGER, pid INTEGER REFERENCES par3(id))")
	_, _ = d.DML().Insert("INSERT INTO par3 VALUES (1)")
	_, _ = d.Unwrap().Exec("INSERT INTO chi3 VALUES (1, 999)")

	rows, err := d.Constraint().ForeignKeyCheckTable("chi3")
	if err != nil {
		t.Fatalf("fk check table: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("want FK violation, got none")
	}
}

func TestConstraint_QuickCheck(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE qc_t (id INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO qc_t VALUES (1)")

	rows, err := d.Constraint().QuickCheck()
	if err != nil {
		t.Fatalf("quick check: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected quick check result")
	}
}

func TestConstraint_IntegrityCheck(t *testing.T) {
	d := openTestDB(t)
	_, _ = d.DDL().CreateTable("CREATE TABLE ic_t (id INTEGER)")
	_, _ = d.DML().Insert("INSERT INTO ic_t VALUES (1)")

	rows, err := d.Constraint().IntegrityCheck()
	if err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected integrity check result")
	}
}

func TestConstraint_UniqueConstraint(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE uc (id INTEGER UNIQUE, v TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = d.DML().Insert("INSERT INTO uc VALUES (1, 'a')")
	_, err = d.DML().Insert("INSERT INTO uc VALUES (1, 'b')")
	if err == nil {
		t.Fatal("expected unique constraint violation")
	}
}

func TestConstraint_NotNullConstraint(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE nn (id INTEGER, v TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DML().Insert("INSERT INTO nn VALUES (1, NULL)")
	if err == nil {
		t.Fatal("expected NOT NULL constraint violation")
	}
}

func TestConstraint_CheckConstraint(t *testing.T) {
	d := openTestDB(t)
	_, err := d.DDL().CreateTable("CREATE TABLE chk (id INTEGER, age INTEGER CHECK(age >= 0))")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = d.DML().Insert("INSERT INTO chk VALUES (1, -1)")
	if err == nil {
		t.Fatal("expected CHECK constraint violation")
	}
}
