package F561

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F561_ForeignKeyEnforcement_L1 tests FK enforcement with PRAGMA foreign_keys = ON.
func TestSQL1999_F561_ForeignKeyEnforcement_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	// Enable FK enforcement
	if _, err := sqlvibeDB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("Failed to enable FK: %v", err)
	}

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateParent", "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateChild", "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES dept(id), name TEXT)"},
		{"InsertDept1", "INSERT INTO dept VALUES (1, 'Engineering')"},
		{"InsertDept2", "INSERT INTO dept VALUES (2, 'Sales')"},
		{"InsertEmp1", "INSERT INTO emp VALUES (1, 1, 'Alice')"},
		{"InsertEmp2", "INSERT INTO emp VALUES (2, 2, 'Bob')"},
	}
	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := sqlvibeDB.Exec(tt.sql); err != nil {
				t.Errorf("%s: unexpected error: %v", tt.name, err)
			}
		})
	}

	// FK violation on INSERT
	t.Run("FKViolationInsert", func(t *testing.T) {
		_, err := sqlvibeDB.Exec("INSERT INTO emp VALUES (3, 999, 'Nobody')")
		if err == nil {
			t.Error("Expected FK violation error, got nil")
		}
	})

	// Valid queries
	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()
	for _, stmt := range []string{
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES dept(id), name TEXT)",
		"INSERT INTO dept VALUES (1, 'Engineering')",
		"INSERT INTO dept VALUES (2, 'Sales')",
		"INSERT INTO emp VALUES (1, 1, 'Alice')",
		"INSERT INTO emp VALUES (2, 2, 'Bob')",
	} {
		if _, err := sqliteDB.Exec(stmt); err != nil {
			t.Skipf("SQLite setup error: %v", err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllEmps", "SELECT id, dept_id, name FROM emp ORDER BY id"},
		{"AllDepts", "SELECT id, name FROM dept ORDER BY id"},
		{"JoinEmpDept", "SELECT emp.name, dept.name FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F561_FKCascadeDelete_L1 tests ON DELETE CASCADE.
func TestSQL1999_F561_FKCascadeDelete_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	if _, err := sqlvibeDB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("Failed to enable FK: %v", err)
	}

	for _, stmt := range []string{
		"CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)",
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, data TEXT, FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE CASCADE)",
		"INSERT INTO parent VALUES (1, 'P1')",
		"INSERT INTO parent VALUES (2, 'P2')",
		"INSERT INTO child VALUES (1, 1, 'C1a')",
		"INSERT INTO child VALUES (2, 1, 'C1b')",
		"INSERT INTO child VALUES (3, 2, 'C2a')",
	} {
		if _, err := sqlvibeDB.Exec(stmt); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	// Delete parent – child rows should be cascade-deleted
	if _, err := sqlvibeDB.Exec("DELETE FROM parent WHERE id = 1"); err != nil {
		t.Fatalf("DELETE error: %v", err)
	}

	rows, err := sqlvibeDB.Query("SELECT COUNT(*) FROM child")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if len(rows.Data) == 0 || rows.Data[0][0] != int64(1) {
		t.Errorf("Expected 1 child row after cascade delete, got %v", rows.Data)
	}
}

// TestSQL1999_F561_FKSetNull_L1 tests ON DELETE SET NULL.
func TestSQL1999_F561_FKSetNull_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	if _, err := sqlvibeDB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("Failed to enable FK: %v", err)
	}

	for _, stmt := range []string{
		"CREATE TABLE category (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE item (id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, FOREIGN KEY(cat_id) REFERENCES category(id) ON DELETE SET NULL)",
		"INSERT INTO category VALUES (1, 'Fruit')",
		"INSERT INTO item VALUES (1, 1, 'Apple')",
		"INSERT INTO item VALUES (2, 1, 'Banana')",
	} {
		if _, err := sqlvibeDB.Exec(stmt); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	if _, err := sqlvibeDB.Exec("DELETE FROM category WHERE id = 1"); err != nil {
		t.Fatalf("DELETE error: %v", err)
	}

	rows, err := sqlvibeDB.Query("SELECT cat_id FROM item ORDER BY id")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	for _, row := range rows.Data {
		if row[0] != nil {
			t.Errorf("Expected NULL cat_id after SET NULL, got %v", row[0])
		}
	}
}

// TestSQL1999_F561_FKRestrict_L1 tests ON DELETE RESTRICT.
func TestSQL1999_F561_FKRestrict_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	if _, err := sqlvibeDB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("Failed to enable FK: %v", err)
	}

	for _, stmt := range []string{
		"CREATE TABLE group_ (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE member (id INTEGER PRIMARY KEY, group_id INTEGER, FOREIGN KEY(group_id) REFERENCES group_(id) ON DELETE RESTRICT)",
		"INSERT INTO group_ VALUES (1, 'Admin')",
		"INSERT INTO member VALUES (1, 1)",
	} {
		if _, err := sqlvibeDB.Exec(stmt); err != nil {
			t.Fatalf("Setup error: %v", err)
		}
	}

	// DELETE should fail because there are child rows
	if _, err := sqlvibeDB.Exec("DELETE FROM group_ WHERE id = 1"); err == nil {
		t.Error("Expected FK RESTRICT error, got nil")
	}
}

// TestSQL1999_F561_PragmaForeignKeys_L1 tests PRAGMA foreign_keys.
func TestSQL1999_F561_PragmaForeignKeys_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	// Default FK disabled
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "PRAGMA foreign_keys", "DefaultFKDisabled")

	// Enable FK
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "PRAGMA foreign_keys = ON", "EnableFK")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "PRAGMA foreign_keys", "FKEnabled")

	// Disable FK again
	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "PRAGMA foreign_keys = OFF", "DisableFK")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "PRAGMA foreign_keys", "FKDisabled")
}
