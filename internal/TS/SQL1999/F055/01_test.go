package F055

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F055_ForeignKeyDeclaration_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateParent", "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateChild", "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), info TEXT)"},
		{"InsertParent1", "INSERT INTO parent VALUES (1, 'ParentOne')"},
		{"InsertParent2", "INSERT INTO parent VALUES (2, 'ParentTwo')"},
		{"InsertChild1", "INSERT INTO child VALUES (1, 1, 'ChildA')"},
		{"InsertChild2", "INSERT INTO child VALUES (2, 1, 'ChildB')"},
		{"InsertChild3", "INSERT INTO child VALUES (3, 2, 'ChildC')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectChildren", "SELECT child.id, child.info, parent.name FROM child INNER JOIN parent ON child.parent_id = parent.id ORDER BY child.id"},
		{"SelectChildrenOfParent1", "SELECT info FROM child WHERE parent_id = 1 ORDER BY id"},
		{"SelectChildrenCount", "SELECT COUNT(*) FROM child"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F055_ForeignKeyOnDelete_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateCategory", "CREATE TABLE category (id INTEGER PRIMARY KEY, label TEXT)"},
		{"CreateProduct", "CREATE TABLE product (id INTEGER PRIMARY KEY, cat_id INTEGER, title TEXT, FOREIGN KEY(cat_id) REFERENCES category(id) ON DELETE SET NULL)"},
		{"InsertCat1", "INSERT INTO category VALUES (1, 'Electronics')"},
		{"InsertCat2", "INSERT INTO category VALUES (2, 'Books')"},
		{"InsertProd1", "INSERT INTO product VALUES (1, 1, 'Phone')"},
		{"InsertProd2", "INSERT INTO product VALUES (2, 1, 'Laptop')"},
		{"InsertProd3", "INSERT INTO product VALUES (3, 2, 'Novel')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT product.id, product.title, category.label FROM product LEFT JOIN category ON product.cat_id = category.id ORDER BY product.id"},
		{"CountAllProducts", "SELECT COUNT(*) FROM product"},
		{"CountElectronics", "SELECT COUNT(*) FROM product WHERE cat_id = 1"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F055_ForeignKeyOnUpdate_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateDept", "CREATE TABLE department (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateStaff", "CREATE TABLE staff (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, FOREIGN KEY(dept_id) REFERENCES department(id) ON UPDATE CASCADE)"},
		{"InsertDept1", "INSERT INTO department VALUES (10, 'Engineering')"},
		{"InsertDept2", "INSERT INTO department VALUES (20, 'Sales')"},
		{"InsertStaff1", "INSERT INTO staff VALUES (1, 10, 'Alice')"},
		{"InsertStaff2", "INSERT INTO staff VALUES (2, 10, 'Bob')"},
		{"InsertStaff3", "INSERT INTO staff VALUES (3, 20, 'Carol')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT staff.id, staff.name, department.name FROM staff INNER JOIN department ON staff.dept_id = department.id ORDER BY staff.id"},
		{"StaffInEngineering", "SELECT COUNT(*) FROM staff WHERE dept_id = 10"},
		{"AllDepts", "SELECT id, name FROM department ORDER BY id"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
