package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03108_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT)"},
		{"CreateDepartments", "CREATE TABLE departments (id INTEGER, name TEXT)"},
		{"CreateProjects", "CREATE TABLE projects (id INTEGER, name TEXT, dept_id INTEGER)"},
		{"InsertData1", "INSERT INTO employees VALUES (1, 'Alice', 'Engineering')"},
		{"InsertData2", "INSERT INTO employees VALUES (2, 'Bob', 'Sales')"},
		{"InsertData3", "INSERT INTO employees VALUES (3, 'Charlie', 'Engineering')"},
		{"InsertData4", "INSERT INTO employees VALUES (4, 'Diana', 'HR')"},
		{"InsertDept1", "INSERT INTO departments VALUES (1, 'Engineering')"},
		{"InsertDept2", "INSERT INTO departments VALUES (2, 'Sales')"},
		{"InsertDept3", "INSERT INTO departments VALUES (3, 'HR')"},
		{"InsertProj1", "INSERT INTO projects VALUES (1, 'Project Alpha', 1)"},
		{"InsertProj2", "INSERT INTO projects VALUES (2, 'Project Beta', 2)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	aliasTests := []struct {
		name string
		sql  string
	}{
		{"SimpleAlias", "SELECT e.name, e.department FROM employees AS e"},
		{"AliasWithoutAS", "SELECT e.name FROM employees e"},
		{"MultipleAliases", "SELECT e1.name, e2.department FROM employees AS e1, employees AS e2 WHERE e1.id < e2.id"},
		{"AliasInWhere", "SELECT e.name FROM employees AS e WHERE e.department = 'Engineering'"},
		{"AliasInOrderBy", "SELECT e.name FROM employees AS e ORDER BY e.name DESC"},
		{"AliasInGroupBy", "SELECT e.department, COUNT(*) FROM employees AS e GROUP BY e.department"},
		{"AliasInHaving", "SELECT e.department, COUNT(*) FROM employees AS e GROUP BY e.department HAVING COUNT(*) > 1"},
		{"QualifiedColumn", "SELECT employees.name FROM employees"},
		{"AliasWithKeyword", "SELECT \"table\".name FROM employees AS \"table\""},
	}

	for _, tt := range aliasTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	joinAliasTests := []struct {
		name string
		sql  string
	}{
		{"JoinWithAliases", "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.department = d.name"},
		{"SelfJoin", "SELECT e1.name AS employee1, e2.name AS employee2 FROM employees AS e1, employees AS e2 WHERE e1.id < e2.id"},
		{"AliasInJoinCondition", "SELECT e1.name, e2.name FROM employees AS e1 JOIN employees AS e2 ON e1.department = e2.department AND e1.id < e2.id"},
		{"MultipleJoinsWithAliases", "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.department = d.name JOIN projects AS p ON d.id = p.dept_id"},
	}

	for _, tt := range joinAliasTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	columnAliasTests := []struct {
		name string
		sql  string
	}{
		{"ColumnAlias", "SELECT name AS employee_name FROM employees"},
		{"ColumnAliasWithoutAS", "SELECT name employee_name FROM employees"},
		{"MultipleColumnAliases", "SELECT id AS emp_id, name AS emp_name FROM employees"},
		{"AliasInOrderBy", "SELECT name AS n FROM employees ORDER BY n DESC"},
		{"AliasInWhere", "SELECT name AS n FROM employees WHERE n LIKE 'A%'"},
		{"ExpressionAlias", "SELECT UPPER(name) AS upper_name FROM employees"},
		{"FunctionAlias", "SELECT COUNT(*) AS total FROM employees"},
		{"AliasInGroupBy", "SELECT department AS dept, COUNT(*) AS count FROM employees GROUP BY dept"},
		{"AliasInHaving", "SELECT department AS dept, COUNT(*) AS count FROM employees GROUP BY dept HAVING count > 1"},
		{"QualifiedStar", "SELECT e.* FROM employees AS e"},
		{"StarWithAlias", "SELECT * FROM employees AS e"},
	}

	for _, tt := range columnAliasTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
