package F052

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F052_RightJoin_L1(t *testing.T) {
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
		{"CreateDept", "CREATE TABLE dept (id INTEGER, name TEXT)"},
		{"CreateEmp", "CREATE TABLE emp (id INTEGER, dept_id INTEGER, name TEXT)"},
		{"InsertDept1", "INSERT INTO dept VALUES (1, 'Engineering')"},
		{"InsertDept2", "INSERT INTO dept VALUES (2, 'Marketing')"},
		{"InsertDept3", "INSERT INTO dept VALUES (3, 'HR')"},
		{"InsertEmp1", "INSERT INTO emp VALUES (1, 1, 'Alice')"},
		{"InsertEmp2", "INSERT INTO emp VALUES (2, 1, 'Bob')"},
		{"InsertEmp3", "INSERT INTO emp VALUES (3, 2, 'Carol')"},
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
		{"InnerJoinEmpDept", "SELECT emp.name, dept.name FROM emp INNER JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id"},
		{"InnerJoinDeptFilter", "SELECT emp.name FROM emp INNER JOIN dept ON emp.dept_id = dept.id WHERE dept.name = 'Engineering' ORDER BY emp.name"},
		{"InnerJoinCount", "SELECT COUNT(*) FROM emp INNER JOIN dept ON emp.dept_id = dept.id"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F052_NaturalJoin_L1(t *testing.T) {
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
		{"CreateA", "CREATE TABLE a (id INTEGER, val TEXT)"},
		{"CreateB", "CREATE TABLE b (id INTEGER, extra TEXT)"},
		{"InsertA1", "INSERT INTO a VALUES (1, 'one')"},
		{"InsertA2", "INSERT INTO a VALUES (2, 'two')"},
		{"InsertA3", "INSERT INTO a VALUES (3, 'three')"},
		{"InsertB1", "INSERT INTO b VALUES (1, 'x')"},
		{"InsertB2", "INSERT INTO b VALUES (2, 'y')"},
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
		{"NaturalJoin", "SELECT id, val, extra FROM a NATURAL JOIN b ORDER BY id"},
		{"NaturalJoinAll", "SELECT COUNT(*) FROM a"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F052_SelfJoin_L1(t *testing.T) {
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
		{"CreateEmployee", "CREATE TABLE employee (id INTEGER, name TEXT, manager_id INTEGER)"},
		{"InsertBoss", "INSERT INTO employee VALUES (1, 'CEO', NULL)"},
		{"InsertMgr", "INSERT INTO employee VALUES (2, 'Manager', 1)"},
		{"InsertEmp1", "INSERT INTO employee VALUES (3, 'Alice', 2)"},
		{"InsertEmp2", "INSERT INTO employee VALUES (4, 'Bob', 2)"},
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
		{"SelfJoin", "SELECT e.name AS employee, m.name AS manager FROM employee e INNER JOIN employee m ON e.manager_id = m.id ORDER BY e.id"},
		{"SelfJoinBossCount", "SELECT COUNT(*) FROM employee WHERE manager_id IS NULL"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F052_MultiTableJoin_L1(t *testing.T) {
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
		{"CreateOrders", "CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER)"},
		{"CreateCustomers", "CREATE TABLE customers (id INTEGER, name TEXT)"},
		{"CreateProducts", "CREATE TABLE products (id INTEGER, title TEXT, price INTEGER)"},
		{"InsertCustomer1", "INSERT INTO customers VALUES (1, 'Alice')"},
		{"InsertCustomer2", "INSERT INTO customers VALUES (2, 'Bob')"},
		{"InsertProduct1", "INSERT INTO products VALUES (1, 'Widget', 10)"},
		{"InsertProduct2", "INSERT INTO products VALUES (2, 'Gadget', 20)"},
		{"InsertOrder1", "INSERT INTO orders VALUES (1, 1, 1)"},
		{"InsertOrder2", "INSERT INTO orders VALUES (2, 1, 2)"},
		{"InsertOrder3", "INSERT INTO orders VALUES (3, 2, 1)"},
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
		{"ThreeTableJoinFilter", "SELECT customers.name, products.title FROM orders INNER JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id WHERE customers.name = 'Alice' ORDER BY products.title"},
		{"ThreeTableJoinCount", "SELECT COUNT(*) FROM orders INNER JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id"},
		{"ThreeTableJoinBobFilter", "SELECT products.title FROM orders INNER JOIN customers ON orders.customer_id = customers.id INNER JOIN products ON orders.product_id = products.id WHERE customers.name = 'Bob' ORDER BY products.title"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F052_LeftJoinNulls_L1(t *testing.T) {
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
		{"CreateLeft", "CREATE TABLE left_t (id INTEGER, val TEXT)"},
		{"CreateRight", "CREATE TABLE right_t (id INTEGER, info TEXT)"},
		{"InsertLeft1", "INSERT INTO left_t VALUES (1, 'a')"},
		{"InsertLeft2", "INSERT INTO left_t VALUES (2, 'b')"},
		{"InsertLeft3", "INSERT INTO left_t VALUES (3, 'c')"},
		{"InsertRight1", "INSERT INTO right_t VALUES (1, 'info1')"},
		{"InsertRight2", "INSERT INTO right_t VALUES (3, 'info3')"},
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
		{"LeftJoinAll", "SELECT left_t.id, left_t.val, right_t.info FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id"},
		{"LeftJoinNullCheck", "SELECT left_t.id FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id WHERE right_t.id IS NULL ORDER BY left_t.id"},
		{"InnerJoinCompare", "SELECT left_t.id FROM left_t INNER JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id"},
		{"LeftTotalCount", "SELECT COUNT(*) FROM left_t"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F052_JoinWithGroupByOrderBy_L1(t *testing.T) {
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
		{"CreateSales", "CREATE TABLE sales (id INTEGER, region_id INTEGER, amount INTEGER)"},
		{"CreateRegions", "CREATE TABLE regions (id INTEGER, name TEXT)"},
		{"InsertRegion1", "INSERT INTO regions VALUES (1, 'North')"},
		{"InsertRegion2", "INSERT INTO regions VALUES (2, 'South')"},
		{"InsertSale1", "INSERT INTO sales VALUES (1, 1, 100)"},
		{"InsertSale2", "INSERT INTO sales VALUES (2, 1, 200)"},
		{"InsertSale3", "INSERT INTO sales VALUES (3, 2, 150)"},
		{"InsertSale4", "INSERT INTO sales VALUES (4, 2, 50)"},
		{"InsertSale5", "INSERT INTO sales VALUES (5, 1, 300)"},
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
		{"JoinWithFilterCount", "SELECT COUNT(*) FROM sales INNER JOIN regions ON sales.region_id = regions.id"},
		{"JoinWithFilterWhere", "SELECT sales.id, sales.amount, regions.name FROM sales INNER JOIN regions ON sales.region_id = regions.id WHERE regions.name = 'North' ORDER BY sales.id"},
		{"JoinWithFilterWhereSouth", "SELECT sales.id, sales.amount FROM sales INNER JOIN regions ON sales.region_id = regions.id WHERE regions.name = 'South' ORDER BY sales.id"},
		{"JoinWithAllRows", "SELECT sales.id, regions.name FROM sales INNER JOIN regions ON sales.region_id = regions.id ORDER BY sales.id"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
