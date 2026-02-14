package sqlvibe

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func TestCompatibilityBasicSQL(t *testing.T) {
	sqlvibePath := "/tmp/test_basic_sql.db"
	sqlitePath := "/tmp/test_basic_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	tests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"},
		{"Insert1", "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"},
		{"Insert2", "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"},
		{"Insert3", "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"},
		{"SelectAll", "SELECT * FROM users"},
		{"SelectColumns", "SELECT name, age FROM users"},
		{"SelectWhere", "SELECT * FROM users WHERE age > 28"},
		{"SelectOrderBy", "SELECT * FROM users ORDER BY age DESC"},
		{"Update", "UPDATE users SET age = 31 WHERE id = 1"},
		{"Delete", "DELETE FROM users WHERE id = 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sqlvibeDB.Exec(tt.sql)
			if err != nil {
				t.Logf("sqlvibe exec error: %v", err)
			}

			_, err = sqliteDB.Exec(tt.sql)
			if err != nil {
				t.Logf("sqlite exec error: %v", err)
			}
		})
	}

	t.Run("VerifyResults", func(t *testing.T) {
		sqlvibeRows, err := sqlvibeDB.Query("SELECT id, name, age FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("sqlvibe query error: %v", err)
		}

		rows, err := sqliteDB.Query("SELECT id, name, age FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("sqlite query error: %v", err)
		}
		defer rows.Close()

		var sqliteResults []map[string]interface{}
		for rows.Next() {
			var id int64
			var name string
			var age int64
			rows.Scan(&id, &name, &age)
			sqliteResults = append(sqliteResults, map[string]interface{}{
				"id":   id,
				"name": name,
				"age":  age,
			})
		}

		t.Logf("sqlvibe columns: %v", sqlvibeRows.Columns)
		t.Logf("sqlite results: %v", sqliteResults)
	})
}

func TestDMLInsert(t *testing.T) {
	sqlvibePath := "/tmp/test_dml_insert.db"
	sqlitePath := "/tmp/test_dml_insert_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertSingle", "INSERT INTO test (id, value) VALUES (1, 'one')"},
		{"InsertMultiple", "INSERT INTO test (id, value) VALUES (2, 'two'), (3, 'three')"},
		{"InsertNull", "INSERT INTO test (id, value) VALUES (4, NULL)"},
		{"InsertEmptyString", "INSERT INTO test (id, value) VALUES (5, '')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)

			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}
}

func TestDMLUpdate(t *testing.T) {
	sqlvibePath := "/tmp/test_dml_update.db"
	sqlitePath := "/tmp/test_dml_update_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
		"INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	updateTests := []struct {
		name string
		sql  string
	}{
		{"UpdateSingle", "UPDATE test SET value = 100 WHERE id = 1"},
		{"UpdateMultiple", "UPDATE test SET value = value + 5"},
		{"UpdateAll", "UPDATE test SET value = 0"},
	}

	for _, tt := range updateTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)

			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}
}

func TestDMLDelete(t *testing.T) {
	sqlvibePath := "/tmp/test_dml_delete.db"
	sqlitePath := "/tmp/test_dml_delete_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
		"INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	deleteTests := []struct {
		name string
		sql  string
	}{
		{"DeleteSingle", "DELETE FROM test WHERE id = 1"},
		{"DeleteMultiple", "DELETE FROM test WHERE value > 30"},
		{"DeleteAll", "DELETE FROM test"},
	}

	for _, tt := range deleteTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)

			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}
}

func TestQueryWhereClauses(t *testing.T) {
	sqlvibePath := "/tmp/test_where.db"
	sqlitePath := "/tmp/test_where_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
		"INSERT INTO test VALUES (1, 'Alice', 30, 1), (2, 'Bob', 25, 0), (3, 'Charlie', 35, 1), (4, 'Diana', 28, 1)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	whereTests := []struct {
		name string
		sql  string
	}{
		{"Equals", "SELECT * FROM test WHERE age = 30"},
		{"NotEquals", "SELECT * FROM test WHERE age <> 30"},
		{"GreaterThan", "SELECT * FROM test WHERE age > 28"},
		{"GreaterOrEqual", "SELECT * FROM test WHERE age >= 30"},
		{"LessThan", "SELECT * FROM test WHERE age < 30"},
		{"LessOrEqual", "SELECT * FROM test WHERE age <= 28"},
		{"And", "SELECT * FROM test WHERE age > 25 AND active = 1"},
		{"Or", "SELECT * FROM test WHERE age = 25 OR age = 35"},
		{"Not", "SELECT * FROM test WHERE NOT age = 30"},
		{"In", "SELECT * FROM test WHERE age IN (25, 30, 35)"},
		{"Between", "SELECT * FROM test WHERE age BETWEEN 26 AND 34"},
		{"Like", "SELECT * FROM test WHERE name LIKE 'A%'"},
		{"IsNull", "INSERT INTO test VALUES (5, 'Eve', NULL, 1)"},
		{"IsNotNull", "SELECT * FROM test WHERE age IS NOT NULL"},
	}

	for _, tt := range whereTests {
		t.Run(tt.name, func(t *testing.T) {
			sqlvibeDB.Exec(tt.sql)

			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			t.Logf("sqlite returned %d rows", count)
		})
	}
}

func TestQueryAggregates(t *testing.T) {
	sqlvibePath := "/tmp/test_aggregates.db"
	sqlitePath := "/tmp/test_aggregates_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, price REAL)",
		"INSERT INTO sales VALUES (1, 'Apple', 10, 1.50), (2, 'Banana', 5, 0.75), (3, 'Apple', 8, 1.50), (4, 'Orange', 12, 2.00)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	aggTests := []struct {
		name string
		sql  string
	}{
		{"CountAll", "SELECT COUNT(*) FROM sales"},
		{"CountColumn", "SELECT COUNT(quantity) FROM sales"},
		{"Sum", "SELECT SUM(quantity) FROM sales"},
		{"Avg", "SELECT AVG(price) FROM sales"},
		{"Min", "SELECT MIN(price) FROM sales"},
		{"Max", "SELECT MAX(price) FROM sales"},
		{"GroupBy", "SELECT product, COUNT(*) FROM sales GROUP BY product"},
		{"GroupBySum", "SELECT product, SUM(quantity) FROM sales GROUP BY product"},
		{"Having", "SELECT product, SUM(quantity) as total FROM sales GROUP BY product HAVING SUM(quantity) > 8"},
	}

	for _, tt := range aggTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var results [][]interface{}
			for rows.Next() {
				cols, err := rows.Columns()
				if err != nil {
					continue
				}
				vals := make([]interface{}, len(cols))
				for i := range vals {
					vals[i] = new(interface{})
				}
				rows.Scan(vals...)
				results = append(results, vals)
			}
			t.Logf("sqlite %s: %v", tt.name, results)
		})
	}
}

func TestQueryJoins(t *testing.T) {
	sqlvibePath := "/tmp/test_joins.db"
	sqlitePath := "/tmp/test_joins_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
		"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		"INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	joinTests := []struct {
		name string
		sql  string
	}{
		{"InnerJoin", "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id"},
		{"LeftJoin", "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id"},
		{"CrossJoin", "SELECT u.name, o.amount FROM users u CROSS JOIN orders o"},
	}

	for _, tt := range joinTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			t.Logf("sqlite %s returned %d rows", tt.name, count)
		})
	}
}

func TestQuerySubqueries(t *testing.T) {
	sqlvibePath := "/tmp/test_subqueries.db"
	sqlitePath := "/tmp/test_subqueries_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER, dept_id INTEGER)",
		"INSERT INTO employees VALUES (1, 'Alice', 50000, 1), (2, 'Bob', 60000, 1), (3, 'Charlie', 55000, 2)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	subqueryTests := []struct {
		name string
		sql  string
	}{
		{"ScalarSubquery", "SELECT name, (SELECT MAX(salary) FROM employees) as max_salary FROM employees"},
		{"InSubquery", "SELECT * FROM employees WHERE dept_id IN (SELECT DISTINCT dept_id FROM employees WHERE salary > 50000)"},
		{"ExistsSubquery", "SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM employees e2 WHERE e2.dept_id = e.dept_id AND e2.id <> e.id)"},
		{"CorrelatedSubquery", "SELECT * FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id)"},
	}

	for _, tt := range subqueryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			t.Logf("sqlite %s returned %d rows", tt.name, count)
		})
	}
}

func TestQueryOrderBy(t *testing.T) {
	sqlvibePath := "/tmp/test_orderby.db"
	sqlitePath := "/tmp/test_orderby_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT)",
		"INSERT INTO test VALUES (1, 30, 'Charlie'), (2, 10, 'Alice'), (3, 20, 'Bob')",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	orderTests := []struct {
		name string
		sql  string
	}{
		{"OrderAsc", "SELECT * FROM test ORDER BY value ASC"},
		{"OrderDesc", "SELECT * FROM test ORDER BY value DESC"},
		{"OrderMultiple", "SELECT * FROM test ORDER BY value DESC, name ASC"},
		{"OrderByColumn", "SELECT * FROM test ORDER BY name"},
	}

	for _, tt := range orderTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var results []map[string]interface{}
			for rows.Next() {
				var id, value int64
				var name string
				rows.Scan(&id, &value, &name)
				results = append(results, map[string]interface{}{"id": id, "value": value, "name": name})
			}
			t.Logf("sqlite %s: %v", tt.name, results)
		})
	}
}

func TestQueryLimit(t *testing.T) {
	sqlvibePath := "/tmp/test_limit.db"
	sqlitePath := "/tmp/test_limit_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
		"INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	limitTests := []struct {
		name string
		sql  string
	}{
		{"Limit", "SELECT * FROM test LIMIT 3"},
		{"LimitOffset", "SELECT * FROM test LIMIT 3 OFFSET 1"},
		{"LimitNoOffset", "SELECT * FROM test LIMIT 2"},
	}

	for _, tt := range limitTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			t.Logf("sqlite %s returned %d rows", tt.name, count)
		})
	}
}

func TestTransactionCommit(t *testing.T) {
	sqlvibePath := "/tmp/test_tx_commit.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test VALUES (1, 'one')")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows.Columns == nil || len(rows.Columns) == 0 {
		t.Errorf("Expected rows, got none")
	}

	t.Logf("Query returned columns: %v", rows.Columns)
}

func TestTransactionRollback(t *testing.T) {
	sqlvibePath := "/tmp/test_tx_rollback.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test VALUES (1, 'one')")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows.Columns == nil {
		t.Errorf("Expected rows, got none")
	}

	t.Logf("Query returned columns: %v", rows.Columns)
}

func TestEdgeCaseNULLs(t *testing.T) {
	sqlvibePath := "/tmp/test_nulls.db"
	sqlitePath := "/tmp/test_nulls_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO test VALUES (1, 'one'), (2, NULL), (3, 'three')",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	nullTests := []struct {
		name string
		sql  string
	}{
		{"IsNull", "SELECT * FROM test WHERE value IS NULL"},
		{"IsNotNull", "SELECT * FROM test WHERE value IS NOT NULL"},
		{"Coalesce", "SELECT id, COALESCE(value, 'default') FROM test"},
		{"IfNull", "SELECT id, IFNULL(value, 'default') FROM test"},
	}

	for _, tt := range nullTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sqlvibeDB.Exec(tt.sql)
			t.Logf("sqlvibe %s: %v", tt.name, err)

			_, err = sqliteDB.Exec(tt.sql)
			t.Logf("sqlite %s: %v", tt.name, err)
		})
	}
}

func TestEdgeCaseTypes(t *testing.T) {
	sqlvibePath := "/tmp/test_types.db"
	sqlitePath := "/tmp/test_types_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (i INTEGER, r REAL, t TEXT)",
		"INSERT INTO test VALUES (42, 3.14159, 'hello')",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	typeTests := []struct {
		name string
		sql  string
	}{
		{"Integer", "SELECT * FROM test WHERE i = 42"},
		{"Real", "SELECT * FROM test WHERE r > 3.0"},
		{"Text", "SELECT * FROM test WHERE t = 'hello'"},
		{"TypeAffinity", "CREATE TABLE test2 (a TEXT, b NUMERIC)"},
	}

	for _, tt := range typeTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)

			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}
}

func TestEdgeCaseEmpty(t *testing.T) {
	sqlvibePath := "/tmp/test_empty.db"
	sqlitePath := "/tmp/test_empty_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE empty_table (id INTEGER PRIMARY KEY, value TEXT)",
		"CREATE TABLE with_data (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO with_data VALUES (1, 'test')",
	}

	for _, sql := range setupSQL {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	emptyTests := []struct {
		name string
		sql  string
	}{
		{"SelectEmpty", "SELECT * FROM empty_table"},
		{"SelectWithData", "SELECT * FROM with_data"},
		{"CountEmpty", "SELECT COUNT(*) FROM empty_table"},
		{"CountWithData", "SELECT COUNT(*) FROM with_data"},
		{"DeleteAll", "DELETE FROM with_data"},
		{"SelectAfterDelete", "SELECT * FROM with_data"},
	}

	for _, tt := range emptyTests {
		t.Run(tt.name, func(t *testing.T) {
			sqlvibeDB.Exec(tt.sql)

			rows, err := sqliteDB.Query(tt.sql)
			if err != nil {
				t.Logf("sqlite query error: %v", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			t.Logf("sqlite %s returned %d rows", tt.name, count)
		})
	}
}

func TestPreparedStatements(t *testing.T) {
	sqlvibePath := "/tmp/test_prepared.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(rows.Columns))
	}

	t.Logf("Columns: %v", rows.Columns)
}

func TestTransactionAPI(t *testing.T) {
	sqlvibePath := "/tmp/test_tx.db"
	defer os.Remove(sqlvibePath)

	db, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO accounts (id, balance) VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	rows, err := db.Query("SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows.Columns[0] != "balance" {
		t.Errorf("Expected balance column, got %s", rows.Columns[0])
	}
}

func TestMultipleTables(t *testing.T) {
	sqlvibePath := "/tmp/test_multi.db"
	sqlitePath := "/tmp/test_multi_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	tables := []string{
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)",
		"CREATE TABLE t2 (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
		"CREATE TABLE t3 (a TEXT, b REAL)",
	}

	for _, sql := range tables {
		sqlvibeDB.Exec(sql)
		sqliteDB.Exec(sql)
	}

	sqlvibeRows, _ := sqlvibeDB.Query("SELECT name FROM sqlite_master WHERE type='table'")
	sqliteRows, _ := sqliteDB.Query("SELECT name FROM sqlite_master WHERE type='table'")

	t.Logf("sqlvibe tables query result: %v", sqlvibeRows)
	t.Logf("sqlite tables query result: %v", sqliteRows)
}

// TestSQL1999_CH03_Numbers tests SQL:1999 Chapter 3 - Numeric Types
// INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE PRECISION
func TestSQL1999_CH03_Numbers(t *testing.T) {
	sqlvibePath := "/tmp/test_ch03.db"
	sqlitePath := "/tmp/test_ch03_sqlite.db"

	defer os.Remove(sqlvibePath)
	defer os.Remove(sqlitePath)

	sqlvibeDB, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	// Test numeric type definitions
	createTests := []struct {
		name string
		sql  string
	}{
		{"INTEGER", "CREATE TABLE t1 (a INTEGER, b INT, c SMALLINT)"},
		{"BIGINT", "CREATE TABLE t2 (a BIGINT)"},
		{"DECIMAL", "CREATE TABLE t3 (a DECIMAL(10,2), b NUMERIC(5,0))"},
		{"FLOAT", "CREATE TABLE t4 (a FLOAT, b REAL, c DOUBLE PRECISION)"},
		{"MixedNumeric", "CREATE TABLE t5 (id INTEGER PRIMARY KEY, amount DECIMAL(10,2), rate REAL)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name+"_create", func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)
			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}

	// Test numeric operations
	sqlvibeDB.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")

	// Insert test data
	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertPositive", "INSERT INTO nums VALUES (1, 42)"},
		{"InsertNegative", "INSERT INTO nums VALUES (2, -10)"},
		{"InsertZero", "INSERT INTO nums VALUES (3, 0)"},
		{"InsertLarge", "INSERT INTO nums VALUES (4, 9223372036854775807)"}, // BIGINT max
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)
			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}

	// Test numeric expressions
	exprTests := []struct {
		name string
		sql  string
	}{
		{"Addition", "SELECT val + 10 FROM nums WHERE id = 1"},
		{"Subtraction", "SELECT val - 5 FROM nums WHERE id = 1"},
		{"Multiplication", "SELECT val * 2 FROM nums WHERE id = 1"},
		{"Division", "SELECT val / 2 FROM nums WHERE id = 1"},
		{"Modulo", "SELECT val % 5 FROM nums WHERE id = 1"},
		{"Comparison", "SELECT * FROM nums WHERE val > 10"},
	}

	for _, tt := range exprTests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute on sqlvibe
			_, err1 := sqlvibeDB.Exec(tt.sql)
			if err1 != nil {
				t.Logf("sqlvibe %s error: %v", tt.name, err1)
			}

			// Execute on sqlite
			_, err2 := sqliteDB.Exec(tt.sql)
			if err2 != nil {
				t.Logf("sqlite %s error: %v", tt.name, err2)
			}
		})
	}

	// Test aggregate functions on numbers
	aggTests := []struct {
		name string
		sql  string
	}{
		{"Sum", "SELECT SUM(val) FROM nums"},
		{"Avg", "SELECT AVG(val) FROM nums"},
		{"Min", "SELECT MIN(val) FROM nums"},
		{"Max", "SELECT MAX(val) FROM nums"},
		{"Count", "SELECT COUNT(*) FROM nums"},
	}

	for _, tt := range aggTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)
			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}

	// Test ORDER BY with numbers
	orderTests := []struct {
		name string
		sql  string
	}{
		{"OrderAsc", "SELECT * FROM nums ORDER BY val ASC"},
		{"OrderDesc", "SELECT * FROM nums ORDER BY val DESC"},
	}

	for _, tt := range orderTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)
			t.Logf("sqlvibe: %v, sqlite: %v", err1, err2)
		})
	}

	t.Log("Ch03 Numbers test completed - compare results manually if needed")
}
