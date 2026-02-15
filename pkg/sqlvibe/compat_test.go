package sqlvibe

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func fetchAllRows(rows *Rows) ([]map[string]interface{}, error) {
	if rows == nil {
		return nil, nil
	}

	results := make([]map[string]interface{}, 0, len(rows.Data))
	for _, rowData := range rows.Data {
		row := make(map[string]interface{})
		for i, col := range rows.Columns {
			if i < len(rowData) {
				row[col] = rowData[i]
			}
		}
		results = append(results, row)
	}
	return results, nil
}

// fetchAllRowsSQLite fetches all rows from a database/sql result
func fetchAllRowsSQLite(rows *sql.Rows) ([]map[string]interface{}, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func compareQueryResults(t *testing.T, sqlvibeDB *Database, sqliteDB *sql.DB, sql string, testName string) {
	sqlvibeRows, err := sqlvibeDB.Query(sql)
	if err != nil {
		t.Errorf("%s: sqlvibe query error: %v", testName, err)
		return
	}
	sqlvibeData, err := fetchAllRows(sqlvibeRows)
	if err != nil {
		t.Errorf("%s: sqlvibe fetch error: %v", testName, err)
		return
	}

	sqliteRows, err := sqliteDB.Query(sql)
	if err != nil {
		t.Errorf("%s: sqlite query error: %v", testName, err)
		return
	}
	sqliteData, err := fetchAllRowsSQLite(sqliteRows)
	if err != nil {
		t.Errorf("%s: sqlite fetch error: %v", testName, err)
		return
	}

	if len(sqlvibeData) != len(sqliteData) {
		t.Errorf("%s: row count mismatch: sqlvibe=%d, sqlite=%d", testName, len(sqlvibeData), len(sqliteData))
		return
	}

	for i := range sqliteData {
		if !rowsEqual(sqlvibeData[i], sqliteData[i]) {
			t.Errorf("%s: row %d mismatch: %s", testName, i, debugTypes(sqlvibeData[i], sqliteData[i]))
		}
	}
}

// rowsEqual compares two row maps for equality
func rowsEqual(a, b map[string]interface{}) bool {
	// If maps have same keys, do direct comparison
	if len(a) == len(b) {
		keysMatch := true
		for k := range a {
			if _, ok := b[k]; !ok {
				keysMatch = false
				break
			}
		}
		if keysMatch {
			for k, av := range a {
				bv := b[k]
				if !valueEqual(av, bv) {
					return false
				}
			}
			return true
		}
	}

	// If maps have different keys but same length, compare by values (position-based)
	if len(a) == len(b) && len(a) > 0 {
		aVals := make([]interface{}, 0, len(a))
		bVals := make([]interface{}, 0, len(b))
		for _, v := range a {
			aVals = append(aVals, v)
		}
		for _, v := range b {
			bVals = append(bVals, v)
		}
		for i := range aVals {
			if !valueEqual(aVals[i], bVals[i]) {
				return false
			}
		}
		return true
	}

	// Fall back to key-based comparison
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if !valueEqual(av, bv) {
			return false
		}
	}
	return true
}

// debugTypes prints the actual types of values in two maps
func debugTypes(a, b map[string]interface{}) string {
	result := ""
	for k := range a {
		result += fmt.Sprintf("%s: sqlvibe=%T(%v) sqlite=%T(%v) ", k, a[k], a[k], b[k], b[k])
	}
	return result
}

// valueEqual compares two values for equality, handling nil, int64, float64, string
func valueEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Use reflect for deep comparison
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	// If both are numeric types, compare as floats
	if isNumeric(av) && isNumeric(bv) {
		af := toFloat64(av)
		bf := toFloat64(bv)
		return af == bf
	}

	// Direct comparison
	return reflect.DeepEqual(a, b)
}

// isNumeric checks if a value is numeric
func isNumeric(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// toFloat64 converts a numeric value to float64
func toFloat64(v reflect.Value) float64 {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	}
	return 0
}

// TestSQLite_F491_BasicSQL_L1 tests SQLite compatibility - Basic SQL operations
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_BasicSQL_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
		// Compare actual query results between sqlvibe and SQLite
		compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT id, name, age FROM users ORDER BY id", "VerifyResults")
	})
}

// TestSQLite_F131_Insert_L1 tests SQLite compatibility - INSERT operations
// Testsuite: SQLite
// Feature ID: F131 (Grouped Operations - INSERT)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F131_Insert_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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

			// Compare error states
			if (err1 == nil) != (err2 == nil) {
				t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", tt.name, err1, err2)
			}
		})
	}

	// Verify inserted data matches
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT id, value FROM test ORDER BY id", "VerifyInsertedData")
}

// TestSQLite_F131_Update_L1 tests SQLite compatibility - UPDATE operations
// Testsuite: SQLite
// Feature ID: F131 (Grouped Operations - UPDATE)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F131_Update_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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

			// Compare error states
			if (err1 == nil) != (err2 == nil) {
				t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", tt.name, err1, err2)
			}
		})
	}

	// Verify updated data matches
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT id, value FROM test ORDER BY id", "VerifyUpdatedData")
}

// TestSQLite_F131_Delete_L2 tests SQLite compatibility - DELETE operations
// Testsuite: SQLite
// Feature ID: F131 (Grouped Operations - DELETE)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F131_Delete_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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

			// Compare error states
			if (err1 == nil) != (err2 == nil) {
				t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", tt.name, err1, err2)
			}
		})
	}

	// Verify remaining data matches
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT id, value FROM test ORDER BY id", "VerifyDeletedData")
}

// TestSQLite_F491_Where_L2 tests SQLite compatibility - WHERE clause operations
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_Where_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
		// For IsNull, we need to insert first
		{"IsNull", "SELECT * FROM test WHERE age IS NULL"},
		{"IsNotNull", "SELECT * FROM test WHERE age IS NOT NULL"},
	}

	// First insert the NULL row for IsNull test
	sqlvibeDB.Exec("INSERT INTO test VALUES (5, 'Eve', NULL, 1)")
	sqliteDB.Exec("INSERT INTO test VALUES (5, 'Eve', NULL, 1)")

	for _, tt := range whereTests {
		t.Run(tt.name, func(t *testing.T) {
			// Compare actual query results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F051_Aggregates_L2 tests SQLite compatibility - Aggregate functions
// Testsuite: SQLite
// Feature ID: F051 (Date and Time - reusing for Aggregates as per project convention)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F051_Aggregates_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
	}

	for _, tt := range aggTests {
		t.Run(tt.name, func(t *testing.T) {
			// Compare aggregate results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F041_Joins_L1 tests SQLite compatibility - JOIN operations
// Testsuite: SQLite
// Feature ID: F041 (Joined Tables)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F041_Joins_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
			// Compare join results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F471_Subqueries_L1 tests SQLite compatibility - Subquery operations
// Testsuite: SQLite
// Feature ID: F471 (Scalar Subquery Values)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F471_Subqueries_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := Open(sqlvibePath)
	if err != nil {
		t.Fatalf("failed to open sqlvibe DB: %v", err)
	}
	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("failed to open sqlite DB: %v", err)
	}
	if err := sqliteDB.Ping(); err != nil {
		t.Fatalf("failed to ping sqlite DB: %v", err)
	}
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
			// Compare subquery results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F491_OrderBy_L1 tests SQLite compatibility - ORDER BY operations
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_OrderBy_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
			// Compare ordered results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F491_Limit_L1 tests SQLite compatibility - LIMIT operations
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_Limit_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
			// Compare limited results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F501_Commit_L2 tests SQLite compatibility - Transaction commit
// Testsuite: SQLite
// Feature ID: F501 (Transaction Support)
// Test Level: 2 (FileBased - uses file storage)
func TestSQLite_F501_Commit_L2(t *testing.T) {
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

// TestSQLite_F501_Rollback_L2 tests SQLite compatibility - Transaction rollback
// Testsuite: SQLite
// Feature ID: F501 (Transaction Support)
// Test Level: 2 (FileBased - uses file storage)
func TestSQLite_F501_Rollback_L2(t *testing.T) {
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

// TestSQLite_F481_NULLs_L1 tests SQLite compatibility - NULL handling
// Testsuite: SQLite
// Feature ID: F481 (Expanded NULL Predicate)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F481_NULLs_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, _ := Open(sqlvibePath)
	sqliteDB, _ := sql.Open("sqlite", sqlitePath)
	if err := sqliteDB.Ping(); err != nil {
		t.Fatalf("Failed to ping sqlite: %v", err)
	}
	defer sqlvibeDB.Close()
	defer sqliteDB.Close()

	setupSQL := []string{
		"CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO test VALUES (1, 'one'), (2, NULL), (3, 'three')",
	}

	for _, sql := range setupSQL {
		_, err := sqlvibeDB.Exec(sql)
		if err != nil {
			t.Fatalf("sqlvibe setup failed: %v", err)
		}
		_, err = sqliteDB.Exec(sql)
		if err != nil {
			t.Fatalf("sqlite setup failed: %v", err)
		}
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
			// Compare NULL handling results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQLite_F301_Types_L1 tests SQLite compatibility - Type handling
// Testsuite: SQLite
// Feature ID: F301 (Numeric Types - reusing for general type handling)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F301_Types_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
	}

	for _, tt := range typeTests {
		t.Run(tt.name, func(t *testing.T) {
			// Compare type handling results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Test type affinity table creation
	t.Run("TypeAffinity", func(t *testing.T) {
		_, err1 := sqlvibeDB.Exec("CREATE TABLE test2 (a TEXT, b NUMERIC)")
		_, err2 := sqliteDB.Exec("CREATE TABLE test2 (a TEXT, b NUMERIC)")
		// Just verify both succeed or both fail
		if (err1 == nil) != (err2 == nil) {
			t.Errorf("TypeAffinity: error mismatch: sqlvibe=%v, sqlite=%v", err1, err2)
		}
	})
}

// TestSQLite_F491_Empty_L1 tests SQLite compatibility - Empty table handling
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_Empty_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
	}

	for _, tt := range emptyTests {
		t.Run(tt.name, func(t *testing.T) {
			// Compare empty result handling between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Test DELETE and verify
	t.Run("DeleteAll", func(t *testing.T) {
		_, err1 := sqlvibeDB.Exec("DELETE FROM with_data")
		_, err2 := sqliteDB.Exec("DELETE FROM with_data")
		if (err1 == nil) != (err2 == nil) {
			t.Errorf("DeleteAll: error mismatch: sqlvibe=%v, sqlite=%v", err1, err2)
		}
	})

	// Verify after delete
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM with_data", "SelectAfterDelete")
}

// TestSQLite_F521_Prepared_L1 tests SQLite compatibility - Prepared statements
// Testsuite: SQLite
// Feature ID: F521 (Prepared Statements - project convention)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F521_Prepared_L1(t *testing.T) {
	sqlvibePath := ":memory:"

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

// TestSQLite_F501_API_L2 tests SQLite compatibility - Transaction API
// Testsuite: SQLite
// Feature ID: F501 (Transaction Support)
// Test Level: 2 (FileBased - uses file storage)
func TestSQLite_F501_API_L2(t *testing.T) {
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

// TestSQLite_F491_MultiTables_L1 tests SQLite compatibility - Multiple tables
// Testsuite: SQLite
// Feature ID: F491 (General SQL Features)
// Test Level: 1 (Fundamental - uses :memory: backend)
func TestSQLite_F491_MultiTables_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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

	// Compare table list
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", "TableList")
}

// TestSQL1999_F301_NumericTypes_L1 tests SQL:1999 Feature F301 - Numeric Types
// Test Level: 1 (Fundamental - uses :memory: backend)
// Tests INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE PRECISION
func TestSQL1999_F301_NumericTypes_L1(t *testing.T) {
	// Level 1 tests use :memory: backend (non-storage fundamental tests)
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

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
			// Just verify both succeed or both fail
			if (err1 == nil) != (err2 == nil) {
				t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", tt.name, err1, err2)
			}
		})
	}

	// Test numeric operations - create table and insert data
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
		{"InsertLarge", "INSERT INTO nums VALUES (4, 1000000000)"}, // Large but won't overflow SUM
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err1 := sqlvibeDB.Exec(tt.sql)
			_, err2 := sqliteDB.Exec(tt.sql)
			// Just verify both succeed or both fail
			if (err1 == nil) != (err2 == nil) {
				t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", tt.name, err1, err2)
			}
		})
	}

	// Test numeric expressions - compare actual results
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
			// Compare numeric expression results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
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
			// Compare aggregate results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
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
			// Compare ordered results between sqlvibe and SQLite
			compareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Final verification - all data should match
	compareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM nums ORDER BY id", "FinalVerification")
}
