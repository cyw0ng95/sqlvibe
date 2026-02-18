package sqlvibe

import (
	"testing"
)

// TestInformationSchema_Columns tests the information_schema.columns view
func TestInformationSchema_Columns(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT NOT NULL, body TEXT)")
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	tests := []struct {
		name      string
		query     string
		expected  []map[string]interface{}
	}{
		{
			name:  "Query all columns",
			query: "SELECT column_name, table_name, is_nullable FROM information_schema.columns ORDER BY table_name, column_name",
			expected: []map[string]interface{}{
				{"column_name": "body", "table_name": "posts", "is_nullable": "YES"},
				{"column_name": "id", "table_name": "posts", "is_nullable": "NO"},
				{"column_name": "title", "table_name": "posts", "is_nullable": "NO"},
				{"column_name": "user_id", "table_name": "posts", "is_nullable": "YES"},
				{"column_name": "age", "table_name": "users", "is_nullable": "YES"},
				{"column_name": "email", "table_name": "users", "is_nullable": "YES"},
				{"column_name": "id", "table_name": "users", "is_nullable": "NO"},
				{"column_name": "name", "table_name": "users", "is_nullable": "NO"},
			},
		},
		{
			name:  "Filter by table name",
			query: "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users' ORDER BY column_name",
			expected: []map[string]interface{}{
				{"column_name": "age", "data_type": "INTEGER"},
				{"column_name": "email", "data_type": "TEXT"},
				{"column_name": "id", "data_type": "INTEGER"},
				{"column_name": "name", "data_type": "TEXT NOT NULL"},
			},
		},
		{
			name:  "Count columns per table",
			query: "SELECT table_name FROM information_schema.columns WHERE table_name = 'posts'",
			expected: []map[string]interface{}{
				{"table_name": "posts"},
				{"table_name": "posts"},
				{"table_name": "posts"},
				{"table_name": "posts"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			var results []map[string]interface{}
			for rows.Next() {
				row := make(map[string]interface{})
				// Scan based on number of columns
				if len(rows.Columns) == 2 {
					var col1, col2 string
					if err := rows.Scan(&col1, &col2); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					row[rows.Columns[0]] = col1
					row[rows.Columns[1]] = col2
				} else if len(rows.Columns) == 3 {
					var col1, col2, col3 string
					if err := rows.Scan(&col1, &col2, &col3); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					row[rows.Columns[0]] = col1
					row[rows.Columns[1]] = col2
					row[rows.Columns[2]] = col3
				} else if len(rows.Columns) == 1 {
					var col1 string
					if err := rows.Scan(&col1); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					row[rows.Columns[0]] = col1
				}
				results = append(results, row)
			}

			if len(results) != len(tt.expected) {
				t.Errorf("Expected %d rows, got %d", len(tt.expected), len(results))
				return
			}

			for i, expected := range tt.expected {
				for key, expectedVal := range expected {
					if results[i][key] != expectedVal {
						t.Errorf("Row %d, column %s: expected %v, got %v", i, key, expectedVal, results[i][key])
					}
				}
			}
		})
	}
}

// TestInformationSchema_Tables tests the information_schema.tables view
func TestInformationSchema_Tables(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables
	_, err = db.Exec("CREATE TABLE t1 (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec("CREATE TABLE t2 (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	rows, err := db.Query("SELECT table_name, table_schema, table_type FROM information_schema.tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	var tables []string
	for rows.Next() {
		var tableName, tableSchema, tableType string
		if err := rows.Scan(&tableName, &tableSchema, &tableType); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		tables = append(tables, tableName)
		
		if tableSchema != "main" {
			t.Errorf("Expected table_schema='main', got '%s'", tableSchema)
		}
		if tableType != "BASE TABLE" {
			t.Errorf("Expected table_type='BASE TABLE', got '%s'", tableType)
		}
	}

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
	if len(tables) >= 1 && tables[0] != "t1" {
		t.Errorf("Expected first table 't1', got '%s'", tables[0])
	}
	if len(tables) >= 2 && tables[1] != "t2" {
		t.Errorf("Expected second table 't2', got '%s'", tables[1])
	}
}

// TestInformationSchema_TableConstraints tests the information_schema.table_constraints view
func TestInformationSchema_TableConstraints(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables with column-level PRIMARY KEY
	_, err = db.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	rows, err := db.Query("SELECT table_name, constraint_type FROM information_schema.table_constraints ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		var tableName, constraintType string
		if err := rows.Scan(&tableName, &constraintType); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		
		if constraintType != "PRIMARY KEY" {
			t.Errorf("Expected constraint_type='PRIMARY KEY', got '%s'", constraintType)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 constraints, got %d", count)
	}
}

// TestInformationSchema_EmptyDatabase tests information_schema with empty database
func TestInformationSchema_EmptyDatabase(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Query columns from empty database
	rows, err := db.Query("SELECT * FROM information_schema.columns")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 columns in empty database, got %d", count)
	}

	// Query tables from empty database
	rows, err = db.Query("SELECT * FROM information_schema.tables")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	count = 0
	for rows.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 tables in empty database, got %d", count)
	}
}

// TestInformationSchema_ComplexConstraints tests various constraint combinations
func TestInformationSchema_ComplexConstraints(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with multiple constraint types
	_, err = db.Exec(`CREATE TABLE complex_table (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		age INTEGER,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'complex_table' ORDER BY column_name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	expected := map[string]string{
		"age":    "YES",
		"email":  "NO",
		"id":     "NO",
		"status": "YES",
	}

	results := make(map[string]string)
	for rows.Next() {
		var colName, isNullable string
		if err := rows.Scan(&colName, &isNullable); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results[colName] = isNullable
	}

	if len(results) != len(expected) {
		t.Errorf("Expected %d columns, got %d", len(expected), len(results))
	}

	for col, expectedNullable := range expected {
		if results[col] != expectedNullable {
			t.Errorf("Column %s: expected is_nullable='%s', got '%s'", col, expectedNullable, results[col])
		}
	}
}
