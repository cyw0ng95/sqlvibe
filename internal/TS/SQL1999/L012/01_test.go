package L012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_L012_L01201_L1(t *testing.T) {
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"CreateQuotedIdentifiers", `CREATE TABLE "MyTable" (id INTEGER PRIMARY KEY, "MyColumn" TEXT)`},
		{"CreateWithUnderscores", "CREATE TABLE my_table_1 (col_a INTEGER, col_b TEXT, col_c REAL)"},
		{"CreateWithNumbers", "CREATE TABLE t123 (col1 INTEGER, col2 TEXT)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec(`INSERT INTO "MyTable" VALUES (1, 'hello')`)
	sqliteDB.Exec(`INSERT INTO "MyTable" VALUES (1, 'hello')`)
	sqlvibeDB.Exec(`INSERT INTO "MyTable" VALUES (2, 'world')`)
	sqliteDB.Exec(`INSERT INTO "MyTable" VALUES (2, 'world')`)

	sqlvibeDB.Exec("INSERT INTO my_table_1 VALUES (10, 'alpha', 1.1)")
	sqliteDB.Exec("INSERT INTO my_table_1 VALUES (10, 'alpha', 1.1)")
	sqlvibeDB.Exec("INSERT INTO my_table_1 VALUES (20, 'beta', 2.2)")
	sqliteDB.Exec("INSERT INTO my_table_1 VALUES (20, 'beta', 2.2)")

	sqlvibeDB.Exec("INSERT INTO t123 VALUES (100, 'x')")
	sqliteDB.Exec("INSERT INTO t123 VALUES (100, 'x')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectQuotedTable", `SELECT * FROM "MyTable" ORDER BY id`},
		{"SelectAllFromQuotedTable", `SELECT * FROM "MyTable" ORDER BY id`},
		{"SelectUnderscoreTable", "SELECT * FROM my_table_1 ORDER BY col_a"},
		{"SelectNumberedTable", "SELECT * FROM t123"},
		// Case-insensitive table reference
		{"SelectCaseInsensitive", "SELECT * FROM MY_TABLE_1 ORDER BY col_a"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
