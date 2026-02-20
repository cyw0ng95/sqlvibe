package F021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F02105_L1(t *testing.T) {
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
		{"TableWithFK", "CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))"},
		{"TableWithMultipleFK", "CREATE TABLE t1 (id INTEGER PRIMARY KEY); CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER REFERENCES t1(id))"},
		{"TableWithOnDelete", "CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)"},
		{"TableWithOnUpdate", "CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON UPDATE CASCADE)"},
		{"TableWithCompositePK", "CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b)); CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES parent(a, b))"},
		{"TableMultipleFKs", "CREATE TABLE t1 (id INTEGER PRIMARY KEY); CREATE TABLE t2 (id INTEGER PRIMARY KEY); CREATE TABLE t3 (id INTEGER, t1_id INTEGER REFERENCES t1(id), t2_id INTEGER REFERENCES t2(id))"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"QueryAllConstraints", "SELECT * FROM information_schema.referential_constraints"},
		{"QueryByConstraintName", "SELECT * FROM information_schema.referential_constraints WHERE constraint_name = 'child_ibfk_1'"},
		{"QueryBySchema", "SELECT * FROM information_schema.referential_constraints WHERE constraint_schema = 'main'"},
		{"QueryFKColumns", "SELECT * FROM information_schema.key_column_usage WHERE constraint_name LIKE '%fk%'"},
		{"QueryMultipleFKs", "SELECT * FROM information_schema.referential_constraints ORDER BY constraint_name"},
		{"QueryWithJoin", "SELECT rc.*, kcu.column_name FROM information_schema.referential_constraints rc JOIN information_schema.key_column_usage kcu ON rc.constraint_name = kcu.constraint_name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Verify query succeeds for information_schema
		})
	}
}
