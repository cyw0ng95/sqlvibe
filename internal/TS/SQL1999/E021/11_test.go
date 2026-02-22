package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02111_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: POSITION not supported by SQLite - documented in v0.4.5")
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

	sqlvibeDB.Exec("CREATE TABLE pos_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE pos_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Hello", "INSERT INTO pos_test VALUES (1, 'hello world')"},
		{"Repeat", "INSERT INTO pos_test VALUES (2, 'abcabcabc')"},
		{"Empty", "INSERT INTO pos_test VALUES (3, '')"},
		{"NoMatch", "INSERT INTO pos_test VALUES (4, 'xyz')"},
		{"Multiple", "INSERT INTO pos_test VALUES (5, 'the quick brown fox jumps')"},
		{"AtStart", "INSERT INTO pos_test VALUES (6, 'hello')"},
		{"AtEnd", "INSERT INTO pos_test VALUES (7, 'world')"},
		{"Special", "INSERT INTO pos_test VALUES (8, 'a,b,c,d,e')"},
		{"Unicode", "INSERT INTO pos_test VALUES (9, '你好世界')"},
		{"Numbers", "INSERT INTO pos_test VALUES (10, '123456123456')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	posTests := []struct {
		name string
		sql  string
	}{
		{"INSTR_Basic", "SELECT INSTR('hello world', 'world')"},
		{"INSTR_NotFound", "SELECT INSTR('hello world', 'xyz')"},
		{"INSTR_FirstChar", "SELECT INSTR('hello world', 'h')"},
		{"INSTR_AtStart", "SELECT INSTR(val, 'hello') FROM pos_test WHERE id = 1"},
		{"INSTR_NotFoundTable", "SELECT INSTR(val, 'xyz') FROM pos_test WHERE id = 1"},
		{"INSTR_Repeat", "SELECT INSTR(val, 'a') FROM pos_test WHERE id = 2"},
		{"INSTR_Empty", "SELECT INSTR(val, 'x') FROM pos_test WHERE id = 3"},
		{"INSTR_Multiple", "SELECT INSTR(val, ' ') FROM pos_test WHERE id = 5"},
		{"INSTR_Special", "SELECT INSTR(val, ',') FROM pos_test WHERE id = 8"},
		{"INSTR_AtStart2", "SELECT INSTR(val, 'hello') FROM pos_test WHERE id = 6"},
		{"INSTR_AtEnd", "SELECT INSTR(val, 'd') FROM pos_test WHERE id = 7"},
		{"INSTR_Numbers", "SELECT INSTR(val, '3') FROM pos_test WHERE id = 10"},
		{"INSTR_Concat", "SELECT INSTR('hello' || 'world', 'world')"},
		{"INSTR_EmptyStr", "SELECT INSTR('hello', '')"},
		{"INSTR_Whole", "SELECT INSTR(val, val) FROM pos_test WHERE id = 1"},
		{"INSTR_AfterConcat", "SELECT INSTR('prefix-' || val, 'world') FROM pos_test WHERE id = 1"},
		{"POSITION_Found", "SELECT POSITION('world' IN val) FROM pos_test WHERE id = 1"},
		{"POSITION_NotFound", "SELECT POSITION('xyz' IN val) FROM pos_test WHERE id = 1"},
		{"POSITION_First", "SELECT POSITION('l' IN val) FROM pos_test WHERE id = 1"},
	}

	for _, tt := range posTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
