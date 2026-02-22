package E021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02102_L1(t *testing.T) {
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
		{"VARCHAR", "CREATE TABLE t1 (a VARCHAR(255))"},
		{"VARCHAR_LARGE", "CREATE TABLE t2 (a VARCHAR(1000))"},
		{"VARCHAR_SMALL", "CREATE TABLE t3 (a VARCHAR(10))"},
		{"VARCHAR_NO_SIZE", "CREATE TABLE t4 (a TEXT)"},
		{"VARCHAR_50", "CREATE TABLE t5 (a VARCHAR(50))"},
		{"VARCHAR_1000", "CREATE TABLE t6 (a VARCHAR(1000))"},
		{"AllVarcharTypes", "CREATE TABLE t7 (a VARCHAR(50), b VARCHAR(100), c TEXT)"},
		{"VarcharWithPK", "CREATE TABLE t8 (id INTEGER PRIMARY KEY, name VARCHAR(255))"},
		{"VarcharMultiple", "CREATE TABLE t9 (a VARCHAR(50), b VARCHAR(100), c VARCHAR(10))"},
		{"VarcharWithNotNull", "CREATE TABLE t10 (a VARCHAR(100) NOT NULL)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE varchars (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE varchars (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Short", "INSERT INTO varchars VALUES (1, 'hello')"},
		{"Long", "INSERT INTO varchars VALUES (2, 'this is a much longer string that can hold more data')"},
		{"Empty", "INSERT INTO varchars VALUES (3, '')"},
		{"Unicode", "INSERT INTO varchars VALUES (4, 'hello world')"},
		{"Special", "INSERT INTO varchars VALUES (5, 'a b c d e f g h i j k l m n o p q r s t u v w x y z')"},
		{"Numbers", "INSERT INTO varchars VALUES (6, '1234567890')"},
		{"Mixed", "INSERT INTO varchars VALUES (7, 'Test123!@#$%')"},
		{"Chinese", "INSERT INTO varchars VALUES (8, '你好世界')"},
		{"Japanese", "INSERT INTO varchars VALUES (9, 'こんにちは')"},
		{"Emoji", "INSERT INTO varchars VALUES (10, 'hello😀world')"},
		{"VeryLong", "INSERT INTO varchars VALUES (11, 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua')"},
		{"Null", "INSERT INTO varchars VALUES (12, NULL)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM varchars ORDER BY id", "VerifyVarchars")

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectShort", "SELECT val FROM varchars WHERE id = 1"},
		{"SelectLong", "SELECT val FROM varchars WHERE id = 2"},
		{"SelectEmpty", "SELECT val FROM varchars WHERE id = 3"},
		{"SelectUnicode", "SELECT val FROM varchars WHERE id = 4"},
		{"SelectNull", "SELECT val FROM varchars WHERE id = 12"},
		{"VarcharLength", "SELECT LENGTH(val) FROM varchars WHERE id = 1"},
		{"VarcharUpper", "SELECT UPPER(val) FROM varchars WHERE id = 1"},
		{"VarcharLower", "SELECT LOWER(val) FROM varchars WHERE id = 4"},
		{"VarcharConcat", "SELECT val || '!' FROM varchars WHERE id = 1"},
		{"VarcharSubstr", "SELECT SUBSTR(val, 1, 5) FROM varchars WHERE id = 1"},
		{"VarcharTrim", "SELECT TRIM(val) FROM varchars WHERE id = 6"},
		{"VarcharLike", "SELECT val LIKE '%hello%' FROM varchars WHERE id = 1"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
