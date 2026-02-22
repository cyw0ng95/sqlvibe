package F291

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F291_F29101_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"InsertASCII", "INSERT INTO t1 VALUES (1, 'hello')"},
		{"InsertUnicode1", "INSERT INTO t1 VALUES (2, '你好')"},
		{"InsertUnicode2", "INSERT INTO t1 VALUES (3, 'こんにちは')"},
		{"InsertUnicode3", "INSERT INTO t1 VALUES (4, '🎉')"},
		{"InsertMixed", "INSERT INTO t1 VALUES (5, 'Hello世界')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unicodeTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1"},
		{"SelectASCII", "SELECT * FROM t1 WHERE name = 'hello'"},
		{"SelectUnicode", "SELECT * FROM t1 WHERE name = '你好'"},
		{"SelectMixed", "SELECT * FROM t1 WHERE name = 'Hello世界'"},
		{"LikeUnicode", "SELECT * FROM t1 WHERE name LIKE '%好%'"},
		{"OrderByUnicode", "SELECT * FROM t1 ORDER BY name"},
		{"LengthUnicode", "SELECT name, LENGTH(name) FROM t1"},
		{"UpperUnicode", "SELECT UPPER(name) FROM t1"},
		{"LowerUnicode", "SELECT LOWER(name) FROM t1"},
		{"ConcatUnicode", "SELECT name || ' ' || name FROM t1 WHERE id = 1"},
	}

	for _, tt := range unicodeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F291_F29102_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t2 (id INTEGER, text TEXT)"},
		{"Insert1", "INSERT INTO t2 VALUES (1, 'ABC')"},
		{"Insert2", "INSERT INTO t2 VALUES (2, 'abc')"},
		{"Insert3", "INSERT INTO t2 VALUES (3, 'ÅÄÖ')"},
		{"Insert4", "INSERT INTO t2 VALUES (4, 'åäö')"},
		{"Insert5", "INSERT INTO t2 VALUES (5, 'ÉÈÊ')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unicodeTests := []struct {
		name string
		sql  string
	}{
		{"UpperAscii", "SELECT UPPER(text) FROM t2 WHERE id = 1"},
		{"LowerAscii", "SELECT LOWER(text) FROM t2 WHERE id = 2"},
		{"LikeCaseSensitive", "SELECT * FROM t2 WHERE text LIKE 'A%'"},
		{"LikeCaseInsensitive", "SELECT * FROM t2 WHERE text LIKE 'a%'"},
		{"EqCaseSensitive", "SELECT * FROM t2 WHERE text = 'ABC'"},
		{"OrderByCollation", "SELECT * FROM t2 ORDER BY text"},
	}

	for _, tt := range unicodeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
