package F875

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F875_InsertOrReplace_L1 validates INSERT OR REPLACE behaviour.
func TestSQL1999_F875_InsertOrReplace_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`,
		`INSERT INTO users VALUES (1, 'Alice', 30)`,
		`INSERT INTO users VALUES (2, 'Bob', 25)`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup: %v", err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup: %v", err)
		}
	}

	muts := []string{
		// Replace existing row by PK
		`INSERT OR REPLACE INTO users VALUES (1, 'Alice Updated', 31)`,
		// Insert new row (no conflict)
		`INSERT OR REPLACE INTO users VALUES (3, 'Charlie', 22)`,
	}
	for _, q := range muts {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe mutation %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite mutation %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT id, name, age FROM users ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM users"},
		{"Row1Updated", "SELECT name FROM users WHERE id = 1"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F875_InsertOrIgnore_L1 validates INSERT OR IGNORE behaviour.
func TestSQL1999_F875_InsertOrIgnore_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)`,
		`INSERT INTO items VALUES (1, 'one')`,
		`INSERT INTO items VALUES (2, 'two')`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup: %v", err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup: %v", err)
		}
	}

	muts := []string{
		// Conflicting row — should be silently ignored
		`INSERT OR IGNORE INTO items VALUES (1, 'one-dup')`,
		// New row — should be inserted
		`INSERT OR IGNORE INTO items VALUES (3, 'three')`,
	}
	for _, q := range muts {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe mutation %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite mutation %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT id, val FROM items ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM items"},
		{"Row1Unchanged", "SELECT val FROM items WHERE id = 1"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F875_Upsert_L1 validates UPSERT (INSERT ... ON CONFLICT DO UPDATE).
func TestSQL1999_F875_Upsert_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setup := []string{
		`CREATE TABLE counters (key TEXT PRIMARY KEY, cnt INTEGER)`,
		`INSERT INTO counters VALUES ('a', 1)`,
		`INSERT INTO counters VALUES ('b', 5)`,
	}
	for _, q := range setup {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe setup: %v", err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite setup: %v", err)
		}
	}

	muts := []string{
		// Update existing row on conflict
		`INSERT INTO counters (key, cnt) VALUES ('a', 10) ON CONFLICT(key) DO UPDATE SET cnt = excluded.cnt`,
		// DO NOTHING on conflict
		`INSERT INTO counters (key, cnt) VALUES ('b', 99) ON CONFLICT(key) DO NOTHING`,
		// Insert new row (no conflict)
		`INSERT INTO counters (key, cnt) VALUES ('c', 3) ON CONFLICT(key) DO NOTHING`,
	}
	for _, q := range muts {
		if _, err := svDB.Exec(q); err != nil {
			t.Fatalf("sqlvibe mutation %q: %v", q, err)
		}
		if _, err := sqliteDB.Exec(q); err != nil {
			t.Fatalf("sqlite mutation %q: %v", q, err)
		}
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AllRows", "SELECT key, cnt FROM counters ORDER BY key"},
		{"Count", "SELECT COUNT(*) FROM counters"},
		{"KeyA", "SELECT cnt FROM counters WHERE key = 'a'"},
		{"KeyB", "SELECT cnt FROM counters WHERE key = 'b'"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F875_StringFunctions_L1 validates REPLACE, SUBSTR, TRIM.
func TestSQL1999_F875_StringFunctions_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	defer svDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	queries := []struct {
		name string
		sql  string
	}{
		{"ReplaceBasic", "SELECT REPLACE('hello world', 'world', 'there')"},
		{"ReplaceNull", "SELECT REPLACE(NULL, 'x', 'y')"},
		{"ReplaceEmpty", "SELECT REPLACE('aabbcc', 'bb', '')"},
		{"SubstrBasic", "SELECT SUBSTR('hello', 2, 3)"},
		{"SubstrNegStart", "SELECT SUBSTR('hello', -3)"},
		{"SubstrNoLen", "SELECT SUBSTR('hello', 2)"},
		{"SubstringAlias", "SELECT SUBSTRING('hello', 1, 3)"},
		{"TrimBoth", "SELECT TRIM('  hello  ')"},
		{"TrimLeft", "SELECT LTRIM('  hello')"},
		{"TrimRight", "SELECT RTRIM('hello  ')"},
		{"TrimChars", "SELECT TRIM('xxhelloxx', 'x')"},
		{"LTrimChars", "SELECT LTRIM('xxhello', 'x')"},
		{"RTrimChars", "SELECT RTRIM('helloxx', 'x')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
