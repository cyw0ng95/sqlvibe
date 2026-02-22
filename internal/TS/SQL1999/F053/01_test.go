package F053

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F053_InsertOnConflictDoNothing_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE kv (id INTEGER PRIMARY KEY, val TEXT)"},
		{"Insert1", "INSERT INTO kv VALUES (1, 'one')"},
		{"Insert2", "INSERT INTO kv VALUES (2, 'two')"},
		{"Insert3", "INSERT INTO kv VALUES (3, 'three')"},
		{"ConflictDoNothing1", "INSERT INTO kv VALUES (1, 'ONE') ON CONFLICT DO NOTHING"},
		{"ConflictDoNothing2", "INSERT INTO kv VALUES (4, 'four') ON CONFLICT DO NOTHING"},
		{"ConflictDoNothing3", "INSERT INTO kv VALUES (2, 'TWO') ON CONFLICT DO NOTHING"},
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
		{"SelectAll", "SELECT id, val FROM kv ORDER BY id"},
		{"Count", "SELECT COUNT(*) FROM kv"},
		{"OriginalUnchanged1", "SELECT val FROM kv WHERE id = 1"},
		{"OriginalUnchanged2", "SELECT val FROM kv WHERE id = 2"},
		{"NewRowInserted", "SELECT val FROM kv WHERE id = 4"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F053_InsertOnConflictDoUpdate_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)"},
		{"Insert1", "INSERT INTO config VALUES ('color', 'red')"},
		{"Insert2", "INSERT INTO config VALUES ('size', 'large')"},
		{"Upsert1", "INSERT INTO config VALUES ('color', 'blue') ON CONFLICT(key) DO UPDATE SET value = excluded.value"},
		{"Upsert2", "INSERT INTO config VALUES ('weight', 'heavy') ON CONFLICT(key) DO UPDATE SET value = excluded.value"},
		{"Upsert3", "INSERT INTO config VALUES ('size', 'small') ON CONFLICT(key) DO UPDATE SET value = excluded.value"},
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
		{"SelectAll", "SELECT key, value FROM config ORDER BY key"},
		{"SelectColor", "SELECT value FROM config WHERE key = 'color'"},
		{"SelectSize", "SELECT value FROM config WHERE key = 'size'"},
		{"Count", "SELECT COUNT(*) FROM config"},
		{"NewKeyInserted", "SELECT value FROM config WHERE key = 'weight'"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F053_UpsertMultipleConflicts_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE scores (player TEXT PRIMARY KEY, score INTEGER)"},
		{"Insert1", "INSERT INTO scores VALUES ('Alice', 100)"},
		{"Insert2", "INSERT INTO scores VALUES ('Bob', 200)"},
		{"Insert3", "INSERT INTO scores VALUES ('Carol', 150)"},
		{"UpsertIgnoreAlice", "INSERT INTO scores VALUES ('Alice', 999) ON CONFLICT DO NOTHING"},
		{"UpsertInsertDave", "INSERT INTO scores VALUES ('Dave', 50) ON CONFLICT DO NOTHING"},
		{"UpsertUpdateBob", "INSERT INTO scores VALUES ('Bob', 250) ON CONFLICT(player) DO UPDATE SET score = excluded.score"},
		{"UpsertInsertEve", "INSERT INTO scores VALUES ('Eve', 175) ON CONFLICT(player) DO UPDATE SET score = excluded.score"},
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
		{"SelectAll", "SELECT player, score FROM scores ORDER BY player"},
		{"SelectAlice", "SELECT score FROM scores WHERE player = 'Alice'"},
		{"SelectBob", "SELECT score FROM scores WHERE player = 'Bob'"},
		{"Count", "SELECT COUNT(*) FROM scores"},
		{"MaxScore", "SELECT MAX(score) FROM scores"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

