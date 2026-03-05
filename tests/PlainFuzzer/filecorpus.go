package PlainFuzzer

import (
	"os"
	"path/filepath"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// generateSeedDatabases creates a set of seed database files in tmpDir and
// returns their paths.  Files are generated at runtime so no binary blobs need
// to be committed to the repository.
func generateSeedDatabases(tmpDir string) []string {
	var paths []string

	if p := generateEmptyDB(tmpDir); p != "" {
		paths = append(paths, p)
	}
	if p := generateSingleTableDB(tmpDir); p != "" {
		paths = append(paths, p)
	}
	if p := generateMultiTableDB(tmpDir); p != "" {
		paths = append(paths, p)
	}
	if p := generateIndexDB(tmpDir); p != "" {
		paths = append(paths, p)
	}
	if p := generateWALDB(tmpDir); p != "" {
		paths = append(paths, p)
	}
	return paths
}

// generateEmptyDB writes a minimal SQLVIBE header so the fuzzer has a valid
// starting point for header-corruption mutations.
func generateEmptyDB(tmpDir string) string {
	path := filepath.Join(tmpDir, "empty.db")
	// Minimal placeholder: just create the file via sqlvibe so the header is valid.
	db, err := sqlvibe.Open(path)
	if err != nil {
		return ""
	}
	db.Close()
	return path
}

// generateSingleTableDB creates a database with one table and a few rows.
func generateSingleTableDB(tmpDir string) string {
	path := filepath.Join(tmpDir, "single_table.db")
	db, err := sqlvibe.Open(path)
	if err != nil {
		return ""
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		return ""
	}
	if _, err := db.Exec("INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"); err != nil {
		return ""
	}
	return path
}

// generateMultiTableDB creates a database with several related tables.
func generateMultiTableDB(tmpDir string) string {
	path := filepath.Join(tmpDir, "multi_table.db")
	db, err := sqlvibe.Open(path)
	if err != nil {
		return ""
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		return ""
	}
	if _, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"); err != nil {
		return ""
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1,'alice',30),(2,'bob',25),(3,'carol',35)"); err != nil {
		return ""
	}
	if _, err := db.Exec("INSERT INTO orders VALUES (1,1,99.9),(2,1,14.5),(3,2,200.0)"); err != nil {
		return ""
	}
	return path
}

// generateIndexDB creates a database with secondary indexes.
func generateIndexDB(tmpDir string) string {
	path := filepath.Join(tmpDir, "with_index.db")
	db, err := sqlvibe.Open(path)
	if err != nil {
		return ""
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"); err != nil {
		return ""
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON products (name)"); err != nil {
		return ""
	}
	if _, err := db.Exec("INSERT INTO products VALUES (1,'widget',9.99),(2,'gadget',49.99),(3,'doohickey',1.99)"); err != nil {
		return ""
	}
	return path
}

// generateWALDB creates a database then enables WAL mode.
func generateWALDB(tmpDir string) string {
	path := filepath.Join(tmpDir, "with_wal.db")
	db, err := sqlvibe.Open(path)
	if err != nil {
		return ""
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)"); err != nil {
		return ""
	}
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		// WAL setup failed; still return the path so we exercise WAL absence.
		_ = os.Remove(path)
		return ""
	}
	if _, err := db.Exec("INSERT INTO log VALUES (1,'start'),(2,'end')"); err != nil {
		return ""
	}
	return path
}
