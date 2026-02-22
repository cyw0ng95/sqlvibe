package sqlvibe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGetTables_Empty verifies GetTables returns empty slice for a fresh database.
func TestGetTables_Empty(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tables, err := db.GetTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}
}

// TestGetTables_Multiple verifies GetTables lists all created tables.
func TestGetTables_Multiple(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE posts (id INT)"); err != nil {
		t.Fatal(err)
	}

	tables, err := db.GetTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
	names := make([]string, len(tables))
	for i, ti := range tables {
		names[i] = ti.Name
	}
	if !contains(names, "users") || !contains(names, "posts") {
		t.Errorf("unexpected table names: %v", names)
	}
}

// TestGetTables_ExcludesSystem verifies GetTables excludes sqlite_* tables.
func TestGetTables_ExcludesSystem(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatal(err)
	}

	tables, err := db.GetTables()
	if err != nil {
		t.Fatal(err)
	}
	for _, ti := range tables {
		if strings.HasPrefix(ti.Name, "sqlite_") {
			t.Errorf("GetTables returned system table: %s", ti.Name)
		}
	}
}

// TestGetTables_Type verifies the Type field is "table".
func TestGetTables_Type(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatal(err)
	}

	tables, err := db.GetTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].Type != "table" {
		t.Errorf("expected type 'table', got %q", tables[0].Type)
	}
}

// TestGetSchema_Single verifies GetSchema returns a CREATE TABLE statement.
func TestGetSchema_Single(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	schema, err := db.GetSchema("users")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(schema, "CREATE TABLE users") {
		t.Errorf("schema missing CREATE TABLE: %q", schema)
	}
	if !strings.Contains(schema, "id") || !strings.Contains(schema, "name") {
		t.Errorf("schema missing columns: %q", schema)
	}
}

// TestGetSchema_NotFound verifies GetSchema returns an error for unknown tables.
func TestGetSchema_NotFound(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.GetSchema("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

// TestGetSchema_Empty verifies GetSchema on fresh DB returns error.
func TestGetSchema_Empty(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.GetSchema("anything")
	if err == nil {
		t.Error("expected error for table in empty database")
	}
}

// TestGetIndexes_Empty verifies GetIndexes returns empty for table with no indexes.
func TestGetIndexes_Empty(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatal(err)
	}

	indexes, err := db.GetIndexes("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 0 {
		t.Errorf("expected 0 indexes, got %d", len(indexes))
	}
}

// TestGetIndexes_WithIndex verifies GetIndexes returns created indexes.
func TestGetIndexes_WithIndex(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON t1 (name)"); err != nil {
		t.Fatal(err)
	}

	indexes, err := db.GetIndexes("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(indexes))
	}
	if indexes[0].Name != "idx_name" {
		t.Errorf("expected index name 'idx_name', got %q", indexes[0].Name)
	}
}

// TestGetColumns_Basic verifies GetColumns returns column metadata.
func TestGetColumns_Basic(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	cols, err := db.GetColumns("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}

	colMap := make(map[string]ColumnInfo)
	for _, c := range cols {
		colMap[c.Name] = c
	}

	if id, ok := colMap["id"]; !ok || !id.PrimaryKey {
		t.Errorf("expected id to be primary key: %+v", colMap["id"])
	}
	if name, ok := colMap["name"]; !ok || !name.NotNull {
		t.Errorf("expected name to be NOT NULL: %+v", colMap["name"])
	}
}

// TestGetColumns_NotFound verifies GetColumns returns error for unknown table.
func TestGetColumns_NotFound(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.GetColumns("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

// TestCheckIntegrity_Valid verifies CheckIntegrity passes on a valid database.
func TestCheckIntegrity_Valid(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	report, err := db.CheckIntegrity()
	if err != nil {
		t.Fatal(err)
	}
	if !report.Valid {
		t.Errorf("expected valid integrity, errors: %v", report.Errors)
	}
}

// TestCheckIntegrity_Empty verifies CheckIntegrity passes on an empty database.
func TestCheckIntegrity_Empty(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	report, err := db.CheckIntegrity()
	if err != nil {
		t.Fatal(err)
	}
	if !report.Valid {
		t.Errorf("expected valid integrity on empty DB, errors: %v", report.Errors)
	}
}

// TestGetDatabaseInfo_Memory verifies GetDatabaseInfo for in-memory database.
func TestGetDatabaseInfo_Memory(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	info, err := db.GetDatabaseInfo()
	if err != nil {
		t.Fatal(err)
	}
	if info.FilePath != ":memory:" {
		t.Errorf("expected ':memory:', got %q", info.FilePath)
	}
	if info.Encoding != "UTF-8" {
		t.Errorf("expected 'UTF-8', got %q", info.Encoding)
	}
}

// TestGetDatabaseInfo_File verifies GetDatabaseInfo for file-backed database.
func TestGetDatabaseInfo_File(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(dbPath)

	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatal(err)
	}

	info, err := db.GetDatabaseInfo()
	if err != nil {
		t.Fatal(err)
	}
	if info.FilePath != dbPath {
		t.Errorf("expected %q, got %q", dbPath, info.FilePath)
	}
}

// TestGetPageStats_Basic verifies GetPageStats returns consistent values.
func TestGetPageStats_Basic(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t1 VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t1 VALUES (2)"); err != nil {
		t.Fatal(err)
	}

	stats, err := db.GetPageStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalPages != stats.LeafPages+stats.InteriorPages+stats.OverflowPages {
		t.Errorf("TotalPages mismatch: %+v", stats)
	}
}

// TestBackupTo verifies BackupTo creates a backup file.
func TestBackupTo(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.db")
	dstPath := filepath.Join(tmpDir, "dst.db")

	db, err := Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1 (id INT)"); err != nil {
		t.Fatal(err)
	}

	if err := db.BackupTo(dstPath); err != nil {
		t.Fatalf("BackupTo failed: %v", err)
	}

	if _, err := os.Stat(dstPath); os.IsNotExist(err) {
		t.Error("backup file was not created")
	}
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
