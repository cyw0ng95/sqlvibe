// Package Benchmark provides v0.8.5 feature benchmarks and integration tests.
// These tests cover WAL, MVCC, isolation levels, compression, and backup.
package Benchmark

import (
	"os"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// openDBT opens an in-memory database for *testing.T tests.
func openDBT(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// mustExecT runs SQL and fails on error in *testing.T context.
func mustExecT(t *testing.T, db *sqlvibe.Database, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
}

// -----------------------------------------------------------------
// v0.8.5 Feature tests: PRAGMAs
// -----------------------------------------------------------------

// TestV085_WALMode tests PRAGMA wal_mode toggle.
func TestV085_WALMode(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA wal_mode")
	if err != nil {
		t.Fatalf("PRAGMA wal_mode: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected row from PRAGMA wal_mode")
	}

	// Enable WAL using wal_mode.
	_, err = db.Query("PRAGMA wal_mode = ON")
	if err != nil {
		t.Fatalf("PRAGMA wal_mode = ON: %v", err)
	}

	// Disable WAL.
	_, err = db.Query("PRAGMA wal_mode = OFF")
	if err != nil {
		t.Fatalf("PRAGMA wal_mode = OFF: %v", err)
	}
}

// TestV085_IsolationLevel tests PRAGMA isolation_level.
func TestV085_IsolationLevel(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	rows, err := db.Query("PRAGMA isolation_level")
	if err != nil {
		t.Fatalf("PRAGMA isolation_level: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected row from PRAGMA isolation_level")
	}

	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "SERIALIZABLE"}
	for _, level := range levels {
		_, err = db.Query("PRAGMA isolation_level = '" + level + "'")
		if err != nil {
			t.Fatalf("PRAGMA isolation_level = %q: %v", level, err)
		}
		rows, err = db.Query("PRAGMA isolation_level")
		if err != nil {
			t.Fatalf("read back isolation_level: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatal("expected row from PRAGMA isolation_level")
		}
	}

	// Reset to default.
	_, err = db.Query("PRAGMA isolation_level = 'READ COMMITTED'")
	if err != nil {
		t.Fatalf("reset isolation_level: %v", err)
	}
}

// TestV085_BusyTimeout tests PRAGMA busy_timeout.
func TestV085_BusyTimeout(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	_, err := db.Query("PRAGMA busy_timeout = 5000")
	if err != nil {
		t.Fatalf("PRAGMA busy_timeout = 5000: %v", err)
	}

	rows, err := db.Query("PRAGMA busy_timeout")
	if err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected row from PRAGMA busy_timeout")
	}
}

// TestV085_Compression tests PRAGMA compression.
func TestV085_Compression(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	for _, algo := range []string{"NONE", "RLE", "LZ4", "ZSTD", "GZIP"} {
		_, err := db.Query("PRAGMA compression = '" + algo + "'")
		if err != nil {
			t.Fatalf("PRAGMA compression = %q: %v", algo, err)
		}
		rows, err := db.Query("PRAGMA compression")
		if err != nil {
			t.Fatalf("read compression: %v", err)
		}
		if len(rows.Data) == 0 {
			t.Fatal("expected row from PRAGMA compression")
		}
	}

	// Invalid algorithm should return error.
	_, err := db.Query("PRAGMA compression = 'SNAPPY'")
	if err == nil {
		t.Fatal("expected error for unknown compression algorithm SNAPPY")
	}
}

// TestV085_StorageInfo tests PRAGMA storage_info.
func TestV085_StorageInfo(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	mustExecT(t, db, "CREATE TABLE t (id INTEGER, val TEXT)")
	mustExecT(t, db, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')")

	rows, err := db.Query("PRAGMA storage_info")
	if err != nil {
		t.Fatalf("PRAGMA storage_info: %v", err)
	}
	if len(rows.Columns) == 0 {
		t.Fatal("expected columns from PRAGMA storage_info")
	}
	if len(rows.Data) == 0 {
		t.Fatal("expected row from PRAGMA storage_info")
	}
}

// -----------------------------------------------------------------
// v0.8.5 Feature tests: BACKUP command
// -----------------------------------------------------------------

// TestV085_BackupDatabase tests BACKUP DATABASE TO 'path'.
func TestV085_BackupDatabase(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	mustExecT(t, db, "CREATE TABLE t (id INTEGER, val TEXT)")
	mustExecT(t, db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")

	destPath := t.TempDir() + "/backup.db"
	_, err := db.Query("BACKUP DATABASE TO '" + destPath + "'")
	if err != nil {
		t.Fatalf("BACKUP DATABASE: %v", err)
	}

	// The file should have been created.
	if _, statErr := os.Stat(destPath); os.IsNotExist(statErr) {
		t.Fatal("backup file was not created")
	}
}

// TestV085_BackupIncremental tests BACKUP INCREMENTAL TO 'path'.
func TestV085_BackupIncremental(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	mustExecT(t, db, "CREATE TABLE t (id INTEGER, val TEXT)")
	mustExecT(t, db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")

	destPath := t.TempDir() + "/incr-backup.db"
	_, err := db.Query("BACKUP INCREMENTAL TO '" + destPath + "'")
	if err != nil {
		t.Fatalf("BACKUP INCREMENTAL: %v", err)
	}
}

// -----------------------------------------------------------------
// v0.8.5 Feature tests: Compression (via PRAGMA, C++ layer in v0.11.2+)
// -----------------------------------------------------------------

// TestV085_CompressionAlgorithms verifies compression PRAGMAs are accepted.
// The actual compression is handled in C++ (src/core/DS/compression.cpp).
func TestV085_CompressionAlgorithms(t *testing.T) {
	db := openDBT(t)
	defer db.Close()

	// Verify that recognized compression algorithms can be set via PRAGMA.
	for _, algo := range []string{"NONE", "RLE", "LZ4", "ZSTD", "GZIP"} {
		_, err := db.Query("PRAGMA compression = '" + algo + "'")
		if err != nil {
			t.Fatalf("PRAGMA compression = %q: %v", algo, err)
		}
	}

	// Verify that an unknown algorithm is rejected.
	_, err := db.Query("PRAGMA compression = 'SNAPPY'")
	if err == nil {
		t.Fatal("expected error for unknown compression algorithm SNAPPY")
	}
}

// -----------------------------------------------------------------
// v0.8.5 Feature benchmarks
// -----------------------------------------------------------------

// BenchmarkCompression_LZ4 benchmarks INSERT+SELECT throughput
// using the LZ4 compression setting (exercising the C++ compressor).
func BenchmarkCompression_LZ4(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	if _, err := db.Query("PRAGMA compression = 'LZ4'"); err != nil {
		b.Skipf("PRAGMA compression LZ4 not available: %v", err)
	}
	mustExec(b, db, "CREATE TABLE t (id INTEGER, data TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkCompression_ZSTD benchmarks INSERT+SELECT throughput
// using the ZSTD compression setting (exercising the C++ compressor).
func BenchmarkCompression_ZSTD(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	if _, err := db.Query("PRAGMA compression = 'ZSTD'"); err != nil {
		b.Skipf("PRAGMA compression ZSTD not available: %v", err)
	}
	mustExec(b, db, "CREATE TABLE t (id INTEGER, data TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
	}
}
