package PlainFuzzer

import (
	"math/rand"
	"os"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// FuzzDBFile is a parallel fuzzer that mutates SQLVIBE binary database files
// to find bugs in the persistence / loading code.  It runs independently of
// FuzzSQL and focuses on file-level corruption rather than SQL query parsing.
//
// Run with:
//
//	go test -fuzz=FuzzDBFile -fuzztime=60s ./internal/TS/PlainFuzzer/...
func FuzzDBFile(f *testing.F) {
	// Generate seed databases at startup so no pre-built binary blobs are
	// required in the repository.
	seedDir := f.TempDir()
	seedPaths := generateSeedDatabases(seedDir)

	for _, path := range seedPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		f.Add(data)
	}

	// Add a trivially small seed so the fuzzer always has at least one corpus entry.
	f.Add([]byte("SQLVIBE\x01"))

	f.Fuzz(func(t *testing.T, fileData []byte) {
		// Write the (possibly mutated) file data to a temp location.
		tmpDir := t.TempDir()
		tmpPath := tmpDir + "/fuzz.db"

		// Apply an additional random mutation so every corpus entry produces
		// novel inputs even without the -fuzz engine's built-in mutation.
		mutator := NewFileMutator(rand.Int63()) //nolint:gosec
		mutated := mutator.Mutate(fileData)

		if err := os.WriteFile(tmpPath, mutated, 0600); err != nil {
			return
		}

		// Recover from any panic so a single crash does not stop the fuzzer.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic on corrupted db file: %v", r)
			}
		}()

		// Attempt to open the database; a clean error is expected and fine.
		db, err := sqlvibe.Open(tmpPath)
		if err != nil {
			return
		}
		defer db.Close()

		// Try a handful of basic operations.  Errors are acceptable; panics are not.
		_, _ = db.Query("SELECT * FROM sqlite_master")
		_, _ = db.Query("SELECT * FROM t1")
		_, _ = db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	})
}
