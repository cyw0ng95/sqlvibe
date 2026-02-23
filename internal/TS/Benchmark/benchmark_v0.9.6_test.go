// Package Benchmark provides v0.9.6 performance benchmarks.
// These benchmarks cover the v0.9.6 features:
//   - SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT
//   - NOT NULL constraint enforcement overhead
//   - UNIQUE constraint enforcement overhead
//   - Foreign Key ON DELETE CASCADE
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// BenchmarkSavepoint measures SAVEPOINT + ROLLBACK TO SAVEPOINT cycle throughput.
func BenchmarkSavepoint(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sp_bench (id INTEGER, val TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sp_bench VALUES (%d, 'init')", i))
	}

	b.Run("SavepointRollback", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustExec(b, db, "BEGIN")
			mustExec(b, db, "SAVEPOINT sp1")
			mustExec(b, db, fmt.Sprintf("INSERT INTO sp_bench VALUES (%d, 'savepoint')", 1000+i))
			mustExec(b, db, "ROLLBACK TO SAVEPOINT sp1")
			mustExec(b, db, "ROLLBACK")
		}
	})

	b.Run("SavepointRelease", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustExec(b, db, "BEGIN")
			mustExec(b, db, "SAVEPOINT sp1")
			mustExec(b, db, fmt.Sprintf("INSERT INTO sp_bench VALUES (%d, 'savepoint')", 2000+i))
			mustExec(b, db, "RELEASE SAVEPOINT sp1")
			mustExec(b, db, "ROLLBACK")
		}
	})
}

// BenchmarkUniqueConstraint measures insert throughput with UNIQUE constraint checks.
func BenchmarkUniqueConstraint(b *testing.B) {
	b.Run("WithUnique", func(b *testing.B) {
		db, err := sqlvibe.Open(":memory:")
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		mustExec(b, db, "CREATE TABLE uniq_bench (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO uniq_bench VALUES (%d, 'user%d@example.com')", i, i))
		}
	})

	b.Run("WithoutUnique", func(b *testing.B) {
		db, err := sqlvibe.Open(":memory:")
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		mustExec(b, db, "CREATE TABLE nouniq_bench (id INTEGER PRIMARY KEY, email TEXT)")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO nouniq_bench VALUES (%d, 'user%d@example.com')", i, i))
		}
	})
}

// BenchmarkNotNullConstraint measures insert throughput with NOT NULL constraint checks.
func BenchmarkNotNullConstraint(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE nn_bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO nn_bench VALUES (%d, 'name%d')", i, i))
	}
}
