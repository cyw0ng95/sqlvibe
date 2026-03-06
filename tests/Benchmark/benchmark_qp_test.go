// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains QP Layer benchmarks for query processing performance.
// In v0.11.2+ the tokenizer and parser are implemented in C++ (src/core/QP/);
// these benchmarks measure end-to-end query latency including the C++ QP layer.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Wave 3: QP Layer - Query Processing (v0.11.2+ via SQL execution path)
// Focus: Measure end-to-end parse + plan + execute latency
// -----------------------------------------------------------------

// BenchmarkQPTokenize measures tokenization overhead via db.Query for simple SQL.
func BenchmarkQPTokenize(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (x INTEGER, y INTEGER, z INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT x FROM t WHERE x > 1 AND y < 100 ORDER BY z")
		for rows.Next() {
		}
	}
}

// BenchmarkQPParseSimple measures simple SELECT parse + execute overhead.
func BenchmarkQPParseSimple(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id, name FROM users WHERE id = 1")
		for rows.Next() {
		}
	}
}

// BenchmarkQPParseComplex measures complex multi-clause query parse + execute overhead.
func BenchmarkQPParseComplex(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)")
	mustExec(b, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d, %d)", i, i%100, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, `SELECT a, b, SUM(c) FROM t1
			JOIN t2 ON t1.a = t2.id
			WHERE a > 1 AND b < 100
			GROUP BY a, b
			HAVING SUM(c) > 10
			ORDER BY b
			LIMIT 10`)
		for rows.Next() {
		}
	}
}

// BenchmarkQPASTBuild measures multi-statement batch parse + execute overhead.
func BenchmarkQPASTBuild(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	mustExec(b, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER)")
	mustExec(b, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, b INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d)", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i))
	}

	queries := []string{
		"SELECT id FROM t",
		"SELECT id, val FROM t WHERE id > 0",
		"SELECT id, val FROM t WHERE id > 0 ORDER BY val DESC LIMIT 5",
		"SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, q)
			for rows.Next() {
			}
		}
	}
}
