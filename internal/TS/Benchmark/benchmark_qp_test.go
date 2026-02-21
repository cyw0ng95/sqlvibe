// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains QP Layer benchmarks for tokenizer and parser performance.
package Benchmark

import (
	"testing"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
)

// -----------------------------------------------------------------
// Wave 3: QP Layer - Query Processing
// Focus: Discover bottlenecks in tokenizer and parser
// -----------------------------------------------------------------

// BenchmarkQPTokenize measures SQL string tokenization overhead
func BenchmarkQPTokenize(b *testing.B) {
	sql := "SELECT a, b, c FROM t WHERE x > 1 AND y < 100 ORDER BY z"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := QP.NewTokenizer(sql)
		_, _ = tokenizer.Tokenize()
	}
}

// BenchmarkQPParseSimple measures simple SELECT parse overhead
func BenchmarkQPParseSimple(b *testing.B) {
	sql := "SELECT id, name FROM users WHERE id = 1"
	tokens, _ := QP.NewTokenizer(sql).Tokenize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := QP.NewParser(tokens)
		_, _ = parser.Parse()
	}
}

// BenchmarkQPParseComplex measures complex query parse overhead
func BenchmarkQPParseComplex(b *testing.B) {
	sql := `SELECT a, b, c FROM t1
		JOIN t2 ON t1.id = t2.id
		WHERE a > 1 AND b < 100
		GROUP BY a
		HAVING SUM(c) > 10
		ORDER BY b
		LIMIT 10`
	tokens, _ := QP.NewTokenizer(sql).Tokenize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := QP.NewParser(tokens)
		_, _ = parser.Parse()
	}
}

// BenchmarkQPASTBuild measures AST construction overhead for a multi-statement batch
func BenchmarkQPASTBuild(b *testing.B) {
	sqls := []string{
		"SELECT id FROM t",
		"SELECT id, val FROM t WHERE id > 0",
		"SELECT id, val FROM t WHERE id > 0 ORDER BY val DESC LIMIT 5",
		"SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id WHERE a > 1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			tokenizer := QP.NewTokenizer(sql)
			tokens, _ := tokenizer.Tokenize()
			parser := QP.NewParser(tokens)
			_, _ = parser.Parse()
		}
	}
}
