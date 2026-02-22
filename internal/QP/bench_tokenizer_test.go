package QP

import (
	"testing"
)

// BenchmarkTokenizer_Identifiers benchmarks tokenizing a query with many identifiers.
func BenchmarkTokenizer_Identifiers(b *testing.B) {
	query := "SELECT id, name, email, age, salary FROM users WHERE active = 1"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		_, _ = t.Tokenize()
	}
}

// BenchmarkTokenizer_Numbers benchmarks tokenizing a query with numeric literals.
func BenchmarkTokenizer_Numbers(b *testing.B) {
	query := "SELECT * FROM data WHERE x > 3.14 AND y < 100 AND z = 42.5"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		_, _ = t.Tokenize()
	}
}

// BenchmarkTokenizer_Strings benchmarks tokenizing a query with string literals.
func BenchmarkTokenizer_Strings(b *testing.B) {
	query := `SELECT * FROM users WHERE name = 'Alice' AND city = 'New York' AND status = 'active'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		_, _ = t.Tokenize()
	}
}

// BenchmarkTokenizer_HexStrings benchmarks tokenizing hex string literals.
func BenchmarkTokenizer_HexStrings(b *testing.B) {
	query := `SELECT * FROM blobs WHERE data = x'DEADBEEF' OR hash = x'CAFEBABE0102030405060708'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		_, _ = t.Tokenize()
	}
}

// BenchmarkTokenizer_FullQuery benchmarks tokenizing a complex SQL query.
func BenchmarkTokenizer_FullQuery(b *testing.B) {
	query := `SELECT u.id, u.name, COUNT(o.id) AS order_count, SUM(o.total) AS total_amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.active = 1 AND o.created_at > '2024-01-01'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY total_amount DESC
LIMIT 100 OFFSET 0`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		_, _ = t.Tokenize()
	}
}

// BenchmarkParser_Select benchmarks parsing a simple SELECT statement.
func BenchmarkParser_Select(b *testing.B) {
	query := "SELECT id, name, age FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		tokens, _ := t.Tokenize()
		p := NewParser(tokens)
		_, _ = p.Parse()
	}
}

// BenchmarkParser_ComplexExpr benchmarks parsing complex expressions.
func BenchmarkParser_ComplexExpr(b *testing.B) {
	query := `SELECT a + b * c - d / e, COALESCE(x, y, z), CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' ELSE 'zero' END FROM t WHERE (x BETWEEN 1 AND 100) AND y IN (1, 2, 3) AND z LIKE 'prefix%'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := NewTokenizer(query)
		tokens, _ := t.Tokenize()
		p := NewParser(tokens)
		_, _ = p.Parse()
	}
}
