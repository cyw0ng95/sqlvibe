package QP

import (
	"regexp"
	"strings"
)

// queryNormalizer replaces string literals and number literals with "?" placeholder.
// Handles SQL escaped single quotes (e.g., 'O”Brien') and floating-point numbers (e.g., 3.14).
var queryNormalizer = regexp.MustCompile(`'(?:[^']|'')*'|[0-9]+(?:\.[0-9]+)?`)

// NormalizeQuery normalizes a SQL string for cache key generation.
// It lowercases, trims, and replaces all literals with "?" placeholders.
// Example: "SELECT * FROM users WHERE id = 1" → "select * from users where id = ?"
func NormalizeQuery(sql string) string {
	sql = strings.ToLower(sql)
	sql = strings.TrimSpace(sql)
	sql = queryNormalizer.ReplaceAllString(sql, "?")
	return sql
}
