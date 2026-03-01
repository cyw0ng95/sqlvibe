package database

import "strings"

// quoteIdent returns a double-quoted, escaped SQL identifier safe for use in
// dynamically-constructed SQL strings.  Double-quote characters within the
// name are escaped by doubling them per the SQL standard.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sanitizeIdent strips characters that are not alphanumeric or underscore from
// name and returns the result.  It is used to safely embed user-supplied
// identifiers (such as savepoint names) in SQL strings that are passed to a
// parser which cannot handle quoted identifiers in that position.
func sanitizeIdent(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
