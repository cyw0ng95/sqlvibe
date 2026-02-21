// Package SQLLogic implements a SQLLogicTest runner for black-box testing
// sqlvibe against real SQLite behaviour.
//
// The test format follows the standard sqllogictest specification:
//
//	statement ok
//	CREATE TABLE t(a INTEGER, b TEXT)
//
//	statement error
//	BAD SQL
//
//	query IT rowsort
//	SELECT a, b FROM t ORDER BY a
//	----
//	1  hello
//	2  world
//
// Type characters: I=integer, T=text, R=real.
// Sort modes: rowsort, valuesort, nosort (default).
package SQLLogic

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

// record is a single parsed test record.
type record struct {
	kind      string // "statement" or "query" or "halt" or "skipif" or "onlyif"
	expectErr bool   // statement error
	queryType string // column type string e.g. "ITR"
	sortMode  string // "rowsort", "valuesort", "" (nosort)
	label     string // optional label after sortMode
	sql       string // the SQL text
	expected  []string // expected output values (one per column per row, flattened)
}

// RunFile parses and runs all records in a single .test file against a fresh
// in-memory sqlvibe database.  Failures are reported via t.
func RunFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	records, err := parseFile(bufio.NewScanner(f))
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}

	passed, failed := 0, 0
	for i, rec := range records {
		label := fmt.Sprintf("%s[%d]", filepath.Base(path), i+1)
		if runRecord(t, db, rec, label) {
			passed++
		} else {
			failed++
		}
	}
	t.Logf("%s: %d passed, %d failed out of %d records",
		filepath.Base(path), passed, failed, len(records))
}

// RunDir runs all *.test files found in dir.
func RunDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".test") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		t.Run(strings.TrimSuffix(e.Name(), ".test"), func(t *testing.T) {
			RunFile(t, path)
		})
	}
}

// runRecord executes one record; returns true on pass, false on fail.
func runRecord(t *testing.T, db *sqlvibe.Database, rec record, label string) bool {
	t.Helper()
	switch rec.kind {
	case "statement":
		return runStatement(t, db, rec, label)
	case "query":
		return runQuery(t, db, rec, label)
	default:
		return true // skip/halt/unknown
	}
}

func runStatement(t *testing.T, db *sqlvibe.Database, rec record, label string) bool {
	t.Helper()
	_, err := db.Exec(rec.sql)
	if rec.expectErr {
		if err == nil {
			t.Errorf("%s: expected error but statement succeeded: %s", label, rec.sql)
			return false
		}
		return true
	}
	if err != nil {
		t.Errorf("%s: statement failed: %v\n  SQL: %s", label, err, rec.sql)
		return false
	}
	return true
}

func runQuery(t *testing.T, db *sqlvibe.Database, rec record, label string) bool {
	t.Helper()
	rows, err := db.Query(rec.sql)
	if err != nil {
		t.Errorf("%s: query error: %v\n  SQL: %s", label, err, rec.sql)
		return false
	}

	// Flatten all result values to strings.
	var got []string
	if rows != nil {
		for _, row := range rows.Data {
			for _, v := range row {
				got = append(got, formatValue(v))
			}
		}
	}

	want := rec.expected

	// Apply sort modes.
	switch rec.sortMode {
	case "rowsort":
		got = sortRows(got, len(rec.queryType))
		want = sortRows(want, len(rec.queryType))
	case "valuesort":
		sortedGot := make([]string, len(got))
		copy(sortedGot, got)
		sort.Strings(sortedGot)
		got = sortedGot

		sortedWant := make([]string, len(want))
		copy(sortedWant, want)
		sort.Strings(sortedWant)
		want = sortedWant
	}

	if !stringSliceEqual(got, want) {
		t.Errorf("%s: result mismatch\n  SQL:  %s\n  got:  %v\n  want: %v",
			label, rec.sql, got, want)
		return false
	}
	return true
}

// formatValue converts a query result value to a string matching sqllogictest
// conventions: NULL → "NULL", integers without decimal point, floats with up
// to 3 decimal places.
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		if math.IsInf(val, 1) {
			return "inf"
		}
		if math.IsInf(val, -1) {
			return "-inf"
		}
		if math.IsNaN(val) {
			return "nan"
		}
		// Format with 3 decimal places then strip trailing zeros and decimal
		// point.  This matches the sqllogictest convention where 175 is
		// represented as "175" and 233.333... as "233.333".
		s := strconv.FormatFloat(val, 'f', 3, 64)
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
		return s
	case bool:
		if val {
			return "1"
		}
		return "0"
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// sortRows sorts a flattened value slice by row.  ncols is the number of
// columns per row (len of queryType string).
func sortRows(vals []string, ncols int) []string {
	if ncols <= 0 || len(vals) == 0 {
		return vals
	}
	rowCount := len(vals) / ncols
	rows := make([][]string, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = vals[i*ncols : (i+1)*ncols]
	}
	sort.Slice(rows, func(a, b int) bool {
		for k := 0; k < ncols; k++ {
			if rows[a][k] != rows[b][k] {
				return rows[a][k] < rows[b][k]
			}
		}
		return false
	})
	result := make([]string, 0, len(vals))
	for _, r := range rows {
		result = append(result, r...)
	}
	return result
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

// parseFile reads all test records from the scanner.
func parseFile(s *bufio.Scanner) ([]record, error) {
	var records []record
	var lines []string
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	if err := s.Err(); err != nil {
		return nil, err
	}

	i := 0
	for i < len(lines) {
		line := lines[i]

		// Skip blank lines and comments.
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}

		// Handle directives: skipif, onlyif, halt.
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "skipif") || strings.HasPrefix(lower, "onlyif") {
			// Skip the entire record that follows.
			i++
			// consume the SQL block until next blank line
			for i < len(lines) && strings.TrimSpace(lines[i]) != "" {
				i++
			}
			continue
		}
		if lower == "halt" {
			break
		}

		// statement ok / statement error
		if strings.HasPrefix(lower, "statement") {
			rec, newI, err := parseStatement(lines, i)
			if err != nil {
				return nil, err
			}
			records = append(records, rec)
			i = newI
			continue
		}

		// query TYPE [sortmode] [label]
		if strings.HasPrefix(lower, "query") {
			rec, newI, err := parseQuery(lines, i)
			if err != nil {
				return nil, err
			}
			records = append(records, rec)
			i = newI
			continue
		}

		// Unknown directive – skip line.
		i++
	}
	return records, nil
}

// parseStatement parses a "statement ok|error" block.
// Returns the record and the next line index.
func parseStatement(lines []string, start int) (record, int, error) {
	rec := record{kind: "statement"}
	directive := strings.ToLower(strings.TrimSpace(lines[start]))
	if strings.Contains(directive, "error") {
		rec.expectErr = true
	}
	i := start + 1

	// Collect SQL lines until blank or another directive.
	var sqlLines []string
	for i < len(lines) {
		l := lines[i]
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			i++
			break
		}
		lower := strings.ToLower(trimmed)
		if isDirective(lower) {
			break
		}
		sqlLines = append(sqlLines, trimmed)
		i++
	}
	rec.sql = strings.Join(sqlLines, " ")
	return rec, i, nil
}

// parseQuery parses a "query TYPE [sortmode] [label]" block.
func parseQuery(lines []string, start int) (record, int, error) {
	rec := record{kind: "query"}
	parts := strings.Fields(lines[start])
	if len(parts) >= 2 {
		rec.queryType = parts[1]
	}
	if len(parts) >= 3 {
		sm := strings.ToLower(parts[2])
		if sm == "rowsort" || sm == "valuesort" || sm == "nosort" {
			rec.sortMode = sm
			if len(parts) >= 4 {
				rec.label = parts[3]
			}
		} else {
			rec.label = parts[2]
		}
	}
	i := start + 1

	// Collect SQL lines until "----" or blank.
	var sqlLines []string
	for i < len(lines) {
		l := strings.TrimSpace(lines[i])
		if l == "----" {
			i++
			break
		}
		if l == "" {
			// no expected results section
			break
		}
		lower := strings.ToLower(l)
		if isDirective(lower) {
			break
		}
		sqlLines = append(sqlLines, l)
		i++
	}
	rec.sql = strings.Join(sqlLines, " ")

	// Collect expected result lines until blank or directive.
	for i < len(lines) {
		l := strings.TrimSpace(lines[i])
		if l == "" {
			i++
			break
		}
		lower := strings.ToLower(l)
		if isDirective(lower) {
			break
		}
		// Each line is one row; values are whitespace-separated.
		cols := strings.Fields(l)
		rec.expected = append(rec.expected, cols...)
		i++
	}
	return rec, i, nil
}

// isDirective returns true if the lowercase trimmed line starts a new record.
func isDirective(s string) bool {
	return strings.HasPrefix(s, "statement") ||
		strings.HasPrefix(s, "query") ||
		strings.HasPrefix(s, "skipif") ||
		strings.HasPrefix(s, "onlyif") ||
		s == "halt" ||
		strings.HasPrefix(s, "#")
}
