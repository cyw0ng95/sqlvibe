package SQLValidator

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/glebarez/go-sqlite"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// Validator runs the same SQL statements against both SQLite and sqlvibe and
// reports any result mismatches.
type Validator struct {
	lcg    *LCG
	gen    *Generator
	lite   *sql.DB
	svibe  *sqlvibe.Database
}

// NewValidator creates a Validator with the given LCG seed.
// Both SQLite and sqlvibe databases are created in-memory and seeded with the
// TPC-C starter dataset.
func NewValidator(seed uint64) (*Validator, error) {
	lcg := NewLCG(seed)
	gen := NewGenerator(lcg)

	// Set up SQLite.
	lite, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	if err := setupSQLite(lite); err != nil {
		lite.Close()
		return nil, fmt.Errorf("setup SQLite: %w", err)
	}

	// Set up sqlvibe.
	svibe, err := sqlvibe.Open(":memory:")
	if err != nil {
		lite.Close()
		return nil, fmt.Errorf("open sqlvibe: %w", err)
	}
	if err := setupSQLVibe(svibe); err != nil {
		lite.Close()
		svibe.Close()
		return nil, fmt.Errorf("setup sqlvibe: %w", err)
	}

	return &Validator{
		lcg:   lcg,
		gen:   gen,
		lite:  lite,
		svibe: svibe,
	}, nil
}

// Close releases all resources held by the Validator.
func (v *Validator) Close() {
	v.lite.Close()
	v.svibe.Close()
}

// Run generates n random SQL statements, executes each against both backends,
// and collects any mismatches.
func (v *Validator) Run(n int) ([]Mismatch, error) {
	var mismatches []Mismatch
	for i := 0; i < n; i++ {
		query := v.gen.Next()
		liteRes := executeSQLite(v.lite, query)
		svibeRes := executeSQLVibe(v.svibe, query)
		if m := Compare(query, liteRes, svibeRes); m != nil {
			mismatches = append(mismatches, *m)
		}
	}
	return mismatches, nil
}

// setupSQLite creates the TPC-C schema and inserts seed data into a SQLite database.
func setupSQLite(db *sql.DB) error {
	// Execute DDL statements one by one.
	for _, stmt := range splitSQL(tpccDDL) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("DDL %q: %w", stmt, err)
		}
	}
	// Execute seed INSERT statements.
	for _, stmt := range splitSQL(tpccSeedSQL) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("seed %q: %w", stmt, err)
		}
	}
	return nil
}

// setupSQLVibe creates the TPC-C schema and inserts seed data into a sqlvibe database.
func setupSQLVibe(db *sqlvibe.Database) error {
	for _, stmt := range splitSQL(tpccDDL) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("DDL %q: %w", stmt, err)
		}
	}
	for _, stmt := range splitSQL(tpccSeedSQL) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("seed %q: %w", stmt, err)
		}
	}
	return nil
}

// executeSQLite runs a query against a SQLite database and collects the result.
func executeSQLite(db *sql.DB, query string) QueryResult {
	rows, err := db.Query(query)
	if err != nil {
		return QueryResult{Err: err}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return QueryResult{Err: err}
	}

	var result [][]interface{}
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		vals := make([]interface{}, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return QueryResult{Err: err}
		}
		// Dereference and normalise types.
		row := make([]interface{}, len(cols))
		for i, v := range vals {
			row[i] = normaliseSQLiteVal(v)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{Err: err}
	}
	return QueryResult{Columns: cols, Rows: result}
}

// normaliseSQLiteVal converts database/sql scan results to canonical types.
func normaliseSQLiteVal(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case []byte:
		return string(x)
	case int64:
		return x
	case float64:
		return x
	case bool:
		if x {
			return int64(1)
		}
		return int64(0)
	default:
		return v
	}
}

// executeSQLVibe runs a query against a sqlvibe database and collects the result.
func executeSQLVibe(db *sqlvibe.Database, query string) QueryResult {
	rows, err := db.Query(query)
	if err != nil {
		return QueryResult{Err: err}
	}
	return QueryResult{
		Columns: rows.Columns,
		Rows:    rows.Data,
	}
}

// splitSQL splits a multi-statement SQL string (separated by ";") into individual
// non-empty statements.
func splitSQL(src string) []string {
	var out []string
	for _, s := range strings.Split(src, ";") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
