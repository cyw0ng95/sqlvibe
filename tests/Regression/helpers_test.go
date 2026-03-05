package Regression

import (
	"database/sql"
	"testing"
)

// qResult is a lightweight holder returned by qDB.
// It exposes .Data and .Columns fields so tests can use the same r.Data[i][j]
// and r.Columns syntax that was previously available on *sqlvibe.Rows.
type qResult struct {
	Columns []string
	Data    [][]interface{}
}

// qDB executes query against db and returns all rows as a *qResult.
// It calls t.Fatalf on any error.
func qDB(t *testing.T, db *sql.DB, query string, args ...interface{}) *qResult {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("qDB %q: %v", query, err)
		return &qResult{}
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("qDB columns %q: %v", query, err)
		return &qResult{}
	}
	var data [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("qDB scan %q: %v", query, err)
			return &qResult{}
		}
		row := make([]interface{}, len(cols))
		copy(row, vals)
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("qDB rows.Err %q: %v", query, err)
	}
	return &qResult{Columns: cols, Data: data}
}

// mustExec executes a statement and calls t.Fatalf on error.
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("mustExec %q: %v", query, err)
	}
}
