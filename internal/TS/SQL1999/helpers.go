package SQL1999

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func sqlvibeDB(t *testing.T, path string) *sqlvibe.Database {
	db, err := sqlvibe.Open(path)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	return db
}

func sqliteDB(t *testing.T, path string) *sql.DB {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	return db
}

func FetchAllRows(rows *sqlvibe.Rows) ([]map[string]interface{}, error) {
	if rows == nil {
		return nil, nil
	}

	results := make([]map[string]interface{}, 0, len(rows.Data))
	for _, rowData := range rows.Data {
		row := make(map[string]interface{})
		for i, col := range rows.Columns {
			if i < len(rowData) {
				row[col] = rowData[i]
			}
		}
		results = append(results, row)
	}
	return results, nil
}

func FetchAllRowsSQLite(rows *sql.Rows) ([]map[string]interface{}, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		// Store values in column ORDER (not by name)
		orderedVals := make([]interface{}, len(columns))
		for i, col := range columns {
			orderedVals[i] = values[i]
			row[col] = values[i]
		}
		row["__ordered__"] = orderedVals
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func CompareQueryResults(t *testing.T, sqlvibeDB *sqlvibe.Database, sqliteDB *sql.DB, sql string, testName string) {
	sqlvibeRows, sqlvibeErr := sqlvibeDB.Query(sql)
	sqliteRows, sqliteErr := sqliteDB.Query(sql)
	if sqliteRows != nil {
		defer sqliteRows.Close()
	}

	// If both error, behavior agrees — pass
	if sqlvibeErr != nil && sqliteErr != nil {
		return
	}
	// If only one errors, that's a mismatch
	if sqlvibeErr != nil {
		t.Errorf("%s: sqlvibe query error: %v", testName, sqlvibeErr)
		return
	}
	if sqliteErr != nil {
		t.Errorf("%s: sqlite query error: %v", testName, sqliteErr)
		return
	}

	sqlvibeResults, err := FetchAllRows(sqlvibeRows)
	if err != nil {
		t.Errorf("%s: sqlvibe fetch error: %v", testName, err)
		return
	}

	sqliteResults, err := FetchAllRowsSQLite(sqliteRows)
	if err != nil {
		t.Errorf("%s: sqlite fetch error: %v", testName, err)
		return
	}

	if len(sqlvibeResults) != len(sqliteResults) {
		t.Errorf("%s: row count mismatch: sqlvibe=%d, sqlite=%d", testName, len(sqlvibeResults), len(sqliteResults))
		return
	}

	// Compare by index (SQLite 3 flavour) - column names differ between sqlvibe and sqlite
	// sqlvibe uses "expr" for expressions, sqlite uses expression text
	for i := range sqlvibeResults {
		sqlvibeRow := sqlvibeResults[i]
		sqliteRow := sqliteResults[i]

		// Get values in column order from sqlvibe
		sqlvibeVals := make([]interface{}, 0, len(sqlvibeRow))
		for _, colName := range sqlvibeRows.Columns {
			if v, ok := sqlvibeRow[colName]; ok {
				sqlvibeVals = append(sqlvibeVals, v)
			} else {
				sqlvibeVals = append(sqlvibeVals, nil)
			}
		}

		// Get ordered values from sqlite (stored in __ordered__ key)
		sqliteVals, ok := sqliteRow["__ordered__"].([]interface{})
		if !ok {
			// Fallback: iterate in original order
			sqliteVals = make([]interface{}, 0, len(sqliteRow))
			for _, colName := range sqlvibeRows.Columns {
				if v, ok := sqliteRow[colName]; ok {
					sqliteVals = append(sqliteVals, v)
				} else {
					sqliteVals = append(sqliteVals, nil)
				}
			}
		}

		if len(sqlvibeVals) != len(sqliteVals) {
			t.Errorf("%s: column count mismatch at row %d: sqlvibe=%d, sqlite=%d", testName, i, len(sqlvibeVals), len(sqliteVals))
			continue
		}

		for j := range sqlvibeVals {
			sqlvibeVal := sqlvibeVals[j]
			sqliteVal := sqliteVals[j]

			sqlvibeStr := fmt.Sprintf("%v", sqlvibeVal)
			sqliteStr := fmt.Sprintf("%v", sqliteVal)
			if sqlvibeStr != sqliteStr {
				t.Errorf("%s: row %d, col %d mismatch: sqlvibe=%v (%T), sqlite=%v (%T)", testName, i, j, sqlvibeVal, sqlvibeVal, sqliteVal, sqliteVal)
			}
		}
	}
}

func CompareExecResults(t *testing.T, sqlvibeDB *sqlvibe.Database, sqliteDB *sql.DB, sql string, testName string) {
	_, err1 := sqlvibeDB.Exec(sql)
	_, err2 := sqliteDB.Exec(sql)

	if (err1 == nil) != (err2 == nil) {
		t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", testName, err1, err2)
	}
}

// QuerySqlvibeOnly executes a query only on sqlvibe and validates it succeeds
// Used for features not supported by SQLite (e.g., information_schema)
func QuerySqlvibeOnly(t *testing.T, sqlvibeDB *sqlvibe.Database, sql string, testName string) *sqlvibe.Rows {
	rows, err := sqlvibeDB.Query(sql)
	if err != nil {
		t.Errorf("%s: sqlvibe query error: %v", testName, err)
		return nil
	}
	return rows
}

func compareSingleValue(t *testing.T, sqlvibeDB *sqlvibe.Database, sqliteDB *sql.DB, sql string, testName string) {
	rows, err := sqlvibeDB.Query(sql)
	if err != nil {
		t.Errorf("%s: sqlvibe query error: %v", testName, err)
		return
	}

	var sqlvibeVal interface{}
	if rows.Next() {
		if len(rows.Data) > 0 {
			sqlvibeVal = rows.Data[0][0]
		}
	}

	rows2, err := sqliteDB.Query(sql)
	if err != nil {
		t.Errorf("%s: sqlite query error: %v", testName, err)
		return
	}
	defer rows2.Close()

	var sqliteVal interface{}
	if rows2.Next() {
		if err := rows2.Scan(&sqliteVal); err != nil {
			t.Errorf("%s: sqlite scan error: %v", testName, err)
			return
		}
	}

	sqlvibeStr := fmt.Sprintf("%v", sqlvibeVal)
	sqliteStr := fmt.Sprintf("%v", sqliteVal)

	if sqlvibeStr != sqliteStr {
		t.Errorf("%s: value mismatch: sqlvibe=%v (%T), sqlite=%v (%T)", testName, sqlvibeVal, sqlvibeVal, sqliteVal, sqliteVal)
	}
}
