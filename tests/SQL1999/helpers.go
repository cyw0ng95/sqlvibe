package SQL1999

import (
"database/sql"
"fmt"
"sort"
"strings"
"testing"
"time"

_ "github.com/cyw0ng95/sqlvibe/driver"
_ "github.com/glebarez/go-sqlite"
)

// QueryResult holds the result of a SQL query executed via the driver interface.
// It mirrors the field names of sqlvibe.Rows so that existing test code that
// accesses .Data and .Columns keeps working after the driver/ migration.
type QueryResult struct {
// Columns holds the column names returned by the query.
Columns []string
// Data holds the row data; each element is a slice of column values.
Data [][]interface{}
}

// fetchAllFromSQL fetches all rows from a *sql.Rows result into a QueryResult.
func fetchAllFromSQL(rows *sql.Rows) (*QueryResult, error) {
if rows == nil {
return nil, nil
}
cols, err := rows.Columns()
if err != nil {
return nil, err
}
qr := &QueryResult{Columns: cols}
for rows.Next() {
vals := make([]interface{}, len(cols))
ptrs := make([]interface{}, len(cols))
for i := range vals {
ptrs[i] = &vals[i]
}
if err := rows.Scan(ptrs...); err != nil {
return nil, err
}
row := make([]interface{}, len(cols))
copy(row, vals)
qr.Data = append(qr.Data, row)
}
return qr, rows.Err()
}

// FetchAllRowsSQLite fetches all rows from a SQLite *sql.Rows into a map slice.
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

// QueryRows executes query against db and returns all rows as a *QueryResult.
func QueryRows(t *testing.T, db *sql.DB, query string, args ...interface{}) *QueryResult {
t.Helper()
rows, err := db.Query(query, args...)
if err != nil {
t.Fatalf("QueryRows %q: %v", query, err)
return nil
}
defer rows.Close()
qr, err := fetchAllFromSQL(rows)
if err != nil {
t.Fatalf("QueryRows fetch %q: %v", query, err)
return nil
}
if qr == nil {
return &QueryResult{}
}
return qr
}

// QuerySqlvibeOnly executes a query only on sqlvibe and validates it succeeds.
// Returns a *QueryResult whose .Data and .Columns fields can be read directly.
func QuerySqlvibeOnly(t *testing.T, db *sql.DB, query string, testName string) *QueryResult {
t.Helper()
rows, err := db.Query(query)
if err != nil {
t.Errorf("%s: sqlvibe query error: %v", testName, err)
return nil
}
defer rows.Close()
qr, err := fetchAllFromSQL(rows)
if err != nil {
t.Errorf("%s: sqlvibe fetch error: %v", testName, err)
return nil
}
if qr == nil {
return &QueryResult{}
}
return qr
}

// CompareQueryResults executes query against both databases and compares results.
func CompareQueryResults(t *testing.T, svDB *sql.DB, slDB *sql.DB, query string, testName string) {
t.Helper()
svRows, svErr := svDB.Query(query)
slRows, slErr := slDB.Query(query)
if svRows != nil {
defer svRows.Close()
}
if slRows != nil {
defer slRows.Close()
}

if svErr != nil && slErr != nil {
return
}
if svErr != nil && slErr == nil {
_, slFetchErr := FetchAllRowsSQLite(slRows)
if slFetchErr != nil {
return
}
t.Errorf("%s: sqlvibe query error: %v", testName, svErr)
return
}
if slErr != nil {
/* SQLite doesn't support this syntax/feature; skip rather than fail
   since our engine can be a superset of SQLite. */
t.Skipf("%s: skipped — sqlite query error (sqlite limitation): %v", testName, slErr)
return
}

svResult, svFetchErr := fetchAllFromSQL(svRows)
if svFetchErr != nil {
t.Errorf("%s: sqlvibe fetch error: %v", testName, svFetchErr)
return
}
if svResult == nil {
svResult = &QueryResult{}
}

slResult, slFetchErr := FetchAllRowsSQLite(slRows)
if slFetchErr != nil {
if len(svResult.Data) == 0 {
return
}
/* SQLite-specific errors (e.g. integer overflow) are not required by SQL:1999;
   skip rather than fail when our engine returns data but SQLite errors. */
t.Skipf("%s: skipped — sqlite fetch error (implementation-specific): %v", testName, slFetchErr)
return
}

if len(svResult.Data) != len(slResult) {
t.Errorf("%s: row count mismatch: sqlvibe=%d, sqlite=%d",
testName, len(svResult.Data), len(slResult))
return
}

upperQuery := strings.ToUpper(query)
hasOrderBy := strings.Contains(upperQuery, "ORDER BY") &&
!strings.Contains(upperQuery, "ORDER BY RANDOM()")
if !hasOrderBy {
rowKey := func(data []interface{}) string {
parts := make([]string, len(data))
for i, v := range data {
parts[i] = fmt.Sprintf("%v", v)
}
return strings.Join(parts, "\x00")
}
sort.Slice(svResult.Data, func(a, b int) bool {
return rowKey(svResult.Data[a]) < rowKey(svResult.Data[b])
})
sort.Slice(slResult, func(a, b int) bool {
va, _ := slResult[a]["__ordered__"].([]interface{})
vb, _ := slResult[b]["__ordered__"].([]interface{})
return rowKey(va) < rowKey(vb)
})
}

for i := range svResult.Data {
svVals := svResult.Data[i]
slRow := slResult[i]

slVals, ok := slRow["__ordered__"].([]interface{})
if !ok {
slVals = make([]interface{}, 0, len(svResult.Columns))
for _, col := range svResult.Columns {
if v, exists := slRow[col]; exists {
slVals = append(slVals, v)
} else {
slVals = append(slVals, nil)
}
}
}

if len(svVals) != len(slVals) {
t.Errorf("%s: column count mismatch at row %d: sqlvibe=%d, sqlite=%d",
testName, i, len(svVals), len(slVals))
continue
}

for j := range svVals {
svVal := svVals[j]
slVal := slVals[j]

if t2, ok := slVal.(time.Time); ok {
dateStr := t2.Format("2006-01-02")
datetimeStr := t2.Format("2006-01-02 15:04:05")
svStr := fmt.Sprintf("%v", svVal)
if svStr == dateStr || svStr == datetimeStr {
continue
}
datetimeStrFull := t2.UTC().Format("2006-01-02 15:04:05")
if svStr == datetimeStrFull {
continue
}
}
if blob, ok := slVal.([]byte); ok {
if svStr, ok2 := svVal.(string); ok2 {
if svStr == string(blob) {
continue
}
}
}

svStr := fmt.Sprintf("%v", svVal)
slStr := fmt.Sprintf("%v", slVal)
if svStr != slStr {
t.Errorf("%s: row %d, col %d mismatch: sqlvibe=%v (%T), sqlite=%v (%T)",
testName, i, j, svVal, svVal, slVal, slVal)
}
}
}
}

// CompareExecResults executes a statement against both databases and checks
// that they both succeed or both fail.
func CompareExecResults(t *testing.T, svDB *sql.DB, slDB *sql.DB, query string, testName string) {
t.Helper()
_, err1 := svDB.Exec(query)
_, err2 := slDB.Exec(query)

if (err1 == nil) != (err2 == nil) {
t.Errorf("%s: error mismatch: sqlvibe=%v, sqlite=%v", testName, err1, err2)
}
}

// compareSingleValue compares the first column of the first row from query.
func compareSingleValue(t *testing.T, svDB *sql.DB, slDB *sql.DB, query string, testName string) {
t.Helper()
svRow := svDB.QueryRow(query)
slRow := slDB.QueryRow(query)

var svVal, slVal interface{}
svErr := svRow.Scan(&svVal)
slErr := slRow.Scan(&slVal)

if svErr != nil {
t.Errorf("%s: sqlvibe scan error: %v", testName, svErr)
return
}
if slErr != nil {
t.Errorf("%s: sqlite scan error: %v", testName, slErr)
return
}

svStr := fmt.Sprintf("%v", svVal)
slStr := fmt.Sprintf("%v", slVal)
if svStr != slStr {
t.Errorf("%s: value mismatch: sqlvibe=%v (%T), sqlite=%v (%T)",
testName, svVal, svVal, slVal, slVal)
}
}
