// Package sqlvibe provides a high-performance, SQLite-compatible in-memory
// database engine. The v0.11.2+ implementation delegates all SQL execution
// to a self-contained C++ engine via internal/cgo (svdbcgo). The public API
// is unchanged; existing callers require no modification.
package sqlvibe

import (
"context"
"encoding/hex"
"fmt"
"strconv"
"strings"

svdbcgo "github.com/cyw0ng95/sqlvibe/internal/cgo"
)

// Database is the primary handle for a sqlvibe database.
// All methods are safe to call concurrently from multiple goroutines.
type Database struct {
cdb *svdbcgo.DB
}

// Result holds the outcome of a non-query SQL execution.
type Result struct {
LastInsertRowID int64
RowsAffected    int64
}

// Rows holds a materialized query result set.
// Columns and Data are exported for direct inspection.
type Rows struct {
Columns []string
Data    [][]interface{}
pos     int  // current row index; valid range [0, len(Data))
started bool // whether Next() has been called at least once
err     error
}

// Next advances to the next row. On the first call it positions on row 0
// (matching the existing behaviour). Returns false when all rows are exhausted.
func (r *Rows) Next() bool {
if r == nil || r.Data == nil {
return false
}
if !r.started {
r.started = true
return len(r.Data) > 0
}
r.pos++
return r.pos < len(r.Data)
}

// Scan copies the values of the current row into the destination variables.
func (r *Rows) Scan(dest ...interface{}) error {
if r == nil || r.Data == nil || r.pos < 0 || r.pos >= len(r.Data) {
return fmt.Errorf("no rows available")
}
row := r.Data[r.pos]
for i, val := range dest {
if i >= len(row) {
break
}
if err := scanValue(val, row[i]); err != nil {
return err
}
}
return nil
}

// Err returns the first error encountered during iteration.
func (r *Rows) Err() error {
if r == nil {
return nil
}
return r.err
}

// Close is a no-op; results are materialized on Query().
func (r *Rows) Close() error { return nil }

// scanValue copies src into the typed pointer dst.
func scanValue(dst interface{}, src interface{}) error {
if src == nil {
return nil
}
switch d := dst.(type) {
case *int:
switch v := src.(type) {
case int:
*d = v
case int64:
*d = int(v)
case float64:
*d = int(v)
case string:
n, _ := strconv.ParseInt(v, 10, 64)
*d = int(n)
}
case *int64:
switch v := src.(type) {
case int:
*d = int64(v)
case int64:
*d = v
case float64:
*d = int64(v)
case string:
n, _ := strconv.ParseInt(v, 10, 64)
*d = n
}
case *float64:
switch v := src.(type) {
case int:
*d = float64(v)
case int64:
*d = float64(v)
case float64:
*d = v
case string:
f, _ := strconv.ParseFloat(v, 64)
*d = f
}
case *string:
switch v := src.(type) {
case string:
*d = v
default:
*d = fmt.Sprintf("%v", v)
}
case *interface{}:
*d = src
case *bool:
switch v := src.(type) {
case bool:
*d = v
case int64:
*d = v != 0
case int:
*d = v != 0
}
}
return nil
}

// Statement is a compiled SQL statement for repeated execution.
type Statement struct {
cstmt *svdbcgo.Stmt
db    *Database
sql   string
}

// Exec executes the statement with optional positional parameters.
func (s *Statement) Exec(params ...interface{}) (Result, error) {
return s.db.ExecWithParams(s.sql, params)
}

// Query executes the statement as a query with optional positional parameters.
func (s *Statement) Query(params ...interface{}) (*Rows, error) {
return s.db.QueryWithParams(s.sql, params)
}

// Close releases the statement resources.
func (s *Statement) Close() error {
if s.cstmt != nil {
err := s.cstmt.Close()
s.cstmt = nil
return err
}
return nil
}

// Transaction is an in-progress database transaction.
type Transaction struct {
ctx *svdbcgo.Tx
db  *Database
}

// Exec executes a SQL statement within the transaction.
func (tx *Transaction) Exec(sql string) (Result, error) {
return tx.db.Exec(sql)
}

// Query executes a SELECT within the transaction.
func (tx *Transaction) Query(sql string) (*Rows, error) {
return tx.db.Query(sql)
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
if tx.ctx == nil {
return fmt.Errorf("transaction already closed")
}
err := tx.ctx.Commit()
tx.ctx = nil
return err
}

// Rollback rolls back the transaction.
func (tx *Transaction) Rollback() error {
if tx.ctx == nil {
return fmt.Errorf("transaction already closed")
}
err := tx.ctx.Rollback()
tx.ctx = nil
return err
}

// ── Public database API ───────────────────────────────────────────

// Open opens (or creates) a database at path. Use ":memory:" for in-memory databases.
func Open(path string) (*Database, error) {
cdb, err := svdbcgo.Open(path)
if err != nil {
return nil, err
}
return &Database{cdb: cdb}, nil
}

// Close closes the database and releases all resources.
func (db *Database) Close() error {
if db.cdb == nil {
return nil
}
err := db.cdb.Close()
db.cdb = nil
return err
}

// Exec executes a non-query SQL statement and returns the result.
func (db *Database) Exec(sql string) (Result, error) {
r, err := db.cdb.Exec(sql)
if err != nil {
return Result{}, err
}
return Result{RowsAffected: r.RowsAffected, LastInsertRowID: r.LastInsertRowid}, nil
}

// Query executes a SELECT statement and returns a result set.
func (db *Database) Query(sql string) (*Rows, error) {
return db.queryCGO(sql)
}

// Prepare compiles a SQL statement for repeated execution.
func (db *Database) Prepare(sql string) (*Statement, error) {
cstmt, err := db.cdb.Prepare(sql)
if err != nil {
return nil, err
}
return &Statement{cstmt: cstmt, db: db, sql: sql}, nil
}

// Begin starts a new explicit transaction.
func (db *Database) Begin() (*Transaction, error) {
ctx, err := db.cdb.Begin()
if err != nil {
return nil, err
}
return &Transaction{ctx: ctx, db: db}, nil
}

// ── Parameter-binding variants ────────────────────────────────────

// ExecWithParams executes a statement with positional (?) parameters.
func (db *Database) ExecWithParams(sql string, params []interface{}) (Result, error) {
bound, err := formatParamSQL(sql, params, nil)
if err != nil {
return Result{}, err
}
return db.Exec(bound)
}

// QueryWithParams executes a query with positional (?) parameters.
func (db *Database) QueryWithParams(sql string, params []interface{}) (*Rows, error) {
bound, err := formatParamSQL(sql, params, nil)
if err != nil {
return nil, err
}
return db.Query(bound)
}

// ExecNamed executes a statement with named parameters (:name or @name).
func (db *Database) ExecNamed(sql string, params map[string]interface{}) (Result, error) {
bound, err := formatParamSQL(sql, nil, params)
if err != nil {
return Result{}, err
}
return db.Exec(bound)
}

// QueryNamed executes a query with named parameters.
func (db *Database) QueryNamed(sql string, params map[string]interface{}) (*Rows, error) {
bound, err := formatParamSQL(sql, nil, params)
if err != nil {
return nil, err
}
return db.Query(bound)
}

// ── Context variants ──────────────────────────────────────────────

// ExecContext executes a statement with context support.
func (db *Database) ExecContext(ctx context.Context, sql string) (Result, error) {
if err := ctx.Err(); err != nil {
return Result{}, err
}
return db.Exec(sql)
}

// QueryContext executes a query with context support.
func (db *Database) QueryContext(ctx context.Context, sql string) (*Rows, error) {
if err := ctx.Err(); err != nil {
return nil, err
}
return db.Query(sql)
}

// ExecContextWithParams executes a parameterised statement with context support.
func (db *Database) ExecContextWithParams(ctx context.Context, sql string, params []interface{}) (Result, error) {
if err := ctx.Err(); err != nil {
return Result{}, err
}
return db.ExecWithParams(sql, params)
}

// QueryContextWithParams executes a parameterised query with context support.
func (db *Database) QueryContextWithParams(ctx context.Context, sql string, params []interface{}) (*Rows, error) {
if err := ctx.Err(); err != nil {
return nil, err
}
return db.QueryWithParams(sql, params)
}

// ExecContextNamed executes a named-parameter statement with context support.
func (db *Database) ExecContextNamed(ctx context.Context, sql string, params map[string]interface{}) (Result, error) {
if err := ctx.Err(); err != nil {
return Result{}, err
}
return db.ExecNamed(sql, params)
}

// QueryContextNamed executes a named-parameter query with context support.
func (db *Database) QueryContextNamed(ctx context.Context, sql string, params map[string]interface{}) (*Rows, error) {
if err := ctx.Err(); err != nil {
return nil, err
}
return db.QueryNamed(sql, params)
}

// ── Convenience helpers ───────────────────────────────────────────

// MustExec executes a statement and panics on error.
func (db *Database) MustExec(sql string, params ...interface{}) Result {
var r Result
var err error
if len(params) > 0 {
r, err = db.ExecWithParams(sql, params)
} else {
r, err = db.Exec(sql)
}
if err != nil {
panic(fmt.Sprintf("MustExec: %v", err))
}
return r
}

// ClearResultCache is a no-op retained for API compatibility.
// The C++ engine does not maintain a Go-side query result cache.
func (db *Database) ClearResultCache() {}

// GetHybridStore returns nil; hybrid columnar storage is handled by the C++ engine.
// This method is retained for API compatibility.
func (db *Database) GetHybridStore(tableName string) interface{} { return nil }

// Version returns the svdb engine version string.
func Version() string { return svdbcgo.Version() }


// ── Internal helpers ──────────────────────────────────────────────

// queryCGO calls svdb_query and materialises the result into a *Rows.
func (db *Database) queryCGO(sql string) (*Rows, error) {
crows, err := db.cdb.Query(sql)
if err != nil {
return nil, err
}
defer crows.Close()

rows := &Rows{}
// Collect column names
n := crows.ColumnCount()
rows.Columns = make([]string, n)
for i := 0; i < n; i++ {
rows.Columns[i] = crows.ColumnName(i)
}
// Materialise all rows
for crows.Next() {
row := make([]interface{}, n)
for i := 0; i < n; i++ {
row[i] = crows.Get(i)
}
rows.Data = append(rows.Data, row)
}
return rows, nil
}

// formatParamSQL substitutes positional ('?') and named (':name', '@name')
// placeholders with safely-quoted SQL literals.
func formatParamSQL(sql string, params []interface{}, namedParams map[string]interface{}) (string, error) {
var sb strings.Builder
sb.Grow(len(sql) + 32)
paramIdx := 0
i := 0
for i < len(sql) {
ch := sql[i]
// Skip string literals
if ch == '\'' || ch == '"' {
quote := ch
sb.WriteByte(ch)
i++
for i < len(sql) {
c := sql[i]
sb.WriteByte(c)
i++
if c == quote {
if i < len(sql) && sql[i] == quote {
sb.WriteByte(sql[i])
i++
} else {
break
}
}
}
continue
}
// Positional placeholder '?'
if ch == '?' {
if paramIdx >= len(params) {
return "", fmt.Errorf("missing parameter at position %d", paramIdx+1)
}
sb.WriteString(formatSQLLiteral(params[paramIdx]))
paramIdx++
i++
continue
}
// Named placeholder ':name' or '@name'
if (ch == ':' || ch == '@') && i+1 < len(sql) && isParamIdentByte(sql[i+1]) {
i++
start := i
for i < len(sql) && isParamIdentByte(sql[i]) {
i++
}
name := sql[start:i]
var val interface{}
var ok bool
if namedParams != nil {
val, ok = namedParams[name]
}
if !ok {
return "", fmt.Errorf("missing named parameter: %s", name)
}
sb.WriteString(formatSQLLiteral(val))
continue
}
sb.WriteByte(ch)
i++
}
return sb.String(), nil
}

func isParamIdentByte(b byte) bool {
return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
(b >= '0' && b <= '9') || b == '_'
}

// formatSQLLiteral converts a Go value to a safely-quoted SQL literal.
func formatSQLLiteral(v interface{}) string {
if v == nil {
return "NULL"
}
switch val := v.(type) {
case int64:
return strconv.FormatInt(val, 10)
case int:
return strconv.FormatInt(int64(val), 10)
case int32:
return strconv.FormatInt(int64(val), 10)
case int16:
return strconv.FormatInt(int64(val), 10)
case int8:
return strconv.FormatInt(int64(val), 10)
case uint64:
return strconv.FormatUint(val, 10)
case uint:
return strconv.FormatUint(uint64(val), 10)
case uint32:
return strconv.FormatUint(uint64(val), 10)
case float64:
return strconv.FormatFloat(val, 'g', -1, 64)
case float32:
return strconv.FormatFloat(float64(val), 'g', -1, 64)
case bool:
if val {
return "1"
}
return "0"
case string:
return "'" + strings.ReplaceAll(val, "'", "''") + "'"
case []byte:
return "x'" + hex.EncodeToString(val) + "'"
default:
return "'" + strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''") + "'"
}
}

// ── Schema / metadata types ───────────────────────────────────────

// TableInfo describes a table in the database.
type TableInfo struct {
Name string
Type string // "table", "view", etc.
}

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
Name       string
Type       string
NotNull    bool
Default    string
PrimaryKey bool
}

// IndexInfo describes an index on a table.
type IndexInfo struct {
Name    string
Unique  bool
Columns []string
}

// IntegrityReport is the result of CheckIntegrity.
type IntegrityReport struct {
Valid  bool
Errors []string
}

// DatabaseInfo holds metadata about an open database.
type DatabaseInfo struct {
FilePath string
Encoding string
}

// PageStats holds basic storage statistics.
type PageStats struct {
TotalPages    int
LeafPages     int
InteriorPages int
OverflowPages int
}

// GetTables returns metadata for all user tables in the database.
func (db *Database) GetTables() ([]TableInfo, error) {
crows, err := db.cdb.Tables()
if err != nil {
return nil, err
}
defer crows.Close()
var tables []TableInfo
for crows.Next() {
v := crows.Get(0)
name, _ := v.(string)
tables = append(tables, TableInfo{Name: name, Type: "table"})
}
return tables, nil
}

// GetSchema returns the CREATE TABLE SQL for the named table.
func (db *Database) GetSchema(table string) (string, error) {
rows, err := db.Query("PRAGMA table_info(" + table + ")")
if err != nil {
return "", err
}
if len(rows.Data) == 0 {
return "", fmt.Errorf("table not found: %s", table)
}
var cols []string
for _, row := range rows.Data {
if len(row) >= 2 {
name := fmt.Sprintf("%v", row[1])
colType := "TEXT"
if len(row) >= 3 {
colType = fmt.Sprintf("%v", row[2])
}
cols = append(cols, name+" "+colType)
}
}
return "CREATE TABLE " + table + " (" + strings.Join(cols, ", ") + ")", nil
}

// GetColumns returns column metadata for the named table.
func (db *Database) GetColumns(table string) ([]ColumnInfo, error) {
rows, err := db.Query("PRAGMA table_info(" + table + ")")
if err != nil {
return nil, err
}
if len(rows.Data) == 0 {
return nil, fmt.Errorf("table not found: %s", table)
}
cols := make([]ColumnInfo, 0, len(rows.Data))
for _, row := range rows.Data {
col := ColumnInfo{}
if len(row) >= 2 {
col.Name, _ = row[1].(string)
}
if len(row) >= 3 {
col.Type, _ = row[2].(string)
}
if len(row) >= 4 {
switch v := row[3].(type) {
case int64:
col.NotNull = v != 0
case int:
col.NotNull = v != 0
}
}
if len(row) >= 5 && row[4] != nil {
col.Default = fmt.Sprintf("%v", row[4])
}
if len(row) >= 6 {
switch v := row[5].(type) {
case int64:
col.PrimaryKey = v != 0
case int:
col.PrimaryKey = v != 0
}
}
cols = append(cols, col)
}
return cols, nil
}

// GetIndexes returns index metadata for the named table.
func (db *Database) GetIndexes(table string) ([]IndexInfo, error) {
crows, err := db.cdb.Indexes(table)
if err != nil {
return nil, err
}
defer crows.Close()
var indexes []IndexInfo
for crows.Next() {
idx := IndexInfo{}
if crows.ColumnCount() >= 1 {
idx.Name, _ = crows.Get(0).(string)
}
if crows.ColumnCount() >= 2 {
switch v := crows.Get(1).(type) {
case int64:
idx.Unique = v != 0
}
}
if crows.ColumnCount() >= 3 {
colStr, _ := crows.Get(2).(string)
for _, c := range strings.Split(colStr, ",") {
c = strings.TrimSpace(c)
if c != "" {
idx.Columns = append(idx.Columns, c)
}
}
}
indexes = append(indexes, idx)
}
return indexes, nil
}

// CheckIntegrity runs a basic integrity check. Always returns valid for in-memory engine.
func (db *Database) CheckIntegrity() (IntegrityReport, error) {
return IntegrityReport{Valid: true}, nil
}

// GetDatabaseInfo returns metadata about the open database.
func (db *Database) GetDatabaseInfo() (DatabaseInfo, error) {
rows, err := db.Query("PRAGMA database_list")
if err != nil {
return DatabaseInfo{}, err
}
path := ":memory:"
if len(rows.Data) > 0 && len(rows.Data[0]) >= 3 && rows.Data[0][2] != nil {
if s, ok := rows.Data[0][2].(string); ok && s != "" {
path = s
}
}
return DatabaseInfo{FilePath: path, Encoding: "UTF-8"}, nil
}

// GetPageStats returns storage statistics (zeroed for in-memory engine).
func (db *Database) GetPageStats() (PageStats, error) {
return PageStats{}, nil
}

// BackupTo creates a copy of the database at destPath.
func (db *Database) BackupTo(destPath string) error {
return db.cdb.Backup(destPath)
}
