// Package profiling provides query profiling and EXPLAIN ANALYZE functionality.
// This is a debug-only extension and should not be included in release builds.
//
// Enable with: go build -tags SVDB_EXT_PROFILING ./...
package profiling

import (
	"fmt"
	"strings"
	"time"

	"github.com/cyw0ng95/sqlvibe/ext"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func init() {
	ext.Register("profiling", &ProfilingExtension{})
}

// ProfilingExtension provides query profiling and EXPLAIN ANALYZE.
type ProfilingExtension struct {
	enabled        bool
	profileBuffer  []QueryProfile
	slowThreshold  int64 // milliseconds
	slowBuffer     []SlowQueryEntry
}

// QueryProfile holds profiling information for a query.
type QueryProfile struct {
	Query  string
	Plan   string
	TimeMs float64
	Rows   int64
}

// SlowQueryEntry holds a slow query log entry.
type SlowQueryEntry struct {
	Query  string
	TimeMs float64
	Plan   string
}

// Name returns the extension name.
func (e *ProfilingExtension) Name() string { return "profiling" }

// Description returns the extension description.
func (e *ProfilingExtension) Description() string {
	return "Query profiling and EXPLAIN ANALYZE (debug only)"
}

// Functions returns the list of SQL functions provided by this extension.
func (e *ProfilingExtension) Functions() []string {
	return []string{}
}

// Opcodes returns the list of VM opcodes provided by this extension.
func (e *ProfilingExtension) Opcodes() []ext.Opcode {
	return nil
}

// Register registers the extension with the database.
func (e *ProfilingExtension) Register(db interface{}) error {
	return nil
}

// Close closes the extension.
func (e *ProfilingExtension) Close() error {
	return nil
}

// CallFunc calls a SQL function provided by this extension.
func (e *ProfilingExtension) CallFunc(name string, args []interface{}) interface{} {
	return nil
}

// TableFunctions returns the table-valued functions provided by this extension.
func (e *ProfilingExtension) TableFunctions() []ext.TableFunction {
	return []ext.TableFunction{
		{Name: "sqlvibe_profile", MinArgs: 0, MaxArgs: 0, Rows: e.profileRows},
		{Name: "sqlvibe_slowlog", MinArgs: 0, MaxArgs: 0, Rows: e.slowlogRows},
	}
}

// HandlePragma handles profiling-related PRAGMA statements.
func (e *ProfilingExtension) HandlePragma(name string, value string) (string, error) {
	switch strings.ToUpper(name) {
	case "PROFILE":
		if value != "" {
			val := strings.ToUpper(value)
			e.enabled = (val == "ON" || val == "1")
			return val, nil
		}
		if e.enabled {
			return "on", nil
		}
		return "off", nil

	case "SLOWLOG":
		if value != "" {
			var threshold int64
			fmt.Sscanf(value, "%d", &threshold)
			if threshold < 0 {
				threshold = 0
			}
			e.slowThreshold = threshold
			return fmt.Sprintf("%d", threshold), nil
		}
		return fmt.Sprintf("%d", e.slowThreshold), nil

	default:
		return "", fmt.Errorf("unknown profiling pragma: %s", name)
	}
}

// ProfileQuery records a query execution profile.
func (e *ProfilingExtension) ProfileQuery(query, plan string, timeMs float64, rows int64) {
	if !e.enabled {
		return
	}

	profile := QueryProfile{
		Query:  query,
		Plan:   plan,
		TimeMs: timeMs,
		Rows:   rows,
	}

	// Add to profile buffer (keep last 100)
	e.profileBuffer = append(e.profileBuffer, profile)
	if len(e.profileBuffer) > 100 {
		e.profileBuffer = e.profileBuffer[1:]
	}

	// Check if slow query
	if e.slowThreshold > 0 && timeMs > float64(e.slowThreshold) {
		slowEntry := SlowQueryEntry{
			Query:  query,
			TimeMs: timeMs,
			Plan:   plan,
		}
		e.slowBuffer = append(e.slowBuffer, slowEntry)
		if len(e.slowBuffer) > 100 {
			e.slowBuffer = e.slowBuffer[1:]
		}
	}
}

// profileRows returns the profile buffer as table function rows.
func (e *ProfilingExtension) profileRows(args []interface{}) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(e.profileBuffer))
	for _, p := range e.profileBuffer {
		rows = append(rows, map[string]interface{}{
			"query":   p.Query,
			"plan":    p.Plan,
			"time_ms": p.TimeMs,
			"rows":    p.Rows,
		})
	}
	return rows, nil
}

// slowlogRows returns the slow query log as table function rows.
func (e *ProfilingExtension) slowlogRows(args []interface{}) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(e.slowBuffer))
	for _, s := range e.slowBuffer {
		rows = append(rows, map[string]interface{}{
			"query":   s.Query,
			"time_ms": s.TimeMs,
			"plan":    s.Plan,
		})
	}
	return rows, nil
}

// GetProfileBuffer returns the current profile buffer.
func (e *ProfilingExtension) GetProfileBuffer() []QueryProfile {
	return e.profileBuffer
}

// GetSlowBuffer returns the current slow query buffer.
func (e *ProfilingExtension) GetSlowBuffer() []SlowQueryEntry {
	return e.slowBuffer
}

// IsEnabled returns whether profiling is enabled.
func (e *ProfilingExtension) IsEnabled() bool {
	return e.enabled
}

// SetEnabled sets whether profiling is enabled.
func (e *ProfilingExtension) SetEnabled(enabled bool) {
	e.enabled = enabled
}

// SetSlowThreshold sets the slow query threshold in milliseconds.
func (e *ProfilingExtension) SetSlowThreshold(ms int64) {
	e.slowThreshold = ms
}

// GetSlowThreshold returns the slow query threshold in milliseconds.
func (e *ProfilingExtension) GetSlowThreshold() int64 {
	return e.slowThreshold
}

// WrapQuery executes a query function and profiles it.
func (e *ProfilingExtension) WrapQuery(query string, fn func() (*sqlvibe.Rows, error), getPlan func() string) (*sqlvibe.Rows, error) {
	startTime := time.Now()
	rows, err := fn()
	execTime := time.Since(startTime).Seconds() * 1000 // Convert to ms

	if err == nil && e.enabled {
		rowCount := int64(0)
		if rows != nil {
			rowCount = int64(len(rows.Data))
		}
		plan := ""
		if getPlan != nil {
			plan = getPlan()
		}
		e.ProfileQuery(query, plan, execTime, rowCount)
	}

	return rows, err
}

// ExplainAnalyze executes a query and returns EXPLAIN ANALYZE output.
func (e *ProfilingExtension) ExplainAnalyze(db *sqlvibe.Database, stmt *QP.ExplainStmt, sql string) (*sqlvibe.Rows, error) {
	// Strip "EXPLAIN ANALYZE" prefix from SQL
	innerSQL := strings.TrimPrefix(sql, "EXPLAIN ANALYZE ")
	innerSQL = strings.TrimPrefix(innerSQL, "EXPLAIN")
	innerSQL = strings.TrimSpace(innerSQL)

	// Execute the query and collect statistics
	startTime := time.Now()
	rows, err := db.Query(innerSQL)
	execTime := time.Since(startTime).Seconds() * 1000 // Convert to ms

	if err != nil {
		return nil, err
	}

	// Count rows returned
	rowCount := int64(0)
	if rows != nil {
		rowCount = int64(len(rows.Data))
	}

	// Build output
	cols := []string{"QUERY PLAN", "ANALYZE"}
	queryPlan := buildSelectPlan(db, stmt.Query)

	analyzeInfo := fmt.Sprintf("run_time=%.3f ms, rows_returned=%d", execTime, rowCount)

	analyzeRows := make([][]interface{}, 0)
	for i, line := range queryPlan {
		if i == 0 {
			analyzeRows = append(analyzeRows, []interface{}{line, analyzeInfo})
		} else {
			analyzeRows = append(analyzeRows, []interface{}{line, ""})
		}
	}

	return &sqlvibe.Rows{Columns: cols, Data: analyzeRows}, nil
}

// buildSelectPlan builds plan lines for EXPLAIN ANALYZE.
func buildSelectPlan(db *sqlvibe.Database, query QP.ASTNode) []string {
	if query.NodeType() != "SelectStmt" {
		return []string{"|--" + strings.ToUpper(query.NodeType())}
	}

	sel := query.(*QP.SelectStmt)
	var lines []string

	if sel.From != nil {
		lines = append(lines, buildTableRefPlan(db, sel.From, sel.Where)...)
	}

	if len(sel.GroupBy) > 0 {
		lines = append(lines, "|--USE TEMP B-TREE (GROUP BY)")
	}

	if len(sel.OrderBy) > 0 {
		lines = append(lines, "|--USE TEMP B-TREE (ORDER BY)")
	}

	return lines
}

// buildTableRefPlan builds plan lines for a table reference.
func buildTableRefPlan(db *sqlvibe.Database, ref *QP.TableRef, where QP.Expr) []string {
	var lines []string

	if ref.Subquery != nil {
		alias := ref.Alias
		if alias == "" {
			alias = "subquery"
		}
		lines = append(lines, "|--SCAN "+alias+" (subquery)")
		return lines
	}

	if ref.Join == nil {
		lines = append(lines, tableAccessLine(db, ref, where))
		return lines
	}

	cur := ref
	visited := make(map[*QP.TableRef]bool)
	for cur != nil && !visited[cur] {
		visited[cur] = true
		lines = append(lines, tableAccessLine(db, cur, where))
		if cur.Join != nil {
			cur = cur.Join.Right
		} else {
			break
		}
	}
	return lines
}

// tableAccessLine produces a SCAN/SEARCH line for a table.
func tableAccessLine(db *sqlvibe.Database, ref *QP.TableRef, where QP.Expr) string {
	name := ref.Name
	if ref.Alias != "" {
		name = ref.Alias
	}

	if whereUsesPrimaryKey(db, ref.Name, where) {
		return "|--SEARCH " + name + " USING INTEGER PRIMARY KEY (rowid=?)"
	}
	return "|--SCAN " + name
}

// whereUsesPrimaryKey returns true when WHERE clause is an equality on primary key.
func whereUsesPrimaryKey(db *sqlvibe.Database, tableName string, where QP.Expr) bool {
	bin, ok := where.(*QP.BinaryExpr)
	if !ok {
		return false
	}

	if bin.Op != QP.TokenEq {
		return false
	}

	colRef, ok := bin.Left.(*QP.ColumnRef)
	if !ok {
		return false
	}

	colName := strings.ToLower(colRef.Name)
	return colName == "id" && (colRef.Table == "" || strings.EqualFold(colRef.Table, tableName))
}
