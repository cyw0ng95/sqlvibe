package sqlvibe

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
)

// vtabState holds the virtual table state for a Database instance.
type vtabState struct {
	mu     sync.RWMutex
	tables map[string]DS.VTab // table name -> vtab instance
}

func newVTabState() *vtabState {
	return &vtabState{tables: make(map[string]DS.VTab)}
}

func (s *vtabState) get(name string) (DS.VTab, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vt, ok := s.tables[strings.ToLower(name)]
	return vt, ok
}

func (s *vtabState) set(name string, vt DS.VTab) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[strings.ToLower(name)] = vt
}

func (s *vtabState) del(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	lower := strings.ToLower(name)
	if _, ok := s.tables[lower]; ok {
		delete(s.tables, lower)
		return true
	}
	return false
}

// parseCreateVirtualTable extracts (name, module, args) from:
//
//	CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module(arg1, arg2, ...)
//
// Returns ("","","",false) if the SQL is not a CREATE VIRTUAL TABLE.
func parseCreateVirtualTable(sql string) (name, module string, args []string, ok bool) {
	up := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(up, "CREATE VIRTUAL TABLE") {
		return
	}
	rest := strings.TrimSpace(sql[len("CREATE VIRTUAL TABLE"):])
	// Optional IF NOT EXISTS
	upRest := strings.ToUpper(rest)
	if strings.HasPrefix(upRest, "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	}
	// Read table name (may be quoted)
	tableName, rest := readIdent(rest)
	if tableName == "" {
		return
	}
	// USING keyword
	rest = strings.TrimSpace(rest)
	upRest2 := strings.ToUpper(rest)
	if !strings.HasPrefix(upRest2, "USING") {
		return
	}
	rest = strings.TrimSpace(rest[5:])
	// Module name
	modName, rest := readIdent(rest)
	if modName == "" {
		return
	}
	rest = strings.TrimSpace(rest)
	// Optional args in parens
	var modArgs []string
	if len(rest) > 0 && rest[0] == '(' {
		rest = rest[1:]
		end := strings.Index(rest, ")")
		if end >= 0 {
			argStr := rest[:end]
			for _, a := range strings.Split(argStr, ",") {
				modArgs = append(modArgs, strings.TrimSpace(a))
			}
		}
	}
	return tableName, modName, modArgs, true
}

// readIdent reads an identifier (possibly quoted with backtick or double-quote) from s.
// Returns (ident, rest).
func readIdent(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", s
	}
	if s[0] == '`' || s[0] == '"' {
		q := s[0]
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			return s[1:], ""
		}
		return s[1 : end+1], strings.TrimSpace(s[end+2:])
	}
	i := 0
	for i < len(s) && (s[i] == '_' || (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	return s[:i], strings.TrimSpace(s[i:])
}

// execCreateVirtualTable handles CREATE VIRTUAL TABLE SQL in the Go layer.
func (db *Database) execCreateVirtualTable(sql string) (bool, error) {
	name, modName, args, ok := parseCreateVirtualTable(sql)
	if !ok {
		return false, nil
	}
	// Check for IF NOT EXISTS
	up := strings.ToUpper(sql)
	ifNotExists := strings.Contains(up, "IF NOT EXISTS")
	if ifNotExists {
		if _, exists := db.vtabs.get(name); exists {
			return true, nil
		}
	} else {
		if _, exists := db.vtabs.get(name); exists {
			return true, fmt.Errorf("table %s already exists", name)
		}
	}
	mod, found := IS.GetVTabModule(strings.ToLower(modName))
	if !found {
		return true, fmt.Errorf("no such module: %s", modName)
	}
	vt, err := mod.Create(args)
	if err != nil {
		return true, err
	}
	db.vtabs.set(name, vt)
	return true, nil
}

// execDropVirtualTable handles DROP TABLE for virtual tables.
// Returns (handled, error). If the table is not virtual, returns (false, nil).
func (db *Database) execDropVirtualTable(sql string) (bool, error) {
	up := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(up, "DROP TABLE") {
		return false, nil
	}
	rest := strings.TrimSpace(sql[10:]) // after "DROP TABLE"
	upRest := strings.ToUpper(rest)
	if strings.HasPrefix(upRest, "IF EXISTS") {
		rest = strings.TrimSpace(rest[9:])
	}
	name, _ := readIdent(rest)
	if name == "" {
		return false, nil
	}
	if !db.vtabs.del(name) {
		return false, nil // not a virtual table, pass to C++
	}
	return true, nil
}

// queryVTab handles SELECT queries against virtual tables.
// Returns (rows, handled, error). If not handled, caller should pass to C++.
func (db *Database) queryVTab(sql string) (*Rows, bool, error) {
	// Detect if the FROM clause references a virtual table or table function.
	// We look for:
	//   SELECT ... FROM name(...) ...  — table function call
	//   SELECT ... FROM vtabName ...  — named virtual table
	up := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(up, "SELECT") {
		return nil, false, nil
	}

	tableName, args, isFunc := extractVTabFromClause(sql)
	if tableName == "" {
		return nil, false, nil
	}

	var vt DS.VTab
	if isFunc {
		// Table function call: resolve module name
		mod, found := IS.GetVTabModule(strings.ToLower(tableName))
		if !found {
			return nil, false, nil
		}
		var err error
		vt, err = mod.Connect(args)
		if err != nil {
			return nil, true, err
		}
	} else {
		var found bool
		vt, found = db.vtabs.get(tableName)
		if !found {
			return nil, false, nil
		}
	}

	// Open cursor and materialize rows
	cols := vt.Columns()
	cursor, err := vt.Open()
	if err != nil {
		return nil, true, err
	}
	defer cursor.Close()

	if err := cursor.Filter(0, "", nil); err != nil {
		return nil, true, err
	}

	var data [][]interface{}
	for !cursor.Eof() {
		row := make([]interface{}, len(cols))
		for i := range cols {
			v, err := cursor.Column(i)
			if err != nil {
				return nil, true, err
			}
			row[i] = v
		}
		data = append(data, row)
		if err := cursor.Next(); err != nil {
			return nil, true, err
		}
	}
	if data == nil {
		data = [][]interface{}{}
	}

	// Apply optional WHERE/ORDER BY/LIMIT/OFFSET from SQL
	rows := applyVTabFilters(sql, cols, data)
	return rows, true, nil
}

// extractVTabFromClause looks for a FROM clause referencing a table function or vtab name.
// Returns (tableName, args, isTableFunction).
func extractVTabFromClause(sql string) (string, []string, bool) {
	// Find FROM keyword (not inside string literals or parens)
	fromIdx := findFromKeyword(sql)
	if fromIdx < 0 {
		return "", nil, false
	}
	after := strings.TrimSpace(sql[fromIdx+4:]) // after "FROM"
	// Check if the table name is followed by '(' — table function syntax
	name, rest := readIdent(after)
	if name == "" {
		return "", nil, false
	}
	rest = strings.TrimSpace(rest)
	if len(rest) > 0 && rest[0] == '(' {
		// Table function: parse args
		rest = rest[1:]
		end := strings.Index(rest, ")")
		if end < 0 {
			return "", nil, false
		}
		argStr := rest[:end]
		var args []string
		for _, a := range strings.Split(argStr, ",") {
			args = append(args, strings.TrimSpace(a))
		}
		return name, args, true
	}
	// Named virtual table
	return name, nil, false
}

// findFromKeyword finds the position of the FROM keyword in sql (case-insensitive).
// Returns -1 if not found.
func findFromKeyword(sql string) int {
	i := 0
	for i < len(sql) {
		if sql[i] == '\'' || sql[i] == '"' {
			q := sql[i]
			i++
			for i < len(sql) && sql[i] != q {
				i++
			}
			if i < len(sql) {
				i++
			}
			continue
		}
		if i+4 <= len(sql) && strings.ToUpper(sql[i:i+4]) == "FROM" {
			// Make sure it's a whole word
			before := i == 0 || !isIdentByte(sql[i-1])
			after := i+4 >= len(sql) || !isIdentByte(sql[i+4])
			if before && after {
				return i
			}
		}
		i++
	}
	return -1
}

func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// applyVTabFilters applies basic WHERE, ORDER BY, LIMIT, OFFSET filters to vtab results.
func applyVTabFilters(sql string, cols []string, data [][]interface{}) *Rows {
	up := strings.ToUpper(sql)

	// Apply WHERE filter (simple value comparison only)
	whereStart := strings.Index(up, " WHERE ")
	if whereStart >= 0 {
		whereExpr := strings.TrimSpace(sql[whereStart+7:])
		// Strip ORDER BY / LIMIT if present
		for _, kw := range []string{"ORDER BY", "GROUP BY", "LIMIT", "HAVING"} {
			if idx := strings.Index(strings.ToUpper(whereExpr), kw); idx >= 0 {
				whereExpr = strings.TrimSpace(whereExpr[:idx])
			}
		}
		data = applyVTabWhere(whereExpr, cols, data)
	}

	// Apply ORDER BY
	if idx := strings.Index(up, " ORDER BY "); idx >= 0 {
		orderStr := strings.TrimSpace(sql[idx+10:])
		if li := strings.Index(strings.ToUpper(orderStr), " LIMIT "); li >= 0 {
			orderStr = orderStr[:li]
		}
		data = applyVTabOrderBy(orderStr, cols, data)
	}

	// Apply LIMIT / OFFSET
	limit, offset := -1, 0
	if li := strings.Index(up, " LIMIT "); li >= 0 {
		limitStr := strings.TrimSpace(sql[li+7:])
		// Check for OFFSET
		if oi := strings.Index(strings.ToUpper(limitStr), " OFFSET "); oi >= 0 {
			if off, err := strconv.Atoi(strings.TrimSpace(limitStr[oi+8:])); err == nil {
				offset = off
			}
			limitStr = strings.TrimSpace(limitStr[:oi])
		}
		if n, err := strconv.Atoi(limitStr); err == nil {
			limit = n
		}
	}
	if offset > 0 {
		if offset >= len(data) {
			data = nil
		} else {
			data = data[offset:]
		}
	}
	if limit >= 0 && limit < len(data) {
		data = data[:limit]
	}

	// Determine output columns
	selCols := cols // default: all
	outCols := cols

	// Parse SELECT list
	selectList := extractSelectList(sql)
	if selectList != "" && selectList != "*" {
		parts := strings.Split(selectList, ",")
		var newCols []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			// Handle AS alias
			up2 := strings.ToUpper(p)
			var colName, alias string
			if idx := strings.Index(up2, " AS "); idx >= 0 {
				colName = strings.TrimSpace(p[:idx])
				alias = strings.TrimSpace(p[idx+4:])
			} else {
				colName = p
				alias = p
			}
			// Handle COUNT(*) and other aggregates
			up3 := strings.ToUpper(colName)
			if strings.HasPrefix(up3, "COUNT(") {
				newCols = append(newCols, alias)
			} else {
				newCols = append(newCols, alias)
			}
			_ = colName
		}
		if len(newCols) > 0 {
			outCols = newCols
			// Remap data
			colIdx := make([]int, len(parts))
			for i, p := range parts {
				p = strings.TrimSpace(p)
				up2 := strings.ToUpper(p)
				colExpr := p
				if idx := strings.Index(up2, " AS "); idx >= 0 {
					colExpr = strings.TrimSpace(p[:idx])
				}
				// Find column index in selCols
				colIdx[i] = -1
				for j, c := range selCols {
					if strings.EqualFold(c, colExpr) {
						colIdx[i] = j
						break
					}
				}
				// Check for COUNT(*) style aggregate
				up3 := strings.ToUpper(colExpr)
				if strings.HasPrefix(up3, "COUNT(") {
					colIdx[i] = -2 // aggregate
				}
			}
			newData := make([][]interface{}, len(data))
			for ri, row := range data {
				newRow := make([]interface{}, len(parts))
				for ci, idx := range colIdx {
					if idx == -2 {
						// COUNT aggregates handled below
						newRow[ci] = nil
					} else if idx >= 0 && idx < len(row) {
						newRow[ci] = row[idx]
					}
				}
				newData[ri] = newRow
			}
			data = newData
		}
	}

	// Handle COUNT(*) aggregate
	for _, p := range strings.Split(extractSelectList(sql), ",") {
		p = strings.TrimSpace(p)
		up3 := strings.ToUpper(p)
		if strings.HasPrefix(up3, "COUNT(") {
			// Return single row with count
			alias := "COUNT(*)"
			if idx := strings.Index(up3, " AS "); idx >= 0 {
				alias = strings.TrimSpace(p[idx+4:])
			}
			count := int64(len(data))
			return &Rows{Columns: []string{alias}, Data: [][]interface{}{{count}}}
		}
	}

	return &Rows{Columns: outCols, Data: data}
}

// applyVTabWhere applies a simple WHERE filter to vtab rows.
func applyVTabWhere(expr string, cols []string, data [][]interface{}) [][]interface{} {
	colIdx := func(name string) int {
		for i, c := range cols {
			if strings.EqualFold(c, name) {
				return i
			}
		}
		return -1
	}

	// Parse simple comparisons: colname op value
	var filtered [][]interface{}
	for _, row := range data {
		if evalSimplePred(expr, cols, row, colIdx) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func evalSimplePred(expr string, cols []string, row []interface{}, colIdx func(string) int) bool {
	expr = strings.TrimSpace(expr)
	// Handle AND
	if idx := findTopLevelAnd(expr); idx >= 0 {
		left := expr[:idx]
		right := expr[idx+3:]
		return evalSimplePred(left, cols, row, colIdx) && evalSimplePred(right, cols, row, colIdx)
	}
	// Handle OR
	if idx := findTopLevelOr(expr); idx >= 0 {
		left := expr[:idx]
		right := expr[idx+2:]
		return evalSimplePred(left, cols, row, colIdx) || evalSimplePred(right, cols, row, colIdx)
	}
	// Try to parse: colname op literal
	for _, op := range []string{"<=", ">=", "<>", "!=", "<", ">", "="} {
		idx := strings.Index(expr, op)
		if idx < 0 {
			continue
		}
		lhs := strings.TrimSpace(expr[:idx])
		rhs := strings.TrimSpace(expr[idx+len(op):])
		ci := colIdx(lhs)
		if ci < 0 || ci >= len(row) {
			continue
		}
		val := row[ci]
		rhsVal := parseVTabLiteral(rhs)
		cmp := compareVals(val, rhsVal)
		switch op {
		case "=":
			return cmp == 0
		case "<>", "!=":
			return cmp != 0
		case "<":
			return cmp < 0
		case "<=":
			return cmp <= 0
		case ">":
			return cmp > 0
		case ">=":
			return cmp >= 0
		}
	}
	return true // unknown predicate: pass through
}

func findTopLevelAnd(s string) int {
	up := strings.ToUpper(s)
	i := 0
	depth := 0
	for i < len(up) {
		if up[i] == '(' {
			depth++
		} else if up[i] == ')' {
			depth--
		} else if depth == 0 && i+4 <= len(up) && up[i:i+4] == " AND" {
			if i+4 == len(up) || !isIdentByte(up[i+4]) {
				return i
			}
		}
		i++
	}
	return -1
}

func findTopLevelOr(s string) int {
	up := strings.ToUpper(s)
	i := 0
	depth := 0
	for i < len(up) {
		if up[i] == '(' {
			depth++
		} else if up[i] == ')' {
			depth--
		} else if depth == 0 && i+3 <= len(up) && up[i:i+3] == " OR" {
			if i+3 == len(up) || !isIdentByte(up[i+3]) {
				return i
			}
		}
		i++
	}
	return -1
}

func parseVTabLiteral(s string) interface{} {
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') {
		return s[1 : len(s)-1]
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func compareVals(a, b interface{}) int {
	ai := toFloat64(a)
	bi := toFloat64(b)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case float64:
		return x
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f
		}
	}
	return 0
}

// applyVTabOrderBy sorts vtab rows by the specified column(s).
func applyVTabOrderBy(orderStr string, cols []string, data [][]interface{}) [][]interface{} {
	// Simple: sort by first column in order list
	parts := strings.SplitN(orderStr, ",", 2)
	part := strings.TrimSpace(parts[0])
	up := strings.ToUpper(part)
	desc := strings.HasSuffix(up, " DESC")
	colName := part
	if desc {
		colName = strings.TrimSpace(part[:len(part)-5])
	} else if strings.HasSuffix(up, " ASC") {
		colName = strings.TrimSpace(part[:len(part)-4])
	}
	ci := -1
	for i, c := range cols {
		if strings.EqualFold(c, colName) {
			ci = i
			break
		}
	}
	if ci < 0 || len(data) == 0 {
		return data
	}

	// Simple insertion sort for small datasets
	sorted := make([][]interface{}, len(data))
	copy(sorted, data)
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 {
			cmp := compareVals(sorted[j][ci], key[ci])
			if (!desc && cmp <= 0) || (desc && cmp >= 0) {
				break
			}
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}
	return sorted
}

// extractSelectList extracts the column list from a SELECT statement.
// e.g., "SELECT a, b FROM t" → "a, b"
func extractSelectList(sql string) string {
	up := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(up, "SELECT") {
		return ""
	}
	after := strings.TrimSpace(sql[6:])
	up2 := strings.ToUpper(after)
	// Skip DISTINCT
	if strings.HasPrefix(up2, "DISTINCT ") {
		after = strings.TrimSpace(after[9:])
	}
	fromIdx := findFromKeyword(sql)
	if fromIdx < 0 {
		return after
	}
	// The select list is between SELECT and FROM
	selIdx := strings.Index(strings.ToUpper(sql), "SELECT") + 6
	list := strings.TrimSpace(sql[selIdx:fromIdx])
	return list
}
