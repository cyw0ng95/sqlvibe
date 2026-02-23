package PlainFuzzer

import (
	"fmt"
	"math/rand"
	"strings"
)

// Schema types for tracking created tables and columns
type ColumnType string

const (
	ColumnTypeInteger ColumnType = "INTEGER"
	ColumnTypeReal    ColumnType = "REAL"
	ColumnTypeText    ColumnType = "TEXT"
	ColumnTypeBlob    ColumnType = "BLOB"
	ColumnTypeUnknown ColumnType = ""
)

type ColumnInfo struct {
	Name string
	Type ColumnType
}

type TableSchema struct {
	Name       string
	Columns    []ColumnInfo
	PrimaryKey int // column index with PRIMARY KEY, -1 if none
}

// SchemaTracker tracks all tables and columns created during fuzzing
type SchemaTracker struct {
	Tables    map[string]*TableSchema
	NextTable int
	Rand      *rand.Rand
}

func NewSchemaTracker(seed int64) *SchemaTracker {
	return &SchemaTracker{
		Tables:    make(map[string]*TableSchema),
		NextTable: 0,
		Rand:      rand.New(rand.NewSource(seed)),
	}
}

// AddTable creates a new table and returns its name
func (s *SchemaTracker) AddTable(numCols int, hasPrimaryKey bool) string {
	tableName := fmt.Sprintf("t%d", s.NextTable)
	s.NextTable++

	table := &TableSchema{
		Name:       tableName,
		Columns:    make([]ColumnInfo, numCols),
		PrimaryKey: -1,
	}

	// Generate random column types
	types := []ColumnType{
		ColumnTypeInteger, ColumnTypeInteger, ColumnTypeInteger, // Higher chance of INTEGER
		ColumnTypeReal,
		ColumnTypeText,
		ColumnTypeText,
		ColumnTypeBlob,
	}

	for i := 0; i < numCols; i++ {
		table.Columns[i] = ColumnInfo{
			Name: fmt.Sprintf("c%d", i),
			Type: types[s.Rand.Intn(len(types))],
		}
	}

	// Add PRIMARY KEY to first column if requested
	if hasPrimaryKey {
		table.PrimaryKey = 0
		table.Columns[0].Type = ColumnTypeInteger
	}

	s.Tables[tableName] = table
	return tableName
}

// GetRandomTable returns a random existing table, or "" if none exist
func (s *SchemaTracker) GetRandomTable() string {
	if len(s.Tables) == 0 {
		return ""
	}
	tables := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		tables = append(tables, name)
	}
	return tables[s.Rand.Intn(len(tables))]
}

// GetTable returns a specific table schema
func (s *SchemaTracker) GetTable(name string) *TableSchema {
	return s.Tables[name]
}

// HasTables returns true if any tables exist
func (s *SchemaTracker) HasTables() bool {
	return len(s.Tables) > 0
}

// DropTable removes a table from the tracker
func (s *SchemaTracker) DropTable(name string) {
	delete(s.Tables, name)
}

// Reset clears all tracked schema
func (s *SchemaTracker) Reset() {
	s.Tables = make(map[string]*TableSchema)
	s.NextTable = 0
}

// GenerateColumnName returns a random column name from existing tables
func (s *SchemaTracker) GenerateColumnName(tableName string) string {
	table := s.Tables[tableName]
	if table == nil || len(table.Columns) == 0 {
		return "c0"
	}
	return table.Columns[s.Rand.Intn(len(table.Columns))].Name
}

// GetColumnType returns the type of a column
func (s *SchemaTracker) GetColumnType(tableName, colName string) ColumnType {
	table := s.Tables[tableName]
	if table == nil {
		return ColumnTypeUnknown
	}
	for _, col := range table.Columns {
		if col.Name == colName {
			return col.Type
		}
	}
	return ColumnTypeUnknown
}

type SQLGenerator struct {
	rand *rand.Rand
	Schema *SchemaTracker
}

func NewSQLGenerator(seed int64) *SQLGenerator {
	return &SQLGenerator{
		rand:   rand.New(rand.NewSource(seed)),
		Schema: NewSchemaTracker(seed),
	}
}

func (g *SQLGenerator) GenerateCreateTable() string {
	types := []string{
		"INTEGER", "INT", "SMALLINT", "BIGINT",
		"REAL", "FLOAT", "DOUBLE",
		"TEXT", "VARCHAR(255)", "CHAR(10)",
		"BLOB",
	}
	numCols := g.rand.Intn(6) + 1
	cols := make([]string, numCols)

	for i := 0; i < numCols; i++ {
		colName := fmt.Sprintf("c%d", i)
		colType := types[g.rand.Intn(len(types))]
		cols[i] = fmt.Sprintf("%s %s", colName, colType)
	}

	if g.rand.Intn(3) == 0 {
		cols = append(cols, fmt.Sprintf("c%d INTEGER PRIMARY KEY", numCols))
	}

	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	return fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(cols, ", "))
}

func (g *SQLGenerator) GenerateDropTable() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
}

func (g *SQLGenerator) GenerateCreateIndex() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	colIdx := g.rand.Intn(5)
	idxName := fmt.Sprintf("idx%d", g.rand.Intn(20))

	if g.rand.Intn(2) == 0 {
		return fmt.Sprintf("CREATE INDEX %s ON %s(c%d)", idxName, tableName, colIdx)
	}
	return fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s(c%d)", idxName, tableName, colIdx)
}

func (g *SQLGenerator) GenerateDropIndex() string {
	idxName := fmt.Sprintf("idx%d", g.rand.Intn(20))
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", idxName)
}

func (g *SQLGenerator) GenerateInsert() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	numVals := g.rand.Intn(6) + 1
	vals := make([]string, numVals)

	for i := 0; i < numVals; i++ {
		vals[i] = g.generateValue()
	}

	if g.rand.Intn(3) == 0 {
		cols := make([]string, numVals)
		for i := 0; i < numVals; i++ {
			cols[i] = fmt.Sprintf("c%d", i)
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(vals, ", "))
}

func (g *SQLGenerator) GenerateMultiRowInsert() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	numVals := g.rand.Intn(4) + 1
	numRows := g.rand.Intn(3) + 1

	values := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		vals := make([]string, numVals)
		for j := 0; j < numVals; j++ {
			vals[j] = g.generateValue()
		}
		values[i] = fmt.Sprintf("(%s)", strings.Join(vals, ", "))
	}

	return fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(values, ", "))
}

func (g *SQLGenerator) generateValue() string {
	typ := g.rand.Intn(8)
	switch typ {
	case 0:
		return fmt.Sprintf("%d", g.rand.Intn(10000)-5000)
	case 1:
		return fmt.Sprintf("%d", g.rand.Intn(1000000))
	case 2:
		return fmt.Sprintf("%f", g.rand.Float64()*1000)
	case 3:
		return fmt.Sprintf("'%s'", g.generateString())
	case 4:
		return fmt.Sprintf("'%s'", g.generateSpecialString())
	case 5:
		return "NULL"
	case 6:
		return fmt.Sprintf("%d", -g.rand.Intn(10000))
	case 7:
		return fmt.Sprintf("%f", -g.rand.Float64()*1000)
	}
	return "NULL"
}

func (g *SQLGenerator) generateString() string {
	length := g.rand.Intn(15) + 1
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[g.rand.Intn(len(chars))]
	}
	return string(result)
}

func (g *SQLGenerator) generateSpecialString() string {
	specials := []string{
		"hello world", "it's a test", "quote'inside", "double\"quote",
		"backslash\\", "newline\n", "tab\ttest", "null\x00char",
		"unicodecafé", "emoji😀", "<html>", "sql'injection",
	}
	return specials[g.rand.Intn(len(specials))]
}

func (g *SQLGenerator) GenerateSelect() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	numCols := g.rand.Intn(4) + 1

	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		if g.rand.Intn(5) == 0 {
			cols[i] = g.generateFunction()
		} else {
			cols[i] = fmt.Sprintf("c%d", i)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), tableName)

	if g.rand.Intn(2) == 1 {
		query += g.generateWhere()
	}

	if g.rand.Intn(3) == 0 {
		query += g.generateGroupBy()
	}

	if g.rand.Intn(2) == 1 {
		query += g.generateOrderBy()
	}

	if g.rand.Intn(4) == 0 {
		query += fmt.Sprintf(" LIMIT %d", g.rand.Intn(100))
	}

	return query
}

func (g *SQLGenerator) generateFunction() string {
	funcs := []string{
		"COUNT(*)", "SUM(c0)", "AVG(c0)", "MIN(c0)", "MAX(c0)",
		"UPPER(c0)", "LOWER(c0)", "LENGTH(c0)", "ABS(c0)",
		"COALESCE(c0, 'default')", "IFNULL(c0, 0)", "NULLIF(c0, 0)",
		"SUBSTR(c0, 1, 3)", "TRIM(c0)", "REPLACE(c0, 'a', 'b')",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

func (g *SQLGenerator) generateWhere() string {
	conds := []string{
		"c0 = 1",
		"c0 > 10",
		"c0 < 100",
		"c0 >= 0",
		"c0 <= 50",
		"c0 != 0",
		"c0 IS NULL",
		"c0 IS NOT NULL",
		"c0 BETWEEN 1 AND 10",
		"c0 NOT BETWEEN 0 AND 100",
		"c0 IN (1, 2, 3)",
		"c0 NOT IN (5, 10, 15)",
		"c0 LIKE '%test%'",
		"c0 LIKE 'test%'",
		"c0 LIKE '_est'",
		"c0 = 'hello'",
		"c0 > 'abc'",
		"c0 IS NULL AND c1 > 0",
		"c0 IS NULL OR c1 = 1",
		"NOT c0 = 1",
	}

	where := conds[g.rand.Intn(len(conds))]
	return " WHERE " + where
}

func (g *SQLGenerator) generateGroupBy() string {
	return fmt.Sprintf(" GROUP BY c0, c1")
}

func (g *SQLGenerator) generateOrderBy() string {
	ords := []string{
		" ORDER BY c0", " ORDER BY c0 ASC", " ORDER BY c0 DESC",
		" ORDER BY c1, c0", " ORDER BY c0 DESC, c1 ASC",
	}
	return ords[g.rand.Intn(len(ords))]
}

func (g *SQLGenerator) GenerateUpdate() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))
	colIdx := g.rand.Intn(5)

	query := fmt.Sprintf("UPDATE %s SET c%d = %s", tableName, colIdx, g.generateValue())

	if g.rand.Intn(2) == 1 {
		query += g.generateWhere()
	}

	return query
}

func (g *SQLGenerator) GenerateDelete() string {
	tableName := fmt.Sprintf("t%d", g.rand.Intn(50))

	query := fmt.Sprintf("DELETE FROM %s", tableName)

	if g.rand.Intn(2) == 1 {
		query += g.generateWhere()
	}

	return query
}

func (g *SQLGenerator) GeneratePragma() string {
	pragma := []string{
		"PRAGMA table_info",
		"PRAGMA table_info(t1)",
		"PRAGMA index_list",
		"PRAGMA index_list(t1)",
		"PRAGMA database_list",
	}
	return pragma[g.rand.Intn(len(pragma))]
}

func (g *SQLGenerator) GenerateJoin() string {
	t1 := fmt.Sprintf("t%d", g.rand.Intn(50))
	t2 := fmt.Sprintf("t%d", g.rand.Intn(50))

	joins := []string{
		fmt.Sprintf("SELECT * FROM %s, %s WHERE %s.c0 = %s.c0", t1, t2, t1, t2),
		fmt.Sprintf("SELECT * FROM %s JOIN %s ON %s.c0 = %s.c0", t1, t2, t1, t2),
		fmt.Sprintf("SELECT * FROM %s LEFT JOIN %s ON %s.c1 = %s.c1", t1, t2, t1, t2),
	}
	return joins[g.rand.Intn(len(joins))]
}

func (g *SQLGenerator) GenerateSubquery() string {
	t1 := fmt.Sprintf("t%d", g.rand.Intn(50))
	t2 := fmt.Sprintf("t%d", g.rand.Intn(50))

	subs := []string{
		fmt.Sprintf("SELECT * FROM %s WHERE c0 IN (SELECT c0 FROM %s)", t1, t2),
		fmt.Sprintf("SELECT * FROM %s WHERE EXISTS (SELECT 1 FROM %s)", t1, t2),
		fmt.Sprintf("SELECT * FROM (SELECT * FROM %s) AS subq", t1),
	}
	return subs[g.rand.Intn(len(subs))]
}

func (g *SQLGenerator) GenerateRandomSQL() string {
	generators := []func() string{
		g.GenerateCreateTable,
		g.GenerateDropTable,
		g.GenerateCreateIndex,
		g.GenerateDropIndex,
		g.GenerateInsert,
		g.GenerateMultiRowInsert,
		g.GenerateSelect,
		g.GenerateUpdate,
		g.GenerateDelete,
		g.GeneratePragma,
		g.GenerateJoin,
		g.GenerateSubquery,
	}
	return generators[g.rand.Intn(len(generators))]()
}

func (g *SQLGenerator) Mutate(sql string) string {
	mutations := []func(string) string{
		// Character-level mutations
		g.mutateAddChar,
		g.mutateRemoveChar,
		g.mutateChangeChar,
		g.mutateAddSpace,
		// Keyword mutations
		g.mutateAddKeyword,
		g.mutateRemoveKeyword,
		g.mutateChangeNumber,
		g.mutateAddNull,
		g.mutateChangeOperator,
		g.mutateAddParen,
		// Structural mutations
		g.mutateRemoveWhere,
		g.mutateFlipJoin,
		g.mutateFlipOrderBy,
		g.mutateChangeLimit,
	}
	return mutations[g.rand.Intn(len(mutations))](sql)
}

func (g *SQLGenerator) mutateAddChar(sql string) string {
	if len(sql) == 0 {
		return sql
	}
	idx := g.rand.Intn(len(sql))
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	c := chars[g.rand.Intn(len(chars))]
	return sql[:idx] + string(c) + sql[idx:]
}

func (g *SQLGenerator) mutateRemoveChar(sql string) string {
	if len(sql) <= 1 {
		return sql
	}
	idx := g.rand.Intn(len(sql))
	return sql[:idx] + sql[idx+1:]
}

func (g *SQLGenerator) mutateChangeChar(sql string) string {
	if len(sql) == 0 {
		return sql
	}
	idx := g.rand.Intn(len(sql))
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _'-"
	c := chars[g.rand.Intn(len(chars))]
	result := []byte(sql)
	result[idx] = c
	return string(result)
}

func (g *SQLGenerator) mutateAddSpace(sql string) string {
	if len(sql) == 0 {
		return sql
	}
	idx := g.rand.Intn(len(sql))
	return sql[:idx] + " " + sql[idx:]
}

func (g *SQLGenerator) mutateAddKeyword(sql string) string {
	keywords := []string{"NOT", "DISTINCT", "ALL", "EXISTS", "UNIQUE"}
	k := keywords[g.rand.Intn(len(keywords))]
	if g.rand.Intn(2) == 0 {
		return k + " " + sql
	}
	return sql + " " + k
}

func (g *SQLGenerator) mutateRemoveKeyword(sql string) string {
	keywords := []string{"DISTINCT", "ALL", "NOT"}
	upper := strings.ToUpper(sql)
	for _, k := range keywords {
		if strings.Contains(upper, k) {
			return strings.Replace(sql, k, "", 1)
		}
	}
	return sql
}

func (g *SQLGenerator) mutateChangeNumber(sql string) string {
	result := sql
	for i := 0; i < len(result); i++ {
		if result[i] >= '0' && result[i] <= '9' {
			if g.rand.Intn(3) == 0 {
				digits := "0123456789"
				result = result[:i] + string(digits[g.rand.Intn(len(digits))]) + result[i+1:]
			}
		}
	}
	return result
}

func (g *SQLGenerator) mutateAddNull(sql string) string {
	if g.rand.Intn(2) == 0 {
		return sql + " NULL"
	}
	return "NULL " + sql
}

func (g *SQLGenerator) mutateChangeOperator(sql string) string {
	ops := map[string]string{
		"=":   "!=",
		"!=":  "=",
		">":   "<=",
		"<":   ">=",
		">=":  ">",
		"<=":  "<",
		"AND": "OR",
		"OR":  "AND",
	}
	for old, new := range ops {
		if strings.Contains(sql, old) {
			return strings.Replace(sql, old, new, 1)
		}
	}
	return sql
}

func (g *SQLGenerator) mutateAddParen(sql string) string {
	if g.rand.Intn(2) == 0 {
		return "(" + sql + ")"
	}
	parts := strings.Split(sql, " ")
	if len(parts) > 1 {
		idx := g.rand.Intn(len(parts)-1) + 1
		parts[idx] = "(" + parts[idx] + ")"
		return strings.Join(parts, " ")
	}
	return sql
}



// generateValueForType generates a value matching the given column type
func (g *SQLGenerator) generateValueForType(colType ColumnType) string {
	switch colType {
	case ColumnTypeInteger:
		// 80% chance of integer, 20% NULL
		if g.rand.Intn(5) == 0 {
			return "NULL"
		}
		return fmt.Sprintf("%d", g.rand.Intn(10000)-5000)
	case ColumnTypeReal:
		if g.rand.Intn(5) == 0 {
			return "NULL"
		}
		return fmt.Sprintf("%f", g.rand.Float64()*1000)
	case ColumnTypeText:
		if g.rand.Intn(5) == 0 {
			return "NULL"
		}
		return fmt.Sprintf("'%s'", g.generateString())
	case ColumnTypeBlob:
		if g.rand.Intn(5) == 0 {
			return "NULL"
		}
		// Generate hex blob
		length := g.rand.Intn(8) + 1
		hexChars := "0123456789abcdef"
		result := make([]byte, length*2)
		for i := 0; i < length*2; i++ {
			result[i] = hexChars[g.rand.Intn(len(hexChars))]
		}
		return fmt.Sprintf("X'%s'", string(result))
	default:
		return g.generateValue()
	}
}

// GenerateSchemaAwareInsert generates INSERT using existing schema
func (g *SQLGenerator) GenerateSchemaAwareInsert() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable() // Fallback: create table first
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil {
		return g.GenerateCreateTable()
	}

	// Generate values matching column types
	vals := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		vals[i] = g.generateValueForType(col.Type)
	}

	// 30% chance of explicit column list
	if g.rand.Intn(3) == 0 {
		cols := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			cols[i] = col.Name
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}

	return fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(vals, ", "))
}

// GenerateSchemaAwareSelect generates SELECT using existing schema
func (g *SQLGenerator) GenerateSchemaAwareSelect() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	// Select random columns
	numCols := g.rand.Intn(len(table.Columns)) + 1
	if numCols > len(table.Columns) {
		numCols = len(table.Columns)
	}

	// Shuffle column indices
	indices := make([]int, len(table.Columns))
	for i := range indices {
		indices[i] = i
	}
	for i := len(indices) - 1; i > 0; i-- {
		j := g.rand.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}

	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		colIdx := indices[i]
		// 10% chance of function
		if g.rand.Intn(10) == 0 {
			funcs := []string{"COUNT(%s)", "MAX(%s)", "MIN(%s)", "AVG(%s)"}
			cols[i] = fmt.Sprintf(funcs[g.rand.Intn(len(funcs))], table.Columns[colIdx].Name)
		} else {
			cols[i] = table.Columns[colIdx].Name
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), tableName)

	// Add WHERE clause using actual column types
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		colIdx := indices[0]
		col := table.Columns[colIdx]
		query += g.generateSchemaAwareWhere(tableName, col)
	}

	// Add ORDER BY
	if g.rand.Intn(4) == 0 && len(table.Columns) > 0 {
		colIdx := indices[g.rand.Intn(len(indices))]
		dir := "ASC"
		if g.rand.Intn(2) == 0 {
			dir = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", table.Columns[colIdx].Name, dir)
	}

	// Add LIMIT
	if g.rand.Intn(4) == 0 {
		query += fmt.Sprintf(" LIMIT %d", g.rand.Intn(100))
	}

	return query
}

// generateSchemaAwareWhere generates WHERE clause matching column type
func (g *SQLGenerator) generateSchemaAwareWhere(tableName string, col ColumnInfo) string {
	var cond string

	switch col.Type {
	case ColumnTypeInteger, ColumnTypeReal:
		ops := []string{"=", ">", "<", ">=", "<=", "!="}
		op := ops[g.rand.Intn(len(ops))]
		if col.Type == ColumnTypeInteger {
			cond = fmt.Sprintf("%s %s %d", col.Name, op, g.rand.Intn(100))
		} else {
			cond = fmt.Sprintf("%s %s %f", col.Name, op, g.rand.Float64()*100)
		}
	case ColumnTypeText:
		if g.rand.Intn(3) == 0 {
			cond = fmt.Sprintf("%s IS NULL", col.Name)
		} else if g.rand.Intn(3) == 0 {
			cond = fmt.Sprintf("%s IS NOT NULL", col.Name)
		} else {
			ops := []string{"=", "!=", "LIKE"}
			op := ops[g.rand.Intn(len(ops))]
			if op == "LIKE" {
				cond = fmt.Sprintf("%s LIKE '%s'", col.Name, g.generateString())
			} else {
				cond = fmt.Sprintf("%s %s '%s'", col.Name, op, g.generateString())
			}
		}
	default:
		cond = fmt.Sprintf("%s IS NOT NULL", col.Name)
	}

	return " WHERE " + cond
}

// GenerateSchemaAwareUpdate generates UPDATE using existing schema
func (g *SQLGenerator) GenerateSchemaAwareUpdate() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	colIdx := g.rand.Intn(len(table.Columns))
	col := table.Columns[colIdx]

	query := fmt.Sprintf("UPDATE %s SET %s = %s",
		tableName, col.Name, g.generateValueForType(col.Type))

	// Add WHERE clause
	if g.rand.Intn(2) == 1 {
		query += g.generateSchemaAwareWhere(tableName, col)
	}

	return query
}

// GenerateSchemaAwareDelete generates DELETE using existing schema
func (g *SQLGenerator) GenerateSchemaAwareDelete() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	query := fmt.Sprintf("DELETE FROM %s", tableName)

	// Add WHERE clause
	if g.rand.Intn(2) == 1 {
		col := table.Columns[g.rand.Intn(len(table.Columns))]
		query += g.generateSchemaAwareWhere(tableName, col)
	}

	return query
}

// GenerateCTE generates a query with Common Table Expression
func (g *SQLGenerator) GenerateCTE() string {
	ctes := []string{
		"WITH cte AS (SELECT 1 AS n) SELECT * FROM cte",
		"WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt",
		"WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT 2 AS b) SELECT a, b FROM cte1, cte2",
	}
	return ctes[g.rand.Intn(len(ctes))]
}

// GenerateWindowFunction generates a query with window functions
func (g *SQLGenerator) GenerateWindowFunction() string {
	funcs := []string{
		"SELECT ROW_NUMBER() OVER (ORDER BY c0) FROM t0",
		"SELECT RANK() OVER (PARTITION BY c0 ORDER BY c1) FROM t0",
		"SELECT DENSE_RANK() OVER (ORDER BY c0) FROM t0",
		"SELECT LAG(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT LEAD(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT SUM(c0) OVER (ORDER BY c0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t0",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

// GenerateUpsert generates INSERT ON CONFLICT (UPSERT) query
func (g *SQLGenerator) GenerateUpsert() string {
	upserts := []string{
		"INSERT INTO t0(c0, c1) VALUES(1, 2) ON CONFLICT(c0) DO NOTHING",
		"INSERT INTO t0(c0, c1) VALUES(1, 2) ON CONFLICT(c0) DO UPDATE SET c1 = excluded.c1",
		"INSERT INTO t0(c0, c1) VALUES(1, 2) ON CONFLICT DO NOTHING",
	}
	return upserts[g.rand.Intn(len(upserts))]
}

// GenerateTransaction generates transaction statements
func (g *SQLGenerator) GenerateTransaction() string {
	trans := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"COMMIT",
		"ROLLBACK",
		"BEGIN; INSERT INTO t0 VALUES(1); COMMIT",
		"BEGIN; INSERT INTO t0 VALUES(1); ROLLBACK",
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO SAVEPOINT sp1",
	}
	return trans[g.rand.Intn(len(trans))]
}

// GenerateAlterTable generates ALTER TABLE statement
func (g *SQLGenerator) GenerateAlterTable() string {
	alters := []string{
		"ALTER TABLE t0 ADD COLUMN new_col TEXT",
		"ALTER TABLE t0 RENAME TO t1",
	}
	return alters[g.rand.Intn(len(alters))]
}

// GenerateView generates CREATE/DROP VIEW statement
func (g *SQLGenerator) GenerateView() string {
	if g.rand.Intn(2) == 0 {
		views := []string{
			"CREATE VIEW v0 AS SELECT 1 AS a",
			"CREATE VIEW v1 AS SELECT c0 FROM t0",
		}
		return views[g.rand.Intn(len(views))]
	}
	return "DROP VIEW IF EXISTS v0"
}

// GenerateExplain generates EXPLAIN query
func (g *SQLGenerator) GenerateExplain() string {
	explains := []string{
		"EXPLAIN SELECT * FROM t0",
		"EXPLAIN QUERY PLAN SELECT * FROM t0 WHERE c0 = 1",
	}
	return explains[g.rand.Intn(len(explains))]
}

// GenerateSetOperation generates UNION/INTERSECT/EXCEPT
func (g *SQLGenerator) GenerateSetOperation() string {
	sets := []string{
		"SELECT 1 UNION SELECT 2",
		"SELECT 1 UNION ALL SELECT 2",
		"SELECT 1 INTERSECT SELECT 2",
		"SELECT 1 EXCEPT SELECT 2",
	}
	return sets[g.rand.Intn(len(sets))]
}

// GenerateCase generates CASE expression
func (g *SQLGenerator) GenerateCase() string {
	cases := []string{
		"SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END",
		"SELECT CASE c0 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t0",
	}
	return cases[g.rand.Intn(len(cases))]
}

// mutateRemoveWhere removes WHERE clause from query
func (g *SQLGenerator) mutateRemoveWhere(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "WHERE")
	if idx > 0 {
		return strings.TrimSpace(sql[:idx])
	}
	return sql
}

// mutateFlipJoin flips INNER JOIN to LEFT JOIN or vice versa
func (g *SQLGenerator) mutateFlipJoin(sql string) string {
	result := strings.ReplaceAll(sql, "INNER JOIN", "_JOIN_")
	result = strings.ReplaceAll(result, "LEFT JOIN", "INNER JOIN")
	result = strings.ReplaceAll(result, "_JOIN_", "LEFT JOIN")
	return result
}

// mutateFlipOrderBy flips ASC to DESC or removes ORDER BY
func (g *SQLGenerator) mutateFlipOrderBy(sql string) string {
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "ORDER BY") {
		if g.rand.Intn(2) == 0 {
			// Flip ASC/DESC
			result := strings.ReplaceAll(sql, " ASC", "_DIR_")
			result = strings.ReplaceAll(result, " DESC", " ASC")
			result = strings.ReplaceAll(result, "_DIR_", " DESC")
			return result
		}
		// Remove ORDER BY
		idx := strings.Index(upper, "ORDER BY")
		// Find where ORDER BY ends (before LIMIT or end of string)
		limitIdx := strings.Index(upper[idx:], "LIMIT")
		if limitIdx > 0 {
			return strings.TrimSpace(sql[:idx+limitIdx])
		}
		return strings.TrimSpace(sql[:idx])
	}
	return sql
}

// mutateChangeLimit changes LIMIT value
func (g *SQLGenerator) mutateChangeLimit(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "LIMIT")
	if idx > 0 {
		// Extract current limit and change it
		rest := sql[idx+5:]
		newLimit := make([]byte, 0)
		for _, c := range rest {
			if c >= '0' && c <= '9' {
				newLimit = append(newLimit, byte(c))
			} else if len(newLimit) > 0 {
				break
			}
		}
		if len(newLimit) > 0 {
			oldLimit := string(newLimit)
			// Change to random value
			newVal := fmt.Sprintf("%d", g.rand.Intn(1000))
			return strings.Replace(sql, "LIMIT "+oldLimit, "LIMIT "+newVal, 1)
		}
	}
	return sql
}

// GenerateSchemaAwareRandomSQL generates random SQL using schema when available
func (g *SQLGenerator) GenerateSchemaAwareRandomSQL() string {
	// If we have schema, prefer schema-aware generators
	if g.Schema.HasTables() {
		// 60% chance to use existing tables, 40% chance for DDL
		if g.rand.Intn(10) < 6 {
			generators := []func() string{
				g.GenerateSchemaAwareInsert,
				g.GenerateSchemaAwareSelect,
				g.GenerateSchemaAwareUpdate,
				g.GenerateSchemaAwareDelete,
				g.GenerateCreateTable, // Still create new tables occasionally
			}
			return generators[g.rand.Intn(len(generators))]()
		}
	}

	// Full random (includes DDL, missing features)
	generators := []func() string{
		g.GenerateCreateTable,
		g.GenerateDropTable,
		g.GenerateCreateIndex,
		g.GenerateDropIndex,
		g.GenerateInsert,
		g.GenerateMultiRowInsert,
		g.GenerateSelect,
		g.GenerateUpdate,
		g.GenerateDelete,
		g.GeneratePragma,
		g.GenerateJoin,
		g.GenerateSubquery,
		g.GenerateCTE,
		g.GenerateWindowFunction,
		g.GenerateUpsert,
		g.GenerateTransaction,
		g.GenerateAlterTable,
		g.GenerateView,
		g.GenerateExplain,
		g.GenerateSetOperation,
		g.GenerateCase,
	}
	return generators[g.rand.Intn(len(generators))]()
}
