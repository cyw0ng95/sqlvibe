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

// ComplexityBudget limits the complexity of generated SQL
type ComplexityBudget struct {
	MaxExprDepth  int // Maximum nesting depth for expressions
	MaxSubqueries int // Maximum number of nested subqueries
	MaxJoins      int // Maximum number of JOINs
	MaxColumns    int // Maximum columns in SELECT
	MaxFunctions  int // Maximum nested function calls
	CurrentDepth  int // Current nesting depth
}

func NewComplexityBudget() *ComplexityBudget {
	return &ComplexityBudget{
		MaxExprDepth:  5,
		MaxSubqueries: 3,
		MaxJoins:      3,
		MaxColumns:    8,
		MaxFunctions:  4,
		CurrentDepth:  0,
	}
}

// CanIncreaseDepth checks if we can increase expression depth
func (b *ComplexityBudget) CanIncreaseDepth() bool {
	return b.CurrentDepth < b.MaxExprDepth
}

// IncreaseDepth increases depth, returns false if at limit
func (b *ComplexityBudget) IncreaseDepth() bool {
	if !b.CanIncreaseDepth() {
		return false
	}
	b.CurrentDepth++
	return true
}

// DecreaseDepth decreases depth
func (b *ComplexityBudget) DecreaseDepth() {
	if b.CurrentDepth > 0 {
		b.CurrentDepth--
	}
}

// Reset resets the depth counter
func (b *ComplexityBudget) Reset() {
	b.CurrentDepth = 0
}

// WithDepth executes a function with increased depth, resetting after
func (b *ComplexityBudget) WithDepth(fn func() string) string {
	if !b.IncreaseDepth() {
		return fn() // At limit, execute without increasing
	}
	defer b.DecreaseDepth()
	return fn()
}

type SQLGenerator struct {
	rand   *rand.Rand
	Schema *SchemaTracker
	Budget *ComplexityBudget
}

func NewSQLGenerator(seed int64) *SQLGenerator {
	return &SQLGenerator{
		rand:   rand.New(rand.NewSource(seed)),
		Schema: NewSchemaTracker(seed),
		Budget: NewComplexityBudget(),
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
		"ALTER TABLE t0 DROP COLUMN c1",
		"ALTER TABLE t0 RENAME COLUMN c0 TO col0",
		"ALTER TABLE t0 RENAME c1 TO col1",
		"ALTER TABLE t0 ADD CONSTRAINT chk1 CHECK (c0 > 0)",
		"ALTER TABLE t0 ADD CONSTRAINT uq1 UNIQUE (c0)",
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
		"SELECT 1 INTERSECT ALL SELECT 1",
		"SELECT 1 EXCEPT SELECT 2",
		"SELECT 1 EXCEPT ALL SELECT 1",
		"SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2",
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

// =============================================================================
// SQLSmith-style Optimizations: Recursive Expression, Cross-feature, Persistent Schema
// =============================================================================

// GenerateRecursiveExpression generates nested expressions using complexity budget
func (g *SQLGenerator) GenerateRecursiveExpression(colType ColumnType) string {
	// Base case: simple column or value
	if g.Budget == nil || !g.Budget.CanIncreaseDepth() {
		return g.generateSimpleExpression(colType)
	}

	return g.Budget.WithDepth(func() string {
		// Randomly choose between nested function, binary op, or simple expression
		choice := g.rand.Intn(4)
		switch choice {
		case 0:
			return g.generateNestedFunction(colType)
		case 1:
			return g.generateBinaryExpression(colType)
		case 2:
			return g.generateCaseExpression(colType)
		default:
			return g.generateSimpleExpression(colType)
		}
	})
}

// generateSimpleExpression generates simple column or literal
func (g *SQLGenerator) generateSimpleExpression(colType ColumnType) string {
	switch colType {
	case ColumnTypeInteger:
		choices := []string{
			fmt.Sprintf("%d", g.rand.Intn(1000)),
			fmt.Sprintf("%d", -g.rand.Intn(100)),
		}
		return choices[g.rand.Intn(len(choices))]
	case ColumnTypeReal:
		return fmt.Sprintf("%f", g.rand.Float64()*100)
	case ColumnTypeText:
		return fmt.Sprintf("'%s'", g.generateString())
	default:
		return "NULL"
	}
}

// generateNestedFunction generates nested function calls
func (g *SQLGenerator) generateNestedFunction(colType ColumnType) string {
	// Generate nested function calls like ABS(COALESCE(col, 0))
	inner := g.GenerateRecursiveExpression(colType)

	funcs := []struct {
		name string
		args int // 0 = *, 1 = single arg
	}{
		{"ABS", 1}, {"LENGTH", 1}, {"UPPER", 1}, {"LOWER", 1},
		{"TRIM", 1}, {"COALESCE", 2}, {"IFNULL", 2}, {"NULLIF", 2},
	}
	f := funcs[g.rand.Intn(len(funcs))]

	if f.args == 2 {
		// Binary function: COALESCE(x, y) or IFNULL(x, y)
		second := g.generateSimpleExpression(colType)
		return fmt.Sprintf("%s(%s, %s)", f.name, inner, second)
	}
	return fmt.Sprintf("%s(%s)", f.name, inner)
}

// generateBinaryExpression generates binary operations
func (g *SQLGenerator) generateBinaryExpression(colType ColumnType) string {
	left := g.GenerateRecursiveExpression(colType)
	right := g.GenerateRecursiveExpression(colType)

	ops := []string{"+", "-", "*", "/"}
	if colType == ColumnTypeText {
		ops = []string{"||"}
	}
	op := ops[g.rand.Intn(len(ops))]

	return fmt.Sprintf("(%s %s %s)", left, op, right)
}

// generateCaseExpression generates CASE WHEN expressions
func (g *SQLGenerator) generateCaseExpression(colType ColumnType) string {
	caseWhen := g.rand.Intn(3) + 1 // 1-3 WHEN clauses

	var whens []string
	for i := 0; i < caseWhen; i++ {
		cond := g.generateSimpleCondition()
		val := g.generateSimpleExpression(colType)
		whens = append(whens, fmt.Sprintf("WHEN %s THEN %s", cond, val))
	}

	elseVal := g.generateSimpleExpression(colType)
	return fmt.Sprintf("CASE %s ELSE %s END", strings.Join(whens, " "), elseVal)
}

// generateSimpleCondition generates simple WHERE conditions
func (g *SQLGenerator) generateSimpleCondition() string {
	ops := []string{"=", "!=", ">", "<", ">=", "<="}
	op := ops[g.rand.Intn(len(ops))]

	left := fmt.Sprintf("c%d", g.rand.Intn(5))
	right := fmt.Sprintf("%d", g.rand.Intn(100))

	return fmt.Sprintf("%s %s %s", left, op, right)
}

// GenerateCrossFeatureQuery generates queries combining multiple SQL features
func (g *SQLGenerator) GenerateCrossFeatureQuery() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	// Build a complex query with multiple features
	var clauses []string

	// SELECT clause with functions
	numCols := g.rand.Intn(len(table.Columns)) + 1
	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		colIdx := g.rand.Intn(len(table.Columns))
		col := table.Columns[colIdx]

		// 30% chance of function on column
		if g.rand.Intn(10) < 3 {
			funcs := []string{"COUNT(%s)", "MAX(%s)", "MIN(%s)", "AVG(%s)", "SUM(%s)"}
			cols[i] = fmt.Sprintf(funcs[g.rand.Intn(len(funcs))], col.Name)
		} else {
			cols[i] = col.Name
		}
	}
	clauses = append(clauses, fmt.Sprintf("SELECT %s", strings.Join(cols, ", ")))

	// FROM clause with JOIN possibility
	if g.Schema.HasTables() && g.rand.Intn(3) == 0 {
		table2Name := g.Schema.GetRandomTable()
		if table2Name != tableName {
			joinType := []string{"JOIN", "LEFT JOIN", "INNER JOIN"}
			clauses = append(clauses, fmt.Sprintf("FROM %s %s %s ON %s.c0 = %s.c0",
				tableName, joinType[g.rand.Intn(len(joinType))], table2Name, tableName, table2Name))
		} else {
			clauses = append(clauses, fmt.Sprintf("FROM %s", tableName))
		}
	} else {
		clauses = append(clauses, fmt.Sprintf("FROM %s", tableName))
	}

	// WHERE clause
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		col := table.Columns[g.rand.Intn(len(table.Columns))]
		where := g.generateSchemaAwareWhere(tableName, col)
		clauses = append(clauses, where)
	}

	// GROUP BY
	if g.rand.Intn(3) == 0 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		clauses = append(clauses, fmt.Sprintf("GROUP BY %s", table.Columns[colIdx].Name))
	}

	// HAVING (only with GROUP BY)
	if len(clauses) > 3 && g.rand.Intn(2) == 1 {
		colIdx := g.rand.Intn(len(table.Columns))
		clauses = append(clauses, fmt.Sprintf("HAVING %s > %d", table.Columns[colIdx].Name, g.rand.Intn(50)))
	}

	// ORDER BY
	if g.rand.Intn(3) == 0 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		dir := []string{"ASC", "DESC"}
		clauses = append(clauses, fmt.Sprintf("ORDER BY %s %s", table.Columns[colIdx].Name, dir[g.rand.Intn(len(dir))]))
	}

	// LIMIT
	if g.rand.Intn(4) == 0 {
		clauses = append(clauses, fmt.Sprintf("LIMIT %d", g.rand.Intn(100)))
	}

	return strings.Join(clauses, " ")
}

// GenerateSubqueryInFrom generates subqueries in FROM clause
func (g *SQLGenerator) GenerateSubqueryInFrom() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	// Generate inner query
	inner := g.GenerateCrossFeatureQuery()

	return fmt.Sprintf("SELECT * FROM (%s) AS subq", inner)
}

// GenerateComplexJoin generates complex JOIN queries
func (g *SQLGenerator) GenerateComplexJoin() string {
	if g.Schema == nil || !g.Schema.HasTables() || len(g.Schema.Tables) < 2 {
		return g.GenerateCreateTable()
	}

	// Get two different tables
	tableNames := make([]string, 0, len(g.Schema.Tables))
	for name := range g.Schema.Tables {
		tableNames = append(tableNames, name)
	}

	if len(tableNames) < 2 {
		return g.GenerateCreateTable()
	}

	// Shuffle and pick two
	t1 := tableNames[g.rand.Intn(len(tableNames))]
	t2 := tableNames[g.rand.Intn(len(tableNames))]
	for t2 == t1 && len(tableNames) > 1 {
		t2 = tableNames[g.rand.Intn(len(tableNames))]
	}

	table1 := g.Schema.GetTable(t1)
	table2 := g.Schema.GetTable(t2)

	if table1 == nil || table2 == nil || len(table1.Columns) == 0 || len(table2.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	// Build JOIN with multiple conditions
	joinTypes := []string{"JOIN", "LEFT JOIN", "INNER JOIN", "CROSS JOIN"}
	joinType := joinTypes[g.rand.Intn(len(joinTypes))]

	col1 := table1.Columns[g.rand.Intn(len(table1.Columns))].Name
	col2 := table2.Columns[g.rand.Intn(len(table2.Columns))].Name

	query := fmt.Sprintf("SELECT * FROM %s %s %s ON %s.%s = %s.%s",
		t1, joinType, t2, t1, col1, t2, col2)

	// Add WHERE
	if g.rand.Intn(2) == 1 {
		whereCol := table1.Columns[g.rand.Intn(len(table1.Columns))].Name
		query += fmt.Sprintf(" WHERE %s.%s > %d", t1, whereCol, g.rand.Intn(50))
	}

	return query
}

// GenerateComplexAggregate generates complex aggregate queries with multiple features
func (g *SQLGenerator) GenerateComplexAggregate() string {
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.GenerateCreateTable()
	}

	// Build aggregate query with multiple aggregates
	numAggs := g.rand.Intn(3) + 1 // 1-3 aggregates
	aggs := make([]string, numAggs)

	for i := 0; i < numAggs; i++ {
		colIdx := g.rand.Intn(len(table.Columns))
		col := table.Columns[colIdx]

		aggFuncs := []string{"COUNT(%s)", "SUM(%s)", "AVG(%s)", "MIN(%s)", "MAX(%s)"}
		// Use COUNT(*) for non-integer columns
		if col.Type != ColumnTypeInteger {
			aggFuncs = []string{"COUNT(%s)", "MIN(%s)", "MAX(%s)"}
		}
		agg := aggFuncs[g.rand.Intn(len(aggFuncs))]
		aggs[i] = fmt.Sprintf(agg, col.Name)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(aggs, ", "), tableName)

	// Add WHERE
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		col := table.Columns[g.rand.Intn(len(table.Columns))]
		query += g.generateSchemaAwareWhere(tableName, col)
	}

	// Add GROUP BY
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		query += fmt.Sprintf(" GROUP BY %s", table.Columns[colIdx].Name)
	}

	// Add ORDER BY with aggregate
	if g.rand.Intn(3) == 0 {
		query += fmt.Sprintf(" ORDER BY %s", aggs[0])
	}

	return query
}

// SQLSmithMode generates SQL in SQLSmith style - schema-aware with complexity control
func (g *SQLGenerator) SQLSmithMode() string {
	// Ensure we have a table
	if !g.Schema.HasTables() {
		return g.GenerateCreateTable()
	}

	// Reset budget for each query
	if g.Budget != nil {
		g.Budget.Reset()
	}

	// Choose generation strategy based on complexity
	strategies := []func() string{
		g.GenerateSchemaAwareSelect,
		g.GenerateSchemaAwareInsert,
		g.GenerateSchemaAwareUpdate,
		g.GenerateSchemaAwareDelete,
		g.GenerateCrossFeatureQuery,
		g.GenerateComplexJoin,
		g.GenerateComplexAggregate,
		g.GenerateSubqueryInFrom,
		g.GenerateCTE,
		g.GenerateWindowFunction,
	}

	return strategies[g.rand.Intn(len(strategies))]()
}

// GenerateDateTime generates date/time related queries
func (g *SQLGenerator) GenerateDateTime() string {
	funcs := []string{
		"SELECT julianday('now')",
		"SELECT unixepoch('now')",
		"SELECT strftime('%Y-%m-%d', 'now')",
		"SELECT strftime('%H:%M:%S', 'now')",
		"SELECT strftime('%w', 'now')",
		"SELECT strftime('%W', 'now')",
		"SELECT strftime('%s', 'now')",
		"SELECT strftime('%J', 'now')",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

// GenerateJSON generates JSON-related queries (if extension available)
func (g *SQLGenerator) GenerateJSON() string {
	funcs := []string{
		"SELECT json('{\"a\":1}')",
		"SELECT json_array(1, 2, 3)",
		"SELECT json_extract('{\"a\":1}', '$.a')",
		"SELECT json_object('a', 1)",
		"SELECT json_set('{\"a\":1}', '$.b', 2)",
		"SELECT json_type('{\"a\":1}', '$.a')",
		"SELECT json_length('{\"a\":[1,2,3]}', '$.a')",
		"SELECT json_valid('{\"a\":1}')",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

// GenerateMath generates math-related queries (if extension available)
func (g *SQLGenerator) GenerateMath() string {
	funcs := []string{
		"SELECT POWER(2, 8)",
		"SELECT SQRT(16)",
		"SELECT MOD(10, 3)",
		"SELECT ABS(-5)",
		"SELECT CEIL(1.5)",
		"SELECT FLOOR(1.5)",
		"SELECT ROUND(3.14159, 2)",
		"SELECT EXP(1)",
		"SELECT LN(2.718)",
		"SELECT LOG(10)",
		"SELECT LOG2(8)",
		"SELECT COS(0)",
		"SELECT SIN(0)",
		"SELECT TAN(0)",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

// GenerateExpression generates complex expressions
func (g *SQLGenerator) GenerateExpression() string {
	ops := []string{"+", "-", "*", "/", "%", "||"}
	op := ops[g.rand.Intn(len(ops))]

	valTypes := []string{
		fmt.Sprintf("%d", g.rand.Intn(100)),
		fmt.Sprintf("%f", g.rand.Float64()*100),
		fmt.Sprintf("'%s'", g.generateString()),
	}

	left := valTypes[g.rand.Intn(len(valTypes))]
	right := valTypes[g.rand.Intn(len(valTypes))]

	return fmt.Sprintf("SELECT %s %s %s", left, op, right)
}

// GenerateComparisonChain generates chained comparisons
func (g *SQLGenerator) GenerateComparisonChain() string {
	comparisons := []string{
		"SELECT 1 < 2 AND 2 < 3",
		"SELECT 1 < 2 < 3",
		"SELECT 10 > 5 AND 5 > 1",
		"SELECT c0 BETWEEN 0 AND 100 FROM t0",
		"SELECT c0 NOT BETWEEN 0 AND 100 FROM t0",
	}
	return comparisons[g.rand.Intn(len(comparisons))]
}

// GenerateExists generates EXISTS/NOT EXISTS queries
func (g *SQLGenerator) GenerateExists() string {
	queries := []string{
		"SELECT EXISTS(SELECT 1)",
		"SELECT NOT EXISTS(SELECT 1)",
		"SELECT * FROM t0 WHERE EXISTS(SELECT 1 FROM t1)",
		"SELECT * FROM t0 WHERE NOT EXISTS(SELECT 1 FROM t1)",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateHaving generates queries with HAVING clause
func (g *SQLGenerator) GenerateHaving() string {
	queries := []string{
		"SELECT c0, COUNT(*) FROM t0 GROUP BY c0 HAVING COUNT(*) > 1",
		"SELECT c0, SUM(c1) FROM t0 GROUP BY c0 HAVING SUM(c1) > 100",
		"SELECT c0, AVG(c1) FROM t0 GROUP BY c0 HAVING AVG(c1) < 50",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateDistinct generates DISTINCT queries
func (g *SQLGenerator) GenerateDistinct() string {
	queries := []string{
		"SELECT DISTINCT c0 FROM t0",
		"SELECT DISTINCT c0, c1 FROM t0",
		"SELECT COUNT(DISTINCT c0) FROM t0",
		"SELECT COUNT(DISTINCT c0), COUNT(DISTINCT c1) FROM t0",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateLimitOffset generates queries with LIMIT/OFFSET
func (g *SQLGenerator) GenerateLimitOffset() string {
	limit := g.rand.Intn(100)
	offset := g.rand.Intn(50)
	queries := []string{
		fmt.Sprintf("SELECT * FROM t0 LIMIT %d", limit),
		fmt.Sprintf("SELECT * FROM t0 LIMIT %d OFFSET %d", limit, offset),
		fmt.Sprintf("SELECT * FROM t0 ORDER BY c0 LIMIT %d", limit),
		fmt.Sprintf("SELECT * FROM t0 LIMIT %d, %d", offset, limit),
		fmt.Sprintf("SELECT * FROM t0 ORDER BY c0 FETCH FIRST %d ROWS ONLY", limit+1),
		fmt.Sprintf("SELECT * FROM t0 ORDER BY c0 FETCH NEXT %d ROW ONLY", limit+1),
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateStandaloneValues generates VALUES (...) standalone statements.
func (g *SQLGenerator) GenerateStandaloneValues() string {
	queries := []string{
		"VALUES (1)",
		"VALUES (1, 'a')",
		"VALUES (1, 'a'), (2, 'b')",
		"VALUES (1, 2, 3)",
		"VALUES (NULL, 0, 'x')",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateCompound generates compound queries (UNION/INTERSECT/EXCEPT)
func (g *SQLGenerator) GenerateCompound() string {
	ops := []string{"UNION", "UNION ALL", "INTERSECT", "EXCEPT"}
	op := ops[g.rand.Intn(len(ops))]

	queries := []string{
		fmt.Sprintf("SELECT c0 FROM t0 %s SELECT c0 FROM t1", op),
		fmt.Sprintf("SELECT 1 %s SELECT 2 %s SELECT 3", op, ops[g.rand.Intn(len(ops))]),
		fmt.Sprintf("SELECT c0 FROM t0 WHERE c0 > 0 %s SELECT c0 FROM t0 WHERE c0 < 100", op),
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateBlob generates BLOB-related queries
func (g *SQLGenerator) GenerateBlob() string {
	queries := []string{
		"SELECT X'0102030405'",
		"SELECT X''",
		"SELECT X'ff'",
		"SELECT CAST(X'0102' AS TEXT)",
		"SELECT HEX(X'0102030405')",
		"SELECT LENGTH(X'0102030405')",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GeneratePragmaExtended generates extended PRAGMA queries
func (g *SQLGenerator) GeneratePragmaExtended() string {
	pragma := []string{
		"PRAGMA page_size = 4096",
		"PRAGMA cache_size = 2000",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA journal_mode = WAL",
		"PRAGMA locking_mode = NORMAL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA read_uncommitted = 1",
		"PRAGMA wal_autocheckpoint = 1000",
		"PRAGMA auto_vacuum = INCREMENTAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA query_only = 0",
		"PRAGMA mmap_size = 268435456",
	}
	return pragma[g.rand.Intn(len(pragma))]
}

// GenerateSchemaAdvanced generates advanced schema patterns
func (g *SQLGenerator) GenerateSchemaAdvanced() string {
	schemas := []string{
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)",
		"CREATE TABLE t1 (id INTEGER, name TEXT, UNIQUE(id, name))",
		"CREATE TABLE t1 (id INTEGER, name TEXT DEFAULT 'unknown')",
		"CREATE TABLE t1 (id INTEGER, data TEXT CHECK(LENGTH(data) > 0))",
		"CREATE TABLE t1 (id INTEGER, name TEXT COLLATE NOCASE)",
		"CREATE TABLE t1 AS SELECT 1 AS id, 'test' AS name",
		"CREATE INDEX idx1 ON t0(c0) WHERE c0 IS NOT NULL",
		"CREATE INDEX idx1 ON t0(LOWER(c1))",
	}
	return schemas[g.rand.Intn(len(schemas))]
}

// GenerateRecursiveCTE generates recursive CTE queries
func (g *SQLGenerator) GenerateRecursiveCTE() string {
	ctes := []string{
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt",
		"WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b<100) SELECT a FROM fib",
		"WITH RECURSIVE tree(id, parent, depth) AS (SELECT 1, NULL, 0 UNION ALL SELECT t.id, t.parent, tree.depth+1 FROM t0 t JOIN tree ON t.parent = tree.id WHERE tree.depth < 5) SELECT * FROM tree",
	}
	return ctes[g.rand.Intn(len(ctes))]
}

// GenerateFlattenedSubquery generates flattened subquery patterns
func (g *SQLGenerator) GenerateFlattenedSubquery() string {
	queries := []string{
		"SELECT * FROM (SELECT c0, COUNT(*) as cnt FROM t0 GROUP BY c0)",
		"SELECT * FROM (SELECT * FROM t0 WHERE c0 > 0) WHERE c0 < 100",
		"SELECT a.* FROM (SELECT * FROM t0) a JOIN (SELECT * FROM t1) b ON a.c0 = b.c0",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateCorrelatedSubquery generates correlated subquery patterns
func (g *SQLGenerator) GenerateCorrelatedSubquery() string {
	queries := []string{
		"SELECT * FROM t0 WHERE c0 > (SELECT AVG(c0) FROM t0)",
		"SELECT * FROM t0 WHERE c0 = (SELECT MAX(c0) FROM t0 WHERE c1 = t0.c1)",
		"SELECT c0, (SELECT COUNT(*) FROM t1 WHERE t1.c0 = t0.c0) FROM t0",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateAdvancedWindow generates advanced window function patterns
func (g *SQLGenerator) GenerateAdvancedWindow() string {
	queries := []string{
		"SELECT c0, SUM(c1) OVER (PARTITION BY c0 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t0",
		"SELECT c0, AVG(c1) OVER (PARTITION BY c0 ORDER BY c1 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t0",
		"SELECT c0, FIRST_VALUE(c1) OVER (PARTITION BY c0 ORDER BY c1) FROM t0",
		"SELECT c0, LAG(c1, 1, 0) OVER (PARTITION BY c0 ORDER BY c1) FROM t0",
	}
	return queries[g.rand.Intn(len(queries))]
}

// GenerateMalformed generates intentionally malformed SQL for robustness testing
func (g *SQLGenerator) GenerateMalformed() string {
	malformed := []string{
		"SELECT IN(c",
		"SELECT MAX(0;",
		"SELECT * FROM WHERE c0 = 1",
		"INSERT INTO t0 VALUES",
		"SELECT * FROM t0 GROUP BY",
		"SELECT * FROM t0 ORDER BY",
		"SELECT * FROM t0 WHERE (c0 = 1",
		"SELECT * FROM t0 WHERE c0 = 1)",
		"BEGIN;",
		"SELECT 1 + ",
		"SELECT * FROM t0 JOIN t1 ON",
		"SELECT CASE WHEN 1 THEN 'a'",
		"SELECT COALESCE(",
		"SELECT SUBSTR('test',",
	}
	return malformed[g.rand.Intn(len(malformed))]
}

// GenerateExtendedRandomSQL extends the random SQL generation with new patterns
func (g *SQLGenerator) GenerateExtendedRandomSQL() string {
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
		g.GenerateDateTime,
		g.GenerateJSON,
		g.GenerateMath,
		g.GenerateExpression,
		g.GenerateComparisonChain,
		g.GenerateExists,
		g.GenerateHaving,
		g.GenerateDistinct,
		g.GenerateLimitOffset,
		g.GenerateCompound,
		g.GenerateBlob,
		g.GeneratePragmaExtended,
		g.GenerateSchemaAdvanced,
		g.GenerateRecursiveCTE,
		g.GenerateFlattenedSubquery,
		g.GenerateCorrelatedSubquery,
		g.GenerateAdvancedWindow,
		g.GenerateMalformed,
		g.GenerateStandaloneValues,
	}
	return generators[g.rand.Intn(len(generators))]()
}
