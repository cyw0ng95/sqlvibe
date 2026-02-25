package PlainFuzzer

import (
	"fmt"
	"math/rand"
	"strings"
)

// =============================================================================
// SQLSmith-style Deep Fuzzing: Type-aware, Schema-aware, Complex expressions
// =============================================================================

// DeepSQLGenerator generates complex SQL with type-aware expressions and deep nesting
type DeepSQLGenerator struct {
	rand   *rand.Rand
	Schema *SchemaTracker
	Budget *ComplexityBudget
}

func NewDeepSQLGenerator(seed int64) *DeepSQLGenerator {
	return &DeepSQLGenerator{
		rand:   rand.New(rand.NewSource(seed)),
		Schema: NewSchemaTracker(seed),
		Budget: NewComplexityBudget(),
	}
}

// DeepGenerateComplexQuery generates a complex query using schema with deep nesting
func (g *DeepSQLGenerator) DeepGenerateComplexQuery() string {
	// Ensure we have tables.Schema.HasTables()
	if !g.Schema.HasTables() {
		numTables := g.rand.Intn(3) + 1
		for i := 0; i < numTables; i++ {
			g.Schema.AddTable(g.rand.Intn(5)+1, g.rand.Intn(3) == 0)
		}
	}

	// Reset budget for complexity control
	g.Budget.Reset()

	// Generate different query types
	queryTypes := []func() string{
		g.generateDeepSelect,
		g.generateDeepJoin,
		g.generateDeepSubquery,
		g.generateDeepAggregate,
		g.generateDeepCTE,
		g.generateDeepWindow,
		g.generateDeepSetOperation,
		g.generateDeepInsertUpdate,
		g.generateDeepPragma,
		g.generateDeepTransaction,
		g.generateDeepAlterView,
		g.generateDeepExpression,
	}

	return queryTypes[g.rand.Intn(len(queryTypes))]()
}

// generateDeepSelect generates SELECT with deeply nested expressions
func (g *DeepSQLGenerator) generateDeepSelect() string {
	if !g.Schema.HasTables() {
		return g.SchemaFallback()
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil || len(table.Columns) == 0 {
		return g.SchemaFallback()
	}

	// Generate SELECT clause with deep expressions
	numCols := g.rand.Intn(4) + 1
	var selectCols []string
	for i := 0; i < numCols; i++ {
		colIdx := g.rand.Intn(len(table.Columns))
		col := table.Columns[colIdx]
		selectCols = append(selectCols, g.generateTypeAwareExpression(col.Type))
	}

	// Build query with multiple clauses
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ", "), tableName)

	// WHERE with complex expression
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		col := table.Columns[g.rand.Intn(len(table.Columns))]
		query += g.generateComplexWhere(col)
	}

	// GROUP BY
	if g.rand.Intn(3) == 0 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		query += fmt.Sprintf(" GROUP BY %s", table.Columns[colIdx].Name)
	}

	// HAVING with aggregate
	if g.rand.Intn(4) == 0 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		_ = colIdx
		query += fmt.Sprintf(" HAVING COUNT(*) > %d", g.rand.Intn(10))
	}

	// ORDER BY
	if g.rand.Intn(3) == 0 && len(table.Columns) > 0 {
		colIdx := g.rand.Intn(len(table.Columns))
		dir := "ASC"
		if g.rand.Intn(2) == 0 {
			dir = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s", table.Columns[colIdx].Name, dir)
	}

	// LIMIT
	if g.rand.Intn(4) == 0 {
		query += fmt.Sprintf(" LIMIT %d", g.rand.Intn(100))
	}

	return query
}

// generateTypeAwareExpression generates expressions matching column type
func (g *DeepSQLGenerator) generateTypeAwareExpression(colType ColumnType) string {
	g.Budget.Reset()

	// Use depth control for complex expressions
	return g.generateRecursiveExpr(colType, 3)
}

func (g *DeepSQLGenerator) generateRecursiveExpr(colType ColumnType, maxDepth int) string {
	if maxDepth <= 0 || (g.Budget != nil && !g.Budget.CanIncreaseDepth()) {
		return g.generateSimpleValue(colType)
	}

	choice := g.rand.Intn(6)
	switch choice {
	case 0:
		// Function with nested expression
		return g.generateNestedFunc(colType, maxDepth-1)
	case 1:
		// Binary expression
		return g.generateBinaryExpr(colType, maxDepth-1)
	case 2:
		// CASE expression
		return g.generateDeepCase(colType, maxDepth-1)
	case 3:
		// Cast expression
		return g.generateCastExpr(colType)
	case 4:
		// Function with multiple args
		return g.generateMultiArgFunc(colType)
	default:
		return g.generateSimpleValue(colType)
	}
}

func (g *DeepSQLGenerator) generateNestedFunc(colType ColumnType, depth int) string {
	inner := g.generateRecursiveExpr(colType, depth-1)

	// Type-appropriate functions
	var funcs []string
	switch colType {
	case ColumnTypeInteger, ColumnTypeReal:
		funcs = []string{
			"ABS(%s)", "LENGTH(%s)", "CEIL(%s)", "FLOOR(%s)", "ROUND(%s, 0)",
			"SIGN(%s)", "SQRT(ABS(%s))",
		}
	case ColumnTypeText:
		funcs = []string{
			"UPPER(%s)", "LOWER(%s)", "TRIM(%s)", "LTRIM(%s)", "RTRIM(%s)",
			"SUBSTR(%s, 1, 10)", "REPLACE(%s, 'a', 'b')", "INSTR(%s, 'a')",
		}
	default:
		funcs = []string{"%s"}
	}

	return fmt.Sprintf(funcs[g.rand.Intn(len(funcs))], inner)
}

func (g *DeepSQLGenerator) generateBinaryExpr(colType ColumnType, depth int) string {
	left := g.generateRecursiveExpr(colType, depth-1)
	right := g.generateRecursiveExpr(colType, depth-1)

	var ops []string
	switch colType {
	case ColumnTypeInteger, ColumnTypeReal:
		ops = []string{"+", "-", "*", "/", "%%"}
	case ColumnTypeText:
		ops = []string{"||"}
	default:
		ops = []string{"+", "-"}
	}

	op := ops[g.rand.Intn(len(ops))]
	return fmt.Sprintf("(%s %s %s)", left, op, right)
}

func (g *DeepSQLGenerator) generateDeepCase(colType ColumnType, depth int) string {
	numWhen := g.rand.Intn(3) + 1
	var whens []string
	for i := 0; i < numWhen; i++ {
		cond := g.generateCondition()
		val := g.generateRecursiveExpr(colType, depth-1)
		whens = append(whens, fmt.Sprintf("WHEN %s THEN %s", cond, val))
	}
	elseVal := g.generateSimpleValue(colType)
	return fmt.Sprintf("CASE %s ELSE %s END", strings.Join(whens, " "), elseVal)
}

func (g *DeepSQLGenerator) generateCastExpr(colType ColumnType) string {
	inner := g.generateSimpleValue(colType)
	var toType string
	switch colType {
	case ColumnTypeInteger:
		toType = "TEXT"
	case ColumnTypeReal:
		toType = "INTEGER"
	case ColumnTypeText:
		toType = "INTEGER"
	default:
		toType = "TEXT"
	}
	return fmt.Sprintf("CAST(%s AS %s)", inner, toType)
}

func (g *DeepSQLGenerator) generateMultiArgFunc(colType ColumnType) string {
	// Functions with multiple arguments
	args := []string{
		g.generateSimpleValue(colType),
		g.generateSimpleValue(colType),
		g.generateSimpleValue(colType),
	}

	switch colType {
	case ColumnTypeInteger, ColumnTypeReal:
		return fmt.Sprintf("COALESCE(%s, %s, %s)", args[0], args[1], args[2])
	case ColumnTypeText:
		return fmt.Sprintf("COALESCE(%s, %s)", args[0], args[1])
	default:
		return fmt.Sprintf("COALESCE(%s, %s)", args[0], args[1])
	}
}

func (g *DeepSQLGenerator) generateSimpleValue(colType ColumnType) string {
	switch colType {
	case ColumnTypeInteger:
		return fmt.Sprintf("%d", g.rand.Intn(1000)-500)
	case ColumnTypeReal:
		return fmt.Sprintf("%f", g.rand.Float64()*100)
	case ColumnTypeText:
		return fmt.Sprintf("'%s'", g.generateString())
	default:
		return "NULL"
	}
}

func (g *DeepSQLGenerator) generateCondition() string {
	conds := []string{
		"c0 > 0", "c0 < 100", "c0 = 1", "c0 >= 10", "c0 <= 50",
		"c0 != 0", "c0 IS NULL", "c0 IS NOT NULL",
		"c0 BETWEEN 1 AND 10", "c0 NOT BETWEEN 0 AND 100",
		"c0 IN (1, 2, 3)", "c0 NOT IN (5, 10)",
		"c0 LIKE '%test%'", "c0 LIKE 'test%'",
	}
	return conds[g.rand.Intn(len(conds))]
}

func (g *DeepSQLGenerator) generateComplexWhere(col ColumnInfo) string {
	// Generate complex WHERE with nested expressions
	_ = g.generateTypeAwareExpression(col.Type) // nested expression for complexity
	ops := []string{"=", ">", "<", ">=", "<=", "!="}
	op := ops[g.rand.Intn(len(ops))]
	return fmt.Sprintf(" WHERE %s %s %s", col.Name, op, g.generateSimpleValue(col.Type))
}

// generateDeepJoin generates complex JOIN queries
func (g *DeepSQLGenerator) generateDeepJoin() string {
	if g.Schema == nil || len(g.Schema.Tables) < 2 {
		return g.SchemaFallback()
	}

	// Get two tables
	tables := make([]string, 0, len(g.Schema.Tables))
	for name := range g.Schema.Tables {
		tables = append(tables, name)
	}
	if len(tables) < 2 {
		return g.SchemaFallback()
	}

	t1 := tables[g.rand.Intn(len(tables))]
	t2 := tables[g.rand.Intn(len(tables))]
	for t2 == t1 && len(tables) > 1 {
		t2 = tables[g.rand.Intn(len(tables))]
	}

	joinTypes := []string{"JOIN", "LEFT JOIN", "INNER JOIN", "CROSS JOIN"}
	joinType := joinTypes[g.rand.Intn(len(joinTypes))]

	query := fmt.Sprintf("SELECT * FROM %s %s %s ON %s.c0 = %s.c0", t1, joinType, t2, t1, t2)

	// Add WHERE
	if g.rand.Intn(2) == 1 {
		query += " WHERE 1=1"
	}

	// Add ORDER BY/LIMIT
	if g.rand.Intn(3) == 0 {
		query += fmt.Sprintf(" ORDER BY %s.c0 LIMIT %d", t1, g.rand.Intn(50))
	}

	return query
}

// generateDeepSubquery generates complex subqueries
func (g *DeepSQLGenerator) generateDeepSubquery() string {
	subqueryTypes := []func() string{
		g.generateScalarSubquery,
		g.generateExistsSubquery,
		g.generateInSubquery,
		g.generateCorrelatedSubquery,
		g.generateNestedSubquery,
	}
	return subqueryTypes[g.rand.Intn(len(subqueryTypes))]()
}

func (g *DeepSQLGenerator) generateScalarSubquery() string {
	if !g.Schema.HasTables() {
		return "SELECT (SELECT MAX(c0) FROM t0)"
	}
	table := g.Schema.GetTable(g.Schema.GetRandomTable())
	if table == nil {
		return "SELECT (SELECT MAX(c0) FROM t0)"
	}
	colName := table.Columns[0].Name
	return fmt.Sprintf("SELECT (SELECT MAX(%s) FROM %s)", colName, table.Name)
}

func (g *DeepSQLGenerator) generateExistsSubquery() string {
	return "SELECT * FROM t0 WHERE EXISTS(SELECT 1 FROM t1)"
}

func (g *DeepSQLGenerator) generateInSubquery() string {
	return "SELECT * FROM t0 WHERE c0 IN (SELECT c1 FROM t1)"
}

func (g *DeepSQLGenerator) generateCorrelatedSubquery() string {
	return "SELECT * FROM t0 WHERE c0 > (SELECT AVG(c0) FROM t0 t2 WHERE t2.c1 = t0.c1)"
}

func (g *DeepSQLGenerator) generateNestedSubquery() string {
	return "SELECT * FROM t0 WHERE c0 > (SELECT AVG(c0) FROM t0 WHERE c0 > (SELECT MIN(c0) FROM t0))"
}

// generateDeepAggregate generates complex aggregate queries
func (g *DeepSQLGenerator) generateDeepAggregate() string {
	if !g.Schema.HasTables() {
		return "SELECT COUNT(*), SUM(c0), AVG(c0), MIN(c0), MAX(c0) FROM t0"
	}

	tableName := g.Schema.GetRandomTable()
	table := g.Schema.GetTable(tableName)
	if table == nil {
		return "SELECT COUNT(*), SUM(c0) FROM t0"
	}

	// Multiple aggregates
	aggs := []string{"COUNT(*)", "SUM(c0)", "AVG(c0)", "MIN(c0)", "MAX(c0)"}
	numAggs := g.rand.Intn(3) + 2
	if numAggs > len(aggs) {
		numAggs = len(aggs)
	}

	var selected []string
	indices := g.rand.Perm(len(aggs))[:numAggs]
	for _, idx := range indices {
		selected = append(selected, aggs[idx])
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selected, ", "), tableName)

	// GROUP BY
	if g.rand.Intn(2) == 1 && len(table.Columns) > 0 {
		query += fmt.Sprintf(" GROUP BY %s", table.Columns[0].Name)
	}

	// HAVING
	if g.rand.Intn(3) == 0 {
		query += " HAVING COUNT(*) > 1"
	}

	return query
}

// generateDeepCTE generates complex CTE queries
func (g *DeepSQLGenerator) generateDeepCTE() string {
	ctes := []string{
		"WITH cte AS (SELECT 1 AS n) SELECT * FROM cte WHERE n > 0",
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt",
		"WITH a AS (SELECT 1 AS v), b AS (SELECT 2 AS v) SELECT a.v, b.v FROM a, b",
		"WITH cte AS (SELECT c0, SUM(c1) AS total FROM t0 GROUP BY c0) SELECT * FROM cte WHERE total > 100",
	}
	return ctes[g.rand.Intn(len(ctes))]
}

// generateDeepWindow generates complex window function queries
func (g *DeepSQLGenerator) generateDeepWindow() string {
	windows := []string{
		"SELECT ROW_NUMBER() OVER (ORDER BY c0), RANK() OVER (ORDER BY c0), DENSE_RANK() OVER (ORDER BY c0) FROM t0",
		"SELECT c0, SUM(c1) OVER (PARTITION BY c0 ORDER BY c1) FROM t0",
		"SELECT c0, LAG(c1) OVER (ORDER BY c0), LEAD(c1) OVER (ORDER BY c0) FROM t0",
		"SELECT NTILE(3) OVER (ORDER BY c0) FROM t0",
		"SELECT FIRST_VALUE(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT PERCENT_RANK() OVER (ORDER BY c0) FROM t0",
		"SELECT SUM(c0) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t0",
	}
	return windows[g.rand.Intn(len(windows))]
}

// generateDeepSetOperation generates UNION/INTERSECT/EXCEPT queries
func (g *DeepSQLGenerator) generateDeepSetOperation() string {
	ops := []string{"UNION", "UNION ALL", "INTERSECT", "EXCEPT"}
	op := ops[g.rand.Intn(len(ops))]

	queries := []string{
		fmt.Sprintf("SELECT c0 FROM t0 %s SELECT c0 FROM t1", op),
		fmt.Sprintf("SELECT 1 %s SELECT 2 %s SELECT 3", op, ops[g.rand.Intn(len(ops))]),
		fmt.Sprintf("SELECT c0 FROM t0 WHERE c0 > 0 %s SELECT c0 FROM t0 WHERE c0 < 100", op),
	}
	return queries[g.rand.Intn(len(queries))]
}

// generateDeepInsertUpdate generates complex DML
func (g *DeepSQLGenerator) generateDeepInsertUpdate() string {
	if g.rand.Intn(2) == 0 {
		// INSERT with subquery
		inserts := []string{
			"INSERT INTO t0 SELECT * FROM t1",
			"INSERT INTO t0(c0, c1) SELECT c0, c1 FROM t1",
			"INSERT INTO t0 VALUES (1, 2) ON CONFLICT(c0) DO NOTHING",
			"INSERT INTO t0 VALUES (1, 2) ON CONFLICT(c0) DO UPDATE SET c1 = excluded.c1",
		}
		return inserts[g.rand.Intn(len(inserts))]
	}

	// UPDATE with subquery
	updates := []string{
		"UPDATE t0 SET c0 = (SELECT MAX(c0) FROM t1)",
		"UPDATE t0 SET c0 = c0 + 1 WHERE c0 IN (SELECT c1 FROM t1)",
	}
	return updates[g.rand.Intn(len(updates))]
}

// SchemaFallback returns a basic query when schema is not available
func (g *DeepSQLGenerator) SchemaFallback() string {
	queries := []string{
		"SELECT COUNT(*) FROM t0",
		"SELECT * FROM t0, t1 WHERE t0.c0 = t1.c0",
		"SELECT MAX(c0), MIN(c0), AVG(c0) FROM t0",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"SELECT ROW_NUMBER() OVER () FROM t0",
	}
	return queries[g.rand.Intn(len(queries))]
}

func (g *DeepSQLGenerator) generateString() string {
	length := g.rand.Intn(10) + 3
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[g.rand.Intn(len(chars))]
	}
	return string(result)
}

// generateDeepPragma generates complex PRAGMA statements
func (g *DeepSQLGenerator) generateDeepPragma() string {
	pragmaTypes := []func() string{
		g.generateWalPragma,
		g.generateStoragePragma,
		g.generateCachePragma,
	}
	return pragmaTypes[g.rand.Intn(len(pragmaTypes))]()
}

func (g *DeepSQLGenerator) generateWalPragma() string {
	walPragmas := []string{
		"PRAGMA wal_autocheckpoint = 1000",
		"PRAGMA wal_autocheckpoint = 0",
		"PRAGMA wal_checkpoint(passive)",
		"PRAGMA wal_checkpoint(full)",
		"PRAGMA wal_checkpoint(truncate)",
		"PRAGMA journal_size_limit = 10000",
		"PRAGMA journal_size_limit = 0",
		"PRAGMA journal_mode = wal",
	}
	return walPragmas[g.rand.Intn(len(walPragmas))]
}

func (g *DeepSQLGenerator) generateStoragePragma() string {
	storagePragmas := []string{
		"PRAGMA shrink_memory",
		"PRAGMA optimize",
		"PRAGMA integrity_check",
		"PRAGMA quick_check",
		"PRAGMA cache_size = 1000",
		"PRAGMA cache_size = -2000",
	}
	return storagePragmas[g.rand.Intn(len(storagePragmas))]
}

func (g *DeepSQLGenerator) generateCachePragma() string {
	cachePragmas := []string{
		"PRAGMA cache_size",
		"PRAGMA cache_grind",
		"PRAGMA page_size",
		"PRAGMA mmap_size = 0",
		"PRAGMA read_uncommitted = 1",
	}
	return cachePragmas[g.rand.Intn(len(cachePragmas))]
}

// generateDeepTransaction generates transaction statements
func (g *DeepSQLGenerator) generateDeepTransaction() string {
	txTypes := []func() string{
		g.generateBasicTransaction,
		g.generateSavepointTransaction,
		g.generateDeferTransaction,
	}
	return txTypes[g.rand.Intn(len(txTypes))]()
}

func (g *DeepSQLGenerator) generateBasicTransaction() string {
	txs := []string{
		"BEGIN; INSERT INTO t0 VALUES(1); COMMIT",
		"BEGIN; UPDATE t0 SET c0 = c0 + 1; ROLLBACK",
		"BEGIN IMMEDIATE; INSERT INTO t0 VALUES(1); COMMIT",
		"BEGIN EXCLUSIVE; INSERT INTO t0 VALUES(1); COMMIT",
	}
	return txs[g.rand.Intn(len(txs))]
}

func (g *DeepSQLGenerator) generateSavepointTransaction() string {
	sps := []string{
		"SAVEPOINT sp1; INSERT INTO t0 VALUES(1); RELEASE SAVEPOINT sp1",
		"SAVEPOINT sp1; UPDATE t0 SET c0 = 1; ROLLBACK TO SAVEPOINT sp1",
		"SAVEPOINT a; SAVEPOINT b; RELEASE SAVEPOINT a",
	}
	return sps[g.rand.Intn(len(sps))]
}

func (g *DeepSQLGenerator) generateDeferTransaction() string {
	defs := []string{
		"BEGIN DEFERRED; SELECT 1; COMMIT",
		"BEGIN; CREATE TABLE tx(a INT); ROLLBACK",
		"BEGIN; DROP TABLE IF EXISTS t1; COMMIT",
	}
	return defs[g.rand.Intn(len(defs))]
}

// generateDeepAlterView generates ALTER and VIEW statements
func (g *DeepSQLGenerator) generateDeepAlterView() string {
	avTypes := []func() string{
		g.generateViewStatement,
		g.generateAlterTable,
		g.generateIndexStatement,
	}
	return avTypes[g.rand.Intn(len(avTypes))]()
}

func (g *DeepSQLGenerator) generateViewStatement() string {
	views := []string{
		"CREATE VIEW v1 AS SELECT 1 AS a",
		"CREATE VIEW IF NOT EXISTS v2 AS SELECT c0, c1 FROM t0",
		"DROP VIEW IF EXISTS v1",
		"CREATE TEMP VIEW temp_v AS SELECT 1",
	}
	return views[g.rand.Intn(len(views))]
}

func (g *DeepSQLGenerator) generateAlterTable() string {
	alters := []string{
		"ALTER TABLE t0 ADD COLUMN new_col TEXT",
		"ALTER TABLE t0 RENAME TO t1",
		"ALTER TABLE t0 RENAME COLUMN c0 TO c1",
	}
	return alters[g.rand.Intn(len(alters))]
}

func (g *DeepSQLGenerator) generateIndexStatement() string {
	indexes := []string{
		"CREATE INDEX idx1 ON t0(c0)",
		"CREATE INDEX IF NOT EXISTS idx2 ON t0(c1 DESC)",
		"CREATE UNIQUE INDEX idx3 ON t0(c0, c1)",
		"CREATE INDEX idx4 ON t0(c0) WHERE c0 > 0",
		"DROP INDEX IF EXISTS idx1",
		"REINDEX",
		"REINDEX t0",
	}
	return indexes[g.rand.Intn(len(indexes))]
}

// generateDeepExpression generates complex expressions
func (g *DeepSQLGenerator) generateDeepExpression() string {
	exprTypes := []func() string{
		g.generateMathExpression,
		g.generateStringExpression,
		g.generateNULLExpression,
		g.generateCastExpression,
	}
	return exprTypes[g.rand.Intn(len(exprTypes))]()
}

func (g *DeepSQLGenerator) generateMathExpression() string {
	mathExprs := []string{
		"SELECT ABS(-1 * c0) FROM t0",
		"SELECT c0 + c1 * 2 FROM t0",
		"SELECT (c0 + c1) / c2 FROM t0",
		"SELECT c0 % 10 FROM t0",
		"SELECT c0 | c1 FROM t0",
		"SELECT c0 & 255 FROM t0",
		"SELECT ~c0 FROM t0",
		"SELECT (c0 << 2) FROM t0",
	}
	return mathExprs[g.rand.Intn(len(mathExprs))]
}

func (g *DeepSQLGenerator) generateStringExpression() string {
	strExprs := []string{
		"SELECT UPPER(c0) FROM t0",
		"SELECT LOWER(c0) FROM t0",
		"SELECT LENGTH(c0) FROM t0",
		"SELECT SUBSTR(c0, 1, 5) FROM t0",
		"SELECT TRIM(c0) FROM t0",
		"SELECT REPLACE(c0, 'a', 'b') FROM t0",
		"SELECT INSTR(c0, 'test') FROM t0",
		"SELECT c0 || c1 FROM t0",
	}
	return strExprs[g.rand.Intn(len(strExprs))]
}

func (g *DeepSQLGenerator) generateNULLExpression() string {
	nullExprs := []string{
		"SELECT COALESCE(NULL, c0, 1) FROM t0",
		"SELECT IFNULL(NULL, 'default') FROM t0",
		"SELECT NULLIF(c0, 0) FROM t0",
		"SELECT IIF(c0 IS NULL, 'yes', 'no') FROM t0",
		"SELECT c0 + NULL FROM t0",
		"SELECT NULL = NULL",
		"SELECT NULL OR 1",
	}
	return nullExprs[g.rand.Intn(len(nullExprs))]
}

func (g *DeepSQLGenerator) generateCastExpression() string {
	castExprs := []string{
		"SELECT CAST(c0 AS TEXT) FROM t0",
		"SELECT CAST('123' AS INTEGER) FROM t0",
		"SELECT CAST(1.5 AS INTEGER) FROM t0",
		"SELECT CAST('1.5' AS REAL) FROM t0",
		"SELECT CAST(c0 AS BLOB) FROM t0",
		"SELECT CAST(X'414243' AS TEXT)",
	}
	return castExprs[g.rand.Intn(len(castExprs))]
}
