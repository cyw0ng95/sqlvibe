package PlainFuzzer

import (
	"fmt"
	"math/rand"
	"strings"
)

type SQLGenerator struct {
	rand *rand.Rand
}

func NewSQLGenerator(seed int64) *SQLGenerator {
	return &SQLGenerator{
		rand: rand.New(rand.NewSource(seed)),
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
		g.mutateAddChar,
		g.mutateRemoveChar,
		g.mutateChangeChar,
		g.mutateAddSpace,
		g.mutateAddKeyword,
		g.mutateRemoveKeyword,
		g.mutateChangeNumber,
		g.mutateAddNull,
		g.mutateChangeOperator,
		g.mutateAddParen,
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
