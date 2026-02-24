package PlainFuzzer

// =============================================================================
// SQL1999 Integration: Extended patterns from SQL1999 test suites
// These functions integrate patterns from the SQL1999 test suite into PlainFuzzer
// =============================================================================

// GenerateSQL1999RecursiveCTE generates recursive CTE patterns from R-series
func (g *SQLGenerator) GenerateSQL1999RecursiveCTE() string {
	ctes := []string{
		// Fibonacci sequence
		"WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b<100) SELECT a FROM fib",
		// Countdown
		"WITH RECURSIVE countdown(n) AS (SELECT 10 UNION ALL SELECT n-1 FROM countdown WHERE n>0) SELECT n FROM countdown",
		// Accumulator pattern
		"WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM nums WHERE x < 100) SELECT x FROM nums",
		// Multi-anchor recursive
		"WITH RECURSIVE series AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM series WHERE n < 5) SELECT * FROM series",
	}
	return ctes[g.rand.Intn(len(ctes))]
}

// GenerateSQL1999WindowFunction generates extended window functions from E111/F441
func (g *SQLGenerator) GenerateSQL1999WindowFunction() string {
	funcs := []string{
		// ROW_NUMBER variations
		"SELECT ROW_NUMBER() OVER (ORDER BY c0) FROM t0",
		"SELECT ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c0) FROM t0",
		// RANK variations
		"SELECT RANK() OVER (ORDER BY c0) FROM t0",
		"SELECT DENSE_RANK() OVER (ORDER BY c0) FROM t0",
		// LAG/LEAD with offsets and defaults
		"SELECT LAG(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT LAG(c0, 1, 0) OVER (ORDER BY c0) FROM t0",
		"SELECT LEAD(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT LEAD(c0, 2, NULL) OVER (ORDER BY c0) FROM t0",
		// FIRST/LAST/NTH_VALUE
		"SELECT FIRST_VALUE(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT LAST_VALUE(c0) OVER (ORDER BY c0) FROM t0",
		"SELECT NTH_VALUE(c0, 2) OVER (ORDER BY c0) FROM t0",
		// NTILE
		"SELECT NTILE(3) OVER (ORDER BY c0) FROM t0",
		// PERCENT_RANK and CUME_DIST
		"SELECT PERCENT_RANK() OVER (ORDER BY c0) FROM t0",
		"SELECT CUME_DIST() OVER (ORDER BY c0) FROM t0",
		// Window frames
		"SELECT SUM(c0) OVER (ORDER BY c0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t0",
		// Multiple window functions
		"SELECT ROW_NUMBER() OVER (ORDER BY c0), RANK() OVER (ORDER BY c0) FROM t0",
	}
	return funcs[g.rand.Intn(len(funcs))]
}

// GenerateSQL1999TypeCast generates CAST patterns from B2_TYPECONV/F201
func (g *SQLGenerator) GenerateSQL1999TypeCast() string {
	casts := []string{
		"SELECT CAST('42' AS INTEGER)",
		"SELECT CAST('3.14' AS REAL)",
		"SELECT CAST(100 AS TEXT)",
		"SELECT CAST(5 AS REAL)",
		"SELECT CAST(3.7 AS INTEGER)",
		"SELECT CAST('1e2' AS REAL)",
		"SELECT CAST('-99' AS INTEGER)",
		"SELECT CAST(NULL AS INTEGER)",
		"SELECT CAST(NULL AS TEXT)",
		"SELECT CAST(NULL AS REAL)",
		"SELECT CAST('3' AS INTEGER) + 2",
		"SELECT CAST('4' AS INTEGER) * 3",
		"SELECT CAST(5 AS TEXT) || ' items'",
	}
	return casts[g.rand.Intn(len(casts))]
}

// GenerateSQL1999Subquery generates subquery patterns from B7_SUBQUERY/F291_ARRAY
func (g *SQLGenerator) GenerateSQL1999Subquery() string {
	subs := []string{
		"SELECT * FROM t0 WHERE c0 = (SELECT MAX(c0) FROM t0)",
		"SELECT c0, (SELECT AVG(c0) FROM t0) AS avg_val FROM t0",
		"SELECT * FROM t0 WHERE c0 IN (SELECT c1 FROM t1)",
		"SELECT * FROM t0 WHERE c0 NOT IN (SELECT c1 FROM t1)",
		"SELECT * FROM t0 WHERE EXISTS (SELECT 1 FROM t1)",
		"SELECT * FROM t0 WHERE NOT EXISTS (SELECT 1 FROM t1)",
		"SELECT * FROM t0 WHERE c0 > (SELECT AVG(c0) FROM t0 t2 WHERE t2.c1 = t0.c1)",
		"SELECT * FROM t0 WHERE c0 > ANY (SELECT c1 FROM t1)",
		"SELECT * FROM t0 WHERE c0 > ALL (SELECT c1 FROM t1)",
		"SELECT * FROM (SELECT c0, COUNT(*) as cnt FROM t0 GROUP BY c0) AS subq",
	}
	return subs[g.rand.Intn(len(subs))]
}

// GenerateSQL1999Transaction generates transaction patterns from E151-E153
func (g *SQLGenerator) GenerateSQL1999Transaction() string {
	trans := []string{
		"BEGIN",
		"BEGIN DEFERRED",
		"BEGIN IMMEDIATE",
		"BEGIN EXCLUSIVE",
		"SAVEPOINT sp1",
		"SAVEPOINT 'save1'",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO SAVEPOINT sp1",
		"SAVEPOINT a; SAVEPOINT b; RELEASE SAVEPOINT a",
		"SAVEPOINT a; ROLLBACK TO SAVEPOINT a",
		"BEGIN; INSERT INTO t0 VALUES(1); COMMIT",
		"BEGIN; INSERT INTO t0 VALUES(1); ROLLBACK",
		"BEGIN; SAVEPOINT sp1; INSERT INTO t0 VALUES(1); RELEASE SAVEPOINT sp1; COMMIT",
	}
	return trans[g.rand.Intn(len(trans))]
}

// GenerateSQL1999Join generates JOIN patterns from F041/F401
func (g *SQLGenerator) GenerateSQL1999Join() string {
	joins := []string{
		"SELECT * FROM t0, t1 WHERE t0.c0 = t1.c0",
		"SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0",
		"SELECT * FROM t0 LEFT JOIN t1 ON t0.c0 = t1.c0",
		"SELECT * FROM t0 INNER JOIN t1 ON t0.c0 = t1.c0",
		"SELECT * FROM t0 CROSS JOIN t1",
		"SELECT * FROM t0 NATURAL JOIN t1",
		"SELECT * FROM t0 JOIN t1 USING(c0)",
		"SELECT * FROM t0 LEFT JOIN t1 USING(c0)",
		"SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c0 JOIN t2 ON t1.c1 = t2.c0",
		"SELECT a.c0, b.c0 FROM t0 AS a JOIN t0 AS b ON a.c1 = b.c1",
	}
	return joins[g.rand.Intn(len(joins))]
}

// GenerateSQL1999Expression generates complex expressions from B8_EXPRESSION
func (g *SQLGenerator) GenerateSQL1999Expression() string {
	exprs := []string{
		"SELECT 1 + 2",
		"SELECT 1 - 2",
		"SELECT 1 * 2",
		"SELECT 1 / 2",
		"SELECT 1 % 2",
		"SELECT 'a' || 'b'",
		"SELECT 1 + '2'",
		"SELECT 1 = 1",
		"SELECT 1 != 1",
		"SELECT 1 < 1",
		"SELECT 1 > 1",
		"SELECT NOT 1",
		"SELECT 1 AND 0",
		"SELECT 1 OR 0",
		"SELECT 1 IS TRUE",
		"SELECT NULL IS NULL",
		"SELECT 1 IS NOT NULL",
		"SELECT 5 BETWEEN 1 AND 10",
		"SELECT 5 NOT BETWEEN 1 AND 10",
		"SELECT 1 IN (1, 2, 3)",
		"SELECT 1 NOT IN (1, 2, 3)",
		"SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END",
	}
	return exprs[g.rand.Intn(len(exprs))]
}

// GenerateSQL1999Aggregate generates aggregate patterns from B5_AGGREGATE/F441
func (g *SQLGenerator) GenerateSQL1999Aggregate() string {
	aggs := []string{
		"SELECT COUNT(*) FROM t0",
		"SELECT SUM(c0) FROM t0",
		"SELECT AVG(c0) FROM t0",
		"SELECT MIN(c0) FROM t0",
		"SELECT MAX(c0) FROM t0",
		"SELECT c0, COUNT(*) FROM t0 GROUP BY c0",
		"SELECT c0, SUM(c1) FROM t0 GROUP BY c0",
		"SELECT DISTINCT c0 FROM t0",
		"SELECT COUNT(DISTINCT c0) FROM t0",
		"SELECT c0, COUNT(*) FROM t0 GROUP BY c0 HAVING COUNT(*) > 1",
		"SELECT GROUP_CONCAT(c0) FROM t0",
		"SELECT GROUP_CONCAT(c0, ',') FROM t0",
		"SELECT TOTAL(c0) FROM t0",
	}
	return aggs[g.rand.Intn(len(aggs))]
}

// GenerateSQL1999RandomSQL generates random SQL covering all SQL1999 patterns
func (g *SQLGenerator) GenerateSQL1999RandomSQL() string {
	generators := []func() string{
		g.GenerateSQL1999RecursiveCTE,
		g.GenerateSQL1999WindowFunction,
		g.GenerateSQL1999TypeCast,
		g.GenerateSQL1999Subquery,
		g.GenerateSQL1999Transaction,
		g.GenerateSQL1999Join,
		g.GenerateSQL1999Expression,
		g.GenerateSQL1999Aggregate,
		g.GenerateCTE,
		g.GenerateWindowFunction,
	}
	return generators[g.rand.Intn(len(generators))]()
}
