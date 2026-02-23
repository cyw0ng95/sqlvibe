# Plan v0.9.5 - SQL Compatibility & Maintenance Statements

## Summary

This plan implements SQL compatibility features and maintenance statements for v0.9.5.

---

## Features

### Window Functions (5 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 1 | **ROW_NUMBER()** | Assign row numbers to result sets |
| 2 | **RANK() / DENSE_RANK()** | Assign ranks with/without gaps |
| 3 | **OVER (PARTITION BY)** | Window partition support |
| 4 | **OVER (ORDER BY)** | Ordered window frames |
| 5 | **OVER (ROWS/RANGE)** | Row and range frame specifications |

### CTE & Advanced DML (4 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 6 | **CTE (WITH)** | Non-recursive common table expressions |
| 7 | **Multi-VALUES INSERT** | INSERT INTO t VALUES (...), (...), (...) |
| 8 | **UPSERT** | INSERT ON CONFLICT DO NOTHING/UPDATE |
| 9 | **SELECT INTO** | SELECT ... INTO temp table |

### Maintenance Statements (4 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 10 | **ANALYZE** | Collect table/index statistics |
| 11 | **EXPLAIN QUERY PLAN** | Display query execution plan |
| 12 | **REINDEX** | Rebuild indexes |
| 13 | **VACUUM** | Compact database file |

### Compatibility Extensions (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 14 | **AUTOINCREMENT** | Primary key auto-increment semantics |
| 15 | **LIKE ESCAPE** | LIKE with ESCAPE clause |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | ROW_NUMBER() | 4h |
| 2 | RANK() / DENSE_RANK() | 3h |
| 3 | OVER (PARTITION BY) | 4h |
| 4 | OVER (ORDER BY) | 3h |
| 5 | OVER (ROWS/RANGE) | 3h |
| 6 | CTE (WITH) | 5h |
| 7 | Multi-VALUES INSERT | 2h |
| 8 | UPSERT | 4h |
| 9 | SELECT INTO | 2h |
| 10 | ANALYZE | 4h |
| 11 | EXPLAIN QUERY PLAN | 3h |
| 12 | REINDEX | 3h |
| 13 | VACUUM | 5h |
| 14 | AUTOINCREMENT | 3h |
| 15 | LIKE ESCAPE | 2h |
| 16 | Testing & Integration | 6h |

**Total:** ~52 hours

---

## Success Criteria

### Phase 1: ROW_NUMBER()

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ROW_NUMBER() | Works | [x] |
| Basic row numbering works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 2: RANK() / DENSE_RANK()

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes RANK() | Works | [x] |
| Parser recognizes DENSE_RANK() | Works | [x] |
| Rank with gaps works | Works | [x] |
| Rank without gaps works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 3: OVER (PARTITION BY)

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes PARTITION BY | Works | [x] |
| Partition grouping works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 4: OVER (ORDER BY)

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ORDER BY in OVER | Works | [x] |
| Ordered window frames work | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 5: OVER (ROWS/RANGE)

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ROWS | Works | [x] |
| Parser recognizes RANGE | Works | [x] |
| Frame boundaries work | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 6: CTE (WITH)

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes WITH | Works | [x] |
| Non-recursive CTE works | Works | [x] |
| Multiple CTEs work | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 7: Multi-VALUES INSERT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes multiple VALUES | Works | [x] |
| Batch insert works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 8: UPSERT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ON CONFLICT | Works | [x] |
| DO NOTHING works | Works | [x] |
| DO UPDATE works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 9: SELECT INTO

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes SELECT INTO | Works | [x] |
| Temp table creation works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 10: ANALYZE

| Criteria | Target | Status |
|----------|--------|--------|
| ANALYZE command executes | Works | [x] |
| Statistics collected | Works | [x] |
| Query optimizer uses stats | Works | [x] |

### Phase 11: EXPLAIN QUERY PLAN

| Criteria | Target | Status |
|----------|--------|--------|
| EXPLAIN QUERY PLAN executes | Works | [x] |
| Plan output is readable | Works | [x] |
| Shows index usage | Works | [x] |

### Phase 12: REINDEX

| Criteria | Target | Status |
|----------|--------|--------|
| REINDEX command executes | Works | [x] |
| Single index rebuild works | Works | [x] |
| All indexes rebuild works | Works | [x] |

### Phase 13: VACUUM

| Criteria | Target | Status |
|----------|--------|--------|
| VACUUM command executes | Works | [x] |
| Database size reduced | Works | [x] |
| Data integrity maintained | Works | [x] |

### Phase 14: AUTOINCREMENT

| Criteria | Target | Status |
|----------|--------|--------|
| AUTOINCREMENT parsed | Works | [x] |
| Unique ID generation works | Works | [x] |
| Differs from plain INTEGER PK | Works | [x] |

### Phase 15: LIKE ESCAPE

| Criteria | Target | Status |
|----------|--------|--------|
| ESCAPE clause parsed | Works | [x] |
| Custom escape character works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 16: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| All unit tests pass | 100% | [x] |
| SQLite comparison tests pass | 100% | [x] |
| New SQL:1999 tests added | Done | [x] |

---

## Implementation Details

### Window Functions

```sql
-- ROW_NUMBER
SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM employees;

-- RANK with gaps
SELECT department, salary, RANK() OVER (PARTITION BY department ORDER BY salary) FROM employees;

-- DENSE_RANK without gaps
SELECT department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary) FROM employees;

-- Moving average with ROWS
SELECT date, price, AVG(price) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM stocks;
```

### CTE

```sql
-- Simple CTE
WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
)
SELECT region, total_sales FROM regional_sales WHERE total_sales > 10000;

-- Multiple CTEs
WITH
    cte1 AS (SELECT * FROM t1 WHERE x > 10),
    cte2 AS (SELECT * FROM t2 WHERE y < 20)
SELECT cte1.a, cte2.b FROM cte1 JOIN cte2 ON cte1.id = cte2.id;
```

### Multi-VALUES INSERT

```sql
INSERT INTO users (name, age) VALUES
    ('Alice', 30),
    ('Bob', 25),
    ('Charlie', 35);
```

### UPSERT

```sql
INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')
ON CONFLICT(id) DO UPDATE SET name = 'John Updated';

INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')
ON CONFLICT(id) DO NOTHING;
```

### EXPLAIN QUERY PLAN

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 25;
-- Output: SCAN users, USING INDEX idx_age
```

### VACUUM

```sql
VACUUM;
VACUUM users;  -- vacuum specific table
```

### AUTOINCREMENT

```sql
CREATE TABLE sequences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);
-- IDs will always be unique and monotonically increasing
```

---

## Expected Results

### Compatibility

| Feature | Status |
|---------|--------|
| ROW_NUMBER() | 100% SQLite compatible |
| RANK() / DENSE_RANK() | 100% SQLite compatible |
| OVER (PARTITION BY) | 100% SQLite compatible |
| OVER (ORDER BY) | 100% SQLite compatible |
| OVER (ROWS/RANGE) | 100% SQLite compatible |
| CTE (WITH) | 100% SQLite compatible |
| Multi-VALUES INSERT | 100% SQLite compatible |
| UPSERT | 100% SQLite compatible |
| SELECT INTO | 100% SQLite compatible |
| ANALYZE | 100% SQLite compatible |
| EXPLAIN QUERY PLAN | 100% SQLite compatible |
| REINDEX | 100% SQLite compatible |
| VACUUM | 100% SQLite compatible |
| AUTOINCREMENT | 100% SQLite compatible |
| LIKE ESCAPE | 100% SQLite compatible |

---

## Dependencies

- Window functions require expression evaluator enhancements
- CTE requires parser and executor modifications
- ANALYZE requires statistics storage in schema
- EXPLAIN QUERY PLAN requires plan representation
