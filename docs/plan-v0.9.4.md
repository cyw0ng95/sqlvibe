# Plan v0.9.4 - SQL Compatibility Expansion

## Summary

This plan implements additional SQL compatibility features for v0.9.4.

## Features

### SQL Constraints & Indexes (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 1 | **CHECK 约束** | CREATE TABLE CHECK (expr) - validate constraints on insert/update |
| 2 | **Partial Index** | CREATE INDEX WHERE expr - index with condition |
| 3 | **Expression Index** | CREATE INDEX ON expr(col) - index on expression |

### DML Extensions (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 4 | **RETURNING 子句** | INSERT/UPDATE/DELETE RETURNING * or column list |
| 5 | **UPDATE ... FROM** | UPDATE t1 SET ... FROM t2 WHERE ... (PostgreSQL style) |
| 6 | **DELETE ... USING** | DELETE FROM t1 USING t1, t2 WHERE ... |

### Advanced Operators (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 7 | **GLOB 操作符** | * and ? wildcards (case-sensitive LIKE alternative) |
| 8 | **MATCH 操作符** | Full-text search operator for FTS tables |

### Schema Operations (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 9 | **COLLATE 支持** | NOCASE, RTRIM, BINARY collation sequences |
| 10 | **ALTER TABLE** | ADD COLUMN, RENAME TO |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | CHECK Constraint | 4h |
| 2 | Partial Index | 6h |
| 3 | Expression Index | 4h |
| 4 | RETURNING Clause | 4h |
| 5 | UPDATE ... FROM | 3h |
| 6 | DELETE ... USING | 2h |
| 7 | GLOB Operator | 2h |
| 8 | MATCH Operator | 3h |
| 9 | COLLATE Support | 3h |
| 10 | ALTER TABLE | 4h |
| 11 | Testing & Integration | 5h |

**Total:** ~40 hours

---

## Success Criteria

### Phase 1: CHECK Constraint

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes CHECK | Works | [x] |
| Constraint evaluated on INSERT | Works | [x] |
| Constraint evaluated on UPDATE | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 2: Partial Index

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes WHERE | Works | [x] |
| Index stores filter condition | Works | [x] |
| Scan uses partial index | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 3: Expression Index

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes expression | Works | [x] |
| Index computes expression | Works | [x] |
| Query uses expression index | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 4: RETURNING Clause

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes RETURNING | Works | [x] |
| INSERT RETURNING works | Works | [x] |
| UPDATE RETURNING works | Works | [x] |
| DELETE RETURNING works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 5: UPDATE ... FROM

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes FROM | Works | [x] |
| Multi-table UPDATE works | Works | [x] |
| JOIN in UPDATE works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 6: DELETE ... USING

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes USING | Works | [x] |
| Multi-table DELETE works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 7: GLOB Operator

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes GLOB | Works | [x] |
| * matches any characters | Works | [x] |
| ? matches single character | Works | [x] |
| Case-sensitive (unlike LIKE) | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 8: MATCH Operator

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes MATCH | Works | [x] |
| Basic match works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 9: COLLATE Support

| Criteria | Target | Status |
|----------|--------|--------|
| COLLATE NOCASE | Works | [x] |
| COLLATE RTRIM | Works | [x] |
| COLLATE BINARY | Works | [x] |
| Column-level COLLATE | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 10: ALTER TABLE

| Criteria | Target | Status |
|----------|--------|--------|
| ALTER TABLE ADD COLUMN | Works | [x] |
| ALTER TABLE RENAME TO | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 11: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| All unit tests pass | 100% | [x] |
| SQLite comparison tests pass | 100% | [x] |
| New SQL:1999 tests added | Done | [x] |

---

## Implementation Details

### CHECK Constraint

```sql
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    balance REAL CHECK(balance >= 0)
);

INSERT INTO accounts VALUES (1, -100);  -- Error: CHECK constraint failed
```

### Partial Index

```sql
CREATE INDEX idx_active ON users(email) WHERE status = 'active';
SELECT email FROM users WHERE status = 'active';  -- Uses index
SELECT email FROM users WHERE status = 'inactive';  -- Full scan
```

### Expression Index

```sql
CREATE INDEX idx_lower_email ON users(LOWER(email));
SELECT * FROM users WHERE LOWER(email) = 'test@example.com';  -- Uses index
```

### RETURNING Clause

```sql
INSERT INTO logs (msg) VALUES ('hello') RETURNING id, msg;
UPDATE accounts SET balance = balance + 10 WHERE id = 1 RETURNING balance;
DELETE FROM logs WHERE id < 100 RETURNING id;
```

### UPDATE ... FROM

```sql
-- Update with JOIN
UPDATE orders o 
SET total = o.total + p.price 
FROM products p 
WHERE o.product_id = p.id;
```

### DELETE ... USING

```sql
DELETE FROM orders 
USING customers 
WHERE orders.customer_id = customers.id 
AND customers.status = 'inactive';
```

### GLOB Operator

```sql
SELECT * FROM files WHERE name GLOB '*.txt';
SELECT * FROM files WHERE name GLOB 'test?';
```

### COLLATE Support

```sql
CREATE TABLE users (
    name TEXT COLLATE NOCASE
);
SELECT * FROM users WHERE name = 'Alice';  -- Case insensitive
```

### ALTER TABLE

```sql
ALTER TABLE users ADD COLUMN status TEXT;
ALTER TABLE users RENAME TO accounts;
```

---

## Expected Results

### Compatibility

| Feature | Status |
|---------|--------|
| CHECK Constraint | 100% SQLite compatible |
| Partial Index | 100% SQLite compatible |
| Expression Index | 100% SQLite compatible |
| RETURNING Clause | 100% SQLite compatible |
| UPDATE ... FROM | 100% SQLite compatible |
| DELETE ... USING | 100% SQLite compatible |
| GLOB Operator | 100% SQLite compatible |
| MATCH Operator | 100% SQLite compatible |
| COLLATE Support | 100% SQLite compatible |
| ALTER TABLE | 100% SQLite compatible |
