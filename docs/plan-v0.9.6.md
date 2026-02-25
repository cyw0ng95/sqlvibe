# Plan v0.9.6 - Transaction & Integrity

## Summary

This plan implements transaction handling and integrity constraints for v0.9.6.

---

## Features

### Explicit Transactions (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 1 | **BEGIN** | START TRANSACTION / BEGIN [DEFERRED/IMMEDIATE/EXCLUSIVE] |
| 2 | **COMMIT** | End transaction and persist changes |
| 3 | **ROLLBACK** | Abort transaction and revert changes |

### Savepoints (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 4 | **SAVEPOINT** | Create named savepoint within transaction |
| 5 | **RELEASE SAVEPOINT** | Release a savepoint |

### Foreign Keys (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 6 | **REFERENCES** | Foreign key constraint in CREATE TABLE |
| 7 | **ON DELETE** | CASCADE, SET NULL, RESTRICT, NO ACTION |
| 8 | **ON UPDATE** | CASCADE, SET NULL, RESTRICT, NO ACTION |

### Constraint Enhancements (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 9 | **NOT NULL** | Explicit NOT NULL constraint |
| 10 | **UNIQUE** | Explicit UNIQUE constraint (table-level) |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | BEGIN (DEFERRED) | 4h |
| 2 | COMMIT | 3h |
| 3 | ROLLBACK | 3h |
| 4 | SAVEPOINT | 4h |
| 5 | RELEASE SAVEPOINT | 3h |
| 6 | REFERENCES (basic) | 5h |
| 7 | ON DELETE actions | 4h |
| 8 | ON UPDATE actions | 4h |
| 9 | NOT NULL constraint | 3h |
| 10 | UNIQUE constraint | 3h |
| 11 | Testing & Integration | 6h |

**Total:** ~42 hours

---

## Success Criteria

### Phase 1: BEGIN

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes BEGIN | Works | [x] |
| BEGIN DEFERRED works | Works | [x] |
| BEGIN IMMEDIATE works | Works | [x] |
| BEGIN EXCLUSIVE works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 2: COMMIT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes COMMIT | Works | [x] |
| Changes persist after commit | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 3: ROLLBACK

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ROLLBACK | Works | [x] |
| Changes reverted after rollback | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 4: SAVEPOINT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes SAVEPOINT | Works | [x] |
| Named savepoints work | Works | [x] |
| Nested savepoints work | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 5: RELEASE SAVEPOINT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes RELEASE SAVEPOINT | Works | [x] |
| Release specific savepoint works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 6: REFERENCES

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes REFERENCES | Works | [x] |
| FK constraint created | Works | [x] |
| Child table FK validation | Works | [x] |
| Parent table existence check | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 7: ON DELETE

| Criteria | Target | Status |
|----------|--------|--------|
| ON DELETE CASCADE works | Works | [x] |
| ON DELETE SET NULL works | Works | [x] |
| ON DELETE RESTRICT works | Works | [x] |
| ON DELETE NO ACTION works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 8: ON UPDATE

| Criteria | Target | Status |
|----------|--------|--------|
| ON UPDATE CASCADE works | Works | [x] |
| ON UPDATE SET NULL works | Works | [x] |
| ON UPDATE RESTRICT works | Works | [x] |
| ON UPDATE NO ACTION works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 9: NOT NULL Constraint

| Criteria | Target | Status |
|----------|--------|--------|
| NOT NULL parsed | Works | [x] |
| Insert NULL rejected | Works | [x] |
| Update NULL rejected | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 10: UNIQUE Constraint

| Criteria | Target | Status |
|----------|--------|--------|
| Table-level UNIQUE parsed | Works | [x] |
| Duplicate values rejected | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 11: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| All unit tests pass | 100% | [x] |
| SQLite comparison tests pass | 100% | [x] |
| New SQL:1999 tests added | Done | [x] |

---

## Implementation Details

### Explicit Transactions

```sql
BEGIN;
INSERT INTO accounts (name, balance) VALUES ('Alice', 1000);
COMMIT;

BEGIN IMMEDIATE;
UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice';
ROLLBACK;

BEGIN EXCLUSIVE;
-- Acquire exclusive lock
COMMIT;
```

### Savepoints

```sql
BEGIN;
INSERT INTO logs (msg) VALUES ('start');
SAVEPOINT sp1;
INSERT INTO logs (msg) VALUES ('checkpoint');
ROLLBACK TO sp1;
INSERT INTO logs (msg) VALUES ('after rollback');
COMMIT;
```

### Foreign Keys

```sql
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    amount REAL
);

-- ON DELETE CASCADE: Deleting customer deletes all their orders
-- ON DELETE SET NULL: Deleting customer sets customer_id to NULL
-- ON DELETE RESTRICT: Prevents deletion if orders exist
```

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    category_id INTEGER REFERENCES categories(id) ON UPDATE CASCADE,
    name TEXT
);

-- ON UPDATE CASCADE: Changing category_id updates all products
```

### NOT NULL Constraint

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);

INSERT INTO users (id, name, email) VALUES (1, NULL, 'a@b.com');
-- Error: NOT NULL constraint failed
```

### UNIQUE Constraint

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE,
    phone TEXT,
    UNIQUE(phone)
);

INSERT INTO users (email, phone) VALUES ('a@b.com', '123');
INSERT INTO users (email, 'a@b.com', '456');
-- Error: UNIQUE constraint failed: users.email
```

---

## Expected Results

### Compatibility

| Feature | Status |
|---------|--------|
| BEGIN (DEFERRED/IMMEDIATE/EXCLUSIVE) | 100% SQLite compatible |
| COMMIT | 100% SQLite compatible |
| ROLLBACK | 100% SQLite compatible |
| SAVEPOINT | 100% SQLite compatible |
| RELEASE SAVEPOINT | 100% SQLite compatible |
| REFERENCES | 100% SQLite compatible |
| ON DELETE CASCADE | 100% SQLite compatible |
| ON DELETE SET NULL | 100% SQLite compatible |
| ON DELETE RESTRICT | 100% SQLite compatible |
| ON DELETE NO ACTION | 100% SQLite compatible |
| ON UPDATE CASCADE | 100% SQLite compatible |
| ON UPDATE SET NULL | 100% SQLite compatible |
| ON UPDATE RESTRICT | 100% SQLite compatible |
| ON UPDATE NO ACTION | 100% SQLite compatible |
| NOT NULL | 100% SQLite compatible |
| UNIQUE | 100% SQLite compatible |

---

## Dependencies

- Transaction handling requires engine-level support
- Savepoints require nested transaction tracking
- Foreign keys require constraint validation engine
- Need schema metadata for FK resolution
