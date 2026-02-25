# Plan v0.9.3 - SQL Compatibility & Performance

## Summary

This plan implements core SQL compatibility features and performance optimizations for v0.9.3.

## Features

### SQL Compatibility (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 1 | **INSERT OR REPLACE** | ON CONFLICT REPLACE - delete and re-insert on conflict |
| 2 | **INSERT OR IGNORE** | ON CONFLICT IGNORE - skip rows on conflict |
| 3 | **UPSERT (ON CONFLICT DO)** | INSERT ... ON CONFLICT DO UPDATE SET - complete upsert support |

### Built-in Functions (3 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 13 | **REPLACE(str, old, new)** | String replacement function |
| 14 | **SUBSTR/SUBSTRING** | Substring extraction (start, length) |
| 15 | **TRIM/LTRIM/RTRIM** | String trimming functions |

### Performance Optimization (2 tasks)

| # | Feature | Description |
|---|---------|-------------|
| 29 | **SIMD Vectorization** | Columnar computation acceleration using SIMD |
| 30 | **Extended Dispatch Opcodes** | Add more opcodes to direct VM dispatch table |

---

## Timeline Estimate

| Phase | Feature | Hours |
|-------|---------|-------|
| 1 | INSERT OR REPLACE/IGNORE | 4h |
| 2 | UPSERT (ON CONFLICT DO) | 6h |
| 3 | REPLACE function | 2h |
| 4 | SUBSTR/SUBSTRING | 2h |
| 5 | TRIM/LTRIM/RTRIM | 2h |
| 6 | SIMD Vectorization | 8h |
| 7 | Extended Dispatch Opcodes | 4h |
| 8 | Testing & Benchmarks | 4h |

**Total:** ~32 hours

---

## Success Criteria

### Phase 1: INSERT OR REPLACE/IGNORE

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes OR REPLACE | Works | [x] |
| Parser recognizes OR IGNORE | Works | [x] |
| Execution deletes/ignores on conflict | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 2: UPSERT

| Criteria | Target | Status |
|----------|--------|--------|
| Parser recognizes ON CONFLICT | Works | [x] |
| DO UPDATE SET parsed | Works | [x] |
| DO NOTHING parsed | Works | [x] |
| Conflict target (column) parsed | Works | [x] |
| Execution applies update | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 3: REPLACE Function

| Criteria | Target | Status |
|----------|--------|--------|
| Function registered | Works | [x] |
| String replacement works | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 4: SUBSTR/SUBSTRING

| Criteria | Target | Status |
|----------|--------|--------|
| Function registered | Works | [x] |
| SUBSTR(str, start, len) works | Works | [x] |
| Negative start handled | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 5: TRIM Functions

| Criteria | Target | Status |
|----------|--------|--------|
| TRIM, LTRIM, RTRIM registered | Works | [x] |
| Character trimming works | Works | [x] |
| Default space trimming | Works | [x] |
| Test against SQLite | Match | [x] |

### Phase 6: SIMD Vectorization

| Criteria | Target | Status |
|----------|--------|--------|
| Integer add/multiply vectorized | Works | [x] |
| Float add/multiply vectorized | Works | [x] |
| Benchmark shows improvement | 2x faster | [x] |

### Phase 7: Extended Dispatch

| Criteria | Target | Status |
|----------|--------|--------|
| Add comparison opcodes | Works | [x] |
| Add string opcodes | Works | [x] |
| Benchmark shows improvement | 20% faster | [x] |

### Phase 8: Testing

| Criteria | Target | Status |
|----------|--------|--------|
| All unit tests pass | 100% | [x] |
| SQLite comparison tests pass | 100% | [x] |
| Benchmarks updated | Done | [x] |

---

## Implementation Details

### INSERT OR REPLACE/IGNORE

```sql
-- OR REPLACE: delete old row and insert new
INSERT OR REPLACE INTO users (id, name) VALUES (1, 'Alice');

-- OR IGNORE: skip on conflict
INSERT OR IGNORE INTO users (id, name) VALUES (1, 'Alice');
```

### UPSERT

```sql
INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice')
ON CONFLICT(id) DO UPDATE SET email = excluded.email;

INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT DO NOTHING;
```

### REPLACE Function

```sql
SELECT REPLACE('hello world', 'world', 'there');
-- Result: 'hello there'
```

### TRIM Functions

```sql
SELECT TRIM('  hello  ');   -- 'hello'
SELECT LTRIM('  hello');    -- 'hello'
SELECT RTRIME('hello  ');   -- 'hello'
```

---

## Expected Results

### Performance

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Columnar int64 add (1K rows) | baseline | SIMD | 2x faster |
| Dispatch opcodes | 10 ops | 30 ops | 20% faster |
| UPSERT | N/A | implemented | feature complete |

### Compatibility

| Feature | Status |
|---------|--------|
| INSERT OR REPLACE | 100% SQLite compatible |
| INSERT OR IGNORE | 100% SQLite compatible |
| UPSERT | 100% SQLite compatible |
| REPLACE function | 100% SQLite compatible |
| SUBSTR/SUBSTRING | 100% SQLite compatible |
| TRIM functions | 100% SQLite compatible |
