# sqlvibe Implementation Plan: Incremental SQLite Compatibility

## Current Status

### Passing Tests (13 tests, ~47 subtests)
- ✅ TestSQL1999_CH03_Numbers: 21/21
- ✅ TestQueryAggregates: 8/8
- ✅ TestQueryLimit: 3/3
- ✅ TestQueryOrderBy: 4/4
- ✅ TestEdgeCaseEmpty: 5/5
- ✅ TestEdgeCaseTypes
- ✅ TestDMLInsert
- ✅ TestDMLUpdate
- ✅ TestDMLDelete
- ✅ TestTransactionAPI
- ✅ TestTransactionCommit
- ✅ TestTransactionRollback
- ✅ TestPreparedStatements

### Failing Tests (5 tests, ~21 subtests)
| Test | Pass/Total | Missing Features |
|------|------------|------------------|
| TestQueryWhereClauses | 6/14 | AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL |
| TestQueryJoins | 0/3 | JOIN (INNER, LEFT, CROSS) |
| TestQuerySubqueries | 1/3 | Scalar subqueries, EXISTS, Correlated |
| TestEdgeCaseNULLs | 0/4 | IS NULL, IS NOT NULL, COALESCE, IFNULL |
| TestMultipleTables | 0/1 | sqlite_master support |

---

## Phase 1: WHERE Clause Enhancements (Quick Wins)

**Priority:** HIGH - Unblocks 8 failing test cases  
**Complexity:** EASY  
**Files to Modify:** `pkg/sqlvibe/database.go`, `internal/QP/parser.go`

### Features to Implement

| Feature | Parser Support | Engine Support | Status |
|---------|---------------|----------------|--------|
| AND | ✅ Exists | ❌ Missing | Needs evalWhere fix |
| OR | ✅ Exists | ❌ Missing | Needs evalWhere fix |
| NOT | ✅ Exists | ❌ Missing | Needs evalWhere fix |
| IN | ✅ Exists | ❌ Missing | Full implementation |
| BETWEEN | ❌ Missing | ❌ Missing | Full implementation |
| LIKE | ❌ Missing | ❌ Missing | Full implementation |
| IS NULL | ❌ Missing | ❌ Missing | Full implementation |
| IS NOT NULL | ❌ Missing | ❌ Missing | Full implementation |

### Implementation Details

**1. Fix AND/OR/NOT in `database.go` evalWhere()**

The parser already handles AND/OR in `parseAndExpr()` and `parseOrExpr()`, but `evalWhere()` in database.go only handles simple BinaryExpr. Need to extend:

```go
func (db *Database) evalWhere(row map[string]interface{}, where QP.Expr) bool {
    switch e := where.(type) {
    case *QP.BinaryExpr:
        // Currently handles only comparison operators
        // Need to add: QP.TokenKeyword with "AND"/"OR"
        if e.Op == QP.TokenKeyword {
            if e.Left != nil && e.Right != nil {
                switch e.Left.(type) {
                case *QP.Literal:
                    // Handle "AND"/"OR" keyword in e.Left
                }
            }
        }
    case *QP.UnaryExpr:
        // Handle NOT
        if e.Op == QP.TokenKeyword && e.Literal == "NOT" {
            return !db.evalWhere(row, e.Expr)
        }
    }
}
```

**2. Add IS NULL / IS NOT NULL support**

Need new AST node type:
```go
type NullCheckExpr struct {
    Expr    Expr
    IsNot   bool  // true = IS NOT NULL, false = IS NULL
}
```

**3. Add BETWEEN support**

Can be expressed as: `a BETWEEN x AND y` = `a >= x AND a <= y`

**4. Add LIKE support**

Implement simple pattern matching with `%` wildcard.

**5. Add IN support**

Handle `column IN (val1, val2, ...)` expression.

### Success Criteria
- TestQueryWhereClauses: 14/14 passing
- 8 test cases fixed

---

## Phase 2: NULL Handling Functions

**Priority:** HIGH - Unblocks 4 failing test cases  
**Complexity:** EASY  
**Files to Modify:** `pkg/sqlvibe/database.go`, `internal/QP/parser.go`

### Features to Implement

| Feature | Parser Support | Engine Support | Status |
|---------|---------------|----------------|--------|
| COALESCE | ❌ Missing | ❌ Missing | Full implementation |
| IFNULL | ❌ Missing | ❌ Missing | Full implementation |
| IS NULL | ❌ Missing | ❌ Missing | From Phase 1 |
| IS NOT NULL | ❌ Missing | ❌ Missing | From Phase 1 |

### Implementation Details

**1. Add COALESCE function**

SQL: `COALESCE(expr1, expr2, ...)` - returns first non-NULL value

```go
// In parser - extend FuncCall handling
case "COALESCE":
    return &FuncCall{Name: "COALESCE", Args: args}, nil
```

**2. Add IFNULL function**

SQL: `IFNULL(expr1, expr2)` - returns expr2 if expr1 is NULL

```go
// In database.go - extend extractValue
case *QP.FuncCall:
    switch fc.Name {
    case "COALESCE":
        for _, arg := range fc.Args {
            val := db.extractValueTyped(arg, colType)
            if val != nil {
                return val
            }
        }
        return nil
    case "IFNULL":
        if len(fc.Args) >= 2 {
            val := db.extractValueTyped(fc.Args[0], colType)
            if val == nil {
                return db.extractValueTyped(fc.Args[1], colType)
            }
            return val
        }
    }
```

### Success Criteria
- TestEdgeCaseNULLs: 4/4 passing
- TestQueryWhereClauses: IS NULL/IS NOT NULL fixed
- 12 test cases fixed (4 NULLs + 8 from Phase 1)

---

## Phase 3: sqlite_master Support

**Priority:** HIGH - Unblocks 1 failing test case, required for tool compatibility  
**Complexity:** EASY-MEDIUM  
**Files to Modify:** `pkg/sqlvibe/database.go`

### Features to Implement

SQLite's `sqlite_master` is a virtual table that contains:
- `type`: "table" or "index"
- `name`: table/index name
- `tbl_name`: for tables, same as name
- `rootpage`: page number of root
- `sql`: CREATE TABLE/INDEX statement

### Implementation Details

```go
func (db *Database) Query(sql string) (*Rows, error) {
    // In SELECT handling, before table lookup:
    if tableName == "sqlite_master" {
        return db.querySqliteMaster()
    }
}

func (db *Database) querySqliteMaster() (*Rows, error) {
    results := make([][]interface{}, 0)
    for tableName := range db.tables {
        sql := fmt.Sprintf("CREATE TABLE %s (...)", tableName) // Simplified
        results = append(results, []interface{}{
            "table",       // type
            tableName,    // name
            tableName,    // tbl_name
            int64(0),     // rootpage
            sql,          // sql
        })
    }
    return &Rows{
        Columns: []string{"type", "name", "tbl_name", "rootpage", "sql"},
        Data:    results,
    }, nil
}
```

### Success Criteria
- TestMultipleTables: 1/1 passing
- 1 test case fixed

---

## Phase 4: JOIN Support

**Priority:** HIGH - Unblocks 3 failing test cases  
**Complexity:** MEDIUM  
**Files to Modify:** `pkg/sqlvibe/database.go`, `internal/QP/parser.go`, `internal/QE/engine.go`

### Features to Implement

| Feature | Parser Support | Engine Support | Status |
|---------|---------------|----------------|--------|
| INNER JOIN | ✅ Exists (TableRef.Join) | ❌ Missing | Full implementation |
| LEFT JOIN | ✅ Exists (Join.Type) | ❌ Missing | Full implementation |
| CROSS JOIN | ✅ Exists (Join.Type) | ❌ Missing | Full implementation |

### Implementation Details

**1. Parser already has Join structures:**
```go
// In parser.go - already defined
type Join struct {
    Type  string  // "INNER", "LEFT", "CROSS"
    Left  *TableRef
    Right *TableRef
    Cond  Expr    // ON condition
}
```

**2. Need to implement join execution in Query()**

```go
func (db *Database) handleJoin(stmt *QP.SelectStmt) (*Rows, error) {
    if stmt.From == nil || stmt.From.Join == nil {
        return nil, nil
    }
    
    join := stmt.From.Join
    
    // Get left table data
    leftData := db.data[join.Left.Name]
    
    // Get right table data  
    rightData := db.data[join.Right.Name]
    
    switch join.Type {
    case "INNER":
        return db.innerJoin(leftData, rightData, join.Cond, stmt)
    case "LEFT":
        return db.leftJoin(leftData, rightData, join.Cond, stmt)
    case "CROSS":
        return db.crossJoin(leftData, rightData, stmt)
    }
    return nil, nil
}
```

### Success Criteria
- TestQueryJoins: 3/3 passing
- 3 test cases fixed

---

## Phase 5: Subquery Support

**Priority:** MEDIUM - Unblocks 3 failing test cases  
**Complexity:** MEDIUM-HARD  
**Files to Modify:** `pkg/sqlvibe/database.go`, `internal/QP/parser.go`

### Features to Implement

| Feature | Parser Support | Engine Support | Status |
|---------|---------------|----------------|--------|
| Scalar Subquery | ❌ Missing | ❌ Missing | Full implementation |
| IN Subquery | ❌ Partial | ❌ Partial | Needs testing |
| EXISTS Subquery | ❌ Missing | ❌ Missing | Full implementation |
| Correlated Subquery | ❌ Missing | ❌ Missing | Full implementation |

### Implementation Details

**1. Add Subquery AST node:**
```go
type SubqueryExpr struct {
    Select *SelectStmt
}
```

**2. Execute subqueries in evalExpr:**
```go
case *QP.SubqueryExpr:
    result, err := db.Query(db.renderSelect(subquery.Select))
    if err != nil || len(result.Data) == 0 {
        return nil, nil
    }
    return result.Data[0][0], nil
```

### Success Criteria
- TestQuerySubqueries: 3/3 passing
- 3 test cases fixed

---

## Implementation Order Summary

| Phase | Features | Complexity | Tests Unlocked | Cumulative |
|-------|----------|------------|----------------|------------|
| 1 | AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL | Easy | 8 | 8 |
| 2 | COALESCE, IFNULL | Easy | 4 | 12 |
| 3 | sqlite_master | Easy-Medium | 1 | 13 |
| 4 | INNER/LEFT/CROSS JOIN | Medium | 3 | 16 |
| 5 | Subqueries | Medium-Hard | 3 | 19 |

### Total Impact
- **Current:** 13 passing tests (~47 passing subtests)
- **After Phase 5:** 18 passing tests (~66 passing subtests)
- **Net gain:** ~19 additional passing subtests

---

## Dependencies and Prerequisites

1. **Parser is mostly ready** - Most AST structures exist, just need evaluation
2. **Test infrastructure ready** - compat_test.go provides SQLite comparison framework
3. **Engine foundation exists** - Filter, Project, TableScan operators working

### Key Risks

1. **Phase 5 (Subqueries)** - May require significant parser changes to detect subqueries vs. function calls
2. **NULL semantics** - SQLite has specific NULL comparison behavior (NULL != NULL)
3. **JOIN optimization** - Current implementation is nested loop; acceptable for small tables

---

## Testing Strategy

Each phase should include:
1. Run specific failing test before implementation
2. Implement feature
3. Run test again to verify fix
4. Run all tests to ensure no regression

```bash
# Test Phase 1
go test -run TestQueryWhereClauses ./pkg/sqlvibe

# Test Phase 2  
go test -run TestEdgeCaseNULLs ./pkg/sqlvibe

# Test Phase 3
go test -run TestMultipleTables ./pkg/sqlvibe

# Test Phase 4
go test -run TestQueryJoins ./pkg/sqlvibe

# Test Phase 5
go test -run TestQuerySubqueries ./pkg/sqlvibe
```
