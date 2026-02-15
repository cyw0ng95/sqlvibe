# sqlvibe: Incremental Implementation Roadmap

## Current State
```
Tests: 13 PASS | 5 FAIL
Subtests: ~47 PASS | ~21 FAIL
```

## Dependency DAG

```
                        ┌─────────────────────────────────────────┐
                        │           PHASE 1: BASE               │
                        │  ┌─────────────────────────────────┐  │
                        │  │ • AND/OR/NOT evaluation         │  │
                        │  │ • IS NULL / IS NOT NULL        │  │
                        │  │ • IN operator                  │  │
                        │  │ • BETWEEN                      │  │
                        │  │ • LIKE                         │  │
                        │  └─────────────────────────────────┘  │
                        │                 │                     │
                        │           8 tests fixed               │
                        └───────────────┬───────────────────────┘
                                        │
        ┌───────────────────────────────┼───────────────────────────────┐
        │                               │                               │
        ▼                               ▼                               ▼
┌───────────────────┐       ┌───────────────────┐       ┌───────────────────┐
│   PHASE 2A        │       │   PHASE 2B        │       │   PHASE 3         │
│   NULL Functions  │       │   Subquery Prep   │       │   sqlite_master   │
│                   │       │                   │       │                   │
│ • COALESCE        │       │ • Subquery AST    │       │ • Virtual table   │
│ • IFNULL          │       │ • IN subquery     │       │ • Table listing   │
│                   │       │   detection       │       │                   │
└─────────┬─────────┘       └─────────┬─────────┘       └─────────┬─────────┘
          │                           │                           │
          │                     depends on                       │
          │                     Phase 1 (IN)                    │
          │                           │                           │
          ▼                           ▼                           ▼
┌───────────────────┐       ┌───────────────────┐       ┌───────────────────┐
│ 4 tests fixed     │       │ 2 tests prep      │       │ 1 test fixed      │
│ (extends Phase 1) │       │                   │       │                   │
└───────────────────┘       └───────────────────┘       └───────────────────┘
                                        │
                                        ▼
                        ┌───────────────────────────────────┐
                        │         PHASE 4: JOINS            │
                        │                                   │
                        │ • INNER JOIN                      │
                        │ • LEFT JOIN                       │
                        │ • CROSS JOIN                      │
                        │                                   │
                        │    (requires Phase 1 for ON)      │
                        └───────────────┬───────────────────┘
                                        │
                                        ▼
                        ┌───────────────────────────────────┐
                        │         PHASE 5: SUBQUERIES       │
                        │                                   │
                        │ • Scalar subquery                 │
                        │ • EXISTS subquery                 │
                        │ • Correlated subquery             │
                        │                                   │
                        └───────────────────────────────────┘
```

## Concise Roadmap

```
Phase 1 (BASE):    WHERE enhancements          [Priority: CRITICAL]  →  8 tests
    │
    ├─► Phase 2A:   NULL functions            [Priority: HIGH]     →  4 tests
    │
    ├─► Phase 2B:   Subquery prep             [Priority: MEDIUM]   →  prep for Phase 5
    │
    └─► Phase 3:    sqlite_master             [Priority: MEDIUM]   →  1 test
        │
        └─► Phase 4:  JOINs                    [Priority: MEDIUM]   →  3 tests
            │
            └─► Phase 5:  Subqueries            [Priority: LOW]    →  3 tests
```

## Quick Reference

| Phase | Features | Dependency | Tests |
|-------|----------|------------|-------|
| **1** | AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL | None (base) | 8 |
| **2A** | COALESCE, IFNULL | Phase 1 | 4 |
| **2B** | Subquery AST, IN subquery | Phase 1 | 0 (prep) |
| **3** | sqlite_master | None | 1 |
| **4** | JOINs | Phase 1 | 3 |
| **5** | Subqueries | Phase 2B | 3 |

## Total Impact
- **Before:** 13 tests pass (~47 subtests)
- **After:** 18 tests pass (~66 subtests)
- **Net Gain:** +19 subtests

## Start Here
**Recommended:** Phase 1 - Fixes most tests with least complexity
