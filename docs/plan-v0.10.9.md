# sqlvibe v0.10.9 Implementation Plan: Advanced Query Optimization

## Overview
This release focuses on building a cost-based optimizer (CBO), adaptive query planning, and a pluggable advanced EXPLAIN extension (SVDB_EXT_EXPLAIN) for smarter, faster SQL execution and improved debugging.

---

## Major Features

### 1. Cost-Based Optimizer (CBO)
- Implement CBO for join order, index selection, predicate pushdown
- Collect/maintain table and column statistics (row counts, histograms)
- Integrate CBO into codegen and VM for plan selection
- Support multi-table joins, nested subqueries, complex predicates
- Add unit and regression tests comparing plans/results to SQLite

### 2. Adaptive Query Re-Planning
- Monitor query execution for runtime statistics (actual row counts, selectivity)
- Implement feedback loop for plan adjustment
- Support plan invalidation and re-optimization on schema/data changes
- Add tests for adaptive planning scenarios

### 3. SVDB_EXT_EXPLAIN Extension
- Design SVDB_EXT_EXPLAIN as a build-tagged extension
- Advanced EXPLAIN output: human-readable diagrams, join order, index usage, predicate evaluation, estimated costs
- CLI integration: .explain, .plan commands available only when built with SVDB_EXT_EXPLAIN
- Document extension in README and provide usage examples
- Add tests for EXPLAIN output correctness and usability

---

## Success Criteria
- CBO produces optimal plans for complex queries
- Adaptive planning improves performance for evolving workloads
- SVDB_EXT_EXPLAIN provides clear, accurate, and helpful debugging output
- All new features covered by unit, integration, and regression tests

---

## Implementation DAG
```mermaid
graph LR
    A[Statistics Collection] --> B[CBO Engine]
    B --> C[Adaptive Planning]
    C --> D[SVDB_EXT_EXPLAIN Extension]
```

---

## Success Checklist
- [ ] Table/column statistics collected and maintained
- [ ] CBO engine implemented and integrated
- [ ] Adaptive query re-planning functional
- [ ] SVDB_EXT_EXPLAIN extension implemented and tested
- [ ] All tests pass, no regressions
- [ ] Documentation updated

---

## Notes
- CBO must be SQLite-compatible for covered SQL
- SVDB_EXT_EXPLAIN is a build-tagged extension, documented in README
- Performance and usability are top priorities
- Update HISTORY.md and docs upon completion
