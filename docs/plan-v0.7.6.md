# Plan v0.7.6 - CG & VM Performance Optimizations

## Summary

Implement targeted performance optimizations for the Code Generator (CG) and Virtual Machine (VM) subsystems, along with comprehensive benchmarks to measure improvements.

**Previous**: v0.7.5 delivers SQLLogicTest integration and SQLite compatibility

---

## Optimization Targets

### CG (Code Generator) Optimizations

| Optimization | Description | Complexity | Impact |
|--------------|-------------|------------|--------|
| Common Subexpression Elimination (CSE) | Avoid redundant expression evaluation | Medium | Medium |
| Strength Reduction | Replace expensive ops with cheaper ones | Low | Low-Medium |
| Peephole Optimizations | Instruction-level pattern matching | Medium | Medium |
| Predicate Pushdown | Push WHERE conditions closer to scans | High | High |

### VM (Virtual Machine) Optimizations

| Optimization | Description | Complexity | Impact |
|--------------|-------------|------------|--------|
| Jump Table Dispatch | Replace switch with computed goto | Medium | High |
| Type Assertion Reduction | Pre-compute common destinations | Medium | Medium |
| Opcode Fusion | Combine sequential operations | Medium | Medium |
| Register Pre-allocation | Pre-allocate register slices | Low | Low-Medium |

---

## Benchmark Suite Requirements

### Current Benchmarks (Existing)

| Benchmark | File | Purpose |
|-----------|------|---------|
| BenchmarkCompileSelect | `bench_test.go` | Compilation speed |
| BenchmarkVMExecution | `bench_test.go` | Basic execution |
| BenchmarkArithmeticOps | `bench_test.go` | Math operations |
| BenchmarkCompareOps | `bench_test.go` | Comparison speed |
| BenchmarkStringOps | `bench_test.go` | String concatenation |

### New Benchmarks Needed

| Benchmark | Target | Expected Baseline |
|-----------|--------|-------------------|
| BenchmarkCG_CSE | CSE optimization | Measures redundant exprs |
| BenchmarkCG_Peephole | Peephole optimization | Measures dead code elimination |
| BenchmarkVM_JumpTable | Jump dispatch | Measures opcode dispatch speed |
| BenchmarkVM_TypeAssertion | Type assertion reduction | Measures P4 parsing overhead |
| BenchmarkVM_ResultRow | Result row generation | Measures allocation overhead |
| BenchmarkVM_CursorScan | Full table scan | Measures cursor iteration |
| BenchmarkVM_JoinExecution | Nested loop join | Measures join performance |
| BenchmarkVM_SubqueryCache | Subquery caching | Measures cache hit rate |
| BenchmarkVM_StringLike | LIKE pattern matching | Measures pattern speed |
| BenchmarkVM_Aggregate | GROUP BY aggregation | Measures aggregate perf |

---

## Implementation Plan

### Phase 1: Benchmark Infrastructure (Week 1)

#### Tasks

1. **Create comprehensive benchmark suite**
   ```
   internal/VM/bench_cg_test.go      # CG benchmarks
   internal/VM/bench_vm_test.go      # VM benchmarks  
   internal/VM/bench_integration_test.go  # End-to-end
   ```

2. **Define baseline metrics**
   - Run all benchmarks before changes
   - Record ns/op for each benchmark
   - Store results for comparison

#### Files to Create

```
internal/VM/
├── bench_cg_test.go           # CG compilation benchmarks
├── bench_vm_test.go           # VM execution benchmarks  
├── bench_integration_test.go  # End-to-end SQL benchmarks
└── benchdata/
    └── testdata.go           # Benchmark test data generators
```

---

### Phase 2: CG Optimizations (Week 2)

#### 2.1 Common Subexpression Elimination (CSE)

```go
// In optimizer.go - track computed expressions
type exprKey struct {
    op   string
    args string  // serialized args
}

func (o *Optimizer) eliminateCommonSubexprs(program *VM.Program) {
    // Track: expression string -> register that holds result
    exprToReg := make(map[exprKey]int)
    
    for i, inst := range program.Instructions {
        key := makeExprKey(inst)
        if prevReg, exists := exprToReg[key]; exists {
            // Replace with Move from cached register
            program.Instructions[i] = VM.Instruction{
                Op: VM.OpMove,
                P1: int32(prevReg),
                P2: int32(inst.P1),
            }
        } else {
            exprToReg[key] = int(inst.P1)
        }
    }
}
```

**Target**: Reduce redundant `a + b` evaluations in expressions like `(a + b) + (a + b)`

#### 2.2 Strength Reduction

```go
// Replace expensive operations with cheaper ones
func (o *Optimizer) reduceStrength(program *VM.Program) {
    for i := range program.Instructions {
        inst := &program.Instructions[i]
        
        // x * 2 -> x + x
        if inst.Op == VM.OpMultiply {
            if c, ok := inst.P4.(int); ok && c == 2 {
                // Replace with addition
                inst.Op = VM.OpAdd
                inst.P4 = inst.P1  // dst = src + src
            }
        }
        
        // x ^ 0 -> x (identity)
        // x & 0xFFFFFFFF -> x (no-op)
    }
}
```

#### 2.3 Peephole Optimizations

```go
// Instruction sequence patterns
func (o *Optimizer) peepholeOptimize(program *VM.Program) {
    insts := program.Instructions
    
    for i := 0; i < len(insts)-1; i++ {
        // LoadConst -> Move -> LoadConst -> Remove redundant Move
        if insts[i].Op == VM.OpLoadConst && insts[i+1].Op == VM.OpMove {
            // Can forward the constant directly
        }
        
        // Jump to next instruction -> Remove jump
        if insts[i].Op == VM.OpGoto {
            target := int(insts[i].P2)
            if target == i+1 {
                insts[i].Op = VM.OpNoop
            }
        }
    }
}
```

#### 2.4 Predicate Pushdown (Join Optimization)

Push WHERE conditions closer to table scans in nested loop joins.

**Complexity**: High - requires data flow analysis

**Deferred to v0.7.7** - requires more analysis

---

### Phase 3: VM Optimizations (Week 3)

#### 3.1 Jump Table Dispatch

```go
// Instead of large switch, use function array
// In exec.go

var opHandlers [256]func(*VM, Instruction)

// Initialize at init()
func init() {
    opHandlers[uint8(OpLoadConst)] = handleLoadConst
    opHandlers[uint8(OpMove)] = handleMove
    // ... fill all handlers
}

func (vm *VM) Exec(ctx interface{}) error {
    for {
        inst := vm.GetInstruction()
        
        // Direct function call - better branch prediction
        if uint8(inst.Op) < 256 {
            opHandlers[uint8(inst.Op)](vm, inst)
        } else {
            // Fallback for unknown ops
        }
    }
}
```

#### 3.2 Type Assertion Reduction

```go
// Pre-compute destination registers in Program
type Instruction struct {
    Op       OpCode
    P1, P2   int32
    P3       string
    P4       interface{}
    
    // Cached for hot path
    dstReg   int   // Pre-extracted if P4 is int
    hasDst   bool
}

// When emitting instructions, pre-compute
func (p *Program) EmitAdd(dst, src1, src2 int) {
    inst := Instruction{
        Op:     OpAdd,
        P1:     int32(src1),
        P2:     int32(src2),
        dstReg: dst,
        hasDst: true,
    }
    p.Instructions = append(p.Instructions, inst)
}
```

#### 3.3 Opcode Fusion

```go
// Combine LoadConst + Column into single op
const OpLoadColumnConst = OpCode(128)  // New opcode

// In exec.go case OpLoadColumnConst:
case OpLoadColumnConst:
    cursor := vm.cursors.Get(int(inst.P1))
    colIdx := int(inst.P2)
    vm.registers[inst.P1] = cursor.Data[cursor.Index][cursor.Columns[colIdx]]
```

#### 3.4 Register Pre-allocation

```go
// In VM.Reset(), pre-allocate register slice
func (vm *VM) Reset() {
    // Pre-allocate to max expected size
    if cap(vm.registers) < 256 {
        vm.registers = make([]interface{}, 256)
    }
    vm.registers = vm.registers[:vm.program.NumRegs]
    
    // Pre-clear instead of creating new slice
    for i := range vm.registers {
        vm.registers[i] = nil
    }
}
```

---

### Phase 4: Benchmark Validation (Week 4)

#### 4.1 Run Before/After Benchmarks

```bash
# Before optimization
go test ./internal/VM/... -bench=. -benchmem -count=3 > bench_before.txt

# After optimization  
go test ./internal/VM/... -bench=. -benchmem -count=3 > bench_after.txt

# Compare
diff bench_before.txt bench_after.txt
```

#### 4.2 Key Metrics to Track

| Metric | Description | Target Improvement |
|--------|-------------|-------------------|
| ns/op | Time per operation | 10-30% reduction |
| B/op | Bytes allocated | 20-40% reduction |
| allocs/op | Allocation count | 30-50% reduction |

#### 4.3 Critical Path Benchmarks

Focus on these high-impact benchmarks:

1. **BenchmarkVM_CursorScan** - Table scan is fundamental
2. **BenchmarkVM_ResultRow** - Result generation is hot path
3. **BenchmarkVM_JoinExecution** - Join performance
4. **BenchmarkCG_CompileSelect** - Compilation speed

---

## Files to Modify

```
internal/CG/
├── optimizer.go              # Add CSE, strength reduction, peephole
├── compiler.go               # Minor changes if needed

internal/VM/
├── exec.go                   # Jump table, opcode fusion
├── engine.go                 # Minor changes
├── instruction.go            # Add cached fields
├── bench_cg_test.go          # NEW - CG benchmarks
├── bench_vm_test.go          # NEW - VM benchmarks
├── bench_integration_test.go # NEW - SQL benchmarks
└── benchdata/
    └── testdata.go          # NEW - Test data generators
```

---

## Tasks

### Phase 1: Infrastructure
- [ ] Create `internal/VM/bench_cg_test.go` with CG benchmarks
- [ ] Create `internal/VM/bench_vm_test.go` with VM benchmarks  
- [ ] Create `internal/VM/benchdata/testdata.go` for test data generation
- [ ] Run baseline benchmarks and record results

### Phase 2: CG Optimizations
- [ ] Implement CSE in optimizer.go
- [ ] Implement strength reduction in optimizer.go
- [ ] Implement peephole optimizations in optimizer.go
- [ ] Run CG benchmarks and verify improvement

### Phase 3: VM Optimizations
- [ ] Implement jump table dispatch in exec.go
- [ ] Add cached dstReg field to Instruction
- [ ] Implement opcode fusion (optional)
- [ ] Optimize register pre-allocation
- [ ] Run VM benchmarks and verify improvement

### Phase 4: Validation
- [ ] Run full benchmark suite
- [ ] Document improvements
- [ ] Ensure no regression in existing tests

---

## Success Criteria

| Criteria | Target |
|----------|--------|
| All existing tests pass | 100% |
| CG benchmarks improve | >10% faster |
| VM benchmarks improve | >15% faster |
| Memory allocations reduced | >20% fewer |
| No new test failures | 0 |

---

## Benchmark Commands

```bash
# Run all benchmarks
go test ./internal/VM/... -bench=. -benchmem

# Run specific benchmark
go test ./internal/VM/... -bench=BenchmarkVM_CursorScan -benchmem

# Compare before/after
go test ./internal/VM/... -bench=. -benchmem -count=5 > results.txt

# Profile specific benchmark
go test ./internal/VM/... -bench=BenchmarkVM_JoinExecution -cpuprofile=cpu.prof
go test ./internal/VM/... -bench=BenchmarkVM_JoinExecution -memprofile=mem.prof

# View profile
go tool pprof cpu.prof
```

---

## Timeline Estimate

| Phase | Tasks | Estimated Hours |
|-------|-------|-----------------|
| 1 | Benchmark Infrastructure | 8 |
| 2 | CG Optimizations | 12 |
| 3 | VM Optimizations | 16 |
| 4 | Validation | 8 |

**Total**: ~44 hours

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Optimizations hurt readability | Add clear comments, keep original switch as fallback |
| Benchmark results vary | Run multiple times, use statistical analysis |
| Regression in edge cases | Full test suite before/after |
| Over-optimization | Focus on hot paths only |

---

## Notes

- All optimizations should maintain backward compatibility
- Keep original switch as fallback during development
- Use build tags to disable optimizations for debugging
- Document any intentional behavior changes
