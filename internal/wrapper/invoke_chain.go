//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

// Package wrapper provides Phase 4 invoke chain fallback implementations.
// These pure-Go versions are used when the SVDB_ENABLE_CGO_VM build tag is absent.
package wrapper

// ─── Phase 4.2: Invoke chain wrapper ─────────────────────────────────────────

// PipelineHashFilter hashes keys and returns indices whose hash falls in the
// target bucket. Pure-Go fallback using FNV-1a.
func PipelineHashFilter(keys [][]byte, seed, bucketCount, targetBucket uint64) []int {
	if len(keys) == 0 || bucketCount == 0 {
		return nil
	}
	result := make([]int, 0, len(keys)/4+1)
	for i, k := range keys {
		h := fnv1a64(k, seed)
		if h%bucketCount == targetBucket {
			result = append(result, i)
		}
	}
	return result
}

// fnv1a64 is a simple FNV-1a 64-bit hash used by the pure-Go fallback.
func fnv1a64(data []byte, seed uint64) uint64 {
	const prime = uint64(1099511628211)
	h := seed ^ uint64(14695981039346656037)
	for _, b := range data {
		h ^= uint64(b)
		h *= prime
	}
	return h
}

// ─── Phase 4.3: Expression batch wrapper ─────────────────────────────────────

// CompareOp represents a SQL comparison operator.
type CompareOp int

const (
	CmpEQ CompareOp = iota // =
	CmpNE                  // !=
	CmpLT                  // <
	CmpLE                  // <=
	CmpGT                  // >
	CmpGE                  // >=
)

// ArithOp represents an arithmetic operator for batch evaluation.
type ArithOp int

const (
	ArithAdd ArithOp = iota // a + b
	ArithSub                // a - b
	ArithMul                // a * b
)

func cmpInt64(a, b int64, op CompareOp) bool {
	switch op {
	case CmpEQ:
		return a == b
	case CmpNE:
		return a != b
	case CmpLT:
		return a < b
	case CmpLE:
		return a <= b
	case CmpGT:
		return a > b
	case CmpGE:
		return a >= b
	}
	return false
}

func cmpFloat64(a, b float64, op CompareOp) bool {
	switch op {
	case CmpEQ:
		return a == b
	case CmpNE:
		return a != b
	case CmpLT:
		return a < b
	case CmpLE:
		return a <= b
	case CmpGT:
		return a > b
	case CmpGE:
		return a >= b
	}
	return false
}

// BatchEvalCompareInt64 compares pairs (a[i], b[i]) with the given operator.
func BatchEvalCompareInt64(a, b []int64, op CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	mask = make([]byte, n)
	for i := 0; i < n; i++ {
		if cmpInt64(a[i], b[i], op) {
			mask[i] = 1
			passCount++
		}
	}
	return mask, passCount
}

// BatchEvalCompareFloat64 is like BatchEvalCompareInt64 for float64 values.
func BatchEvalCompareFloat64(a, b []float64, op CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	mask = make([]byte, n)
	for i := 0; i < n; i++ {
		if cmpFloat64(a[i], b[i], op) {
			mask[i] = 1
			passCount++
		}
	}
	return mask, passCount
}

// BatchArithAndCompareInt64 computes tmp[i] = arith(a[i], b[i]) then compares tmp[i] to threshold.
func BatchArithAndCompareInt64(a, b []int64, arith ArithOp, threshold int64, cmp CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	mask = make([]byte, n)
	for i := 0; i < n; i++ {
		var tmp int64
		switch arith {
		case ArithSub:
			tmp = a[i] - b[i]
		case ArithMul:
			tmp = a[i] * b[i]
		default:
			tmp = a[i] + b[i]
		}
		if cmpInt64(tmp, threshold, cmp) {
			mask[i] = 1
			passCount++
		}
	}
	return mask, passCount
}

// ─── Phase 4.4: Storage access wrapper ───────────────────────────────────────

// AggOp represents an aggregation operation.
type AggOp int

const (
	AggSum   AggOp = iota // SUM
	AggMin                // MIN
	AggMax                // MAX
	AggCount              // COUNT
)

// ScanFilterInt64 scans a column and returns indices of rows where the value
// satisfies the comparison predicate.
func ScanFilterInt64(column []int64, op CompareOp, threshold int64) []int {
	result := make([]int, 0, len(column)/4+1)
	for i, v := range column {
		if cmpInt64(v, threshold, op) {
			result = append(result, i)
		}
	}
	return result
}

// ScanFilterFloat64 is like ScanFilterInt64 for float64 columns.
func ScanFilterFloat64(column []float64, op CompareOp, threshold float64) []int {
	result := make([]int, 0, len(column)/4+1)
	for i, v := range column {
		if cmpFloat64(v, threshold, op) {
			result = append(result, i)
		}
	}
	return result
}

// ScanAggregateInt64 scans a column, optionally filters it, then computes an aggregate.
func ScanAggregateInt64(column []int64, filterOp CompareOp, filterThreshold int64, hasFilter bool, agg AggOp) (aggValue int64, matchCount int) {
	const minInt64 = int64(-1 << 63)
	const maxInt64 = int64(^uint64(0) >> 1)
	sum := int64(0)
	mn := maxInt64
	mx := minInt64
	cnt := 0
	for _, v := range column {
		if hasFilter && !cmpInt64(v, filterThreshold, filterOp) {
			continue
		}
		sum += v
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
		cnt++
	}
	switch agg {
	case AggMin:
		if cnt > 0 {
			return mn, cnt
		}
		return 0, 0
	case AggMax:
		if cnt > 0 {
			return mx, cnt
		}
		return 0, 0
	case AggCount:
		return int64(cnt), cnt
	default:
		return sum, cnt
	}
}
