package opt

// simd.go provides vectorized batch operations for columnar int64 and float64 data.
// These functions use 4-way loop unrolling, which the Go compiler can leverage for
// auto-vectorization on amd64/arm64 architectures.

// VectorAddInt64 computes dst[i] = a[i] + b[i] for all i.
// len(dst) must equal len(a) and len(b).
func VectorAddInt64(dst, a, b []int64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] + b[i]
		dst[i+1] = a[i+1] + b[i+1]
		dst[i+2] = a[i+2] + b[i+2]
		dst[i+3] = a[i+3] + b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] + b[i]
	}
}

// VectorSubInt64 computes dst[i] = a[i] - b[i] for all i.
func VectorSubInt64(dst, a, b []int64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] - b[i]
		dst[i+1] = a[i+1] - b[i+1]
		dst[i+2] = a[i+2] - b[i+2]
		dst[i+3] = a[i+3] - b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] - b[i]
	}
}

// VectorMulInt64 computes dst[i] = a[i] * b[i] for all i.
func VectorMulInt64(dst, a, b []int64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] * b[i]
		dst[i+1] = a[i+1] * b[i+1]
		dst[i+2] = a[i+2] * b[i+2]
		dst[i+3] = a[i+3] * b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] * b[i]
	}
}

// VectorSumInt64 returns the sum of all elements in a.
func VectorSumInt64(a []int64) int64 {
	n := len(a)
	var s0, s1, s2, s3 int64
	i := 0
	for ; i <= n-4; i += 4 {
		s0 += a[i]
		s1 += a[i+1]
		s2 += a[i+2]
		s3 += a[i+3]
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += a[i]
	}
	return sum
}

// VectorAddFloat64 computes dst[i] = a[i] + b[i] for all i.
func VectorAddFloat64(dst, a, b []float64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] + b[i]
		dst[i+1] = a[i+1] + b[i+1]
		dst[i+2] = a[i+2] + b[i+2]
		dst[i+3] = a[i+3] + b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] + b[i]
	}
}

// VectorSubFloat64 computes dst[i] = a[i] - b[i] for all i.
func VectorSubFloat64(dst, a, b []float64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] - b[i]
		dst[i+1] = a[i+1] - b[i+1]
		dst[i+2] = a[i+2] - b[i+2]
		dst[i+3] = a[i+3] - b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] - b[i]
	}
}

// VectorMulFloat64 computes dst[i] = a[i] * b[i] for all i.
func VectorMulFloat64(dst, a, b []float64) {
	n := len(dst)
	i := 0
	for ; i <= n-4; i += 4 {
		dst[i] = a[i] * b[i]
		dst[i+1] = a[i+1] * b[i+1]
		dst[i+2] = a[i+2] * b[i+2]
		dst[i+3] = a[i+3] * b[i+3]
	}
	for ; i < n; i++ {
		dst[i] = a[i] * b[i]
	}
}

// VectorSumFloat64 returns the sum of all elements in a using 4-way unrolling.
func VectorSumFloat64(a []float64) float64 {
	n := len(a)
	var s0, s1, s2, s3 float64
	i := 0
	for ; i <= n-4; i += 4 {
		s0 += a[i]
		s1 += a[i+1]
		s2 += a[i+2]
		s3 += a[i+3]
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += a[i]
	}
	return sum
}

// VectorMinInt64 returns the minimum value in a. Returns (0, false) for empty slices.
func VectorMinInt64(a []int64) (int64, bool) {
	if len(a) == 0 {
		return 0, false
	}
	min := a[0]
	for _, v := range a[1:] {
		if v < min {
			min = v
		}
	}
	return min, true
}

// VectorMaxInt64 returns the maximum value in a. Returns (0, false) for empty slices.
func VectorMaxInt64(a []int64) (int64, bool) {
	if len(a) == 0 {
		return 0, false
	}
	max := a[0]
	for _, v := range a[1:] {
		if v > max {
			max = v
		}
	}
	return max, true
}

// VectorMinFloat64 returns the minimum value in a. Returns (0.0, false) for empty slices.
func VectorMinFloat64(a []float64) (float64, bool) {
	if len(a) == 0 {
		return 0, false
	}
	min := a[0]
	for _, v := range a[1:] {
		if v < min {
			min = v
		}
	}
	return min, true
}

// VectorMaxFloat64 returns the maximum value in a. Returns (0.0, false) for empty slices.
func VectorMaxFloat64(a []float64) (float64, bool) {
	if len(a) == 0 {
		return 0, false
	}
	max := a[0]
	for _, v := range a[1:] {
		if v > max {
			max = v
		}
	}
	return max, true
}
