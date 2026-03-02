package SQLValidator

// LCG implements a Linear Congruential Generator using Knuth MMIX parameters
// for a deterministic, reproducible pseudo-random number stream.
//
// Parameters:
//
//	Multiplier: 6364136223846793005
//	Increment:  1442695040888963407
//	Modulus:    2^64 (implicit via uint64 overflow)
type LCG struct {
	state uint64
}

// NewLCG creates a new LCG with the given seed.
func NewLCG(seed uint64) *LCG {
	return &LCG{state: seed}
}

// Next advances the LCG state and returns the next pseudo-random uint64.
func (l *LCG) Next() uint64 {
	l.state = l.state*6364136223846793005 + 1442695040888963407
	return l.state
}

// Intn returns a pseudo-random int in [0, n).
// Returns 0 if n <= 0.
func (l *LCG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(l.Next() % uint64(n))
}

// Float64 returns a pseudo-random float64 in [0.0, 1.0).
func (l *LCG) Float64() float64 {
	return float64(l.Next()>>11) / (1 << 53)
}

// Choice returns a uniformly random element from items.
// Returns "" if items is empty.
func (l *LCG) Choice(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return items[l.Intn(len(items))]
}
