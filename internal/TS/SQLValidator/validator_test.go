package SQLValidator

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestSQLValidator_TPC_C runs the validator for 1000 randomly-generated SQL
// statements (LCG seed 42) against the TPC-C starter schema.
// Any result mismatch between SQLite and sqlvibe is reported as a test failure.
func TestSQLValidator_TPC_C(t *testing.T) {
	seeds := []uint64{1, 2, 7, 42}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			v, err := NewValidator(seed)
			if err != nil {
				t.Fatalf("NewValidator: %v", err)
			}
			defer v.Close()

			mismatches, err := v.Run(1000, nil)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			for i, m := range mismatches {
				t.Errorf("mismatch[%d]:\n  SQL:    %s\n  SQLite: %s\n  SVibe:  %s\n  Reason: %s",
					i,
					m.Query,
					fmtResult(m.SQLiteResult),
					fmtResult(m.SQLVibeResult),
					m.Reason,
				)
			}
			if len(mismatches) == 0 {
				t.Logf("All 1000 statements matched between SQLite and sqlvibe (seed=%d)", seed)
			}
		})
	}
}

// TestSQLValidator_Stress runs concurrent validation across multiple seeds.
// This stresses the engine with parallel query execution.
func TestSQLValidator_Stress(t *testing.T) {
	// Generate many seeds for concurrent testing
	var seeds []uint64
	for i := uint64(0); i < 20; i++ {
		seeds = append(seeds, i*1000+1)
	}

	statementsPerSeed := 500
	totalStatements := len(seeds) * statementsPerSeed

	t.Logf("Running stress test: %d seeds x %d statements = %d total",
		len(seeds), statementsPerSeed, totalStatements)

	var wg sync.WaitGroup
	var mu sync.Mutex
	totalMismatches := 0

	// Callback to count mismatches in real-time
	onMatch := func(idx int, query string, liteRes, svibeRes QueryResult) {
		if Compare(query, liteRes, svibeRes) != nil {
			mu.Lock()
			totalMismatches++
			mu.Unlock()
		}
	}

	// Run all seeds concurrently
	for _, seed := range seeds {
		wg.Add(1)
		go func(s uint64) {
			defer wg.Done()
			v, err := NewValidator(s)
			if err != nil {
				t.Logf("NewValidator(seed=%d) failed: %v", s, err)
				return
			}
			defer v.Close()

			v.Run(statementsPerSeed, onMatch)
		}(seed)
	}

	wg.Wait()

	t.Logf("Stress test complete: %d total mismatches out of %d statements",
		totalMismatches, totalStatements)
}

// TestSQLValidator_FastFail stops at first mismatch for quick feedback.
func TestSQLValidator_FastFail(t *testing.T) {
	seed := uint64(42)
	statements := 10000

	t.Logf("Running fast-fail test: seed=%d, statements=%d", seed, statements)

	v, err := NewValidator(seed)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	var firstMismatch *Mismatch
	var mismatchIdx int
	v.Run(statements, func(idx int, query string, liteRes, svibeRes QueryResult) {
		if m := Compare(query, liteRes, svibeRes); m != nil && firstMismatch == nil {
			firstMismatch = m
			mismatchIdx = idx
		}
	})

	if firstMismatch != nil {
		t.Errorf("First mismatch found at statement #%d (seed=%d):\n  SQL:    %s\n  SQLite: %s\n  SVibe:  %s\n  Reason: %s",
			mismatchIdx, seed,
			firstMismatch.Query,
			fmtResult(firstMismatch.SQLiteResult),
			fmtResult(firstMismatch.SQLVibeResult),
			firstMismatch.Reason)
	} else {
		t.Logf("All %d statements matched (seed=%d)", statements, seed)
	}
}

// TestSQLValidator_DiscoverBugs runs multiple seeds to discover as many bugs as possible.
func TestSQLValidator_DiscoverBugs(t *testing.T) {
	// Test many different seeds to find various bug patterns
	seeds := []uint64{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		100, 200, 300, 400, 500,
		1000, 2000, 3000, 4000, 5000,
		10000, 20000, 30000, 40000, 50000,
		12345, 54321, 99999, 11111, 22222,
	}

	statementsPerSeed := 200
	bugTypes := make(map[string]int)
	var allMismatches []Mismatch

	for _, seed := range seeds {
		v, err := NewValidator(seed)
		if err != nil {
			t.Logf("NewValidator(seed=%d) failed: %v", seed, err)
			continue
		}

		mismatches, _ := v.Run(statementsPerSeed, nil)
		v.Close()

		for _, m := range mismatches {
			allMismatches = append(allMismatches, m)
			// Categorize bug by keywords in the reason
			reason := m.Reason
			switch {
			case contains(reason, "ORDER BY clause should come after"):
				bugTypes["UNION ORDER BY position"]++
			case contains(reason, "DISTINCT") || contains(reason, "row count differs") && contains(m.Query, "DISTINCT"):
				bugTypes["DISTINCT dedup"]++
			case contains(reason, "no such column"):
				bugTypes["Subquery column ref"]++
			case contains(reason, "IN (SELECT") || contains(reason, "EXISTS"):
				bugTypes["Subquery IN/EXISTS"]++
			case contains(reason, "BETWEEN"):
				bugTypes["BETWEEN predicate"]++
			default:
				bugTypes["Other"]++
			}
		}
	}

	t.Logf("=== Bug Summary ===")
	t.Logf("Total seeds tested: %d", len(seeds))
	t.Logf("Total statements: %d", len(seeds)*statementsPerSeed)
	t.Logf("Total mismatches: %d (%.1f%%)", len(allMismatches), float64(len(allMismatches))*100/float64(len(seeds)*statementsPerSeed))
	t.Logf("")
	t.Logf("Bug categories:")
	for bugType, count := range bugTypes {
		t.Logf("  %s: %d", bugType, count)
	}

	// Show first 5 unique bugs
	t.Logf("")
	t.Logf("=== Sample Bugs (first 5 unique) ===")
	shown := make(map[string]bool)
	count := 0
	for _, m := range allMismatches {
		key := extractBugKey(m)
		if !shown[key] && count < 5 {
			shown[key] = true
			count++
			t.Logf("Bug #%d: %s", count, m.Query)
			t.Logf("  Reason: %s", m.Reason)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func extractBugKey(m Mismatch) string {
	// Create a simple key based on bug type
	reason := m.Reason
	query := m.Query
	if contains(reason, "ORDER BY clause should come after") {
		return "UNION"
	}
	if contains(reason, "DISTINCT") || contains(query, "DISTINCT") {
		return "DISTINCT"
	}
	if contains(reason, "no such column") || contains(reason, "EXISTS") || contains(reason, "IN (SELECT") {
		return "SUBQUERY"
	}
	return "OTHER"
}

// TestSQLValidator_Reproduce runs a specific seed and statement count for bug reproduction.
// Use -seed and -count flags to specify parameters:
//
//	go test -v -run TestSQLValidator_Reproduce -seed=42 -count=100
func TestSQLValidator_Reproduce(t *testing.T) {
	seed := uint64(42)
	count := 100

	v, err := NewValidator(seed)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	mismatches, err := v.Run(count, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for i, m := range mismatches {
		t.Errorf("mismatch[%d]:\n  SQL:    %s\n  SQLite: %s\n  SVibe:  %s\n  Reason: %s",
			i,
			m.Query,
			fmtResult(m.SQLiteResult),
			fmtResult(m.SQLVibeResult),
			m.Reason,
		)
	}

	if len(mismatches) == 0 {
		t.Logf("All %d statements matched (seed=%d)", count, seed)
	} else {
		t.Logf("Found %d mismatches out of %d statements (seed=%d)", len(mismatches), count, seed)
	}
}

// TestSQLValidator_Regression replays specific LCG seeds that have triggered
// mismatches in the past. Add new entries here whenever a bug is found.
func TestSQLValidator_Regression(t *testing.T) {
	// Each entry describes a regression case.
	type regCase struct {
		desc  string
		seed  uint64
		count int
	}

	// Regression cases — populated as bugs are found.
	// Format: {description, seed, statement_count}
	cases := []regCase{
		{"FastFail seed=42", 42, 100},
		// Regression: DISTINCT deduplication failed when ORDER BY had extra non-SELECT
		// columns (seed=1 triggered the bug with DISTINCT + ORDER BY on non-projected cols).
		{"DISTINCT+OrderBy extra cols seed=1", 1, 1000},
		{"DISTINCT+OrderBy extra cols seed=2", 2, 500},
		// Regression: subquery generator was missing ORDER BY for IN/EXISTS LIMIT queries.
		{"Subquery IN/EXISTS ORDER BY seed=7", 7, 500},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			v, err := NewValidator(tc.seed)
			if err != nil {
				t.Fatalf("NewValidator(seed=%d): %v", tc.seed, err)
			}
			defer v.Close()

			mismatches, err := v.Run(tc.count, nil)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			for i, m := range mismatches {
				t.Errorf("mismatch[%d]:\n  SQL:    %s\n  Reason: %s",
					i, m.Query, m.Reason)
			}
		})
	}
}

// TestSQLValidator_Coverage measures which SQL patterns are tested and their bug detection rate.
func TestSQLValidator_Coverage(t *testing.T) {
	seed := uint64(42)
	statements := 5000

	patternStats := make(map[string]struct {
		generated    int
		bugs         int
		mismatchRate float64
	})

	v, err := NewValidator(seed)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	v.Run(statements, func(idx int, query string, liteRes, svibeRes QueryResult) {
		pattern := classifyQuery(query)
		stats := patternStats[pattern]
		stats.generated++
		if Compare(query, liteRes, svibeRes) != nil {
			stats.bugs++
		}
		if stats.generated > 0 {
			stats.mismatchRate = float64(stats.bugs) / float64(stats.generated) * 100
		}
		patternStats[pattern] = stats
	})

	format := "=== Coverage Report (seed=%d, statements=%d) ==="
	t.Logf(format, seed, statements)
	t.Logf("")
	t.Logf("%-25s %10s %10s %12s", "Pattern", "Generated", "Bugs", "Mismatch %")
	t.Logf("------------------------------------------------------------")

	var totalGen, totalBugs int
	for pattern, stats := range patternStats {
		t.Logf("%-25s %10d %10d %11.1f%%", pattern, stats.generated, stats.bugs, stats.mismatchRate)
		totalGen += stats.generated
		totalBugs += stats.bugs
	}
	t.Logf("------------------------------------------------------------")
	t.Logf("%-25s %10d %10d %11.1f%%", "TOTAL", totalGen, totalBugs, float64(totalBugs)*100/float64(totalGen))
}

// TestSQLValidator_DeepCoverage runs extensive validation with multiple seeds and pattern categories.
func TestSQLValidator_DeepCoverage(t *testing.T) {
	seeds := []uint64{1001, 2002, 3003, 4004, 5005}
	statementsPerSeed := 2000

	patternStats := make(map[string]struct {
		generated int
		bugs      int
	})

	var totalGen, totalBugs int

	for _, seed := range seeds {
		v, err := NewValidator(seed)
		if err != nil {
			t.Logf("NewValidator(seed=%d) failed: %v", seed, err)
			continue
		}

		v.Run(statementsPerSeed, func(idx int, query string, liteRes, svibeRes QueryResult) {
			pattern := classifyQuery(query)
			stats := patternStats[pattern]
			stats.generated++
			if Compare(query, liteRes, svibeRes) != nil {
				stats.bugs++
				totalBugs++
			}
			patternStats[pattern] = stats
			totalGen++
		})
		v.Close()
	}

	t.Logf("=== Deep Coverage Report (%d seeds x %d statements) ===", len(seeds), statementsPerSeed)
	t.Logf("")
	t.Logf("%-25s %10s %10s %12s", "Pattern", "Generated", "Bugs", "Mismatch %")
	t.Logf("------------------------------------------------------------")

	for pattern, stats := range patternStats {
		rate := 0.0
		if stats.generated > 0 {
			rate = float64(stats.bugs) / float64(stats.generated) * 100
		}
		t.Logf("%-25s %10d %10d %11.1f%%", pattern, stats.generated, stats.bugs, rate)
	}
	t.Logf("------------------------------------------------------------")
	t.Logf("%-25s %10d %10d %11.1f%%", "TOTAL", totalGen, totalBugs, float64(totalBugs)*100/float64(totalGen))

	// Log bugs found but don't fail - this test is for coverage discovery
	if totalBugs > 0 {
		t.Logf("Discovered %d bugs out of %d statements (for investigation)", totalBugs, totalGen)
	}
}

// TestSQLValidator_AdvancedPatterns tests specific advanced SQL patterns in depth.
func TestSQLValidator_AdvancedPatterns(t *testing.T) {
	seed := uint64(7777)
	statements := 3000

	advancedPatterns := map[string]int{
		"MULTI_JOIN":      0,
		"EXCEPT":          0,
		"INTERSECT":       0,
		"NOT_IN_SUBQUERY": 0,
		"SCALAR_SUBQUERY": 0,
		"NOT_BETWEEN":     0,
		"GLOB":            0,
		"NULLIF":          0,
		"CASE_WHEN":       0,
		"MULTI_GROUP_BY":  0,
		"HAVING":          0,
		"SUBQUERY":        0,
	}

	v, err := NewValidator(seed)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	var bugs int
	v.Run(statements, func(idx int, query string, liteRes, svibeRes QueryResult) {
		pattern := classifyQuery(query)
		if _, ok := advancedPatterns[pattern]; ok {
			advancedPatterns[pattern]++
		}
		if Compare(query, liteRes, svibeRes) != nil {
			bugs++
		}
	})

	t.Logf("=== Advanced Pattern Coverage (seed=%d, statements=%d) ===", seed, statements)
	for pattern, count := range advancedPatterns {
		t.Logf("%-25s %5d generated", pattern, count)
	}

	// Log bugs found but don't fail - this test is for pattern coverage discovery
	if bugs > 0 {
		t.Logf("Discovered %d bugs (for investigation)", bugs)
	}
}

// TestSQLValidator_EdgeCases tests edge cases and boundary conditions.
func TestSQLValidator_EdgeCases(t *testing.T) {
	seed := uint64(9999)
	statements := 2000

	v, err := NewValidator(seed)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	defer v.Close()

	var bugs int
	var edgeCaseBugs []string

	v.Run(statements, func(idx int, query string, liteRes, svibeRes QueryResult) {
		if m := Compare(query, liteRes, svibeRes); m != nil {
			bugs++
			if len(edgeCaseBugs) < 5 {
				edgeCaseBugs = append(edgeCaseBugs, fmt.Sprintf("SQL: %s\n  Reason: %s", m.Query, m.Reason))
			}
		}
	})

	t.Logf("=== Edge Case Report (seed=%d, statements=%d) ===", seed, statements)
	t.Logf("Total mismatches: %d", bugs)

	if len(edgeCaseBugs) > 0 {
		t.Logf("Sample edge case bugs:")
		for i, bug := range edgeCaseBugs {
			t.Logf("  Bug %d: %s", i+1, bug)
		}
	}

	// Log bugs found but don't fail - this test is for edge case discovery
	if bugs > 0 {
		t.Logf("Discovered %d edge case mismatches (for investigation)", bugs)
	}
}

func classifyQuery(query string) string {
	q := strings.ToUpper(query)
	switch {
	case strings.Contains(q, "UNION ALL") || strings.Contains(q, "UNION SELECT"):
		return "UNION"
	case strings.Contains(q, "EXCEPT"):
		return "EXCEPT"
	case strings.Contains(q, "INTERSECT"):
		return "INTERSECT"
	case strings.Contains(q, "DISTINCT"):
		return "DISTINCT"
	case strings.Contains(q, "GROUP BY") && strings.Contains(q, ","):
		return "MULTI_GROUP_BY"
	case strings.Contains(q, "GROUP BY"):
		return "GROUP_BY"
	case strings.Contains(q, "HAVING"):
		return "HAVING"
	case strings.Contains(q, "CASE WHEN"):
		return "CASE_WHEN"
	case strings.Contains(q, "LIKE"):
		return "LIKE"
	case strings.Contains(q, "GLOB"):
		return "GLOB"
	case strings.Contains(q, "COALESCE") || strings.Contains(q, "IFNULL"):
		return "COALESCE"
	case strings.Contains(q, "NULLIF"):
		return "NULLIF"
	case strings.Contains(q, "CAST"):
		return "CAST"
	case strings.Contains(q, "SUBSTR") || strings.Contains(q, "LENGTH") || strings.Contains(q, "UPPER") || strings.Contains(q, "LOWER"):
		return "STRING_FUNC"
	case strings.Contains(q, "OFFSET"):
		return "OFFSET"
	case strings.Contains(q, "NOT IN (SELECT"):
		return "NOT_IN_SUBQUERY"
	case strings.Contains(q, "IN (SELECT") || strings.Contains(q, "EXISTS"):
		return "SUBQUERY"
	case strings.Contains(q, "NOT BETWEEN"):
		return "NOT_BETWEEN"
	case strings.Contains(q, "BETWEEN"):
		return "BETWEEN"
	case strings.Contains(q, "IS NULL") || strings.Contains(q, "IS NOT NULL"):
		return "NULL_PREDICATE"
	case strings.Contains(q, "INNER JOIN") && strings.Count(q, "JOIN") >= 2:
		return "MULTI_JOIN"
	case strings.Contains(q, "JOIN"):
		return "JOIN"
	case strings.Contains(q, "SCALAR_SUBQUERY") || strings.Contains(q, "(SELECT"):
		return "SCALAR_SUBQUERY"
	case strings.Contains(q, "ORDER BY"):
		return "ORDER_BY"
	case strings.Contains(q, "LIMIT"):
		return "LIMIT"
	default:
		return "SIMPLE_SELECT"
	}
}
