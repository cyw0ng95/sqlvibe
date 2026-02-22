package sqlvibe

import "sync/atomic"

// CacheLineSize is the typical CPU cache line size in bytes.
const CacheLineSize = 64

// PrefetchDepth is the default number of rows to look ahead during scans.
const PrefetchDepth = 8

// CacheLinePad is a byte array sized to one cache line, used for padding
// structs to prevent false sharing between CPU cores.
type CacheLinePad [CacheLineSize]byte

// AlignedCounter is an atomic int64 counter padded to prevent false sharing
// between adjacent hot variables on different cache lines.
type AlignedCounter struct {
	_   CacheLinePad
	val int64
	_   CacheLinePad
}

// Add atomically adds n to the counter and returns the new value.
func (ac *AlignedCounter) Add(n int64) int64 {
	return atomic.AddInt64(&ac.val, n)
}

// Get atomically reads the current counter value.
func (ac *AlignedCounter) Get() int64 {
	return atomic.LoadInt64(&ac.val)
}

// ScanPrefetcher hints the CPU to warm cache lines for upcoming row accesses
// during sequential scans, reducing cache-miss stalls.
type ScanPrefetcher struct{}

// PrefetchRows touches the row at index idx+depth to encourage the CPU to
// prefetch it into cache before it is needed.  It is a no-op when the target
// index is out of bounds.
func (p *ScanPrefetcher) PrefetchRows(rows []map[string]interface{}, idx int, depth int) {
	target := idx + depth
	if target < len(rows) {
		// Read one byte from the target row to trigger a cache-line fetch.
		// This is a software prefetch hint; the actual value is discarded.
		row := rows[target]
		for range row {
			break
		}
	}
}
