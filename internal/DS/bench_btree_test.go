package DS

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/PB"
)

// newBenchPageManager creates a temp-file-backed PageManager for benchmarks.
func newBenchPageManager(b *testing.B) (*PageManager, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "ds_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(dir, "bench.db")
	file, err := PB.OpenFile(path, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	pm, err := NewPageManager(file, 4096)
	if err != nil {
		file.Close()
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	cleanup := func() {
		file.Close()
		os.RemoveAll(dir)
	}
	return pm, cleanup
}

// BenchmarkBTree_Insert benchmarks B-Tree insert operations.
func BenchmarkBTree_Insert(b *testing.B) {
	pm, cleanup := newBenchPageManager(b)
	defer cleanup()

	bt := NewBTree(pm, 0, true)
	key := make([]byte, 9)
	val := []byte("benchmark value data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutVarint(key, int64(i+1))
		bt.Insert(key, val) //nolint:errcheck
	}
}

// BenchmarkBTree_Search benchmarks B-Tree search on a pre-populated tree.
func BenchmarkBTree_Search(b *testing.B) {
	pm, cleanup := newBenchPageManager(b)
	defer cleanup()

	bt := NewBTree(pm, 0, true)
	key := make([]byte, 9)
	val := []byte("benchmark value data")

	// Pre-populate with 200 rows
	const rows = 200
	for i := 1; i <= rows; i++ {
		PutVarint(key, int64(i))
		if err := bt.Insert(key, val); err != nil {
			b.Fatal(err)
		}
	}

	searchKey := make([]byte, 9)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutVarint(searchKey, int64((i%rows)+1))
		bt.Search(searchKey) //nolint:errcheck
	}
}

// BenchmarkBTree_Cursor benchmarks B-Tree cursor traversal.
func BenchmarkBTree_Cursor(b *testing.B) {
	pm, cleanup := newBenchPageManager(b)
	defer cleanup()

	bt := NewBTree(pm, 0, true)
	key := make([]byte, 9)
	val := []byte("benchmark value data")

	// Pre-populate with 100 rows
	const rows = 100
	for i := 1; i <= rows; i++ {
		PutVarint(key, int64(i))
		if err := bt.Insert(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := bt.NewCursor()
		if err := c.First(); err != nil {
			b.Fatal(err)
		}
		for c.Valid() {
			if err := c.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkCache_Get benchmarks LRU cache hit performance.
func BenchmarkCache_Get(b *testing.B) {
	pm, cleanup := newBenchPageManager(b)
	defer cleanup()

	// Allocate and write a page so it gets into the cache
	pageNum, err := pm.AllocatePage()
	if err != nil {
		b.Fatal(err)
	}
	page, err := pm.ReadPage(pageNum)
	if err != nil {
		b.Fatal(err)
	}
	page.Data[0] = 0x0d
	if err := pm.WritePage(page); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.ReadPage(pageNum) //nolint:errcheck
	}
}

// BenchmarkCache_Set benchmarks LRU cache miss+set performance.
func BenchmarkCache_Set(b *testing.B) {
	pm, cleanup := newBenchPageManager(b)
	defer cleanup()

	// Pre-allocate pages
	const numPages = 50
	pageNums := make([]uint32, numPages)
	for i := range pageNums {
		n, err := pm.AllocatePage()
		if err != nil {
			b.Fatal(err)
		}
		pageNums[i] = n
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.ReadPage(pageNums[i%numPages]) //nolint:errcheck
	}
}

