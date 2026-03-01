package pragma

import (
	"runtime"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandlePageSize handles PRAGMA page_size [= N].
func HandlePageSize(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("page_size", val)
		cols, rows := Result("page_size", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("page_size", 4096)
	cols, rows := Result("page_size", v)
	return cols, rows, nil
}

// HandleMmapSize handles PRAGMA mmap_size [= N].
func HandleMmapSize(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("mmap_size", val)
		cols, rows := Result("mmap_size", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("mmap_size", 0)
	cols, rows := Result("mmap_size", v)
	return cols, rows, nil
}

// HandleStorageInfo returns storage statistics.
func HandleStorageInfo(ctx Ctx) ([]string, [][]interface{}, error) {
	m := ctx.StorageMetrics()
	cols := []string{"page_count", "used_pages", "free_pages", "compression_ratio", "wal_size", "total_rows", "total_tables"}
	rows := [][]interface{}{{
		int64(m.PageCount),
		int64(m.UsedPages),
		int64(m.FreePages),
		m.CompressionRatio,
		m.WALSize,
		int64(m.TotalRows),
		int64(m.TotalTables),
	}}
	return cols, rows, nil
}

// estimatedBytesPerRow is a rough per-row memory estimate for row-store reporting.
// 256 bytes approximates a typical mixed-type row (a few small integer/string columns).
// Tables with large TEXT/BLOB columns will be significantly underestimated.
const estimatedBytesPerRow = 256

// HandleMemoryStats returns detailed memory usage statistics.
func HandleMemoryStats(ctx Ctx) ([]string, [][]interface{}, error) {
	m := ctx.StorageMetrics()
	pageCacheUsed := int64(m.UsedPages * 4096)
	pageCacheMax := int64(m.PageCount * 4096)
	rowStoreUsed := int64(m.TotalRows * estimatedBytesPerRow)
	walSize := m.WALSize
	totalMemory := pageCacheUsed + rowStoreUsed + walSize

	cols := []string{"page_cache_used", "page_cache_max", "row_store_used", "wal_size", "total_memory"}
	rows := [][]interface{}{{pageCacheUsed, pageCacheMax, rowStoreUsed, walSize, totalMemory}}
	return cols, rows, nil
}

// HandleMaxRows handles PRAGMA max_rows [= N].
func HandleMaxRows(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		if val < 0 {
			val = 0
		}
		ctx.SetPragmaInt("max_rows", val)
		cols, rows := Result("max_rows", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("max_rows", 0)
	cols, rows := Result("max_rows", v)
	return cols, rows, nil
}

// HandleFreelistCount handles PRAGMA freelist_count (read-only).
// Returns the number of unused/free pages in the database file.
func HandleFreelistCount(ctx Ctx) ([]string, [][]interface{}, error) {
	m := ctx.StorageMetrics()
	cols, rows := Result("freelist_count", int64(m.FreePages))
	return cols, rows, nil
}

// HandlePageCount handles PRAGMA page_count (read-only).
// Returns the total number of pages in the database file.
func HandlePageCount(ctx Ctx) ([]string, [][]interface{}, error) {
	m := ctx.StorageMetrics()
	cols, rows := Result("page_count", int64(m.PageCount))
	return cols, rows, nil
}

// HandleMemoryStatus returns Go runtime memory statistics in a format
// compatible with SQLite's sqlite3_memory_status()/PRAGMA memory_status.
func HandleMemoryStatus() ([]string, [][]interface{}, error) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	cols := []string{"heap_alloc", "heap_sys", "heap_in_use", "heap_idle", "heap_released", "num_gc", "total_alloc"}
	rows := [][]interface{}{{
		int64(ms.HeapAlloc),
		int64(ms.HeapSys),
		int64(ms.HeapInuse),
		int64(ms.HeapIdle),
		int64(ms.HeapReleased),
		int64(ms.NumGC),
		int64(ms.TotalAlloc),
	}}
	return cols, rows, nil
}

// HandleHeapLimit handles PRAGMA heap_limit [= N].
// Sets or queries the advisory maximum heap size in bytes.
// This is informational only – Go's GC manages actual heap limits.
func HandleHeapLimit(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetMaxMemoryBytes(val)
		cols, rows := Result("heap_limit", val)
		return cols, rows, nil
	}
	v := ctx.GetMaxMemoryBytes()
	cols, rows := Result("heap_limit", v)
	return cols, rows, nil
}
