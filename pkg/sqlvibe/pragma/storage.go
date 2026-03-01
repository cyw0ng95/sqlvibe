package pragma

import (
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

// estimatedBytesPerRow is a rough estimate used for row-store memory reporting.
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
