package pragma

import (
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandleCacheSize handles PRAGMA cache_size [= N].
func HandleCacheSize(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		size := ctx.CacheSize()
		cols, rows := Result("cache_size", int64(size))
		return cols, rows, nil
	}
	n := int(IntValue(stmt.Value))
	ctx.SetCacheCapacity(n)
	cols, rows := EmptyResult()
	return cols, rows, nil
}

// HandleCacheMemory handles PRAGMA cache_memory [= N].
func HandleCacheMemory(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		if val < 0 {
			val = 0
		}
		ctx.SetPragmaInt("cache_memory", val)
		if val > 0 {
			maxPages := int(val / 4096)
			if maxPages > 0 {
				ctx.SetPageManagerMaxPages(maxPages)
			}
		}
		cols, rows := Result("cache_memory", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("cache_memory", 0)
	cols, rows := Result("cache_memory", v)
	return cols, rows, nil
}

// HandleCacheSpill handles PRAGMA cache_spill [= N].
func HandleCacheSpill(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("cache_spill", val)
		cols, rows := Result("cache_spill", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("cache_spill", 1)
	cols, rows := Result("cache_spill", v)
	return cols, rows, nil
}

// HandleCacheGrind returns detailed cache statistics.
func HandleCacheGrind(ctx Ctx) ([]string, [][]interface{}, error) {
	m := ctx.StorageMetrics()
	cols := []string{"pages_cached", "pages_free", "hits", "misses"}
	rows := [][]interface{}{{int64(m.UsedPages), int64(m.FreePages), int64(0), int64(0)}}
	return cols, rows, nil
}
