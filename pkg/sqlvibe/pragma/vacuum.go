package pragma

import (
	"fmt"
	"os"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandleAutoVacuum handles PRAGMA auto_vacuum [= N].
func HandleAutoVacuum(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("auto_vacuum", val)
		cols, rows := Result("auto_vacuum", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("auto_vacuum", 0)
	cols, rows := Result("auto_vacuum", v)
	return cols, rows, nil
}

// HandleIncrementalVacuum handles PRAGMA incremental_vacuum [(N)].
// It reclaims up to N free pages (or all free pages when N is 0 or omitted).
// In the in-memory engine there are no physical free pages, so the operation
// returns the count of logically free pages reported by the storage layer.
func HandleIncrementalVacuum(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	// Determine how many pages were requested (0 = all).
	n := int64(0)
	if stmt.Value != nil {
		n = IntValue(stmt.Value)
		if n < 0 {
			n = 0
		}
	}
	// For the in-memory engine, honor auto_vacuum mode but there are no
	// physical pages to reclaim. Return the number of pages actually freed.
	freed := int64(0)
	m := ctx.StorageMetrics()
	available := int64(m.FreePages)
	if n == 0 || n > available {
		freed = available
	} else {
		freed = n
	}
	cols, rows := Result("freelist_count", freed)
	return cols, rows, nil
}

// HandleShrinkMemory handles PRAGMA shrink_memory.
func HandleShrinkMemory(ctx Ctx) ([]string, [][]interface{}, error) {
	ctx.ClearCaches()
	cols, rows := Result("shrink_memory", int64(0))
	return cols, rows, nil
}

// HandleOptimize handles PRAGMA optimize.
func HandleOptimize(ctx Ctx) ([]string, [][]interface{}, error) {
	if err := ctx.RunAnalyze(); err != nil {
		return nil, nil, fmt.Errorf("PRAGMA optimize: %w", err)
	}
	cols, rows := Result("optimize", "ok")
	return cols, rows, nil
}

// HandleIntegrityCheck handles PRAGMA integrity_check.
func HandleIntegrityCheck(ctx Ctx) ([]string, [][]interface{}, error) {
	errs, err := ctx.CheckDBIntegrity()
	if err != nil {
		return nil, nil, fmt.Errorf("PRAGMA integrity_check: %w", err)
	}
	var data [][]interface{}
	if len(errs) == 0 {
		data = append(data, []interface{}{"ok"})
	} else {
		for _, msg := range errs {
			data = append(data, []interface{}{msg})
		}
	}
	return []string{"integrity_check"}, data, nil
}

// HandleQuickCheck handles PRAGMA quick_check.
func HandleQuickCheck(ctx Ctx) ([]string, [][]interface{}, error) {
	dbPath := ctx.GetDBPath()
	if dbPath != ":memory:" {
		fi, err := os.Stat(dbPath)
		if err != nil {
			cols, rows := Result("quick_check", "file not found: "+dbPath)
			return cols, rows, nil
		}
		if fi.Size() == 0 && ctx.HasAnyTables() {
			cols, rows := Result("quick_check", "empty file with in-memory tables")
			return cols, rows, nil
		}
	}
	cols, rows := Result("quick_check", "ok")
	return cols, rows, nil
}

// HandleJournalSizeLimit handles PRAGMA journal_size_limit [= N].
func HandleJournalSizeLimit(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		v := ctx.GetPragmaInt("journal_size_limit", -1)
		cols, rows := Result("journal_size_limit", v)
		return cols, rows, nil
	}
	val := IntValue(stmt.Value)
	ctx.SetPragmaInt("journal_size_limit", val)
	if ctx.GetJournalMode() == "wal" && val >= 0 {
		if sz := ctx.GetWALSize(); sz > val {
			_, _ = ctx.CheckpointPassive()
		}
	}
	cols, rows := Result("journal_size_limit", val)
	return cols, rows, nil
}
