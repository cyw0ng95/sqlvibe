package pragma

import (
	"fmt"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandleIsolationLevel handles PRAGMA isolation_level [= LEVEL].
func HandleIsolationLevel(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		cols, rows := Result("isolation_level", ctx.GetIsolationLevel())
		return cols, rows, nil
	}
	var val string
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		if s, ok := v.Value.(string); ok {
			val = s
		}
	case *QP.ColumnRef:
		val = v.Name
	default:
		return nil, nil, fmt.Errorf("PRAGMA isolation_level: unsupported value %T", stmt.Value)
	}
	if err := ctx.SetIsolationLevel(val); err != nil {
		return nil, nil, fmt.Errorf("PRAGMA isolation_level: %w", err)
	}
	cols, rows := Result("isolation_level", ctx.GetIsolationLevel())
	return cols, rows, nil
}

// HandleBusyTimeout handles PRAGMA busy_timeout [= N].
func HandleBusyTimeout(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		cols, rows := Result("busy_timeout", int64(ctx.GetBusyTimeout()))
		return cols, rows, nil
	}
	var ms int64
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		switch val := v.Value.(type) {
		case int64:
			ms = val
		case float64:
			ms = int64(val)
		default:
			return nil, nil, fmt.Errorf("PRAGMA busy_timeout: unsupported value type %T", v.Value)
		}
	default:
		return nil, nil, fmt.Errorf("PRAGMA busy_timeout: unsupported value %T", stmt.Value)
	}
	if ms < 0 {
		return nil, nil, fmt.Errorf("PRAGMA busy_timeout: value must be >= 0")
	}
	ctx.SetBusyTimeout(int(ms))
	cols, rows := Result("busy_timeout", ms)
	return cols, rows, nil
}

// HandleQueryTimeout handles PRAGMA query_timeout [= N].
func HandleQueryTimeout(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		if val < 0 {
			val = 0
		}
		ctx.SetQueryTimeoutMs(val)
		cols, rows := Result("query_timeout", val)
		return cols, rows, nil
	}
	cols, rows := Result("query_timeout", ctx.GetQueryTimeoutMs())
	return cols, rows, nil
}

// HandleMaxMemory handles PRAGMA max_memory [= N].
func HandleMaxMemory(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		if val < 0 {
			val = 0
		}
		ctx.SetMaxMemoryBytes(val)
		cols, rows := Result("max_memory", val)
		return cols, rows, nil
	}
	cols, rows := Result("max_memory", ctx.GetMaxMemoryBytes())
	return cols, rows, nil
}

// HandleQueryCacheSize handles PRAGMA query_cache_size [= N].
func HandleQueryCacheSize(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		if val < 0 {
			val = 0
		}
		ctx.SetQueryCacheMax(int(val))
		cols, rows := Result("query_cache_size", val)
		return cols, rows, nil
	}
	cols, rows := Result("query_cache_size", int64(ctx.GetQueryCacheMax()))
	return cols, rows, nil
}
