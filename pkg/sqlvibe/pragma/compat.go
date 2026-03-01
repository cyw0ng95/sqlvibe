package pragma

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandleCompression handles PRAGMA compression [= NAME].
func HandleCompression(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		cols, rows := Result("compression", ctx.GetCompressionName())
		return cols, rows, nil
	}
	var name string
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		if s, ok := v.Value.(string); ok {
			name = strings.ToUpper(s)
		}
	case *QP.ColumnRef:
		name = strings.ToUpper(v.Name)
	default:
		return nil, nil, fmt.Errorf("PRAGMA compression: unsupported value %T", stmt.Value)
	}
	if _, err := DS.NewCompressor(name, 0); err != nil {
		return nil, nil, fmt.Errorf("PRAGMA compression: %w", err)
	}
	if err := ctx.SetCompressionName(name); err != nil {
		return nil, nil, fmt.Errorf("PRAGMA compression: %w", err)
	}
	cols, rows := Result("compression", name)
	return cols, rows, nil
}

// HandleLockingMode handles PRAGMA locking_mode [= MODE].
func HandleLockingMode(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := strings.ToUpper(StrValue(stmt.Value))
		ctx.SetPragmaStr("locking_mode", val)
		cols, rows := Result("locking_mode", strings.ToLower(val))
		return cols, rows, nil
	}
	v := ctx.GetPragmaStr("locking_mode", "normal")
	cols, rows := Result("locking_mode", strings.ToLower(v))
	return cols, rows, nil
}

// HandleSynchronous handles PRAGMA synchronous [= N].
func HandleSynchronous(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("synchronous", val)
		cols, rows := Result("synchronous", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("synchronous", 2)
	cols, rows := Result("synchronous", v)
	return cols, rows, nil
}

// HandleQueryOnly handles PRAGMA query_only [= N].
func HandleQueryOnly(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("query_only", val)
		cols, rows := Result("query_only", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("query_only", 0)
	cols, rows := Result("query_only", v)
	return cols, rows, nil
}

// HandleTempStore handles PRAGMA temp_store [= N].
func HandleTempStore(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("temp_store", val)
		cols, rows := Result("temp_store", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("temp_store", 0)
	cols, rows := Result("temp_store", v)
	return cols, rows, nil
}

// HandleReadUncommitted handles PRAGMA read_uncommitted [= N].
func HandleReadUncommitted(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("read_uncommitted", val)
		cols, rows := Result("read_uncommitted", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("read_uncommitted", 0)
	cols, rows := Result("read_uncommitted", v)
	return cols, rows, nil
}
