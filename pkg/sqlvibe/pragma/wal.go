package pragma

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HandleJournalMode handles PRAGMA journal_mode [= MODE].
func HandleJournalMode(ctx Ctx, stmt *QP.PragmaStmt, dbPath string) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		cols, rows := Result("journal_mode", ctx.GetJournalMode())
		return cols, rows, nil
	}

	var mode string
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		if s, ok := v.Value.(string); ok {
			mode = strings.ToLower(s)
		}
	case *QP.ColumnRef:
		mode = strings.ToLower(v.Name)
	default:
		return nil, nil, fmt.Errorf("PRAGMA journal_mode: unsupported value %T", stmt.Value)
	}

	switch mode {
	case "wal":
		if ctx.GetJournalMode() != "wal" {
			if err := ctx.OpenWALMode(dbPath, 4096); err != nil {
				return nil, nil, fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
			}
		}
	case "delete":
		if ctx.GetJournalMode() == "wal" {
			ctx.CloseWALMode()
		}
	default:
		return nil, nil, fmt.Errorf("PRAGMA journal_mode: unsupported mode %q", mode)
	}

	cols, rows := Result("journal_mode", ctx.GetJournalMode())
	return cols, rows, nil
}

// HandleWALCheckpoint handles PRAGMA wal_checkpoint [= MODE].
func HandleWALCheckpoint(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if ctx.GetJournalMode() != "wal" {
		cols := []string{"busy", "log", "checkpointed"}
		rows := [][]interface{}{{int64(0), int64(0), int64(0)}}
		return cols, rows, nil
	}

	mode := "passive"
	if stmt.Value != nil {
		mode = strings.ToLower(StrValue(stmt.Value))
	}

	var busy, logRemoved, checkpointed int
	var err error
	switch mode {
	case "truncate":
		busy, logRemoved, checkpointed, err = ctx.CheckpointTruncate()
	case "full":
		busy, logRemoved, checkpointed, err = ctx.CheckpointFull()
	default:
		moved, cpErr := ctx.CheckpointPassive()
		if cpErr != nil {
			return nil, nil, fmt.Errorf("PRAGMA wal_checkpoint: %w", cpErr)
		}
		busy, logRemoved, checkpointed = 0, moved, moved
	}
	if err != nil {
		return nil, nil, fmt.Errorf("PRAGMA wal_checkpoint: %w", err)
	}

	cols := []string{"busy", "log", "checkpointed"}
	rows := [][]interface{}{{int64(busy), int64(logRemoved), int64(checkpointed)}}
	return cols, rows, nil
}

// HandleWALMode handles PRAGMA wal_mode [= ON/OFF].
func HandleWALMode(ctx Ctx, stmt *QP.PragmaStmt, dbPath string) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		mode := "off"
		if ctx.GetJournalMode() == "wal" {
			mode = "on"
		}
		cols, rows := Result("wal_mode", mode)
		return cols, rows, nil
	}

	var val string
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		if s, ok := v.Value.(string); ok {
			val = strings.ToLower(s)
		}
	case *QP.ColumnRef:
		val = strings.ToLower(v.Name)
	default:
		return nil, nil, fmt.Errorf("PRAGMA wal_mode: unsupported value %T", stmt.Value)
	}

	var journalMode string
	if val == "on" {
		journalMode = "wal"
	} else {
		journalMode = "delete"
	}
	synthetic := &QP.PragmaStmt{Name: "journal_mode", Value: &QP.ColumnRef{Name: journalMode}}
	return HandleJournalMode(ctx, synthetic, dbPath)
}

// HandleWALAutoCheckpoint handles PRAGMA wal_autocheckpoint [= N].
func HandleWALAutoCheckpoint(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value == nil {
		n := int64(ctx.GetAutoCheckpointN())
		if n == 0 {
			n = 1000 // SQLite default
		}
		cols, rows := Result("wal_autocheckpoint", n)
		return cols, rows, nil
	}
	n := IntValue(stmt.Value)
	ctx.StartAutoCheckpoint(int(n))
	cols, rows := Result("wal_autocheckpoint", n)
	return cols, rows, nil
}

// HandleWALTruncate handles PRAGMA wal_truncate [= ON/OFF].
func HandleWALTruncate(ctx Ctx, stmt *QP.PragmaStmt) ([]string, [][]interface{}, error) {
	if stmt.Value != nil {
		val := IntValue(stmt.Value)
		ctx.SetPragmaInt("wal_truncate", val)
		if ctx.GetJournalMode() == "wal" && val != 0 {
			_, _ = ctx.CheckpointPassive()
		}
		cols, rows := Result("wal_truncate", val)
		return cols, rows, nil
	}
	v := ctx.GetPragmaInt("wal_truncate", 0)
	cols, rows := Result("wal_truncate", v)
	return cols, rows, nil
}
