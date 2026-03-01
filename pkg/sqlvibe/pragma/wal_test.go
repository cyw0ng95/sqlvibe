package pragma_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

func TestHandleJournalMode_Read(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "journal_mode"}
	cols, rows, err := pragma.HandleJournalMode(ctx, stmt, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "delete" {
		t.Errorf("expected delete, got %v", rows[0][0])
	}
	_ = cols
}

func TestHandleJournalMode_SetWAL(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "journal_mode", Value: &QP.ColumnRef{Name: "wal"}}
	_, rows, err := pragma.HandleJournalMode(ctx, stmt, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "wal" {
		t.Errorf("expected wal, got %v", rows[0][0])
	}
}

func TestHandleWALCheckpoint_NotInWAL(t *testing.T) {
	ctx := newMock() // journalMode = "delete"
	stmt := &QP.PragmaStmt{Name: "wal_checkpoint"}
	cols, rows, err := pragma.HandleWALCheckpoint(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Errorf("expected 3 cols, got %d", len(cols))
	}
	if rows[0][0] != int64(0) {
		t.Errorf("expected busy=0, got %v", rows[0][0])
	}
}

func TestHandleWALAutoCheckpoint(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "wal_autocheckpoint", Value: &QP.Literal{Value: int64(500)}}
	_, rows, err := pragma.HandleWALAutoCheckpoint(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(500) {
		t.Errorf("expected 500, got %v", rows[0][0])
	}
	if ctx.autoCheckpointSet != 500 {
		t.Errorf("expected autoCheckpointSet=500, got %d", ctx.autoCheckpointSet)
	}
}

func TestHandleWALAutoCheckpoint_Default(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "wal_autocheckpoint"}
	_, rows, err := pragma.HandleWALAutoCheckpoint(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1000) {
		t.Errorf("expected default 1000, got %v", rows[0][0])
	}
}

func TestHandleWALTruncate(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "wal_truncate", Value: &QP.Literal{Value: int64(1)}}
	_, rows, err := pragma.HandleWALTruncate(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1) {
		t.Errorf("expected 1, got %v", rows[0][0])
	}
}
