package pragma_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

func TestHandleAutoVacuum_Read(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "auto_vacuum"}
	_, rows, err := pragma.HandleAutoVacuum(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(0) {
		t.Errorf("expected default 0, got %v", rows[0][0])
	}
}

func TestHandleAutoVacuum_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "auto_vacuum", Value: &QP.Literal{Value: int64(1)}}
	_, rows, err := pragma.HandleAutoVacuum(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1) {
		t.Errorf("expected 1, got %v", rows[0][0])
	}
}

func TestHandleShrinkMemory(t *testing.T) {
	ctx := newMock()
	_, _, err := pragma.HandleShrinkMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ctx.cacheCleared {
		t.Error("expected caches to be cleared")
	}
}

func TestHandleOptimize(t *testing.T) {
	ctx := newMock()
	_, rows, err := pragma.HandleOptimize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "ok" {
		t.Errorf("expected ok, got %v", rows[0][0])
	}
}

func TestHandleIntegrityCheck_OK(t *testing.T) {
	ctx := newMock()
	ctx.integrityErrors = nil
	_, rows, err := pragma.HandleIntegrityCheck(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "ok" {
		t.Errorf("expected ok, got %v", rows[0][0])
	}
}

func TestHandleQuickCheck(t *testing.T) {
	ctx := newMock()
	_, rows, err := pragma.HandleQuickCheck(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "ok" {
		t.Errorf("expected ok, got %v", rows[0][0])
	}
}

func TestHandleJournalSizeLimit(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "journal_size_limit", Value: &QP.Literal{Value: int64(1048576)}}
	_, rows, err := pragma.HandleJournalSizeLimit(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1048576) {
		t.Errorf("expected 1048576, got %v", rows[0][0])
	}
}
