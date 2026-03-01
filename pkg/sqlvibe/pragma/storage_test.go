package pragma_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

func TestHandlePageSize_Read(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "page_size"}
	cols, rows, err := pragma.HandlePageSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(4096) {
		t.Errorf("expected default 4096, got %v", rows[0][0])
	}
	_ = cols
}

func TestHandlePageSize_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "page_size", Value: &QP.Literal{Value: int64(8192)}}
	cols, rows, err := pragma.HandlePageSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(8192) {
		t.Errorf("expected 8192, got %v", rows[0][0])
	}
	_ = cols
}

func TestHandleMmapSize(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "mmap_size", Value: &QP.Literal{Value: int64(1048576)}}
	_, rows, err := pragma.HandleMmapSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1048576) {
		t.Errorf("expected 1048576, got %v", rows[0][0])
	}
}

func TestHandleStorageInfo(t *testing.T) {
	ctx := newMock()
	ctx.storageMetrics = pragma.StorageMetrics{PageCount: 10, UsedPages: 7, FreePages: 3, TotalRows: 100, TotalTables: 2}
	cols, rows, err := pragma.HandleStorageInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 7 {
		t.Errorf("expected 7 cols, got %d", len(cols))
	}
	if rows[0][0] != int64(10) {
		t.Errorf("expected page_count=10, got %v", rows[0][0])
	}
}

func TestHandleMemoryStats(t *testing.T) {
	ctx := newMock()
	ctx.storageMetrics = pragma.StorageMetrics{UsedPages: 4, PageCount: 8, TotalRows: 50}
	cols, rows, err := pragma.HandleMemoryStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 5 {
		t.Errorf("expected 5 cols, got %d", len(cols))
	}
	// page_cache_used = 4 * 4096 = 16384
	if rows[0][0] != int64(16384) {
		t.Errorf("expected page_cache_used=16384, got %v", rows[0][0])
	}
}

func TestHandleMaxRows(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "max_rows", Value: &QP.Literal{Value: int64(1000)}}
	_, rows, err := pragma.HandleMaxRows(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1000) {
		t.Errorf("expected 1000, got %v", rows[0][0])
	}
}

func TestHandleFreelistCount(t *testing.T) {
	ctx := newMock()
	ctx.storageMetrics = pragma.StorageMetrics{FreePages: 7}
	cols, rows, err := pragma.HandleFreelistCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0] != "freelist_count" {
		t.Errorf("unexpected cols: %v", cols)
	}
	if rows[0][0] != int64(7) {
		t.Errorf("expected freelist_count=7, got %v", rows[0][0])
	}
}

func TestHandlePageCount(t *testing.T) {
	ctx := newMock()
	ctx.storageMetrics = pragma.StorageMetrics{PageCount: 42}
	cols, rows, err := pragma.HandlePageCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0] != "page_count" {
		t.Errorf("unexpected cols: %v", cols)
	}
	if rows[0][0] != int64(42) {
		t.Errorf("expected page_count=42, got %v", rows[0][0])
	}
}

func TestHandleMemoryStatus(t *testing.T) {
	cols, rows, err := pragma.HandleMemoryStatus()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 7 {
		t.Errorf("expected 7 cols, got %d: %v", len(cols), cols)
	}
	if len(rows) != 1 || len(rows[0]) != 7 {
		t.Fatalf("unexpected rows shape: %v", rows)
	}
	// heap_alloc should be a non-negative int64.
	heapAlloc, ok := rows[0][0].(int64)
	if !ok || heapAlloc < 0 {
		t.Errorf("expected non-negative heap_alloc int64, got %v", rows[0][0])
	}
}

func TestHandleHeapLimit_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "heap_limit", Value: &QP.Literal{Value: int64(1 << 30)}}
	_, rows, err := pragma.HandleHeapLimit(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(1<<30) {
		t.Errorf("expected 1<<30, got %v", rows[0][0])
	}
}

func TestHandleHeapLimit_Read(t *testing.T) {
	ctx := newMock()
	ctx.maxMemoryBytes = int64(512 * 1024 * 1024)
	stmt := &QP.PragmaStmt{Name: "heap_limit"}
	_, rows, err := pragma.HandleHeapLimit(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(512*1024*1024) {
		t.Errorf("expected %d, got %v", int64(512*1024*1024), rows[0][0])
	}
}
