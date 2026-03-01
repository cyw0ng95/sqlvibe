package pragma_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

func TestHandleIsolationLevel_Read(t *testing.T) {
	ctx := newMock()
	ctx.isolationLevel = "READ COMMITTED"
	stmt := &QP.PragmaStmt{Name: "isolation_level"}
	_, rows, err := pragma.HandleIsolationLevel(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "READ COMMITTED" {
		t.Errorf("expected READ COMMITTED, got %v", rows[0][0])
	}
}

func TestHandleIsolationLevel_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "isolation_level", Value: &QP.Literal{Value: "SERIALIZABLE"}}
	_, _, err := pragma.HandleIsolationLevel(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.isolationLevel != "SERIALIZABLE" {
		t.Errorf("expected SERIALIZABLE, got %s", ctx.isolationLevel)
	}
}

func TestHandleBusyTimeout_Read(t *testing.T) {
	ctx := newMock()
	ctx.busyTimeout = 5000
	stmt := &QP.PragmaStmt{Name: "busy_timeout"}
	_, rows, err := pragma.HandleBusyTimeout(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(5000) {
		t.Errorf("expected 5000, got %v", rows[0][0])
	}
}

func TestHandleBusyTimeout_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "busy_timeout", Value: &QP.Literal{Value: int64(3000)}}
	_, _, err := pragma.HandleBusyTimeout(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.busyTimeout != 3000 {
		t.Errorf("expected 3000, got %d", ctx.busyTimeout)
	}
}

func TestHandleQueryTimeout(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "query_timeout", Value: &QP.Literal{Value: int64(10000)}}
	_, rows, err := pragma.HandleQueryTimeout(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(10000) {
		t.Errorf("expected 10000, got %v", rows[0][0])
	}
}

func TestHandleMaxMemory(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "max_memory", Value: &QP.Literal{Value: int64(104857600)}}
	_, rows, err := pragma.HandleMaxMemory(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(104857600) {
		t.Errorf("expected 104857600, got %v", rows[0][0])
	}
}

func TestHandleQueryCacheSize(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "query_cache_size", Value: &QP.Literal{Value: int64(256)}}
	_, rows, err := pragma.HandleQueryCacheSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(256) {
		t.Errorf("expected 256, got %v", rows[0][0])
	}
	if ctx.queryCacheMax != 256 {
		t.Errorf("expected queryCacheMax=256, got %d", ctx.queryCacheMax)
	}
}
