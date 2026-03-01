package pragma_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

// mockCtx implements pragma.Ctx for testing.
type mockCtx struct {
	cacheSize             int
	cacheCapSet           int
	maxPagesSet           int
	pragmaInts            map[string]int64
	pragmaStrs            map[string]string
	storageMetrics        pragma.StorageMetrics
	compressionName       string
	journalMode           string
	walSize               int64
	autoCheckpointN       int
	autoCheckpointSet     int
	checkpointPassiveCalled bool
	isolationLevel        string
	busyTimeout           int
	cacheCleared          bool
	analyzeErr            error
	integrityErrors       []string
	dbPath                string
	hasAnyTables          bool
	queryTimeoutMs        int64
	maxMemoryBytes        int64
	queryCacheMax         int
}

func newMock() *mockCtx {
	return &mockCtx{
		pragmaInts:  make(map[string]int64),
		pragmaStrs:  make(map[string]string),
		journalMode: "delete",
		dbPath:      ":memory:",
	}
}

func (m *mockCtx) CacheSize() int                       { return m.cacheSize }
func (m *mockCtx) SetCacheCapacity(n int)               { m.cacheCapSet = n }
func (m *mockCtx) SetPageManagerMaxPages(n int)         { m.maxPagesSet = n }
func (m *mockCtx) GetPragmaInt(name string, def int64) int64 {
	if v, ok := m.pragmaInts[name]; ok {
		return v
	}
	return def
}
func (m *mockCtx) SetPragmaInt(name string, val int64)  { m.pragmaInts[name] = val }
func (m *mockCtx) GetPragmaStr(name string, def string) string {
	if v, ok := m.pragmaStrs[name]; ok {
		return v
	}
	return def
}
func (m *mockCtx) SetPragmaStr(name string, val string) { m.pragmaStrs[name] = val }
func (m *mockCtx) StorageMetrics() pragma.StorageMetrics { return m.storageMetrics }
func (m *mockCtx) GetCompressionName() string           { return m.compressionName }
func (m *mockCtx) SetCompressionName(name string) error { m.compressionName = name; return nil }
func (m *mockCtx) GetJournalMode() string               { return m.journalMode }
func (m *mockCtx) OpenWALMode(_ string, _ int) error    { m.journalMode = "wal"; return nil }
func (m *mockCtx) CloseWALMode()                        { m.journalMode = "delete" }
func (m *mockCtx) CheckpointPassive() (int, error)      { m.checkpointPassiveCalled = true; return 0, nil }
func (m *mockCtx) CheckpointFull() (int, int, int, error) { return 0, 0, 0, nil }
func (m *mockCtx) CheckpointTruncate() (int, int, int, error) { return 0, 0, 0, nil }
func (m *mockCtx) GetWALSize() int64                    { return m.walSize }
func (m *mockCtx) GetAutoCheckpointN() int              { return m.autoCheckpointN }
func (m *mockCtx) StartAutoCheckpoint(n int)            { m.autoCheckpointSet = n }
func (m *mockCtx) GetIsolationLevel() string            { return m.isolationLevel }
func (m *mockCtx) SetIsolationLevel(l string) error     { m.isolationLevel = l; return nil }
func (m *mockCtx) GetBusyTimeout() int                  { return m.busyTimeout }
func (m *mockCtx) SetBusyTimeout(ms int)                { m.busyTimeout = ms }
func (m *mockCtx) ClearCaches()                         { m.cacheCleared = true }
func (m *mockCtx) RunAnalyze() error                    { return m.analyzeErr }
func (m *mockCtx) CheckDBIntegrity() ([]string, error)  { return m.integrityErrors, nil }
func (m *mockCtx) GetDBPath() string                    { return m.dbPath }
func (m *mockCtx) HasAnyTables() bool                   { return m.hasAnyTables }
func (m *mockCtx) GetQueryTimeoutMs() int64             { return m.queryTimeoutMs }
func (m *mockCtx) SetQueryTimeoutMs(ms int64)           { m.queryTimeoutMs = ms }
func (m *mockCtx) GetMaxMemoryBytes() int64             { return m.maxMemoryBytes }
func (m *mockCtx) SetMaxMemoryBytes(b int64)            { m.maxMemoryBytes = b }
func (m *mockCtx) GetQueryCacheMax() int                { return m.queryCacheMax }
func (m *mockCtx) SetQueryCacheMax(n int)               { m.queryCacheMax = n }
func (m *mockCtx) GetPlanCacheEnabled() bool            { return m.pragmaInts["cache_plan"] != 0 }
func (m *mockCtx) SetPlanCacheEnabled(enabled bool)     {
	if enabled {
		m.pragmaInts["cache_plan"] = 1
	} else {
		m.pragmaInts["cache_plan"] = 0
	}
}
func (m *mockCtx) GetStmtCacheLen() int                 { return 0 }

func TestHandleCacheSize_Read(t *testing.T) {
	ctx := newMock()
	ctx.cacheSize = 100
	stmt := &QP.PragmaStmt{Name: "cache_size"}
	cols, rows, err := pragma.HandleCacheSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0] != "cache_size" {
		t.Errorf("unexpected cols: %v", cols)
	}
	if rows[0][0] != int64(100) {
		t.Errorf("expected 100, got %v", rows[0][0])
	}
}

func TestHandleCacheSize_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "cache_size", Value: &QP.Literal{Value: int64(200)}}
	_, _, err := pragma.HandleCacheSize(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.cacheCapSet != 200 {
		t.Errorf("expected capacity 200, got %d", ctx.cacheCapSet)
	}
}

func TestHandleCacheMemory_Set(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "cache_memory", Value: &QP.Literal{Value: int64(8192)}}
	cols, rows, err := pragma.HandleCacheMemory(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) == 0 || rows[0][0] != int64(8192) {
		t.Errorf("unexpected result: %v %v", cols, rows)
	}
	if ctx.pragmaInts["cache_memory"] != 8192 {
		t.Errorf("expected cache_memory=8192, got %d", ctx.pragmaInts["cache_memory"])
	}
	if ctx.maxPagesSet != 2 { // 8192/4096 = 2
		t.Errorf("expected maxPages=2, got %d", ctx.maxPagesSet)
	}
}

func TestHandleCacheMemory_Read(t *testing.T) {
	ctx := newMock()
	ctx.pragmaInts["cache_memory"] = int64(4096)
	stmt := &QP.PragmaStmt{Name: "cache_memory"}
	cols, rows, err := pragma.HandleCacheMemory(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != int64(4096) {
		t.Errorf("expected 4096, got %v", rows[0][0])
	}
	_ = cols
}

func TestHandleCacheSpill(t *testing.T) {
	ctx := newMock()
	stmt := &QP.PragmaStmt{Name: "cache_spill", Value: &QP.Literal{Value: int64(0)}}
	_, _, err := pragma.HandleCacheSpill(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.pragmaInts["cache_spill"] != 0 {
		t.Errorf("expected 0, got %d", ctx.pragmaInts["cache_spill"])
	}
}

func TestHandleCacheGrind(t *testing.T) {
	ctx := newMock()
	ctx.storageMetrics = pragma.StorageMetrics{UsedPages: 5, FreePages: 3}
	cols, rows, err := pragma.HandleCacheGrind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 {
		t.Errorf("expected 4 cols, got %d", len(cols))
	}
	_ = rows
}

func TestHandleCachePlan_Read(t *testing.T) {
ctx := newMock()
ctx.pragmaInts["cache_plan"] = 1 // start enabled
stmt := &QP.PragmaStmt{Name: "cache_plan"}
cols, rows, err := pragma.HandleCachePlan(ctx, stmt)
if err != nil {
t.Fatal(err)
}
if len(cols) != 1 || cols[0] != "cache_plan" {
t.Errorf("unexpected cols: %v", cols)
}
if rows[0][0] != int64(1) {
t.Errorf("expected 1 (enabled), got %v", rows[0][0])
}
}

func TestHandleCachePlan_Disable(t *testing.T) {
ctx := newMock()
ctx.pragmaInts["cache_plan"] = 1
lit := &QP.Literal{Value: int64(0)}
stmt := &QP.PragmaStmt{Name: "cache_plan", Value: lit}
_, _, err := pragma.HandleCachePlan(ctx, stmt)
if err != nil {
t.Fatal(err)
}
if ctx.pragmaInts["cache_plan"] != 0 {
t.Errorf("expected cache_plan=0 after disabling")
}
}

func TestHandleCachePlan_Enable(t *testing.T) {
ctx := newMock()
ctx.pragmaInts["cache_plan"] = 0
lit := &QP.Literal{Value: int64(1)}
stmt := &QP.PragmaStmt{Name: "cache_plan", Value: lit}
cols, rows, err := pragma.HandleCachePlan(ctx, stmt)
if err != nil {
t.Fatal(err)
}
_ = cols
if rows[0][0] != int64(1) {
t.Errorf("expected 1 after enabling, got %v", rows[0][0])
}
}
