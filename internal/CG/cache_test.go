package CG_test

import (
	"testing"
	"time"

	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
)

func TestStmtCache_PutAndGet(t *testing.T) {
	sc := CG.NewStmtCache(10)
	sql := "SELECT 1"
	stmt := &CG.ParsedStatement{SQL: sql, ParsedAt: time.Now()}
	sc.Put(sql, stmt)
	got, ok := sc.Get(sql)
	if !ok {
		t.Fatal("expected to find cached statement")
	}
	if got.SQL != sql {
		t.Errorf("expected SQL %q, got %q", sql, got.SQL)
	}
}

func TestStmtCache_Miss(t *testing.T) {
	sc := CG.NewStmtCache(10)
	_, ok := sc.Get("SELECT 99")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestStmtCache_Invalidate(t *testing.T) {
	sc := CG.NewStmtCache(10)
	sql := "SELECT 1"
	sc.Put(sql, &CG.ParsedStatement{SQL: sql, ParsedAt: time.Now()})
	sc.Invalidate()
	_, ok := sc.Get(sql)
	if ok {
		t.Error("expected cache miss after Invalidate")
	}
}

func TestStmtCache_Len(t *testing.T) {
	sc := CG.NewStmtCache(10)
	if sc.Len() != 0 {
		t.Errorf("expected 0 entries initially, got %d", sc.Len())
	}
	sc.Put("SELECT 1", &CG.ParsedStatement{SQL: "SELECT 1", ParsedAt: time.Now()})
	sc.Put("SELECT 2", &CG.ParsedStatement{SQL: "SELECT 2", ParsedAt: time.Now()})
	if sc.Len() != 2 {
		t.Errorf("expected 2 entries, got %d", sc.Len())
	}
}

func TestStmtCache_LRUEviction(t *testing.T) {
	sc := CG.NewStmtCache(2)
	sc.Put("A", &CG.ParsedStatement{SQL: "A", ParsedAt: time.Now()})
	time.Sleep(time.Millisecond) // ensure different timestamps
	sc.Put("B", &CG.ParsedStatement{SQL: "B", ParsedAt: time.Now()})
	time.Sleep(time.Millisecond)
	// Adding C should evict the oldest (A)
	sc.Put("C", &CG.ParsedStatement{SQL: "C", ParsedAt: time.Now()})
	if sc.Len() != 2 {
		t.Errorf("expected 2 entries after eviction, got %d", sc.Len())
	}
	if _, ok := sc.Get("A"); ok {
		t.Error("expected A to be evicted")
	}
	if _, ok := sc.Get("C"); !ok {
		t.Error("expected C to be present")
	}
}

func TestPlanCache_GetPut(t *testing.T) {
	pc := CG.NewPlanCache(10)
	prog := CG.MustCompile("SELECT 1")
	pc.Put("SELECT 1", prog)
	got, ok := pc.Get("SELECT 1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got != prog {
		t.Error("expected same program pointer")
	}
}

func TestPlanCache_Invalidate(t *testing.T) {
	pc := CG.NewPlanCache(10)
	prog := CG.MustCompile("SELECT 1")
	pc.Put("SELECT 1", prog)
	pc.Invalidate()
	_, ok := pc.Get("SELECT 1")
	if ok {
		t.Error("expected cache miss after Invalidate")
	}
}
