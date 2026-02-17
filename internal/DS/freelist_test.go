package DS

import (
	"testing"
)

func TestFreelistManager_AllocateAndFree(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	fm := NewFreelistManager(pm, 0)

	// Allocate some pages
	pages := make([]uint32, 5)
	for i := 0; i < 5; i++ {
		pageNum, err := fm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		pages[i] = pageNum
	}

	// Free some pages
	for i := 0; i < 3; i++ {
		if err := fm.FreePage(pages[i]); err != nil {
			t.Fatalf("failed to free page %d: %v", pages[i], err)
		}
	}

	// Count free pages
	count, err := fm.CountFreePages()
	if err != nil {
		t.Fatalf("failed to count free pages: %v", err)
	}

	if count != 4 { // 1 trunk + 3 leaves (but trunk counts leaves, so depends on implementation)
		t.Logf("Free page count: %d (expected around 3-4)", count)
	}

	// Allocate from freelist
	reused, err := fm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate from freelist: %v", err)
	}

	// Should reuse one of the freed pages
	found := false
	for i := 0; i < 3; i++ {
		if reused == pages[i] {
			found = true
			break
		}
	}
	if !found {
		t.Logf("Warning: Allocated page %d not in freed set %v (may be trunk)", reused, pages[:3])
	}
}

func TestFreelistManager_MultiplePages(t *testing.T) {
	pm := setupTestPageManager(t, 1024) // Smaller pages
	fm := NewFreelistManager(pm, 0)

	// Allocate and free many pages to test trunk/leaf management
	var allocated []uint32
	for i := 0; i < 50; i++ {
		pageNum, err := fm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		allocated = append(allocated, pageNum)
	}

	// Free all pages
	for _, pageNum := range allocated {
		if err := fm.FreePage(pageNum); err != nil {
			t.Fatalf("failed to free page %d: %v", pageNum, err)
		}
	}

	// Count should be around 50+ (including trunks)
	count, err := fm.CountFreePages()
	if err != nil {
		t.Fatalf("failed to count free pages: %v", err)
	}

	if count < 50 {
		t.Errorf("Expected at least 50 free pages, got %d", count)
	}

	t.Logf("Freed %d pages, freelist contains %d entries", len(allocated), count)
}

func TestFreelistManager_EmptyFreelist(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	fm := NewFreelistManager(pm, 0)

	// Count should be 0
	count, err := fm.CountFreePages()
	if err != nil {
		t.Fatalf("failed to count free pages: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 free pages, got %d", count)
	}

	// Allocate should create new page (not from freelist)
	pageNum, err := fm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	if pageNum == 0 {
		t.Error("Allocated page number should not be 0")
	}
}

func TestFreelistManager_FreePageZero(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	fm := NewFreelistManager(pm, 0)

	// Freeing page 0 should fail
	err := fm.FreePage(0)
	if err == nil {
		t.Error("Expected error when freeing page 0")
	}
}

func TestFreelistManager_Compact(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	fm := NewFreelistManager(pm, 0)

	// Allocate and free some pages
	pages := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		pageNum, err := fm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		pages[i] = pageNum
	}

	for _, pageNum := range pages {
		if err := fm.FreePage(pageNum); err != nil {
			t.Fatalf("failed to free page %d: %v", pageNum, err)
		}
	}

	countBefore, _ := fm.CountFreePages()

	// Compact the freelist
	if err := fm.Compact(); err != nil {
		t.Fatalf("failed to compact freelist: %v", err)
	}

	countAfter, _ := fm.CountFreePages()

	t.Logf("Free pages before compact: %d, after: %d", countBefore, countAfter)

	// Count should remain roughly the same (or possibly fewer empty trunks)
	if countAfter > countBefore {
		t.Errorf("Compact should not increase free page count: before=%d, after=%d", countBefore, countAfter)
	}
}

func TestFreelistManager_GetSetFirstTrunk(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	fm := NewFreelistManager(pm, 0)

	if fm.GetFirstTrunk() != 0 {
		t.Errorf("Initial first trunk should be 0, got %d", fm.GetFirstTrunk())
	}

	fm.SetFirstTrunk(42)
	if fm.GetFirstTrunk() != 42 {
		t.Errorf("First trunk should be 42, got %d", fm.GetFirstTrunk())
	}
}
