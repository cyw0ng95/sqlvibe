package DS

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCPageManager_Create tests C++ PageManager creation.
func TestCPageManager_Create(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	if pm.PageSize() != 4096 {
		t.Errorf("expected page size 4096, got %d", pm.PageSize())
	}
	if pm.NumPages() != 1 {
		t.Errorf("expected 1 page, got %d", pm.NumPages())
	}
}

// TestCPageManager_ReadWritePage tests reading and writing pages.
func TestCPageManager_ReadWritePage(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate a new page
	pageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	if pageNum != 2 {
		t.Errorf("expected page 2, got %d", pageNum)
	}

	// Write data to the page
	page := NewPage(pageNum, 4096)
	data := []byte("hello world from C++ PageManager")
	page.Data[0] = byte(PageTypeLeafTbl) // Set page type in first byte
	copy(page.Data[1:], data)
	page.Type = PageTypeLeafTbl

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Sync to ensure data is flushed
	if err := pm.Sync(); err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Close and reopen to verify persistence
	pm.Close()

	pm2, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to reopen C++ PageManager: %v", err)
	}
	defer pm2.Close()

	// Read the page back
	readPage, err := pm2.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	// Verify data
	if readPage.Type != PageTypeLeafTbl {
		t.Errorf("expected page type %d, got %d", PageTypeLeafTbl, readPage.Type)
	}
	if string(readPage.Data[1:1+len(data)]) != string(data) {
		t.Errorf("expected %s, got %s", string(data), string(readPage.Data[1:1+len(data)]))
	}
}

// TestCPageManager_FreePage tests freeing pages.
func TestCPageManager_FreePage(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate and free a page
	pageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	if err := pm.FreePage(pageNum); err != nil {
		t.Fatalf("failed to free page: %v", err)
	}
}

// TestCPageManager_Header tests header operations.
func TestCPageManager_Header(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	header, err := pm.Header()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if header.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", header.PageSize)
	}
	if header.TextEncoding != 1 {
		t.Errorf("expected UTF-8 encoding (1), got %d", header.TextEncoding)
	}
}

// TestCPageManager_MultiplePages tests allocating multiple pages.
func TestCPageManager_MultiplePages(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate multiple pages
	var pageNums []uint32
	for i := 0; i < 10; i++ {
		pageNum, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		pageNums = append(pageNums, pageNum)
	}

	if pm.NumPages() != 11 { // 1 header + 10 allocated
		t.Errorf("expected 11 pages, got %d", pm.NumPages())
	}

	// Write to all pages
	for i, pageNum := range pageNums {
		page := NewPage(pageNum, 4096)
		page.Data[0] = byte(PageTypeLeafTbl)
		page.Type = PageTypeLeafTbl
		page.Data[1] = byte(i)
		if err := pm.WritePage(page); err != nil {
			t.Fatalf("failed to write page %d: %v", pageNum, err)
		}
	}

	// Verify all pages
	for i, pageNum := range pageNums {
		readPage, err := pm.ReadPage(pageNum)
		if err != nil {
			t.Fatalf("failed to read page %d: %v", pageNum, err)
		}
		if readPage.Data[1] != byte(i) {
			t.Errorf("page %d: expected data %d, got %d", pageNum, i, readPage.Data[1])
		}
	}
}

// TestCPageManager_InvalidPageSize tests validation of page sizes.
func TestCPageManager_InvalidPageSize(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	// Invalid page sizes should fail
	invalidSizes := []uint32{0, 256, 511, 513, 1000, 65537, 131072}
	for _, sz := range invalidSizes {
		_, err := NewCPageManager(testPath, sz, 0)
		if err == nil {
			t.Errorf("expected error for invalid page size %d, got nil", sz)
		}
	}
}

// TestCPageManager_Reopen tests reopening an existing database.
func TestCPageManager_Reopen(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	// Create and write
	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}

	pageNum, _ := pm.AllocatePage()
	page := NewPage(pageNum, 4096)
	page.Data[0] = byte(PageTypeLeafTbl)
	page.Type = PageTypeLeafTbl
	copy(page.Data[1:], "persistent data")
	pm.WritePage(page)
	pm.Sync()
	pm.Close()

	// Reopen and verify
	pm2, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pm2.Close()

	if pm2.NumPages() != 2 {
		t.Errorf("expected 2 pages after reopen, got %d", pm2.NumPages())
	}

	readPage, err := pm2.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}
	if string(readPage.Data[1:16]) != "persistent data" {
		t.Errorf("data not persisted")
	}
}

// TestCPageManager_ConcurrentAccess tests that the C++ PageManager handles concurrent access.
func TestCPageManager_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test_cgo.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		t.Fatalf("failed to create C++ PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate pages concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			pageNum, err := pm.AllocatePage()
			if err != nil {
				t.Errorf("failed to allocate page: %v", err)
				return
			}
			page := NewPage(pageNum, 4096)
			page.Data[0] = byte(PageTypeLeafTbl)
			page.Type = PageTypeLeafTbl
			if err := pm.WritePage(page); err != nil {
				t.Errorf("failed to write page: %v", err)
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify page count
	expectedPages := uint32(11) // 1 header + 10 allocated
	if pm.NumPages() != expectedPages {
		t.Errorf("expected %d pages, got %d", expectedPages, pm.NumPages())
	}
}

// BenchmarkCPageManager_AllocatePage benchmarks page allocation.
func BenchmarkCPageManager_AllocatePage(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench_cgo_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	testPath := filepath.Join(tmpDir, "bench.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer pm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.AllocatePage()
	}
}

// BenchmarkCPageManager_ReadWritePage benchmarks page read/write.
func BenchmarkCPageManager_ReadWritePage(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench_cgo_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	testPath := filepath.Join(tmpDir, "bench.db")

	pm, err := NewCPageManager(testPath, 4096, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer pm.Close()

	pageNum, _ := pm.AllocatePage()
	page := NewPage(pageNum, 4096)
	page.Data[0] = byte(PageTypeLeafTbl)
	page.Type = PageTypeLeafTbl

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.WritePage(page)
		pm.ReadPage(pageNum)
	}
}
