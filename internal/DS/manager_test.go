package DS

import (
	"path/filepath"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/PB"
)

func TestPageManagerCreate(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	if pm.PageSize() != 4096 {
		t.Errorf("expected page size 4096, got %d", pm.PageSize())
	}
	if pm.NumPages() != 1 {
		t.Errorf("expected 1 page, got %d", pm.NumPages())
	}
}

func TestPageManagerReadWritePage(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	page, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	if page != 2 {
		t.Errorf("expected page 2, got %d", page)
	}

	data := []byte("hello world")
	pm.WritePage(&Page{Num: page, Data: data, Type: PageTypeLeafTbl})

	file2, err := PB.OpenFile(testPath, PB.O_RDONLY)
	if err != nil {
		t.Fatalf("failed to reopen file: %v", err)
	}
	defer file2.Close()

	pm2, err := NewPageManager(file2, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	readPage, err := pm2.ReadPage(page)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	if string(readPage.Data[:len(data)]) != string(data) {
		t.Errorf("expected %s, got %s", string(data), string(readPage.Data[:len(data)]))
	}
}

func TestPageManagerFreeList(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	page1, _ := pm.AllocatePage()
	page2, _ := pm.AllocatePage()

	pm.FreePage(page1)
	pm.FreePage(page2)

	page3, _ := pm.AllocatePage()
	if page3 != page2 {
		t.Errorf("expected page %d from free list, got %d", page2, page3)
	}
}

func TestPageManagerHeader(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	header := pm.Header()
	if header.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", header.PageSize)
	}
	if header.TextEncoding != 1 {
		t.Errorf("expected UTF-8 encoding, got %d", header.TextEncoding)
	}
}

func TestPageManagerSync(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	if err := pm.Sync(); err != nil {
		t.Fatalf("failed to sync: %v", err)
	}
}
