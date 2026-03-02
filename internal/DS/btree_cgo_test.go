package DS

import (
	"testing"
)

// TestCBTree_Basic tests basic B-Tree operations via the C++ CGO wrapper.
func TestCBTree_Basic(t *testing.T) {
	pm := setupTestPageManager(t, 4096)

	// Allocate a root page
	rootPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	// Initialize page as table leaf
	page, err := pm.ReadPage(rootPage)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	page.Data[0] = 0x0d // table leaf
	page.Data[5] = byte(len(page.Data) >> 8)
	page.Data[6] = byte(len(page.Data))
	if err := pm.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	bt := NewCBTree(pm, rootPage, true)
	if bt == nil {
		t.Fatal("NewCBTree returned nil")
	}

	if bt.Depth() == 0 {
		t.Error("Depth should be > 0 for initialized tree")
	}
}

// TestCBTree_NilPageManager tests that NewCBTree panics on nil pm.
func TestCBTree_NilPageManager(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil PageManager")
		}
	}()
	_ = NewCBTree(nil, 1, true)
}

// TestCBTree_BinarySearchPage tests the page binary search wrapper.
func TestCBTree_BinarySearchPage(t *testing.T) {
	// Empty page data should return -1
	result := BinarySearchPage(nil, []byte{1}, true)
	if result != -1 {
		t.Errorf("expected -1 for nil page, got %d", result)
	}

	result = BinarySearchPage([]byte{0x0d, 0, 0, 0, 0, 0, 0, 0}, []byte{}, true)
	if result != -1 {
		t.Errorf("expected -1 for empty key, got %d", result)
	}
}

