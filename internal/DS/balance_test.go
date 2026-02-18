package DS

import (
	"encoding/binary"
	"testing"
)

func TestPageBalancer_IsPageOverfull(t *testing.T) {
	pm := setupTestPageManager(t, 1024)
	pb := NewPageBalancer(pm)

	// Create a test page
	pageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	page, err := pm.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	// Initialize as table leaf page
	page.Data[0] = 0x0d // table leaf
	binary.BigEndian.PutUint16(page.Data[3:5], 0) // 0 cells
	binary.BigEndian.PutUint16(page.Data[5:7], 1024) // content at end

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Empty page should not be overfull
	overfull, err := pb.IsPageOverfull(pageNum)
	if err != nil {
		t.Fatalf("IsPageOverfull failed: %v", err)
	}
	if overfull {
		t.Error("Empty page should not be overfull")
	}

	// Simulate a page that's 95% full
	binary.BigEndian.PutUint16(page.Data[3:5], 100) // 100 cells
	binary.BigEndian.PutUint16(page.Data[5:7], 100) // content starts at byte 100

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	overfull, err = pb.IsPageOverfull(pageNum)
	if err != nil {
		t.Fatalf("IsPageOverfull failed: %v", err)
	}
	if !overfull {
		t.Error("Nearly full page should be overfull")
	}
}

func TestPageBalancer_IsPageUnderfull(t *testing.T) {
	pm := setupTestPageManager(t, 1024)
	pb := NewPageBalancer(pm)

	pageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	page, err := pm.ReadPage(pageNum)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	// Initialize as table leaf page with minimal data
	page.Data[0] = 0x0d
	binary.BigEndian.PutUint16(page.Data[3:5], 2) // 2 cells
	binary.BigEndian.PutUint16(page.Data[5:7], 1000) // content at byte 1000 (very little data)

	if err := pm.WritePage(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	underfull, err := pb.IsPageUnderfull(pageNum)
	if err != nil {
		t.Fatalf("IsPageUnderfull failed: %v", err)
	}
	if !underfull {
		t.Error("Sparse page should be underfull")
	}
}

func TestPageBalancer_CompareKeys(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected int
	}{
		{
			name:     "equal",
			a:        []byte{1, 2, 3},
			b:        []byte{1, 2, 3},
			expected: 0,
		},
		{
			name:     "less than",
			a:        []byte{1, 2, 3},
			b:        []byte{1, 2, 4},
			expected: -1,
		},
		{
			name:     "greater than",
			a:        []byte{1, 2, 4},
			b:        []byte{1, 2, 3},
			expected: 1,
		},
		{
			name:     "different length - shorter less",
			a:        []byte{1, 2},
			b:        []byte{1, 2, 3},
			expected: -1,
		},
		{
			name:     "different length - longer greater",
			a:        []byte{1, 2, 3},
			b:        []byte{1, 2},
			expected: 1,
		},
		{
			name:     "empty keys",
			a:        []byte{},
			b:        []byte{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareKeys(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareKeys(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestPageBalancer_MergePages_Simple(t *testing.T) {
	pm := setupTestPageManager(t, 4096) // Larger page for easier testing
	pb := NewPageBalancer(pm)

	// Allocate two pages
	leftPageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate left page: %v", err)
	}

	rightPageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate right page: %v", err)
	}

	// Initialize both as empty table leaf pages
	leftPage, _ := pm.ReadPage(leftPageNum)
	leftPage.Data[0] = 0x0d // table leaf
	binary.BigEndian.PutUint16(leftPage.Data[3:5], 0) // 0 cells
	binary.BigEndian.PutUint16(leftPage.Data[5:7], 4096) // content at end
	pm.WritePage(leftPage)

	rightPage, _ := pm.ReadPage(rightPageNum)
	rightPage.Data[0] = 0x0d // table leaf
	binary.BigEndian.PutUint16(rightPage.Data[3:5], 0) // 0 cells
	binary.BigEndian.PutUint16(rightPage.Data[5:7], 4096) // content at end
	pm.WritePage(rightPage)

	// Merge should succeed for empty pages
	merged, err := pb.MergePages(leftPageNum, rightPageNum)
	if err != nil {
		t.Fatalf("MergePages failed: %v", err)
	}
	if !merged {
		t.Error("Expected successful merge of empty pages")
	}
}

func TestPageBalancer_RedistributeCells_Balanced(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	pb := NewPageBalancer(pm)

	// Allocate two pages
	leftPageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate left page: %v", err)
	}

	rightPageNum, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate right page: %v", err)
	}

	// Initialize both as table leaf pages with equal cells
	leftPage, _ := pm.ReadPage(leftPageNum)
	leftPage.Data[0] = 0x0d
	binary.BigEndian.PutUint16(leftPage.Data[3:5], 5) // 5 cells
	binary.BigEndian.PutUint16(leftPage.Data[5:7], 4000)
	pm.WritePage(leftPage)

	rightPage, _ := pm.ReadPage(rightPageNum)
	rightPage.Data[0] = 0x0d
	binary.BigEndian.PutUint16(rightPage.Data[3:5], 5) // 5 cells
	binary.BigEndian.PutUint16(rightPage.Data[5:7], 4000)
	pm.WritePage(rightPage)

	// Redistribution of balanced pages should be no-op
	err = pb.RedistributeCells(leftPageNum, rightPageNum)
	if err != nil {
		t.Fatalf("RedistributeCells failed: %v", err)
	}

	// Verify pages still have 5 cells each
	leftPage, _ = pm.ReadPage(leftPageNum)
	rightPage, _ = pm.ReadPage(rightPageNum)

	leftCells := binary.BigEndian.Uint16(leftPage.Data[3:5])
	rightCells := binary.BigEndian.Uint16(rightPage.Data[3:5])

	if leftCells != 5 || rightCells != 5 {
		t.Errorf("Expected 5 cells in each page, got left=%d, right=%d", leftCells, rightCells)
	}
}
