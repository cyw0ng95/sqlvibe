package DS

import (
	"bytes"
	"fmt"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// PageBalancer handles BTree page balancing operations.
// Heavy byte-level operations (split/merge/redistribute/overfull checks) are
// delegated to the C++ layer via balance_cgo.go (Boundary-CGO: inner ops stay in C++).
type PageBalancer struct {
	pm *PageManager
	om *OverflowManager
}

// NewPageBalancer creates a new page balancer
func NewPageBalancer(pm *PageManager) *PageBalancer {
	return &PageBalancer{
		pm: pm,
		om: NewOverflowManager(pm),
	}
}

// SplitLeafPage splits an overfull leaf page into two pages.
// Returns the new right page number and the divider key.
// Delegates the actual cell rearrangement to the C++ balance layer.
func (pb *PageBalancer) SplitLeafPage(pageNum uint32) (rightPage uint32, dividerKey []byte, err error) {
	util.Assert(pageNum > 0, "page number cannot be zero")
	util.AssertNotNil(pb.pm, "PageManager")
	util.AssertNotNil(pb.om, "OverflowManager")

	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read page: %w", err)
	}

	// Validate page type
	pageType := page.Data[0]
	util.Assert(pageType == 0x0d || pageType == 0x02, "invalid page type for leaf split: 0x%02x", pageType)

	// Let C++ determine the optimal split point (Boundary-CGO)
	splitPoint := CBalanceCalculateSplitPoint(page.Data)
	if splitPoint < 0 {
		return 0, nil, fmt.Errorf("failed to calculate split point (too few cells)")
	}

	// Allocate new right page
	rightPage, err = pb.pm.AllocatePage()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate right page: %w", err)
	}

	rightPageData, err := pb.pm.ReadPage(rightPage)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read right page: %w", err)
	}

	// Copy the type byte so C++ knows what page type to initialise
	rightPageData.Data[0] = pageType

	// Perform the split via C++ (Boundary-CGO: all byte-level cell ops in C++)
	key, ok := CBalanceSplitLeaf(page.Data, rightPageData.Data, splitPoint)
	if !ok {
		_ = pb.pm.FreePage(rightPage)
		return 0, nil, fmt.Errorf("C++ split leaf failed")
	}
	dividerKey = key

	if err := pb.pm.WritePage(page); err != nil {
		return 0, nil, fmt.Errorf("failed to write left page: %w", err)
	}
	if err := pb.pm.WritePage(rightPageData); err != nil {
		return 0, nil, fmt.Errorf("failed to write right page: %w", err)
	}

	return rightPage, dividerKey, nil
}

// RedistributeCells moves cells from one page to a sibling to balance them.
// Delegates the byte-level cell moves to the C++ balance layer.
func (pb *PageBalancer) RedistributeCells(leftPageNum, rightPageNum uint32) error {
	util.Assert(leftPageNum > 0, "left page number cannot be zero")
	util.Assert(rightPageNum > 0, "right page number cannot be zero")
	util.AssertNotNil(pb.pm, "PageManager")

	leftPage, err := pb.pm.ReadPage(leftPageNum)
	if err != nil {
		return fmt.Errorf("failed to read left page: %w", err)
	}

	rightPage, err := pb.pm.ReadPage(rightPageNum)
	if err != nil {
		return fmt.Errorf("failed to read right page: %w", err)
	}

	// Use C++ to redistribute cells between pages (Boundary-CGO).
	// Compute actual cell counts to determine move direction and count.
	leftCells := int(CPageGetNumCells(leftPage.Data))
	rightCells := int(CPageGetNumCells(rightPage.Data))
	totalCells := leftCells + rightCells
	if totalCells < 2 {
		return fmt.Errorf("not enough cells to redistribute: %d", totalCells)
	}
	targetLeft := totalCells / 2
	diff := targetLeft - leftCells
	if diff == 0 {
		// Already balanced — no-op.
		return nil
	}
	if diff > 0 {
		// Move cells from right → left via C++ (Boundary-CGO)
		if !CBalanceRedistribute(rightPage.Data, leftPage.Data, diff) {
			return fmt.Errorf("C++ redistribute cells (right→left) failed")
		}
	} else {
		// Move cells from left → right via C++ (Boundary-CGO)
		if !CBalanceRedistribute(leftPage.Data, rightPage.Data, -diff) {
			return fmt.Errorf("C++ redistribute cells (left→right) failed")
		}
	}

	if err := pb.pm.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write left page: %w", err)
	}
	if err := pb.pm.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right page: %w", err)
	}

	return nil
}

// MergePages merges two sibling pages into the left page.
// Returns true if merge was successful, false if pages don't fit together.
// Delegates byte-level cell merging to the C++ balance layer.
func (pb *PageBalancer) MergePages(leftPageNum, rightPageNum uint32) (bool, error) {
	util.Assert(leftPageNum > 0, "left page number cannot be zero")
	util.Assert(rightPageNum > 0, "right page number cannot be zero")
	util.AssertNotNil(pb.pm, "PageManager")

	leftPage, err := pb.pm.ReadPage(leftPageNum)
	if err != nil {
		return false, fmt.Errorf("failed to read left page: %w", err)
	}

	rightPage, err := pb.pm.ReadPage(rightPageNum)
	if err != nil {
		return false, fmt.Errorf("failed to read right page: %w", err)
	}

	// Use C++ to merge pages (Boundary-CGO: all byte-level cell ops in C++)
	if !CBalanceMergePages(leftPage.Data, rightPage.Data) {
		// Pages don't fit together
		return false, nil
	}

	if err := pb.pm.WritePage(leftPage); err != nil {
		return false, fmt.Errorf("failed to write merged page: %w", err)
	}

	// Free right page
	if err := pb.pm.FreePage(rightPageNum); err != nil {
		return false, fmt.Errorf("failed to free right page: %w", err)
	}

	return true, nil
}

// IsPageOverfull checks if a page needs to be split.
// Delegates the threshold check to the C++ balance layer.
func (pb *PageBalancer) IsPageOverfull(pageNum uint32) (bool, error) {
	util.Assert(pageNum > 0, "page number cannot be zero")
	util.AssertNotNil(pb.pm, "PageManager")

	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return false, err
	}

	// Use C++ to check overfull (Boundary-CGO)
	return CBalanceIsOverfull(page.Data), nil
}

// IsPageUnderfull checks if a page should be merged or redistributed.
// Delegates the threshold check to the C++ balance layer.
func (pb *PageBalancer) IsPageUnderfull(pageNum uint32) (bool, error) {
	util.Assert(pageNum > 0, "page number cannot be zero")
	util.AssertNotNil(pb.pm, "PageManager")

	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return false, err
	}

	// Use C++ to check underfull (Boundary-CGO)
	return CBalanceIsUnderfull(page.Data), nil
}

// CompareKeys compares two keys for ordering.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

