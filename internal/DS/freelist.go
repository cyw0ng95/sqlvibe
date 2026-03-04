package DS

import (
	"fmt"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// Freelist management for SQLite-style page allocation
// Freelist uses trunk pages that point to leaf pages.
// Low-level byte operations on trunk pages are delegated to the C++ layer
// via freelist_cgo.go (Boundary-CGO: inner byte-level ops stay in C++).

const (
	FreelistTrunkHeaderSize = 8
)

// FreelistManager handles free page management
type FreelistManager struct {
	pm         PageManagerInterface
	firstTrunk uint32 // First trunk page number
}

// NewFreelistManager creates a new freelist manager
func NewFreelistManager(pm PageManagerInterface, firstTrunk uint32) *FreelistManager {
	util.AssertNotNil(pm, "PageManager")
	return &FreelistManager{
		pm:         pm,
		firstTrunk: firstTrunk,
	}
}

// AllocatePage allocates a page from the freelist or creates a new one
func (fm *FreelistManager) AllocatePage() (uint32, error) {
	if fm.firstTrunk == 0 {
		// No freelist, allocate new page
		return fm.pm.AllocatePage()
	}

	// Read trunk page
	trunkPage, err := fm.pm.ReadPage(fm.firstTrunk)
	if err != nil {
		return 0, fmt.Errorf("failed to read trunk page: %w", err)
	}

	// Use C++ to parse trunk header (Boundary-CGO: delegate byte ops to C++)
	nextTrunk, numLeaves, ok := CFreelistParseTrunk(trunkPage.Data)
	if !ok {
		return 0, fmt.Errorf("failed to parse trunk page")
	}

	if numLeaves == 0 {
		// Trunk page has no leaves, use the trunk itself
		pageNum := fm.firstTrunk
		fm.firstTrunk = nextTrunk

		// Zero out the page before returning
		for i := range trunkPage.Data {
			trunkPage.Data[i] = 0
		}
		if err := fm.pm.WritePage(trunkPage); err != nil {
			return 0, fmt.Errorf("failed to clear trunk page: %w", err)
		}

		return pageNum, nil
	}

	// Pop the last leaf entry via C++ getter
	leafPage := CFreelistGetEntry(trunkPage.Data, numLeaves-1)
	if !CFreelistWriteTrunk(trunkPage.Data, nextTrunk, numLeaves-1) {
		return 0, fmt.Errorf("failed to update trunk count")
	}

	if err := fm.pm.WritePage(trunkPage); err != nil {
		return 0, fmt.Errorf("failed to update trunk page: %w", err)
	}

	// Zero out the allocated page
	page, err := fm.pm.ReadPage(leafPage)
	if err != nil {
		return 0, fmt.Errorf("failed to read allocated page: %w", err)
	}
	for i := range page.Data {
		page.Data[i] = 0
	}
	if err := fm.pm.WritePage(page); err != nil {
		return 0, fmt.Errorf("failed to clear allocated page: %w", err)
	}

	return leafPage, nil
}

// FreePage adds a page to the freelist
func (fm *FreelistManager) FreePage(pageNum uint32) error {
	if pageNum == 0 {
		return fmt.Errorf("cannot free page 0")
	}

	if fm.firstTrunk == 0 {
		// No trunk exists, make this page the first trunk
		fm.firstTrunk = pageNum

		page, err := fm.pm.ReadPage(pageNum)
		if err != nil {
			return fmt.Errorf("failed to read page: %w", err)
		}

		// Initialize as trunk with 0 leaves via C++ (Boundary-CGO)
		if !CFreelistWriteTrunk(page.Data, 0, 0) {
			return fmt.Errorf("failed to initialize trunk page")
		}

		if err := fm.pm.WritePage(page); err != nil {
			return fmt.Errorf("failed to write trunk page: %w", err)
		}

		return nil
	}

	// Read current trunk
	trunkPage, err := fm.pm.ReadPage(fm.firstTrunk)
	if err != nil {
		return fmt.Errorf("failed to read trunk page: %w", err)
	}

	maxLeaves := CFreelistMaxEntries(fm.pm.PageSize())
	_, numLeaves, _ := CFreelistParseTrunk(trunkPage.Data)

	if numLeaves < maxLeaves {
		// Add to current trunk via C++ (Boundary-CGO)
		if !CFreelistAddEntry(trunkPage.Data, pageNum) {
			return fmt.Errorf("failed to add entry to trunk page")
		}

		if err := fm.pm.WritePage(trunkPage); err != nil {
			return fmt.Errorf("failed to update trunk page: %w", err)
		}

		return nil
	}

	// Trunk is full, make freed page a new trunk
	page, err := fm.pm.ReadPage(pageNum)
	if err != nil {
		return fmt.Errorf("failed to read page: %w", err)
	}

	// Initialize new trunk pointing to old trunk via C++ (Boundary-CGO)
	if !CFreelistWriteTrunk(page.Data, fm.firstTrunk, 0) {
		return fmt.Errorf("failed to initialize new trunk page")
	}

	if err := fm.pm.WritePage(page); err != nil {
		return fmt.Errorf("failed to write new trunk page: %w", err)
	}

	// Update first trunk pointer
	fm.firstTrunk = pageNum

	return nil
}

// GetFirstTrunk returns the first trunk page number
func (fm *FreelistManager) GetFirstTrunk() uint32 {
	return fm.firstTrunk
}

// SetFirstTrunk updates the first trunk page number
func (fm *FreelistManager) SetFirstTrunk(pageNum uint32) {
	fm.firstTrunk = pageNum
}

// CountFreePages counts the total number of free pages
func (fm *FreelistManager) CountFreePages() (int, error) {
	if fm.firstTrunk == 0 {
		return 0, nil
	}

	count := 0
	currentTrunk := fm.firstTrunk

	for currentTrunk != 0 {
		trunkPage, err := fm.pm.ReadPage(currentTrunk)
		if err != nil {
			return 0, fmt.Errorf("failed to read trunk page %d: %w", currentTrunk, err)
		}

		// Count the trunk itself
		count++

		// Use C++ to parse trunk header (Boundary-CGO)
		nextTrunk, numLeaves, ok := CFreelistParseTrunk(trunkPage.Data)
		if !ok {
			return 0, fmt.Errorf("failed to parse trunk page %d", currentTrunk)
		}
		count += int(numLeaves)

		// Move to next trunk
		currentTrunk = nextTrunk

		// Safety check
		if count > 1000000 {
			return 0, fmt.Errorf("freelist chain too long (possible corruption)")
		}
	}

	return count, nil
}

// Compact removes empty trunk pages from the freelist
func (fm *FreelistManager) Compact() error {
	if fm.firstTrunk == 0 {
		return nil
	}

	var prevTrunk uint32
	currentTrunk := fm.firstTrunk

	for currentTrunk != 0 {
		trunkPage, err := fm.pm.ReadPage(currentTrunk)
		if err != nil {
			return fmt.Errorf("failed to read trunk page %d: %w", currentTrunk, err)
		}

		// Use C++ to parse trunk header (Boundary-CGO)
		nextTrunk, numLeaves, ok := CFreelistParseTrunk(trunkPage.Data)
		if !ok {
			return fmt.Errorf("failed to parse trunk page %d", currentTrunk)
		}

		if numLeaves == 0 && nextTrunk != 0 {
			// Empty trunk with more trunks after it, remove it
			if prevTrunk == 0 {
				// Removing first trunk
				fm.firstTrunk = nextTrunk
			} else {
				// Update previous trunk to skip this one via C++ (Boundary-CGO)
				prevPage, err := fm.pm.ReadPage(prevTrunk)
				if err != nil {
					return fmt.Errorf("failed to read previous trunk page: %w", err)
				}
				_, prevLeaves, _ := CFreelistParseTrunk(prevPage.Data)
				if !CFreelistWriteTrunk(prevPage.Data, nextTrunk, prevLeaves) {
					return fmt.Errorf("failed to update previous trunk next pointer")
				}
				if err := fm.pm.WritePage(prevPage); err != nil {
					return fmt.Errorf("failed to update previous trunk page: %w", err)
				}
			}

			currentTrunk = nextTrunk
			continue
		}

		prevTrunk = currentTrunk
		currentTrunk = nextTrunk
	}

	return nil
}

