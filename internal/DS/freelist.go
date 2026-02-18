package DS

import (
	"encoding/binary"
	"fmt"
)

// Freelist management for SQLite-style page allocation
// Freelist uses trunk pages that point to leaf pages
//
// Trunk page format:
// - Bytes 0-3: Next trunk page (0 if last)
// - Bytes 4-7: Number of leaf page pointers
// - Bytes 8+: Array of page numbers (up to (pageSize-8)/4 entries)

const (
	FreelistTrunkHeaderSize = 8
)

// FreelistManager handles free page management
type FreelistManager struct {
	pm         *PageManager
	firstTrunk uint32 // First trunk page number
}

// NewFreelistManager creates a new freelist manager
func NewFreelistManager(pm *PageManager, firstTrunk uint32) *FreelistManager {
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

	nextTrunk := binary.BigEndian.Uint32(trunkPage.Data[0:4])
	numLeaves := binary.BigEndian.Uint32(trunkPage.Data[4:8])

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

	// Pop a leaf from the trunk
	leafPage := binary.BigEndian.Uint32(trunkPage.Data[8+(numLeaves-1)*4 : 8+numLeaves*4])
	numLeaves--
	binary.BigEndian.PutUint32(trunkPage.Data[4:8], numLeaves)

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

		// Initialize as trunk with 0 leaves
		binary.BigEndian.PutUint32(page.Data[0:4], 0) // next trunk = 0
		binary.BigEndian.PutUint32(page.Data[4:8], 0) // num leaves = 0

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

	numLeaves := binary.BigEndian.Uint32(trunkPage.Data[4:8])
	maxLeaves := uint32((fm.pm.PageSize() - FreelistTrunkHeaderSize) / 4)

	if numLeaves < maxLeaves {
		// Add to current trunk
		binary.BigEndian.PutUint32(trunkPage.Data[8+numLeaves*4:8+(numLeaves+1)*4], pageNum)
		numLeaves++
		binary.BigEndian.PutUint32(trunkPage.Data[4:8], numLeaves)

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

	// Initialize new trunk
	binary.BigEndian.PutUint32(page.Data[0:4], fm.firstTrunk) // point to old trunk
	binary.BigEndian.PutUint32(page.Data[4:8], 0)             // 0 leaves

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

		// Count leaves
		numLeaves := binary.BigEndian.Uint32(trunkPage.Data[4:8])
		count += int(numLeaves)

		// Move to next trunk
		currentTrunk = binary.BigEndian.Uint32(trunkPage.Data[0:4])

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

		nextTrunk := binary.BigEndian.Uint32(trunkPage.Data[0:4])
		numLeaves := binary.BigEndian.Uint32(trunkPage.Data[4:8])

		if numLeaves == 0 && nextTrunk != 0 {
			// Empty trunk with more trunks after it, remove it
			if prevTrunk == 0 {
				// Removing first trunk
				fm.firstTrunk = nextTrunk
			} else {
				// Update previous trunk to skip this one
				prevPage, err := fm.pm.ReadPage(prevTrunk)
				if err != nil {
					return fmt.Errorf("failed to read previous trunk page: %w", err)
				}
				binary.BigEndian.PutUint32(prevPage.Data[0:4], nextTrunk)
				if err := fm.pm.WritePage(prevPage); err != nil {
					return fmt.Errorf("failed to update previous trunk page: %w", err)
				}
			}

			// Note: We don't actually delete the page, just remove it from the chain
			// It will be reclaimed if needed

			currentTrunk = nextTrunk
			continue
		}

		prevTrunk = currentTrunk
		currentTrunk = nextTrunk
	}

	return nil
}
