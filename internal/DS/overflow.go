package DS

import (
	"encoding/binary"
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/SF/util"
)

// Overflow page format:
// - First 4 bytes: Next overflow page number (0 if last)
// - Remaining bytes: Payload data

const (
	OverflowPageHeaderSize = 4
)

// OverflowManager handles overflow page operations
type OverflowManager struct {
	pm *PageManager
}

// NewOverflowManager creates a new overflow manager
func NewOverflowManager(pm *PageManager) *OverflowManager {
	util.AssertNotNil(pm, "PageManager")
	return &OverflowManager{pm: pm}
}

// WriteOverflowChain writes a large payload across multiple overflow pages
// Returns the first overflow page number
func (om *OverflowManager) WriteOverflowChain(payload []byte) (uint32, error) {
	util.AssertNotNil(payload, "payload")

	if len(payload) == 0 {
		return 0, nil
	}

	util.Assert(om.pm.PageSize() > OverflowPageHeaderSize, "page size too small for overflow: %d", om.pm.PageSize())

	usableSize := om.pm.PageSize() - OverflowPageHeaderSize
	var firstPage uint32
	var prevPage uint32

	offset := 0
	for offset < len(payload) {
		// Allocate a new overflow page
		pageNum, err := om.pm.AllocatePage()
		if err != nil {
			return 0, fmt.Errorf("failed to allocate overflow page: %w", err)
		}

		if firstPage == 0 {
			firstPage = pageNum
		}

		// Link previous page to this one
		if prevPage != 0 {
			prevPageData, err := om.pm.ReadPage(prevPage)
			if err != nil {
				return 0, fmt.Errorf("failed to read previous overflow page: %w", err)
			}
			binary.BigEndian.PutUint32(prevPageData.Data[:4], pageNum)
			if err := om.pm.WritePage(prevPageData); err != nil {
				return 0, fmt.Errorf("failed to update previous overflow page: %w", err)
			}
		}

		// Write data to current page
		page, err := om.pm.ReadPage(pageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to read overflow page: %w", err)
		}

		// Determine how much to write
		remaining := len(payload) - offset
		writeSize := remaining
		if writeSize > usableSize {
			writeSize = usableSize
		}

		// Set next page pointer (0 if last page)
		binary.BigEndian.PutUint32(page.Data[:4], 0)

		// Write payload data
		copy(page.Data[OverflowPageHeaderSize:OverflowPageHeaderSize+writeSize], payload[offset:offset+writeSize])

		if err := om.pm.WritePage(page); err != nil {
			return 0, fmt.Errorf("failed to write overflow page: %w", err)
		}

		prevPage = pageNum
		offset += writeSize
	}

	return firstPage, nil
}

// ReadOverflowChain reads a complete payload from an overflow chain
func (om *OverflowManager) ReadOverflowChain(firstPage uint32, totalSize int) ([]byte, error) {
	util.Assert(firstPage > 0 || totalSize == 0, "firstPage must be positive when totalSize > 0")
	util.Assert(totalSize >= 0, "totalSize cannot be negative: %d", totalSize)

	if firstPage == 0 || totalSize == 0 {
		return nil, nil
	}

	result := make([]byte, 0, totalSize)
	usableSize := om.pm.PageSize() - OverflowPageHeaderSize

	currentPage := firstPage
	for currentPage != 0 && len(result) < totalSize {
		page, err := om.pm.ReadPage(currentPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page %d: %w", currentPage, err)
		}

		// Read next page pointer
		nextPage := binary.BigEndian.Uint32(page.Data[:4])

		// Determine how much to read from this page
		remaining := totalSize - len(result)
		readSize := remaining
		if readSize > usableSize {
			readSize = usableSize
		}

		// Append data
		result = append(result, page.Data[OverflowPageHeaderSize:OverflowPageHeaderSize+readSize]...)

		currentPage = nextPage
	}

	if len(result) != totalSize {
		return nil, fmt.Errorf("overflow chain incomplete: expected %d bytes, got %d", totalSize, len(result))
	}

	return result, nil
}

// FreeOverflowChain frees all pages in an overflow chain
func (om *OverflowManager) FreeOverflowChain(firstPage uint32) error {
	if firstPage == 0 {
		return nil
	}

	currentPage := firstPage
	for currentPage != 0 {
		page, err := om.pm.ReadPage(currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page %d: %w", currentPage, err)
		}

		// Read next page pointer before freeing
		nextPage := binary.BigEndian.Uint32(page.Data[:4])

		// Free current page
		if err := om.pm.FreePage(currentPage); err != nil {
			return fmt.Errorf("failed to free overflow page %d: %w", currentPage, err)
		}

		currentPage = nextPage
	}

	return nil
}

// GetOverflowChainLength calculates the number of pages in an overflow chain
func (om *OverflowManager) GetOverflowChainLength(firstPage uint32) (int, error) {
	if firstPage == 0 {
		return 0, nil
	}

	count := 0
	currentPage := firstPage
	for currentPage != 0 {
		count++
		page, err := om.pm.ReadPage(currentPage)
		if err != nil {
			return 0, fmt.Errorf("failed to read overflow page %d: %w", currentPage, err)
		}
		currentPage = binary.BigEndian.Uint32(page.Data[:4])

		// Safety check to prevent infinite loops
		if count > 10000 {
			return 0, fmt.Errorf("overflow chain too long (possible corruption)")
		}
	}

	return count, nil
}
