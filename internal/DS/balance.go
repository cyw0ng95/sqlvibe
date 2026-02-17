package DS

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// PageBalancer handles BTree page balancing operations
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

// SplitLeafPage splits an overfull leaf page into two pages
// Returns the new right page number and the divider key
func (pb *PageBalancer) SplitLeafPage(pageNum uint32) (rightPage uint32, dividerKey []byte, err error) {
	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read page: %w", err)
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

	// Parse existing cells
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))
	if numCells < 2 {
		return 0, nil, fmt.Errorf("page has too few cells to split: %d", numCells)
	}

	// Split point: put half cells on each page
	splitPoint := numCells / 2

	// Extract cell pointers
	cellPointers := make([]uint16, numCells)
	for i := 0; i < numCells; i++ {
		offset := 8 + i*2 // Leaf page header is 8 bytes
		cellPointers[i] = binary.BigEndian.Uint16(page.Data[offset : offset+2])
	}

	// Determine if this is a table leaf or index leaf
	pageType := page.Data[0]
	isTableLeaf := (pageType == 0x0d)

	// Initialize right page with same type
	rightPageData.Data[0] = pageType
	binary.BigEndian.PutUint16(rightPageData.Data[1:3], 0) // freeblock
	binary.BigEndian.PutUint16(rightPageData.Data[3:5], uint16(numCells-splitPoint))
	binary.BigEndian.PutUint16(rightPageData.Data[5:7], uint16(pb.pm.PageSize())) // cell content area
	binary.BigEndian.PutUint16(rightPageData.Data[7:9], 0) // fragmented bytes

	// Copy right half cells to new page
	newContentOffset := pb.pm.PageSize()
	for i := splitPoint; i < numCells; i++ {
		cellOffset := int(cellPointers[i])
		
		// Determine cell size (simplified - assumes no overflow for now)
		var cellSize int
		if isTableLeaf {
			// Read payload size varint
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			// Read rowid varint
			_, n2 := GetVarint(page.Data[cellOffset+n:])
			cellSize = n + n2 + int(payloadSize)
		} else {
			// Index leaf: just payload size
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			cellSize = n + int(payloadSize)
		}

		// Copy cell data to right page
		newContentOffset -= cellSize
		copy(rightPageData.Data[newContentOffset:], page.Data[cellOffset:cellOffset+cellSize])

		// Add cell pointer
		pointerOffset := 8 + (i-splitPoint)*2
		binary.BigEndian.PutUint16(rightPageData.Data[pointerOffset:pointerOffset+2], uint16(newContentOffset))
	}

	// Update right page cell content area start
	binary.BigEndian.PutUint16(rightPageData.Data[5:7], uint16(newContentOffset))

	// Write right page
	if err := pb.pm.WritePage(rightPageData); err != nil {
		return 0, nil, fmt.Errorf("failed to write right page: %w", err)
	}

	// Update left page (remove moved cells)
	binary.BigEndian.PutUint16(page.Data[3:5], uint16(splitPoint))

	// Compact left page cells
	newLeftContent := pb.pm.PageSize()
	for i := 0; i < splitPoint; i++ {
		cellOffset := int(cellPointers[i])
		
		var cellSize int
		if isTableLeaf {
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			_, n2 := GetVarint(page.Data[cellOffset+n:])
			cellSize = n + n2 + int(payloadSize)
		} else {
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			cellSize = n + int(payloadSize)
		}

		newLeftContent -= cellSize
		copy(page.Data[newLeftContent:], page.Data[cellOffset:cellOffset+cellSize])
		
		// Update cell pointer
		pointerOffset := 8 + i*2
		binary.BigEndian.PutUint16(page.Data[pointerOffset:pointerOffset+2], uint16(newLeftContent))
	}

	binary.BigEndian.PutUint16(page.Data[5:7], uint16(newLeftContent))

	// Write updated left page
	if err := pb.pm.WritePage(page); err != nil {
		return 0, nil, fmt.Errorf("failed to write left page: %w", err)
	}

	// Extract divider key (first key of right page)
	firstCellOffset := int(binary.BigEndian.Uint16(rightPageData.Data[8:10]))
	if isTableLeaf {
		// For table leaf, divider is the rowid
		_, n := GetVarint(rightPageData.Data[firstCellOffset:])
		rowid, _ := GetVarint(rightPageData.Data[firstCellOffset+n:])
		dividerKey = make([]byte, 9)
		PutVarint(dividerKey, rowid)
	} else {
		// For index leaf, divider is the key
		payloadSize, n := GetVarint(rightPageData.Data[firstCellOffset:])
		dividerKey = make([]byte, int(payloadSize))
		copy(dividerKey, rightPageData.Data[firstCellOffset+n:firstCellOffset+n+int(payloadSize)])
	}

	return rightPage, dividerKey, nil
}

// RedistributeCells moves cells from one page to a sibling to balance them
func (pb *PageBalancer) RedistributeCells(leftPageNum, rightPageNum uint32) error {
	leftPage, err := pb.pm.ReadPage(leftPageNum)
	if err != nil {
		return fmt.Errorf("failed to read left page: %w", err)
	}

	rightPage, err := pb.pm.ReadPage(rightPageNum)
	if err != nil {
		return fmt.Errorf("failed to read right page: %w", err)
	}

	leftCells := int(binary.BigEndian.Uint16(leftPage.Data[3:5]))
	rightCells := int(binary.BigEndian.Uint16(rightPage.Data[3:5]))

	totalCells := leftCells + rightCells
	if totalCells < 2 {
		return fmt.Errorf("not enough cells to redistribute: %d", totalCells)
	}

	// Target: roughly equal distribution
	targetLeft := totalCells / 2
	diff := targetLeft - leftCells

	if diff == 0 {
		// Already balanced
		return nil
	}

	if diff > 0 {
		// Move cells from right to left
		return pb.moveCells(rightPageNum, leftPageNum, diff)
	} else {
		// Move cells from left to right
		return pb.moveCells(leftPageNum, rightPageNum, -diff)
	}
}

// moveCells moves n cells from source page to destination page
func (pb *PageBalancer) moveCells(srcPageNum, dstPageNum uint32, n int) error {
	if n <= 0 {
		return nil
	}

	srcPage, err := pb.pm.ReadPage(srcPageNum)
	if err != nil {
		return fmt.Errorf("failed to read source page: %w", err)
	}

	dstPage, err := pb.pm.ReadPage(dstPageNum)
	if err != nil {
		return fmt.Errorf("failed to read destination page: %w", err)
	}

	srcCells := int(binary.BigEndian.Uint16(srcPage.Data[3:5]))
	if n > srcCells {
		return fmt.Errorf("cannot move %d cells from page with %d cells", n, srcCells)
	}

	// Extract cells to move (first n cells from source)
	cellsToMove := make([][]byte, n)
	for i := 0; i < n; i++ {
		pointerOffset := 8 + i*2
		cellOffset := int(binary.BigEndian.Uint16(srcPage.Data[pointerOffset : pointerOffset+2]))
		
		// Determine cell size
		payloadSize, varintN := GetVarint(srcPage.Data[cellOffset:])
		cellSize := varintN + int(payloadSize)
		
		// Copy cell data
		cellsToMove[i] = make([]byte, cellSize)
		copy(cellsToMove[i], srcPage.Data[cellOffset:cellOffset+cellSize])
	}

	// Remove cells from source
	newSrcCells := srcCells - n
	binary.BigEndian.PutUint16(srcPage.Data[3:5], uint16(newSrcCells))

	// Shift remaining cell pointers
	for i := 0; i < newSrcCells; i++ {
		srcOffset := 8 + (i+n)*2
		dstOffset := 8 + i*2
		binary.BigEndian.PutUint16(srcPage.Data[dstOffset:dstOffset+2],
			binary.BigEndian.Uint16(srcPage.Data[srcOffset:srcOffset+2]))
	}

	if err := pb.pm.WritePage(srcPage); err != nil {
		return fmt.Errorf("failed to write source page: %w", err)
	}

	// Add cells to destination (append at end)
	dstCells := int(binary.BigEndian.Uint16(dstPage.Data[3:5]))
	contentStart := int(binary.BigEndian.Uint16(dstPage.Data[5:7]))

	for i, cell := range cellsToMove {
		// Write cell data
		contentStart -= len(cell)
		copy(dstPage.Data[contentStart:], cell)

		// Add cell pointer
		pointerOffset := 8 + (dstCells+i)*2
		binary.BigEndian.PutUint16(dstPage.Data[pointerOffset:pointerOffset+2], uint16(contentStart))
	}

	binary.BigEndian.PutUint16(dstPage.Data[3:5], uint16(dstCells+n))
	binary.BigEndian.PutUint16(dstPage.Data[5:7], uint16(contentStart))

	if err := pb.pm.WritePage(dstPage); err != nil {
		return fmt.Errorf("failed to write destination page: %w", err)
	}

	return nil
}

// MergePages merges two sibling pages into the left page
// Returns true if merge was successful, false if pages don't fit together
func (pb *PageBalancer) MergePages(leftPageNum, rightPageNum uint32) (bool, error) {
	leftPage, err := pb.pm.ReadPage(leftPageNum)
	if err != nil {
		return false, fmt.Errorf("failed to read left page: %w", err)
	}

	rightPage, err := pb.pm.ReadPage(rightPageNum)
	if err != nil {
		return false, fmt.Errorf("failed to read right page: %w", err)
	}

	leftCells := int(binary.BigEndian.Uint16(leftPage.Data[3:5]))
	rightCells := int(binary.BigEndian.Uint16(rightPage.Data[3:5]))

	leftContent := int(binary.BigEndian.Uint16(leftPage.Data[5:7]))
	rightContent := int(binary.BigEndian.Uint16(rightPage.Data[5:7]))

	// Calculate space needed
	leftUsed := pb.pm.PageSize() - leftContent
	rightUsed := pb.pm.PageSize() - rightContent

	totalUsed := leftUsed + rightUsed + (leftCells+rightCells)*2 // cell pointers

	if totalUsed > pb.pm.PageSize()-8 { // 8 bytes for header
		// Pages don't fit together
		return false, nil
	}

	// Merge: copy all cells from right page to left page
	newContentStart := leftContent

	for i := 0; i < rightCells; i++ {
		pointerOffset := 8 + i*2
		cellOffset := int(binary.BigEndian.Uint16(rightPage.Data[pointerOffset : pointerOffset+2]))
		
		// Determine cell size
		payloadSize, n := GetVarint(rightPage.Data[cellOffset:])
		cellSize := n + int(payloadSize)

		// Copy cell to left page
		newContentStart -= cellSize
		copy(leftPage.Data[newContentStart:], rightPage.Data[cellOffset:cellOffset+cellSize])

		// Add cell pointer
		newPointerOffset := 8 + (leftCells+i)*2
		binary.BigEndian.PutUint16(leftPage.Data[newPointerOffset:newPointerOffset+2], uint16(newContentStart))
	}

	// Update left page header
	binary.BigEndian.PutUint16(leftPage.Data[3:5], uint16(leftCells+rightCells))
	binary.BigEndian.PutUint16(leftPage.Data[5:7], uint16(newContentStart))

	if err := pb.pm.WritePage(leftPage); err != nil {
		return false, fmt.Errorf("failed to write merged page: %w", err)
	}

	// Free right page
	if err := pb.pm.FreePage(rightPageNum); err != nil {
		return false, fmt.Errorf("failed to free right page: %w", err)
	}

	return true, nil
}

// IsPageOverfull checks if a page needs to be split
func (pb *PageBalancer) IsPageOverfull(pageNum uint32) (bool, error) {
	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return false, err
	}

	contentStart := int(binary.BigEndian.Uint16(page.Data[5:7]))
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))

	// Calculate used space
	headerSize := 8 // leaf page header
	cellPointerArea := numCells * 2
	contentUsed := pb.pm.PageSize() - contentStart

	usedSpace := headerSize + cellPointerArea + contentUsed
	threshold := pb.pm.PageSize() * 9 / 10 // 90% full

	return usedSpace > threshold, nil
}

// IsPageUnderfull checks if a page should be merged or redistributed
func (pb *PageBalancer) IsPageUnderfull(pageNum uint32) (bool, error) {
	page, err := pb.pm.ReadPage(pageNum)
	if err != nil {
		return false, err
	}

	contentStart := int(binary.BigEndian.Uint16(page.Data[5:7]))
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))

	// Calculate used space
	headerSize := 8
	cellPointerArea := numCells * 2
	contentUsed := pb.pm.PageSize() - contentStart

	usedSpace := headerSize + cellPointerArea + contentUsed
	threshold := pb.pm.PageSize() / 3 // Less than 33% full

	return usedSpace < threshold, nil
}

// CompareKeys compares two keys for ordering
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func CompareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}
