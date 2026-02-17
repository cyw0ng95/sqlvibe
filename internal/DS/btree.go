package DS

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// BTree represents a B-Tree using the new encoding infrastructure
type BTree struct {
	pm           *PageManager
	om           *OverflowManager
	balancer     *PageBalancer
	freelist     *FreelistManager
	rootPage     uint32
	isTable      bool
}

// BTreeCursor represents a position in the B-Tree
type BTreeCursor struct {
	bt       *BTree
	path     []cursorLevel // Path from root to current position
	valid    bool
}

type cursorLevel struct {
	pageNum  uint32
	cellIdx  int
}

// NewBTree creates a new B-Tree
func NewBTree(pm *PageManager, rootPage uint32, isTable bool) *BTree {
	return &BTree{
		pm:           pm,
		om:           NewOverflowManager(pm),
		balancer:     NewPageBalancer(pm),
		freelist:     NewFreelistManager(pm, 0),
		rootPage:     rootPage,
		isTable:      isTable,
	}
}

// RootPage returns the root page number
func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

// IsTable returns true if this is a table B-Tree
func (bt *BTree) IsTable() bool {
	return bt.isTable
}

// Search finds a value by key in the B-Tree
func (bt *BTree) Search(key []byte) ([]byte, error) {
	if bt.rootPage == 0 {
		return nil, nil
	}

	page, err := bt.pm.ReadPage(bt.rootPage)
	if err != nil {
		return nil, err
	}

	return bt.searchPage(page, key)
}

func (bt *BTree) searchPage(page *Page, key []byte) ([]byte, error) {
	pageType := page.Data[0]
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))

	// Binary search for the cell
	cellIdx := bt.findCell(page, key)

	if pageType == 0x0d || pageType == 0x02 { // Leaf pages
		if cellIdx >= numCells {
			return nil, nil // Not found
		}

		// Read cell
		cellPointerOffset := 8 + cellIdx*2
		cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

		if pageType == 0x0d { // Table leaf
			// Decode table leaf cell
			cell, err := DecodeTableLeafCell(page.Data[cellOffset:])
			if err != nil {
				return nil, err
			}

			// Check if key matches rowid
			keyRowid, _ := GetVarint(key)
			if cell.Rowid == keyRowid {
				return cell.Payload, nil
			}
			return nil, nil

		} else { // Index leaf (0x02)
			// Decode index leaf cell
			cell, err := DecodeIndexLeafCell(page.Data[cellOffset:])
			if err != nil {
				return nil, err
			}

			// Compare keys
			if bytes.Equal(cell.Key, key) {
				return cell.Key, nil
			}
			return nil, nil
		}
	}

	// Interior page - recurse
	var childPage uint32
	if cellIdx < numCells {
		cellPointerOffset := 8 + cellIdx*2
		cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

		if pageType == 0x05 { // Table interior
			cell, err := DecodeTableInteriorCell(page.Data[cellOffset:])
			if err != nil {
				return nil, err
			}
			childPage = cell.LeftChild
		} else { // Index interior (0x0a)
			cell, err := DecodeIndexInteriorCell(page.Data[cellOffset:])
			if err != nil {
				return nil, err
			}
			childPage = cell.LeftChild
		}
	} else {
		// Use rightmost pointer
		childPage = binary.BigEndian.Uint32(page.Data[8:12])
	}

	childPageData, err := bt.pm.ReadPage(childPage)
	if err != nil {
		return nil, err
	}

	return bt.searchPage(childPageData, key)
}

// findCell performs binary search to find the insertion point for a key
func (bt *BTree) findCell(page *Page, key []byte) int {
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))
	pageType := page.Data[0]

	left, right := 0, numCells
	for left < right {
		mid := (left + right) / 2

		cellPointerOffset := 8 + mid*2
		cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

		var cellKey []byte
		if pageType == 0x0d { // Table leaf
			// Extract rowid as key
			_, n := GetVarint(page.Data[cellOffset:])
			rowid, _ := GetVarint(page.Data[cellOffset+n:])
			cellKey = make([]byte, 9)
			PutVarint(cellKey, rowid)
		} else if pageType == 0x02 { // Index leaf
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			cellKey = page.Data[cellOffset+n : cellOffset+n+int(payloadSize)]
		} else if pageType == 0x05 { // Table interior
			cell, _ := DecodeTableInteriorCell(page.Data[cellOffset:])
			cellKey = make([]byte, 9)
			PutVarint(cellKey, cell.Rowid)
		} else { // Index interior (0x0a)
			payloadSize, n := GetVarint(page.Data[cellOffset:])
			_, n2 := GetVarint(page.Data[cellOffset+n:])
			cellKey = page.Data[cellOffset+n+n2 : cellOffset+n+n2+int(payloadSize)]
		}

		cmp := CompareKeys(key, cellKey)
		if cmp < 0 {
			right = mid
		} else if cmp > 0 {
			left = mid + 1
		} else {
			return mid
		}
	}

	return left
}

// Insert inserts a key-value pair into the B-Tree
func (bt *BTree) Insert(key []byte, value []byte) error {
	if bt.rootPage == 0 {
		// Create root page
		pageNum, err := bt.pm.AllocatePage()
		if err != nil {
			return err
		}
		bt.rootPage = pageNum

		// Initialize as leaf page
		page, err := bt.pm.ReadPage(pageNum)
		if err != nil {
			return err
		}

		if bt.isTable {
			page.Data[0] = 0x0d // Table leaf
		} else {
			page.Data[0] = 0x02 // Index leaf
		}
		binary.BigEndian.PutUint16(page.Data[3:5], 0) // 0 cells
		binary.BigEndian.PutUint16(page.Data[5:7], uint16(bt.pm.PageSize()))

		if err := bt.pm.WritePage(page); err != nil {
			return err
		}
	}

	return bt.insertIntoPage(bt.rootPage, key, value)
}

func (bt *BTree) insertIntoPage(pageNum uint32, key []byte, value []byte) error {
	page, err := bt.pm.ReadPage(pageNum)
	if err != nil {
		return err
	}

	pageType := page.Data[0]
	isLeaf := (pageType == 0x0d || pageType == 0x02)

	if !isLeaf {
		// Interior page: find child and recurse
		childPage := bt.findChildForInsert(page, key)
		if err := bt.insertIntoPage(childPage, key, value); err != nil {
			return err
		}

		// Check if child split
		overfull, _ := bt.balancer.IsPageOverfull(childPage)
		if overfull {
			// TODO: Handle split and update parent
			return fmt.Errorf("page splitting not fully implemented yet")
		}

		return nil
	}

	// Leaf page: insert cell
	if err := bt.insertCell(pageNum, key, value); err != nil {
		return err
	}

	// Check if page needs splitting
	overfull, _ := bt.balancer.IsPageOverfull(pageNum)
	if overfull {
		// TODO: Implement split logic
		return fmt.Errorf("page splitting not fully implemented yet")
	}

	return nil
}

func (bt *BTree) findChildForInsert(page *Page, key []byte) uint32 {
	cellIdx := bt.findCell(page, key)
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))

	if cellIdx < numCells {
		cellPointerOffset := 8 + cellIdx*2
		cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

		pageType := page.Data[0]
		if pageType == 0x05 { // Table interior
			cell, _ := DecodeTableInteriorCell(page.Data[cellOffset:])
			return cell.LeftChild
		} else { // Index interior
			cell, _ := DecodeIndexInteriorCell(page.Data[cellOffset:])
			return cell.LeftChild
		}
	}

	// Rightmost child
	return binary.BigEndian.Uint32(page.Data[8:12])
}

func (bt *BTree) insertCell(pageNum uint32, key []byte, value []byte) error {
	page, err := bt.pm.ReadPage(pageNum)
	if err != nil {
		return err
	}

	pageType := page.Data[0]
	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))
	contentStart := int(binary.BigEndian.Uint16(page.Data[5:7]))

	// Encode cell
	var cellData []byte
	if pageType == 0x0d { // Table leaf
		rowid, _ := GetVarint(key)
		cellData = EncodeTableLeafCell(rowid, value, 0)
	} else { // Index leaf (0x02)
		cellData = EncodeIndexLeafCell(key, 0)
	}

	if len(cellData) == 0 {
		return fmt.Errorf("failed to encode cell")
	}

	// Find insertion point
	insertIdx := bt.findCell(page, key)

	// Calculate new content start (growing downward from end of page)
	newContentStart := contentStart - len(cellData)
	
	// Ensure we have space (simple check - proper check would consider fragmentation)
	headerEnd := 8 + (numCells+1)*2 // Header + all cell pointers including new one
	if newContentStart < headerEnd {
		return fmt.Errorf("page full - need to split")
	}

	// Write cell data at new content area
	copy(page.Data[newContentStart:newContentStart+len(cellData)], cellData)

	// Shift existing cell pointers to make room for new pointer
	for i := numCells; i > insertIdx; i-- {
		src := 8 + (i-1)*2
		dst := 8 + i*2
		binary.BigEndian.PutUint16(page.Data[dst:dst+2],
			binary.BigEndian.Uint16(page.Data[src:src+2]))
	}

	// Set new cell pointer
	pointerOffset := 8 + insertIdx*2
	binary.BigEndian.PutUint16(page.Data[pointerOffset:pointerOffset+2], uint16(newContentStart))

	// Update header
	binary.BigEndian.PutUint16(page.Data[3:5], uint16(numCells+1))
	binary.BigEndian.PutUint16(page.Data[5:7], uint16(newContentStart))

	return bt.pm.WritePage(page)
}

// NewCursor creates a new cursor positioned at the start of the B-Tree
func (bt *BTree) NewCursor() *BTreeCursor {
	return &BTreeCursor{
		bt:    bt,
		path:  make([]cursorLevel, 0),
		valid: false,
	}
}

// First positions the cursor at the first entry
func (c *BTreeCursor) First() error {
	c.path = c.path[:0]
	c.valid = false

	if c.bt.rootPage == 0 {
		return nil
	}

	// Navigate to leftmost leaf
	pageNum := c.bt.rootPage
	for {
		page, err := c.bt.pm.ReadPage(pageNum)
		if err != nil {
			return err
		}

		pageType := page.Data[0]
		c.path = append(c.path, cursorLevel{pageNum: pageNum, cellIdx: 0})

		if pageType == 0x0d || pageType == 0x02 { // Leaf
			c.valid = true
			return nil
		}

		// Interior: go to leftmost child
		cellPointerOffset := 8
		cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

		if pageType == 0x05 { // Table interior
			cell, _ := DecodeTableInteriorCell(page.Data[cellOffset:])
			pageNum = cell.LeftChild
		} else { // Index interior
			cell, _ := DecodeIndexInteriorCell(page.Data[cellOffset:])
			pageNum = cell.LeftChild
		}
	}
}

// Valid returns true if the cursor points to a valid entry
func (c *BTreeCursor) Valid() bool {
	return c.valid
}

// Key returns the key at the current cursor position
func (c *BTreeCursor) Key() ([]byte, error) {
	if !c.valid || len(c.path) == 0 {
		return nil, fmt.Errorf("invalid cursor position")
	}

	level := c.path[len(c.path)-1]
	page, err := c.bt.pm.ReadPage(level.pageNum)
	if err != nil {
		return nil, err
	}

	cellPointerOffset := 8 + level.cellIdx*2
	cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

	pageType := page.Data[0]
	if pageType == 0x0d { // Table leaf
		cell, err := DecodeTableLeafCell(page.Data[cellOffset:])
		if err != nil {
			return nil, err
		}
		key := make([]byte, 9)
		PutVarint(key, cell.Rowid)
		return key, nil
	} else { // Index leaf
		cell, err := DecodeIndexLeafCell(page.Data[cellOffset:])
		if err != nil {
			return nil, err
		}
		return cell.Key, nil
	}
}

// Value returns the value at the current cursor position
func (c *BTreeCursor) Value() ([]byte, error) {
	if !c.valid || len(c.path) == 0 {
		return nil, fmt.Errorf("invalid cursor position")
	}

	level := c.path[len(c.path)-1]
	page, err := c.bt.pm.ReadPage(level.pageNum)
	if err != nil {
		return nil, err
	}

	pageType := page.Data[0]
	if pageType != 0x0d { // Only table leaf has values
		return nil, nil
	}

	cellPointerOffset := 8 + level.cellIdx*2
	cellOffset := int(binary.BigEndian.Uint16(page.Data[cellPointerOffset : cellPointerOffset+2]))

	cell, err := DecodeTableLeafCell(page.Data[cellOffset:])
	if err != nil {
		return nil, err
	}

	return cell.Payload, nil
}

// Next moves the cursor to the next entry
func (c *BTreeCursor) Next() error {
	if !c.valid || len(c.path) == 0 {
		return fmt.Errorf("invalid cursor position")
	}

	// Try to move to next cell in current page
	level := &c.path[len(c.path)-1]
	page, err := c.bt.pm.ReadPage(level.pageNum)
	if err != nil {
		return err
	}

	numCells := int(binary.BigEndian.Uint16(page.Data[3:5]))
	if level.cellIdx+1 < numCells {
		level.cellIdx++
		return nil
	}

	// End of page: move up and right
	// TODO: Implement proper cursor navigation across pages
	c.valid = false
	return nil
}
