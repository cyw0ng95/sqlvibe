package ds

type BTree struct {
	pm       *PageManager
	rootPage uint32
	isTable  bool
}

type BTreeCursor struct {
	bt   *BTree
	page *Page
	cell int
}

type Cell struct {
	Key     []byte
	Payload []byte
	Child   uint32
}

func NewBTree(pm *PageManager, rootPage uint32, isTable bool) *BTree {
	return &BTree{
		pm:       pm,
		rootPage: rootPage,
		isTable:  isTable,
	}
}

func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

func (bt *BTree) IsTable() bool {
	return bt.isTable
}

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
	if page.Type == PageTypeLeafTbl || page.Type == PageTypeLeafIdx {
		return bt.searchLeaf(page, key)
	}
	return bt.searchInterior(page, key)
}

func (bt *BTree) searchLeaf(page *Page, key []byte) ([]byte, error) {
	numCells := int(bt.readUint16(page.Data[3:5]))
	cellPointers := bt.readCellPointers(page)

	for i := 0; i < numCells; i++ {
		cellOffset := int(cellPointers[i])
		cellKey := bt.readKey(page, cellOffset, page.Type == PageTypeLeafTbl)

		cmp := bt.compareKeys(key, cellKey)
		if cmp == 0 {
			_, payload := bt.readCellPayload(page, cellOffset, page.Type == PageTypeLeafTbl)
			return payload, nil
		}
		if cmp < 0 {
			return nil, nil
		}
	}
	return nil, nil
}

func (bt *BTree) searchInterior(page *Page, key []byte) ([]byte, error) {
	numCells := int(bt.readUint16(page.Data[3:5]))
	cellPointers := bt.readCellPointers(page)

	var childPage uint32
	for i := 0; i < numCells; i++ {
		cellOffset := int(cellPointers[i])
		cellKey := bt.readKey(page, cellOffset, page.Type == PageTypeInteriorTbl)

		if bt.compareKeys(key, cellKey) < 0 {
			childPage = bt.readChildPage(page, cellOffset, page.Type == PageTypeInteriorTbl)
			break
		}
		if i == numCells-1 {
			childPage = bt.readChildPage(page, cellOffset, page.Type == PageTypeInteriorTbl)
		}
	}

	if childPage == 0 {
		return nil, nil
	}

	child, err := bt.pm.ReadPage(childPage)
	if err != nil {
		return nil, err
	}

	return bt.searchPage(child, key)
}

func (bt *BTree) Insert(key, payload []byte) error {
	if bt.rootPage == 0 {
		return bt.createRoot(key, payload)
	}

	page, err := bt.pm.ReadPage(bt.rootPage)
	if err != nil {
		return err
	}

	if bt.isFull(page) {
		return bt.splitAndInsert(bt.rootPage, key, payload)
	}

	return bt.insertIntoPage(page, key, payload)
}

func (bt *BTree) createRoot(key, payload []byte) error {
	pageNum, err := bt.pm.AllocatePage()
	if err != nil {
		return err
	}

	pageSize := bt.pm.PageSize()
	page := NewPage(pageNum, pageSize)
	page.Type = PageTypeLeafTbl
	if !bt.isTable {
		page.Type = PageTypeLeafIdx
	}

	page.Data[0] = byte(page.Type)
	cellOffset := pageSize - 8 - len(payload)
	bt.writeUint16(page.Data[1:3], uint16(cellOffset))
	bt.writeUint16(page.Data[3:5], 1)
	bt.writeUint16(page.Data[5:7], 0)
	bt.writeUint16(page.Data[7:9], 0)

	rowID := bt.bytesToUint64(key)
	bt.writeUint64(page.Data[cellOffset:], rowID)
	copy(page.Data[cellOffset+8:], payload)

	bt.writeUint16(page.Data[8:10], uint16(cellOffset))

	bt.rootPage = pageNum

	return bt.pm.WritePage(page)
}

func (bt *BTree) isFull(page *Page) bool {
	numCells := int(bt.readUint16(page.Data[3:5]))
	cellPointers := bt.readCellPointers(page)

	if numCells == 0 {
		firstFree := bt.readUint16(page.Data[1:3])
		return firstFree < 4
	}

	lastCellOffset := int(cellPointers[numCells-1])
	firstFree := bt.readUint16(page.Data[1:3])

	cellSize := bt.estimateCellSize(page.Type == PageTypeLeafTbl)
	return lastCellOffset-cellSize < int(firstFree)
}

func (bt *BTree) insertIntoPage(page *Page, key, payload []byte) error {
	numCells := int(bt.readUint16(page.Data[3:5]))
	cellPointers := bt.readCellPointers(page)

	insertPos := numCells
	for i := 0; i < numCells; i++ {
		cellKey := bt.readKey(page, int(cellPointers[i]), page.Type == PageTypeLeafTbl)
		if bt.compareKeys(key, cellKey) < 0 {
			insertPos = i
			break
		}
	}

	for i := numCells; i > insertPos; i-- {
		cellPointers[i] = cellPointers[i-1]
	}

	bt.writeCell(page, insertPos, key, payload, 0)
	bt.writeCellPointers(page, cellPointers)

	numCells++
	bt.writeUint16(page.Data[3:5], uint16(numCells))

	return bt.pm.WritePage(page)
}

func (bt *BTree) splitAndInsert(pageNum uint32, key, payload []byte) error {
	return nil
}

func (bt *BTree) Delete(key []byte) error {
	return nil
}

func (bt *BTree) First() (*BTreeCursor, error) {
	if bt.rootPage == 0 {
		return nil, nil
	}

	page, err := bt.pm.ReadPage(bt.rootPage)
	if err != nil {
		return nil, err
	}

	for page.Type == PageTypeInteriorTbl || page.Type == PageTypeInteriorIdx {
		childPage := bt.readChildPage(page, 0, page.Type == PageTypeInteriorTbl)
		page, err = bt.pm.ReadPage(childPage)
		if err != nil {
			return nil, err
		}
	}

	if bt.readUint16(page.Data[3:5]) == 0 {
		return nil, nil
	}

	return &BTreeCursor{bt: bt, page: page, cell: 0}, nil
}

func (c *BTreeCursor) Next() ([]byte, []byte, error) {
	if c.page == nil {
		return nil, nil, nil
	}

	numCells := int(c.bt.readUint16(c.page.Data[3:5]))
	if c.cell >= numCells-1 {
		return nil, nil, nil
	}

	c.cell++
	cellPointers := c.bt.readCellPointers(c.page)
	cellOffset := int(cellPointers[c.cell])
	key, payload := c.bt.readCellPayload(c.page, cellOffset, c.page.Type == PageTypeLeafTbl)

	return key, payload, nil
}

func (c *BTreeCursor) Close() error {
	c.page = nil
	return nil
}

func (bt *BTree) readUint16(data []byte) uint16 {
	return uint16(data[0])<<8 | uint16(data[1])
}

func (bt *BTree) writeUint16(data []byte, val uint16) {
	data[0] = byte(val >> 8)
	data[1] = byte(val)
}

func (bt *BTree) writeUint64(data []byte, val uint64) {
	data[0] = byte(val >> 56)
	data[1] = byte(val >> 48)
	data[2] = byte(val >> 40)
	data[3] = byte(val >> 32)
	data[4] = byte(val >> 24)
	data[5] = byte(val >> 16)
	data[6] = byte(val >> 8)
	data[7] = byte(val)
}

func (bt *BTree) readCellPointers(page *Page) []uint16 {
	numCells := int(bt.readUint16(page.Data[3:5]))
	pointers := make([]uint16, numCells)
	for i := 0; i < numCells; i++ {
		offset := 8 + i*2
		pointers[i] = bt.readUint16(page.Data[offset : offset+2])
	}
	return pointers
}

func (bt *BTree) writeCellPointers(page *Page, pointers []uint16) {
	for i := range pointers {
		offset := 8 + i*2
		bt.writeUint16(page.Data[offset:offset+2], pointers[i])
	}
}

func (bt *BTree) readKey(page *Page, offset int, isTable bool) []byte {
	if isTable {
		rowID := bt.bytesToUint64(page.Data[offset:])
		key := make([]byte, 8)
		key[0] = byte(rowID >> 56)
		key[1] = byte(rowID >> 48)
		key[2] = byte(rowID >> 40)
		key[3] = byte(rowID >> 32)
		key[4] = byte(rowID >> 24)
		key[5] = byte(rowID >> 16)
		key[6] = byte(rowID >> 8)
		key[7] = byte(rowID)
		return key
	}

	payloadLen, n := bt.readVarint(page.Data[offset:])
	payloadEnd := offset + n + int(payloadLen)
	return page.Data[offset:payloadEnd]
}

func (bt *BTree) readCellPayload(page *Page, offset int, isTable bool) ([]byte, []byte) {
	if isTable {
		rowID := bt.bytesToUint64(page.Data[offset:])
		key := make([]byte, 8)
		key[0] = byte(rowID >> 56)
		key[1] = byte(rowID >> 48)
		key[2] = byte(rowID >> 40)
		key[3] = byte(rowID >> 32)
		key[4] = byte(rowID >> 24)
		key[5] = byte(rowID >> 16)
		key[6] = byte(rowID >> 8)
		key[7] = byte(rowID)
		payload := page.Data[offset+8:]
		return key, payload
	}

	payloadLen, n := bt.readVarint(page.Data[offset:])
	payloadOffset := offset + n
	payload := page.Data[payloadOffset : payloadOffset+int(payloadLen)]
	return nil, payload
}

func (bt *BTree) writeCell(page *Page, cell int, key, payload []byte, child uint32) {
	cellPointers := bt.readCellPointers(page)

	if page.Type == PageTypeLeafTbl {
		rowID := bt.bytesToUint64(key)
		cellOffset := int(bt.readUint16(page.Data[1:3])) - 8 - len(payload) - 2
		cellPointers[cell] = uint16(cellOffset)

		bt.writeUint64(page.Data[cellOffset:], rowID)
		copy(page.Data[cellOffset+8:], payload)

		bt.writeUint16(page.Data[1:3], uint16(cellOffset+8+len(payload)))
	} else if page.Type == PageTypeInteriorTbl {
		cellOffset := int(bt.readUint16(page.Data[1:3])) - 8 - 4 - 2
		cellPointers[cell] = uint16(cellOffset)

		bt.writeUint64(page.Data[cellOffset:], uint64(child))
		bt.writeUint64(page.Data[cellOffset+4:], bt.bytesToUint64(key))

		bt.writeUint16(page.Data[1:3], uint16(cellOffset+12))
	} else if page.Type == PageTypeLeafIdx {
		cellOffset := int(bt.readUint16(page.Data[1:3])) - len(key) - len(payload) - 2
		cellPointers[cell] = uint16(cellOffset)

		copy(page.Data[cellOffset:], key)
		offset := cellOffset + len(key)
		copy(page.Data[offset:], payload)

		bt.writeUint16(page.Data[1:3], uint16(cellOffset+len(key)+len(payload)))
	}

	bt.writeCellPointers(page, cellPointers)
}

func (bt *BTree) readChildPage(page *Page, offset int, isTable bool) uint32 {
	if isTable {
		_, n := bt.readVarint(page.Data[offset:])
		child, _ := bt.readVarint(page.Data[offset+n:])
		return uint32(child)
	}
	return uint32(bt.bytesToUint64(page.Data[offset : offset+4]))
}

func (bt *BTree) readVarint(data []byte) (uint64, int) {
	var result uint64
	var n int
	for i := 0; i < 9 && i < len(data); i++ {
		b := data[i]
		if i < 8 {
			result |= uint64(b&0x7f) << (i * 7)
		} else {
			result |= uint64(b) << (i * 7)
		}
		n++
		if b < 0x80 {
			break
		}
	}
	return result, n
}

func (bt *BTree) writeVarint(data []byte, val uint64) int {
	n := 0
	for {
		if n >= 9 {
			break
		}
		if val < 0x80 {
			data[n] = byte(val)
			n++
			break
		}
		data[n] = byte((val & 0x7f) | 0x80)
		val >>= 7
		n++
	}
	return n
}

func (bt *BTree) compareKeys(a, b []byte) int {
	alen := len(a)
	blen := len(b)
	min := alen
	if blen < min {
		min = blen
	}

	for i := 0; i < min; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	if alen < blen {
		return -1
	}
	if alen > blen {
		return 1
	}
	return 0
}

func (bt *BTree) estimateCellSize(isTable bool) int {
	if isTable {
		return 10
	}
	return 20
}

func (bt *BTree) bytesToUint64(b []byte) uint64 {
	var result uint64
	for i, v := range b {
		result |= uint64(v) << (uint(i) * 8)
	}
	return result
}
