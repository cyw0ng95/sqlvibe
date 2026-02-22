package storage

// IndexEngine manages bitmap and skip-list indexes for fast lookups.
type IndexEngine struct {
	bitmaps   map[string]map[string]*RoaringBitmap // colName → valueStr → bitmap
	skipLists map[string]*SkipList                 // colName → SkipList
	hasBitmap map[string]bool
	hasSkip   map[string]bool
}

// NewIndexEngine creates an empty IndexEngine.
func NewIndexEngine() *IndexEngine {
	return &IndexEngine{
		bitmaps:   make(map[string]map[string]*RoaringBitmap),
		skipLists: make(map[string]*SkipList),
		hasBitmap: make(map[string]bool),
		hasSkip:   make(map[string]bool),
	}
}

// AddBitmapIndex creates a bitmap index for colName.
func (ie *IndexEngine) AddBitmapIndex(colName string) {
	if !ie.hasBitmap[colName] {
		ie.bitmaps[colName] = make(map[string]*RoaringBitmap)
		ie.hasBitmap[colName] = true
	}
}

// AddSkipListIndex creates a skip-list index for colName.
func (ie *IndexEngine) AddSkipListIndex(colName string) {
	if !ie.hasSkip[colName] {
		ie.skipLists[colName] = NewSkipList()
		ie.hasSkip[colName] = true
	}
}

// HasBitmapIndex reports whether a bitmap index exists for colName.
func (ie *IndexEngine) HasBitmapIndex(colName string) bool { return ie.hasBitmap[colName] }

// HasSkipListIndex reports whether a skip-list index exists for colName.
func (ie *IndexEngine) HasSkipListIndex(colName string) bool { return ie.hasSkip[colName] }

// IndexRow updates all indexes for a newly inserted row.
func (ie *IndexEngine) IndexRow(rowIdx uint32, colName string, val Value) {
	if ie.hasBitmap[colName] {
		key := val.String()
		if ie.bitmaps[colName][key] == nil {
			ie.bitmaps[colName][key] = NewRoaringBitmap()
		}
		ie.bitmaps[colName][key].Add(rowIdx)
	}
	if ie.hasSkip[colName] {
		ie.skipLists[colName].Insert(val, rowIdx)
	}
}

// UnindexRow removes a row from all indexes.
func (ie *IndexEngine) UnindexRow(rowIdx uint32, colName string, val Value) {
	if ie.hasBitmap[colName] {
		key := val.String()
		if rb, ok := ie.bitmaps[colName][key]; ok {
			rb.Remove(rowIdx)
		}
	}
	if ie.hasSkip[colName] {
		ie.skipLists[colName].Delete(val, rowIdx)
	}
}

// LookupEqual returns a bitmap of row indices where colName == val.
func (ie *IndexEngine) LookupEqual(colName string, val Value) *RoaringBitmap {
	if ie.hasBitmap[colName] {
		key := val.String()
		if rb, ok := ie.bitmaps[colName][key]; ok {
			return rb.Clone()
		}
		return NewRoaringBitmap()
	}
	if ie.hasSkip[colName] {
		idxs := ie.skipLists[colName].Find(val)
		rb := NewRoaringBitmap()
		for _, i := range idxs {
			rb.Add(i)
		}
		return rb
	}
	return nil // no index
}

// LookupRange returns a bitmap of row indices where colName is in [lo, hi].
func (ie *IndexEngine) LookupRange(colName string, lo, hi Value, inclusive bool) *RoaringBitmap {
	if ie.hasSkip[colName] {
		idxs := ie.skipLists[colName].Range(lo, hi, inclusive)
		rb := NewRoaringBitmap()
		for _, i := range idxs {
			rb.Add(i)
		}
		return rb
	}
	return nil // no index
}
