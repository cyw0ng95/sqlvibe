package DS

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

// BitmapColumns returns the names of all columns that have a bitmap index.
func (ie *IndexEngine) BitmapColumns() []string {
	cols := make([]string, 0, len(ie.hasBitmap))
	for col := range ie.hasBitmap {
		cols = append(cols, col)
	}
	return cols
}

// BitmapMap returns the full value→bitmap map for colName.
// Returns nil if no bitmap index exists for that column.
func (ie *IndexEngine) BitmapMap(colName string) map[string]*RoaringBitmap {
	return ie.bitmaps[colName]
}

// SetBitmap replaces (or inserts) the bitmap for colName[key].
// Useful for deserializing persisted indexes.
func (ie *IndexEngine) SetBitmap(colName, key string, rb *RoaringBitmap) {
	if ie.bitmaps[colName] == nil {
		ie.bitmaps[colName] = make(map[string]*RoaringBitmap)
		ie.hasBitmap[colName] = true
	}
	ie.bitmaps[colName][key] = rb
}

// SkipListColumns returns the names of all columns that have a skip-list index.
func (ie *IndexEngine) SkipListColumns() []string {
	cols := make([]string, 0, len(ie.hasSkip))
	for col := range ie.hasSkip {
		cols = append(cols, col)
	}
	return cols
}

// SkipList returns the SkipList for colName, or nil if none exists.
func (ie *IndexEngine) SkipList(colName string) *SkipList {
	return ie.skipLists[colName]
}

// IndexMeta describes the metadata for a named index.
type IndexMeta struct {
Name      string
TableName string
Columns   []string
IsPrimary bool
IsUnique  bool
}

// CoversColumns returns true if all required columns are present in the index.
func (im *IndexMeta) CoversColumns(required []string) bool {
colSet := make(map[string]bool, len(im.Columns))
for _, c := range im.Columns {
colSet[c] = true
}
for _, r := range required {
if !colSet[r] {
return false
}
}
return true
}

// getDistinctValues returns all distinct values stored for colName in the bitmap index.
func (ie *IndexEngine) getDistinctValues(colName string) []Value {
if !ie.hasBitmap[colName] {
return nil
}
vals := make([]Value, 0, len(ie.bitmaps[colName]))
for key := range ie.bitmaps[colName] {
vals = append(vals, ParseValue(key))
}
return vals
}

// DistinctCount returns the number of distinct indexed values for colName.
func (ie *IndexEngine) DistinctCount(colName string) int {
if ie.hasBitmap[colName] {
return len(ie.bitmaps[colName])
}
return 0
}

// SkipScan performs a skip scan over all distinct leading-column values and
// unions the per-prefix intersections with the filter bitmap.
func (ie *IndexEngine) SkipScan(leadingCol, filterCol string, filterVal Value) *RoaringBitmap {
if !ie.hasBitmap[leadingCol] || !ie.hasBitmap[filterCol] {
return nil
}
filterKey := filterVal.String()
filterBM, ok := ie.bitmaps[filterCol][filterKey]
if !ok {
return NewRoaringBitmap()
}
result := NewRoaringBitmap()
for _, leadBM := range ie.bitmaps[leadingCol] {
intersection := leadBM.Clone()
intersection.IntersectWith(filterBM)
result.UnionInPlace(intersection)
}
return result
}
