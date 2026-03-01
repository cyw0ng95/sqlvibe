//go:build !SVDB_ENABLE_CGO_DS
// +build !SVDB_ENABLE_CGO_DS

package DS

import "unsafe"

// useCGODS controls whether to use CGO for DS operations
// In pure Go build, this is always false
var useCGODS = false

// BTreeCGO is a stub type for pure Go builds
type BTreeCGO struct {
	handle unsafe.Pointer
}

// NewBTreeCGO returns nil in pure Go builds - the regular Go BTree is used
func NewBTreeCGO(pm *PageManager, rootPage uint32, isTable bool) *BTreeCGO {
	return nil
}

// Close is a no-op in pure Go builds
func (bt *BTreeCGO) Close() {}

// Search returns nil in pure Go builds - the regular Go BTree.Search is used
func (bt *BTreeCGO) Search(key []byte) ([]byte, error) {
	return nil, nil
}

// GetVarintCGO falls back to Go implementation
func GetVarintCGO(buf []byte) (int64, int, error) {
	v, n := GetVarint(buf)
	return v, n, nil
}

// PutVarintCGO falls back to Go implementation
func PutVarintCGO(buf []byte, v int64) (int, error) {
	n := PutVarint(buf, v)
	return n, nil
}

// VarintLenCGO falls back to Go implementation
func VarintLenCGO(v int64) int {
	return VarintLen(v)
}

// EncodeTableLeafCellCGO falls back to Go implementation
func EncodeTableLeafCellCGO(rowid int64, payload []byte, overflowPage uint32) ([]byte, error) {
	return EncodeTableLeafCell(rowid, payload, overflowPage), nil
}

// DecodeTableLeafCellCGO falls back to Go implementation
func DecodeTableLeafCellCGO(buf []byte) (rowid int64, payload []byte, overflowPage uint32, err error) {
	cell, err := DecodeTableLeafCell(buf)
	if err != nil {
		return 0, nil, 0, err
	}
	return cell.Rowid, cell.Payload, cell.OverflowPage, nil
}

// BinarySearchCGO falls back to Go implementation
func BinarySearchCGO(pageData []byte, key []byte, isTable bool) (int, error) {
	// Fall back to Go implementation
	return -1, nil
}
