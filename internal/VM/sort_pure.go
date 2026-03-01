//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import (
	"bytes"
	"sort"
)

// SortInt64 sorts int64 slice in place (pure Go)
func SortInt64(data []int64) {
	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})
}

// SortInt64WithIndices sorts int64 with index tracking (pure Go)
func SortInt64WithIndices(data []int64, indices []int64) {
	if len(indices) == 0 {
		SortInt64(data)
		return
	}

	// Create index array
	idx := make([]int, len(data))
	for i := range idx {
		idx[i] = i
	}

	// Sort indices by data values
	sort.Slice(idx, func(i, j int) bool {
		return data[idx[i]] < data[idx[j]]
	})

	// Rearrange data and indices
	newData := make([]int64, len(data))
	newIndices := make([]int64, len(indices))
	for i, oi := range idx {
		newData[i] = data[oi]
		newIndices[i] = indices[oi]
	}

	copy(data, newData)
	copy(indices, newIndices)
}

// SortStrings sorts string slice (pure Go)
func SortStrings(data []string) {
	sort.Strings(data)
}

// SortStringsWithIndices sorts strings with index tracking (pure Go)
func SortStringsWithIndices(data []string, indices []int64) {
	if len(indices) == 0 {
		SortStrings(data)
		return
	}

	idx := make([]int, len(data))
	for i := range idx {
		idx[i] = i
	}

	sort.Slice(idx, func(i, j int) bool {
		return data[idx[i]] < data[idx[j]]
	})

	newData := make([]string, len(data))
	newIndices := make([]int64, len(indices))
	for i, oi := range idx {
		newData[i] = data[oi]
		newIndices[i] = indices[oi]
	}

	copy(data, newData)
	copy(indices, newIndices)
}

// SortBytes sorts byte slices by content (pure Go)
func SortBytes(data [][]byte) {
	sort.Slice(data, func(i, j int) bool {
		return bytes.Compare(data[i], data[j]) < 0
	})
}
