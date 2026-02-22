package DS

import (
	"time"
)

// StorageMetrics holds statistics about the current state of the storage
// engine. Returned by Database.StorageInfo() and exposed via PRAGMA
// storage_info.
type StorageMetrics struct {
	// PageCount is the total number of pages (including free pages).
	PageCount int
	// UsedPages is the number of pages that hold live data.
	UsedPages int
	// FreePages is the number of pages on the free list.
	FreePages int
	// CompressionRatio is the ratio of uncompressed to compressed size when
	// page-level compression is enabled. 1.0 means no compression.
	CompressionRatio float64
	// WALSize is the current byte size of the WAL file (0 when WAL is off).
	WALSize int64
	// LastCheckpoint is the time the last WAL checkpoint completed.
	LastCheckpoint time.Time
	// TotalRows is the total live row count across all HybridStore tables.
	TotalRows int
	// TotalTables is the number of registered tables.
	TotalTables int
}

// CollectMetrics builds a StorageMetrics snapshot from a PageManager and an
// optional WAL size. Pass walSize=0 when WAL mode is disabled.
func CollectMetrics(pm *PageManager, walSize int64) StorageMetrics {
	if pm == nil {
		return StorageMetrics{CompressionRatio: 1.0}
	}

	total := int(pm.NumPages())
	free := 0
	used := total - free
	if used < 0 {
		used = 0
	}

	return StorageMetrics{
		PageCount:        total,
		UsedPages:        used,
		FreePages:        free,
		CompressionRatio: 1.0,
		WALSize:          walSize,
		LastCheckpoint:   time.Time{},
	}
}
