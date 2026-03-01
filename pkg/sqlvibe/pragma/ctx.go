package pragma

// Ctx is the interface implemented by *Database for pragma handlers.
type Ctx interface {
	// Cache
	CacheSize() int
	SetCacheCapacity(n int)
	SetPageManagerMaxPages(maxPages int)

	// Settings store
	GetPragmaInt(name string, defaultVal int64) int64
	SetPragmaInt(name string, val int64)
	GetPragmaStr(name string, defaultVal string) string
	SetPragmaStr(name string, val string)

	// Storage metrics
	StorageMetrics() StorageMetrics
	SetCompressionName(name string) error
	GetCompressionName() string

	// WAL
	GetJournalMode() string
	OpenWALMode(dbPath string, pageSize int) error
	CloseWALMode()
	CheckpointPassive() (moved int, err error)
	CheckpointFull() (busy, log, checkpointed int, err error)
	CheckpointTruncate() (busy, log, checkpointed int, err error)
	GetWALSize() int64
	GetAutoCheckpointN() int
	StartAutoCheckpoint(n int)

	// Transaction
	GetIsolationLevel() string
	SetIsolationLevel(level string) error
	GetBusyTimeout() int
	SetBusyTimeout(ms int)

	// Vacuum / maintenance
	ClearCaches()
	RunAnalyze() error
	CheckDBIntegrity() (errors []string, err error)
	GetDBPath() string
	HasAnyTables() bool

	// Query limits
	GetQueryTimeoutMs() int64
	SetQueryTimeoutMs(ms int64)
	GetMaxMemoryBytes() int64
	SetMaxMemoryBytes(bytes int64)
	GetQueryCacheMax() int
	SetQueryCacheMax(n int)

	// Plan cache
	GetPlanCacheEnabled() bool
	SetPlanCacheEnabled(enabled bool)
	GetStmtCacheLen() int
}

// StorageMetrics holds storage statistics.
type StorageMetrics struct {
	PageCount        int
	UsedPages        int
	FreePages        int
	CompressionRatio float64
	WALSize          int64
	TotalRows        int
	TotalTables      int
}
