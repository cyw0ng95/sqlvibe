package sqlvibe

import (
	"os"
	"runtime"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/TM"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

// --- Cache ---

func (db *Database) CacheSize() int {
	return db.cache.Size()
}

func (db *Database) SetCacheCapacity(n int) {
	db.cache.SetCapacity(n)
}

func (db *Database) SetPageManagerMaxPages(maxPages int) {
	if db.pm != nil {
		db.pm.SetMaxPages(maxPages)
	}
}

// --- Settings ---

func (db *Database) GetPragmaInt(name string, defaultVal int64) int64 {
	if v, ok := db.pragmaSettings[name]; ok {
		if n, ok := v.(int64); ok {
			return n
		}
	}
	return defaultVal
}

func (db *Database) SetPragmaInt(name string, val int64) {
	db.pragmaSettings[name] = val
}

func (db *Database) GetPragmaStr(name string, defaultVal string) string {
	if v, ok := db.pragmaSettings[name]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultVal
}

func (db *Database) SetPragmaStr(name string, val string) {
	db.pragmaSettings[name] = val
}

// --- Storage metrics ---

func (db *Database) StorageMetrics() pragma.StorageMetrics {
	var walSize int64
	if db.wal != nil {
		walSize = db.wal.Size()
	}
	m := DS.CollectMetrics(db.pm, walSize)
	totalRows := 0
	for _, rows := range db.data {
		totalRows += len(rows)
	}
	return pragma.StorageMetrics{
		PageCount:        m.PageCount,
		UsedPages:        m.UsedPages,
		FreePages:        m.FreePages,
		CompressionRatio: m.CompressionRatio,
		WALSize:          walSize,
		TotalRows:        totalRows,
		TotalTables:      len(db.tables),
	}
}

func (db *Database) GetCompressionName() string {
	return db.compressionName
}

func (db *Database) SetCompressionName(name string) error {
	db.compressionName = strings.ToUpper(name)
	return nil
}

// --- WAL ---

func (db *Database) GetJournalMode() string {
	return db.journalMode
}

func (db *Database) OpenWALMode(dbPath string, pageSize int) error {
	if dbPath != ":memory:" {
		walPath := dbPath + "-wal"
		if db.wal != nil {
			_ = db.wal.Close()
			db.wal = nil
		}
		wal, err := TM.OpenWAL(walPath, db.pm.PageSize())
		if err != nil {
			return err
		}
		if err := db.txMgr.EnableWAL(walPath, db.pm.PageSize()); err != nil {
			_ = wal.Close()
			return err
		}
		db.wal = wal
	}
	db.journalMode = "wal"
	return nil
}

func (db *Database) CloseWALMode() {
	if db.wal != nil {
		_, _ = db.wal.Checkpoint()
		_ = db.wal.Close()
		db.wal = nil
		_ = db.txMgr.DisableWAL()
		if db.dbPath != ":memory:" {
			_ = os.Remove(db.dbPath + "-wal")
		}
	}
	db.journalMode = "delete"
}

func (db *Database) CheckpointPassive() (int, error) {
	if db.wal == nil {
		return 0, nil
	}
	return db.wal.Checkpoint()
}

func (db *Database) CheckpointFull() (int, int, int, error) {
	if db.wal == nil {
		return 0, 0, 0, nil
	}
	return db.wal.CheckpointFull()
}

func (db *Database) CheckpointTruncate() (int, int, int, error) {
	if db.wal == nil {
		return 0, 0, 0, nil
	}
	return db.wal.CheckpointTruncate()
}

func (db *Database) GetWALSize() int64 {
	if db.wal != nil {
		return db.wal.Size()
	}
	return 0
}

func (db *Database) GetAutoCheckpointN() int {
	return db.autoCheckpointN
}

// StartAutoCheckpoint implements pragma.Ctx. It delegates to the unexported
// startAutoCheckpoint so that the method is accessible via the interface while
// keeping the core WAL logic encapsulated in database.go.
func (db *Database) StartAutoCheckpoint(n int) {
	db.startAutoCheckpoint(n)
}

// --- Transaction ---

func (db *Database) GetIsolationLevel() string {
	return db.isolationConfig.GetIsolationLevel()
}

func (db *Database) SetIsolationLevel(level string) error {
	return db.isolationConfig.SetIsolationLevel(level)
}

func (db *Database) GetBusyTimeout() int {
	return db.isolationConfig.BusyTimeout
}

func (db *Database) SetBusyTimeout(ms int) {
	db.isolationConfig.BusyTimeout = ms
}

// --- Vacuum ---

func (db *Database) ClearCaches() {
	if db.cache != nil {
		db.cache.Clear()
	}
	if db.queryCache != nil {
		db.queryCache.Invalidate()
	}
	if db.planCache != nil {
		db.planCache.Invalidate()
	}
	runtime.GC()
}

func (db *Database) RunAnalyze() error {
	_, err := db.handleAnalyze(&QP.AnalyzeStmt{})
	return err
}

func (db *Database) CheckDBIntegrity() ([]string, error) {
	report, err := db.CheckIntegrity()
	if err != nil {
		return nil, err
	}
	return report.Errors, nil
}

func (db *Database) GetDBPath() string {
	return db.dbPath
}

func (db *Database) HasAnyTables() bool {
	return len(db.tables) > 0
}

// --- Query limits ---

func (db *Database) GetQueryTimeoutMs() int64 {
	return db.queryTimeoutMs
}

func (db *Database) SetQueryTimeoutMs(ms int64) {
	db.queryTimeoutMs = ms
}

func (db *Database) GetMaxMemoryBytes() int64 {
	return db.maxMemoryBytes
}

func (db *Database) SetMaxMemoryBytes(bytes int64) {
	db.maxMemoryBytes = bytes
}

func (db *Database) GetQueryCacheMax() int {
	return db.queryCacheMax
}

func (db *Database) SetQueryCacheMax(n int) {
	db.queryCacheMax = n
	db.queryCache = newQueryResultCache(n)
}
