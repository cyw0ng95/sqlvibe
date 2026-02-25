package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// handleVacuum handles VACUUM [INTO 'path'].
func (db *Database) handleVacuum(stmt *QP.VacuumStmt) (*Rows, error) {
	if stmt.DestPath == "" {
		// In-place vacuum: for in-memory DB this is a no-op.
		return nil, nil
	}
	// VACUUM INTO: delegate to the full backup path.
	return db.execFullBackup(stmt.DestPath)
}

// handleAnalyze handles ANALYZE [target].
func (db *Database) handleAnalyze(stmt *QP.AnalyzeStmt) (*Rows, error) {
	if db.tableStats == nil {
		db.tableStats = make(map[string]int64)
	}
	if stmt.Target == "" {
		for tableName := range db.tables {
			db.tableStats[tableName] = db.getTableRowCount(tableName)
		}
	} else {
		db.tableStats[stmt.Target] = db.getTableRowCount(stmt.Target)
	}
	return nil, nil
}

func (db *Database) getTableRowCount(tableName string) int64 {
	rows, ok := db.data[tableName]
	if !ok {
		return 0
	}
	return int64(len(rows))
}

// handleReindex handles REINDEX [target].
// If target is empty, all indexes are rebuilt. If target matches a table name,
// all indexes on that table are rebuilt. Otherwise, the named index is rebuilt.
func (db *Database) handleReindex(stmt *QP.ReindexStmt) (*Rows, error) {
	if stmt.Target == "" {
		// Rebuild all indexes.
		for idxName := range db.indexes {
			db.buildIndexData(idxName)
		}
		return nil, nil
	}
	// Rebuild indexes matching the target (by index name or table name).
	target := strings.ToLower(stmt.Target)
	rebuilt := false
	for idxName, idx := range db.indexes {
		if strings.ToLower(idxName) == target || strings.ToLower(idx.Table) == target {
			db.buildIndexData(idxName)
			rebuilt = true
		}
	}
	if !rebuilt {
		// Target not found — silently succeed (matches SQLite behavior).
	}
	return nil, nil
}

// querySqliteStat1 returns the collected ANALYZE statistics.
func (db *Database) querySqliteStat1() (*Rows, error) {
	cols := []string{"tbl", "idx", "stat"}
	data := make([][]interface{}, 0)
	for tbl, count := range db.tableStats {
		data = append(data, []interface{}{tbl, nil, fmt.Sprintf("%d", count)})
	}
	for idxName, idx := range db.indexes {
		cnt := db.tableStats[idx.Table]
		data = append(data, []interface{}{idx.Table, idxName, fmt.Sprintf("%d", cnt)})
	}
	return &Rows{Columns: cols, Data: data}, nil
}
