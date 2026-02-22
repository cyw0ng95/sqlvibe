package sqlvibe

import (
	"fmt"
	"os"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/TM"
)

func (db *Database) handlePragma(stmt *QP.PragmaStmt) (*Rows, error) {
	switch stmt.Name {
	case "cache_size":
		return db.pragmaCacheSize(stmt)
	case "table_info":
		return db.pragmaTableInfo(stmt)
	case "index_list":
		return db.pragmaIndexList(stmt)
	case "database_list":
		return db.pragmaDatabaseList()
	case "journal_mode":
		return db.pragmaJournalMode(stmt)
	case "wal_checkpoint":
		return db.pragmaWALCheckpoint(stmt)
	case "wal_mode":
		return db.pragmaWALMode(stmt)
	case "isolation_level":
		return db.pragmaIsolationLevel(stmt)
	case "busy_timeout":
		return db.pragmaBusyTimeout(stmt)
	case "compression":
		return db.pragmaCompression(stmt)
	case "storage_info":
		return db.pragmaStorageInfo()
	default:
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}
}

func (db *Database) pragmaTableInfo(stmt *QP.PragmaStmt) (*Rows, error) {
	var tableName string
	if stmt.Value != nil {
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				tableName = s
			}
		case *QP.ColumnRef:
			tableName = v.Name
		}
	}

	if tableName == "" {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	schema, exists := db.tables[tableName]
	if !exists {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	columns := []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	data := make([][]interface{}, 0)
	colOrder := db.columnOrder[tableName]
	pkCols := db.primaryKeys[tableName]

	for i, colName := range colOrder {
		colType := schema[colName]
		isPK := int64(0)
		for _, pk := range pkCols {
			if pk == colName {
				isPK = 1
				break
			}
		}
		data = append(data, []interface{}{int64(i), colName, colType, int64(0), nil, isPK})
	}

	return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) pragmaIndexList(stmt *QP.PragmaStmt) (*Rows, error) {
	var tableName string
	if stmt.Value != nil {
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				tableName = s
			}
		case *QP.ColumnRef:
			tableName = v.Name
		}
	}

	if tableName == "" {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	columns := []string{"seq", "name", "unique", "origin", "partial"}
	data := make([][]interface{}, 0)

	seq := 0
	for _, idx := range db.indexes {
		if idx.Table == tableName {
			unique := int64(0)
			if idx.Unique {
				unique = 1
			}
			data = append(data, []interface{}{int64(seq), idx.Name, unique, "c", int64(0)})
			seq++
		}
	}

	return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) pragmaDatabaseList() (*Rows, error) {
	rows := [][]interface{}{
		{int64(0), "main", db.dbPath},
	}
	return &Rows{Columns: []string{"seq", "name", "file"}, Data: rows}, nil
}

// pragmaCacheSize handles PRAGMA cache_size and PRAGMA cache_size = N.
// Positive N is a page count; negative N is a size in KiB (SQLite convention).
func (db *Database) pragmaCacheSize(stmt *QP.PragmaStmt) (*Rows, error) {
	if stmt.Value == nil {
		// Read current capacity - return the current number of cached pages.
		size := db.cache.Size()
		return &Rows{
			Columns: []string{"cache_size"},
			Data:    [][]interface{}{{int64(size)}},
		}, nil
	}

	// Set new capacity.
	var n int
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		switch val := v.Value.(type) {
		case int64:
			if val > int64(^uint(0)>>1) || val < -int64(^uint(0)>>1)-1 {
				return nil, fmt.Errorf("PRAGMA cache_size: value %d out of range", val)
			}
			n = int(val)
		case float64:
			n = int(val)
		default:
			return nil, fmt.Errorf("PRAGMA cache_size: unsupported value type %T", v.Value)
		}
	default:
		return nil, fmt.Errorf("PRAGMA cache_size: unsupported value %T", stmt.Value)
	}

	db.cache.SetCapacity(n)
	return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
}

func (db *Database) pragmaJournalMode(stmt *QP.PragmaStmt) (*Rows, error) {
	if stmt.Value == nil {
		// Return current mode
		return &Rows{
			Columns: []string{"journal_mode"},
			Data:    [][]interface{}{{db.journalMode}},
		}, nil
	}

	// Parse requested mode
	var mode string
	switch v := stmt.Value.(type) {
	case *QP.Literal:
		if s, ok := v.Value.(string); ok {
			mode = strings.ToLower(s)
		}
	case *QP.ColumnRef:
		mode = strings.ToLower(v.Name)
	default:
		return nil, fmt.Errorf("PRAGMA journal_mode: unsupported value %T", stmt.Value)
	}

	switch mode {
	case "wal":
		if db.journalMode == "wal" {
			break
		}
		if db.dbPath != ":memory:" {
			walPath := db.dbPath + "-wal"
			// Close any existing WAL before opening a new one
			if db.wal != nil {
				_ = db.wal.Close()
				db.wal = nil
			}
			wal, err := TM.OpenWAL(walPath, db.pm.PageSize())
			if err != nil {
				return nil, fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
			}
			if err := db.txMgr.EnableWAL(walPath, db.pm.PageSize()); err != nil {
				_ = wal.Close()
				return nil, fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
			}
			db.wal = wal
		}
		db.journalMode = "wal"

	case "delete":
		if db.journalMode == "wal" {
			// Checkpoint before switching back to delete mode.
			// Errors during checkpoint and cleanup are intentionally suppressed
			// here: we still want to complete the mode switch even if the WAL
			// is in a partially-written state.
			if db.wal != nil {
				_, _ = db.wal.Checkpoint()
				_ = db.wal.Close()
				db.wal = nil
				_ = db.txMgr.DisableWAL()
				if db.dbPath != ":memory:" {
					_ = os.Remove(db.dbPath + "-wal")
				}
			}
		}
		db.journalMode = "delete"

	default:
		return nil, fmt.Errorf("PRAGMA journal_mode: unsupported mode %q", mode)
	}

	return &Rows{
		Columns: []string{"journal_mode"},
		Data:    [][]interface{}{{db.journalMode}},
	}, nil
}

func (db *Database) pragmaWALCheckpoint(stmt *QP.PragmaStmt) (*Rows, error) {
	if db.wal == nil {
		// Not in WAL mode - return zeroes like SQLite does
		return &Rows{
			Columns: []string{"busy", "log", "checkpointed"},
			Data:    [][]interface{}{{int64(0), int64(0), int64(0)}},
		}, nil
	}

	moved, err := db.wal.Checkpoint()
	if err != nil {
		return nil, fmt.Errorf("PRAGMA wal_checkpoint: %w", err)
	}

	return &Rows{
		Columns: []string{"busy", "log", "checkpointed"},
		Data:    [][]interface{}{{int64(0), int64(moved), int64(moved)}},
	}, nil
}

// pragmaWALMode handles PRAGMA wal_mode and PRAGMA wal_mode = ON/OFF.
// wal_mode is an alias for journal_mode focused specifically on WAL toggle.
func (db *Database) pragmaWALMode(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value == nil {
mode := "off"
if db.journalMode == "wal" {
mode = "on"
}
return &Rows{
Columns: []string{"wal_mode"},
Data:    [][]interface{}{{mode}},
}, nil
}
var val string
switch v := stmt.Value.(type) {
case *QP.Literal:
if s, ok := v.Value.(string); ok {
val = strings.ToLower(s)
}
case *QP.ColumnRef:
val = strings.ToLower(v.Name)
default:
return nil, fmt.Errorf("PRAGMA wal_mode: unsupported value %T", stmt.Value)
}
synthetic := &QP.PragmaStmt{Name: "journal_mode", Value: &QP.ColumnRef{Name: func() string {
if val == "on" {
return "wal"
}
return "delete"
}()}}
return db.pragmaJournalMode(synthetic)
}

// pragmaIsolationLevel handles PRAGMA isolation_level and PRAGMA isolation_level = LEVEL.
func (db *Database) pragmaIsolationLevel(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value == nil {
return &Rows{
Columns: []string{"isolation_level"},
Data:    [][]interface{}{{db.isolationConfig.GetIsolationLevel()}},
}, nil
}
var val string
switch v := stmt.Value.(type) {
case *QP.Literal:
if s, ok := v.Value.(string); ok {
val = s
}
case *QP.ColumnRef:
val = v.Name
default:
return nil, fmt.Errorf("PRAGMA isolation_level: unsupported value %T", stmt.Value)
}
if err := db.isolationConfig.SetIsolationLevel(val); err != nil {
return nil, fmt.Errorf("PRAGMA isolation_level: %w", err)
}
return &Rows{
Columns: []string{"isolation_level"},
Data:    [][]interface{}{{db.isolationConfig.GetIsolationLevel()}},
}, nil
}

// pragmaBusyTimeout handles PRAGMA busy_timeout and PRAGMA busy_timeout = N (ms).
func (db *Database) pragmaBusyTimeout(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value == nil {
return &Rows{
Columns: []string{"busy_timeout"},
Data:    [][]interface{}{{int64(db.isolationConfig.BusyTimeout)}},
}, nil
}
var ms int64
switch v := stmt.Value.(type) {
case *QP.Literal:
switch val := v.Value.(type) {
case int64:
ms = val
case float64:
ms = int64(val)
default:
return nil, fmt.Errorf("PRAGMA busy_timeout: unsupported value type %T", v.Value)
}
default:
return nil, fmt.Errorf("PRAGMA busy_timeout: unsupported value %T", stmt.Value)
}
if ms < 0 {
return nil, fmt.Errorf("PRAGMA busy_timeout: value must be >= 0")
}
db.isolationConfig.BusyTimeout = int(ms)
return &Rows{
Columns: []string{"busy_timeout"},
Data:    [][]interface{}{{ms}},
}, nil
}

// pragmaCompression handles PRAGMA compression and PRAGMA compression = NAME.
func (db *Database) pragmaCompression(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value == nil {
return &Rows{
Columns: []string{"compression"},
Data:    [][]interface{}{{db.compressionName}},
}, nil
}
var name string
switch v := stmt.Value.(type) {
case *QP.Literal:
if s, ok := v.Value.(string); ok {
name = strings.ToUpper(s)
}
case *QP.ColumnRef:
name = strings.ToUpper(v.Name)
default:
return nil, fmt.Errorf("PRAGMA compression: unsupported value %T", stmt.Value)
}
// Validate by attempting to create the compressor.
if _, err := DS.NewCompressor(name, 0); err != nil {
return nil, fmt.Errorf("PRAGMA compression: %w", err)
}
db.compressionName = name
return &Rows{
Columns: []string{"compression"},
Data:    [][]interface{}{{name}},
}, nil
}

// pragmaStorageInfo returns storage statistics.
func (db *Database) pragmaStorageInfo() (*Rows, error) {
var walSize int64
if db.wal != nil {
walSize = db.wal.Size()
}
m := DS.CollectMetrics(db.pm, walSize)
m.TotalTables = len(db.tables)
for _, rows := range db.data {
m.TotalRows += len(rows)
}
columns := []string{"page_count", "used_pages", "free_pages", "compression_ratio", "wal_size", "total_rows", "total_tables"}
row := []interface{}{
int64(m.PageCount),
int64(m.UsedPages),
int64(m.FreePages),
m.CompressionRatio,
m.WALSize,
int64(m.TotalRows),
int64(m.TotalTables),
}
return &Rows{Columns: columns, Data: [][]interface{}{row}}, nil
}
