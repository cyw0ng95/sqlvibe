package sqlvibe

import (
	"fmt"
	"os"
	"runtime"
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
	case "index_info":
		return db.pragmaIndexInfo(stmt)
	case "foreign_key_list":
		return db.pragmaForeignKeyList(stmt)
	case "function_list":
		return db.pragmaFunctionList()
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
	case "foreign_keys":
		return db.pragmaForeignKeys(stmt)
	case "encoding":
		return &Rows{Columns: []string{"encoding"}, Data: [][]interface{}{{"UTF-8"}}}, nil
	case "collation_list":
		return &Rows{
			Columns: []string{"seq", "name"},
			Data:    [][]interface{}{{int64(0), "BINARY"}, {int64(1), "NOCASE"}, {int64(2), "RTRIM"}},
		}, nil
	case "sqlite_sequence":
		return db.pragmaSQLiteSequence()
	case "page_size":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["page_size"] = val
			return &Rows{Columns: []string{"page_size"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("page_size", 4096)
		return &Rows{Columns: []string{"page_size"}, Data: [][]interface{}{{v}}}, nil
	case "mmap_size":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["mmap_size"] = val
			return &Rows{Columns: []string{"mmap_size"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("mmap_size", 0)
		return &Rows{Columns: []string{"mmap_size"}, Data: [][]interface{}{{v}}}, nil
	case "locking_mode":
		if stmt.Value != nil {
			val := strings.ToUpper(pragmaStrValue(stmt.Value))
			db.pragmaSettings["locking_mode"] = val
			return &Rows{Columns: []string{"locking_mode"}, Data: [][]interface{}{{strings.ToLower(val)}}}, nil
		}
		v := db.getPragmaStr("locking_mode", "normal")
		return &Rows{Columns: []string{"locking_mode"}, Data: [][]interface{}{{strings.ToLower(v)}}}, nil
	case "synchronous":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["synchronous"] = val
			return &Rows{Columns: []string{"synchronous"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("synchronous", 2)
		return &Rows{Columns: []string{"synchronous"}, Data: [][]interface{}{{v}}}, nil
	case "auto_vacuum":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["auto_vacuum"] = val
			return &Rows{Columns: []string{"auto_vacuum"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("auto_vacuum", 0)
		return &Rows{Columns: []string{"auto_vacuum"}, Data: [][]interface{}{{v}}}, nil
	case "query_only":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["query_only"] = val
			return &Rows{Columns: []string{"query_only"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("query_only", 0)
		return &Rows{Columns: []string{"query_only"}, Data: [][]interface{}{{v}}}, nil
	case "temp_store":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["temp_store"] = val
			return &Rows{Columns: []string{"temp_store"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("temp_store", 0)
		return &Rows{Columns: []string{"temp_store"}, Data: [][]interface{}{{v}}}, nil
	case "read_uncommitted":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["read_uncommitted"] = val
			return &Rows{Columns: []string{"read_uncommitted"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("read_uncommitted", 0)
		return &Rows{Columns: []string{"read_uncommitted"}, Data: [][]interface{}{{v}}}, nil
	case "cache_spill":
		if stmt.Value != nil {
			val := pragmaIntValue(stmt.Value)
			db.pragmaSettings["cache_spill"] = val
			return &Rows{Columns: []string{"cache_spill"}, Data: [][]interface{}{{val}}}, nil
		}
		v := db.getPragmaInt("cache_spill", 1)
		return &Rows{Columns: []string{"cache_spill"}, Data: [][]interface{}{{v}}}, nil
	// v0.9.10 additions
	case "wal_autocheckpoint":
		return db.pragmaWALAutoCheckpoint(stmt)
	case "shrink_memory":
		return db.pragmaShrinkMemory()
	case "optimize":
		return db.pragmaOptimize()
	case "integrity_check":
		return db.pragmaIntegrityCheck()
	case "quick_check":
		return db.pragmaQuickCheck()
	case "journal_size_limit":
		return db.pragmaJournalSizeLimit(stmt)
	case "cache_grind":
		return db.pragmaCacheGrind()
	// v0.9.13 additions
	case "query_timeout":
		return db.pragmaQueryTimeout(stmt)
	case "max_memory":
		return db.pragmaMaxMemory(stmt)
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

func (db *Database) pragmaIndexInfo(stmt *QP.PragmaStmt) (*Rows, error) {
	var idxName string
	if stmt.Value != nil {
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				idxName = s
			}
		case *QP.ColumnRef:
			idxName = v.Name
		}
	}
	columns := []string{"seqno", "cid", "name"}
	if idxName == "" {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	idx, exists := db.indexes[idxName]
	if !exists {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	data := make([][]interface{}, 0, len(idx.Columns))
	// Build column-id lookup from the table schema
	colOrderMap := make(map[string]int)
	if colOrder, ok := db.columnOrder[idx.Table]; ok {
		for i, c := range colOrder {
			colOrderMap[c] = i
		}
	}
	for seqno, colName := range idx.Columns {
		cid := int64(-1) // expression index
		if id, ok := colOrderMap[colName]; ok {
			cid = int64(id)
		}
		data = append(data, []interface{}{int64(seqno), cid, colName})
	}
	return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) pragmaForeignKeyList(stmt *QP.PragmaStmt) (*Rows, error) {
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
	columns := []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
	if tableName == "" {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	fks, ok := db.foreignKeys[tableName]
	if !ok {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	data := make([][]interface{}, 0)
	for id, fk := range fks {
		onUpdate := actionName(fk.OnUpdate)
		onDelete := actionName(fk.OnDelete)
		for seq, fromCol := range fk.ChildColumns {
			toCol := ""
			if seq < len(fk.ParentColumns) {
				toCol = fk.ParentColumns[seq]
			}
			data = append(data, []interface{}{
				int64(id), int64(seq), fk.ParentTable,
				fromCol, toCol, onUpdate, onDelete, "NONE",
			})
		}
	}
	return &Rows{Columns: columns, Data: data}, nil
}

// actionName converts a ReferenceAction to its SQLite string representation.
func actionName(a QP.ReferenceAction) string {
	switch a {
	case QP.ReferenceCascade:
		return "CASCADE"
	case QP.ReferenceSetNull:
		return "SET NULL"
	case QP.ReferenceSetDefault:
		return "SET DEFAULT"
	case QP.ReferenceRestrict:
		return "RESTRICT"
	default:
		return "NO ACTION"
	}
}

// pragmaFunctionList returns the list of built-in scalar functions.
func (db *Database) pragmaFunctionList() (*Rows, error) {
	columns := []string{"name"}
	names := []string{
		"abs", "ceil", "coalesce", "count", "date", "datetime",
		"floor", "glob", "hex", "ifnull", "iif", "instr",
		"julianday", "last_insert_rowid", "length", "like", "lower",
		"ltrim", "max", "min", "nullif", "printf", "quote",
		"random", "randomblob", "replace", "round", "rtrim",
		"sign", "soundex", "sqrt", "strftime", "substr", "substring",
		"time", "total_changes", "trim", "typeof", "unhex",
		"unicode", "unixepoch", "upper", "zeroblob",
	}
	data := make([][]interface{}, 0, len(names))
	for _, n := range names {
		data = append(data, []interface{}{n})
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

	// Determine checkpoint mode: passive (default), full, or truncate.
	mode := "passive"
	if stmt.Value != nil {
		mode = strings.ToLower(pragmaStrValue(stmt.Value))
	}

	var busy, logRemoved, checkpointed int
	var err error
	switch mode {
	case "truncate":
		busy, logRemoved, checkpointed, err = db.wal.CheckpointTruncate()
	case "full":
		busy, logRemoved, checkpointed, err = db.wal.CheckpointFull()
	default: // passive
		moved, cpErr := db.wal.Checkpoint()
		if cpErr != nil {
			return nil, fmt.Errorf("PRAGMA wal_checkpoint: %w", cpErr)
		}
		busy, logRemoved, checkpointed = 0, moved, moved
	}
	if err != nil {
		return nil, fmt.Errorf("PRAGMA wal_checkpoint: %w", err)
	}

	return &Rows{
		Columns: []string{"busy", "log", "checkpointed"},
		Data:    [][]interface{}{{int64(busy), int64(logRemoved), int64(checkpointed)}},
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

// pragmaForeignKeys handles PRAGMA foreign_keys and PRAGMA foreign_keys = ON/OFF.
func (db *Database) pragmaForeignKeys(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value == nil {
val := int64(0)
if db.foreignKeysEnabled {
val = 1
}
return &Rows{
Columns: []string{"foreign_keys"},
Data:    [][]interface{}{{val}},
}, nil
}
var enable bool
switch v := stmt.Value.(type) {
case *QP.Literal:
switch val := v.Value.(type) {
case int64:
enable = val != 0
case float64:
enable = val != 0
case bool:
enable = val
default:
s := strings.ToUpper(fmt.Sprintf("%v", val))
enable = s == "ON" || s == "1" || s == "TRUE"
}
case *QP.ColumnRef:
s := strings.ToUpper(v.Name)
enable = s == "ON" || s == "1" || s == "TRUE"
default:
return nil, fmt.Errorf("PRAGMA foreign_keys: unsupported value %T", stmt.Value)
}
db.foreignKeysEnabled = enable
// SQLite returns no rows when setting a pragma value
return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
}

// pragmaSQLiteSequence returns the current autoincrement sequences.
func (db *Database) pragmaSQLiteSequence() (*Rows, error) {
columns := []string{"name", "seq"}
data := make([][]interface{}, 0)
for tableName, seq := range db.seqValues {
data = append(data, []interface{}{tableName, seq})
}
return &Rows{Columns: columns, Data: data}, nil
}

func (db *Database) getPragmaInt(name string, defaultVal int64) int64 {
	if v, ok := db.pragmaSettings[name]; ok {
		if n, ok := v.(int64); ok {
			return n
		}
	}
	return defaultVal
}

func (db *Database) getPragmaStr(name string, defaultVal string) string {
	if v, ok := db.pragmaSettings[name]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultVal
}

func pragmaIntValue(expr QP.Expr) int64 {
	switch v := expr.(type) {
	case *QP.Literal:
		if n, ok := v.Value.(int64); ok {
			return n
		}
		if f, ok := v.Value.(float64); ok {
			return int64(f)
		}
		if s, ok := v.Value.(string); ok {
			switch strings.ToUpper(s) {
			case "ON", "FULL", "EXCLUSIVE":
				return 1
			case "OFF", "NONE", "NORMAL":
				return 0
			}
		}
	case *QP.ColumnRef:
		switch strings.ToUpper(v.Name) {
		case "ON", "FULL", "EXCLUSIVE":
			return 1
		case "OFF", "NONE", "NORMAL":
			return 0
		}
	}
	return 0
}

func pragmaStrValue(expr QP.Expr) string {
	switch v := expr.(type) {
	case *QP.Literal:
		return fmt.Sprintf("%v", v.Value)
	case *QP.ColumnRef:
		return v.Name
	}
	return ""
}

// --- v0.9.10 new PRAGMAs ---

// pragmaWALAutoCheckpoint handles PRAGMA wal_autocheckpoint and
// PRAGMA wal_autocheckpoint = N.  When N is set, a background goroutine
// automatically checkpoints the WAL after N frames accumulate.
// N = 0 disables the background auto-checkpoint.
func (db *Database) pragmaWALAutoCheckpoint(stmt *QP.PragmaStmt) (*Rows, error) {
	if stmt.Value == nil {
		n := int64(db.autoCheckpointN)
		if n == 0 {
			n = 1000 // SQLite default
		}
		return &Rows{Columns: []string{"wal_autocheckpoint"}, Data: [][]interface{}{{n}}}, nil
	}
	n := pragmaIntValue(stmt.Value)
	db.startAutoCheckpoint(int(n))
	return &Rows{Columns: []string{"wal_autocheckpoint"}, Data: [][]interface{}{{n}}}, nil
}

// pragmaShrinkMemory releases unused memory from page cache, plan cache, and
// query result cache, and requests a Go garbage collection pass.
func (db *Database) pragmaShrinkMemory() (*Rows, error) {
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
	return &Rows{Columns: []string{"shrink_memory"}, Data: [][]interface{}{{int64(0)}}}, nil
}

// pragmaOptimize delegates to the ANALYZE implementation to refresh
// query-planner statistics for all tables.
func (db *Database) pragmaOptimize() (*Rows, error) {
	_, err := db.handleAnalyze(&QP.AnalyzeStmt{})
	if err != nil {
		return nil, fmt.Errorf("PRAGMA optimize: %w", err)
	}
	return &Rows{Columns: []string{"optimize"}, Data: [][]interface{}{{"ok"}}}, nil
}

// pragmaIntegrityCheck performs a full database integrity check.
// Returns "ok" when no problems are found, or a list of error messages.
func (db *Database) pragmaIntegrityCheck() (*Rows, error) {
	report, err := db.CheckIntegrity()
	if err != nil {
		return nil, fmt.Errorf("PRAGMA integrity_check: %w", err)
	}
	rows := make([][]interface{}, 0, len(report.Errors)+1)
	if len(report.Errors) == 0 {
		rows = append(rows, []interface{}{"ok"})
	} else {
		for _, msg := range report.Errors {
			rows = append(rows, []interface{}{msg})
		}
	}
	return &Rows{Columns: []string{"integrity_check"}, Data: rows}, nil
}

// pragmaQuickCheck performs a fast header and file-size sanity check.
// Returns "ok" when the basic structure looks valid.
func (db *Database) pragmaQuickCheck() (*Rows, error) {
	if db.dbPath != ":memory:" {
		fi, err := os.Stat(db.dbPath)
		if err != nil {
			return &Rows{
				Columns: []string{"quick_check"},
				Data:    [][]interface{}{{"file not found: " + db.dbPath}},
			}, nil
		}
		if fi.Size() == 0 && len(db.tables) > 0 {
			return &Rows{
				Columns: []string{"quick_check"},
				Data:    [][]interface{}{{"empty file with in-memory tables"}},
			}, nil
		}
	}
	return &Rows{Columns: []string{"quick_check"}, Data: [][]interface{}{{"ok"}}}, nil
}

// pragmaJournalSizeLimit handles PRAGMA journal_size_limit and
// PRAGMA journal_size_limit = N.  N is the maximum WAL size in bytes;
// -1 means unlimited (SQLite default).
func (db *Database) pragmaJournalSizeLimit(stmt *QP.PragmaStmt) (*Rows, error) {
	if stmt.Value == nil {
		v := db.getPragmaInt("journal_size_limit", -1)
		return &Rows{Columns: []string{"journal_size_limit"}, Data: [][]interface{}{{v}}}, nil
	}
	val := pragmaIntValue(stmt.Value)
	db.pragmaSettings["journal_size_limit"] = val
	// If WAL is active and the limit is exceeded, truncate via checkpoint.
	if db.wal != nil && val >= 0 {
		if sz := db.wal.Size(); sz > val {
			_, _ = db.wal.Checkpoint()
		}
	}
	return &Rows{Columns: []string{"journal_size_limit"}, Data: [][]interface{}{{val}}}, nil
}

// pragmaCacheGrind returns detailed cache statistics.
func (db *Database) pragmaCacheGrind() (*Rows, error) {
	cols := []string{"pages_cached", "pages_free", "hits", "misses"}
	// The DS cache does not expose hit/miss counters yet; return what we can.
	var pagesCached, pagesFree int64
	if db.pm != nil {
		m := DS.CollectMetrics(db.pm, 0)
		pagesCached = int64(m.UsedPages)
		pagesFree = int64(m.FreePages)
	}
	return &Rows{
		Columns: cols,
		Data: [][]interface{}{{pagesCached, pagesFree, int64(0), int64(0)}},
	}, nil
}


// pragmaQueryTimeout handles PRAGMA query_timeout = N (milliseconds).
// A value of 0 disables the per-query default timeout.
func (db *Database) pragmaQueryTimeout(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value != nil {
val := pragmaIntValue(stmt.Value)
if val < 0 {
val = 0
}
db.queryTimeoutMs = int64(val)
return &Rows{Columns: []string{"query_timeout"}, Data: [][]interface{}{{int64(val)}}}, nil
}
return &Rows{Columns: []string{"query_timeout"}, Data: [][]interface{}{{db.queryTimeoutMs}}}, nil
}

// pragmaMaxMemory handles PRAGMA max_memory = N (bytes).
// A value of 0 disables the result-set memory limit.
func (db *Database) pragmaMaxMemory(stmt *QP.PragmaStmt) (*Rows, error) {
if stmt.Value != nil {
val := pragmaIntValue(stmt.Value)
if val < 0 {
val = 0
}
db.maxMemoryBytes = int64(val)
return &Rows{Columns: []string{"max_memory"}, Data: [][]interface{}{{int64(val)}}}, nil
}
return &Rows{Columns: []string{"max_memory"}, Data: [][]interface{}{{db.maxMemoryBytes}}}, nil
}
