package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/pragma"
)

// wrapPragmaResult converts pragma handler results to *Rows.
func wrapPragmaResult(cols []string, data [][]interface{}, err error) (*Rows, error) {
	if err != nil {
		return nil, err
	}
	return &Rows{Columns: cols, Data: data}, nil
}

func (db *Database) handlePragma(stmt *QP.PragmaStmt) (*Rows, error) {
	switch stmt.Name {
	// --- Info pragmas (require Database internals directly) ---
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
	case "sqlite_sequence":
		return db.pragmaSQLiteSequence()
	case "foreign_keys":
		return db.pragmaForeignKeys(stmt)
	case "table_list":
		return db.pragmaTableList()
	case "index_xinfo":
		return db.pragmaIndexXInfo(stmt)
	case "foreign_key_check":
		return db.pragmaForeignKeyCheck(stmt)
	case "encoding":
		return &Rows{Columns: []string{"encoding"}, Data: [][]interface{}{{"UTF-8"}}}, nil
	case "collation_list":
		return &Rows{
			Columns: []string{"seq", "name"},
			Data:    [][]interface{}{{int64(0), "BINARY"}, {int64(1), "NOCASE"}, {int64(2), "RTRIM"}},
		}, nil

	// --- Cache pragmas ---
	case "cache_size":
		return wrapPragmaResult(pragma.HandleCacheSize(db, stmt))
	case "cache_memory":
		return wrapPragmaResult(pragma.HandleCacheMemory(db, stmt))
	case "cache_spill":
		return wrapPragmaResult(pragma.HandleCacheSpill(db, stmt))
	case "cache_grind":
		return wrapPragmaResult(pragma.HandleCacheGrind(db))
	case "cache_plan":
		return wrapPragmaResult(pragma.HandleCachePlan(db, stmt))

	// --- Storage pragmas ---
	case "page_size":
		return wrapPragmaResult(pragma.HandlePageSize(db, stmt))
	case "mmap_size":
		return wrapPragmaResult(pragma.HandleMmapSize(db, stmt))
	case "storage_info":
		return wrapPragmaResult(pragma.HandleStorageInfo(db))
	case "memory_stats":
		return wrapPragmaResult(pragma.HandleMemoryStats(db))
	case "memory_status":
		return wrapPragmaResult(pragma.HandleMemoryStatus())
	case "heap_limit":
		return wrapPragmaResult(pragma.HandleHeapLimit(db, stmt))
	case "compression":
		return wrapPragmaResult(pragma.HandleCompression(db, stmt))

	// --- WAL pragmas ---
	case "journal_mode":
		return wrapPragmaResult(pragma.HandleJournalMode(db, stmt, db.dbPath))
	case "wal_checkpoint":
		return wrapPragmaResult(pragma.HandleWALCheckpoint(db, stmt))
	case "wal_mode":
		return wrapPragmaResult(pragma.HandleWALMode(db, stmt, db.dbPath))
	case "wal_autocheckpoint":
		return wrapPragmaResult(pragma.HandleWALAutoCheckpoint(db, stmt))
	case "wal_truncate":
		return wrapPragmaResult(pragma.HandleWALTruncate(db, stmt))

	// --- Vacuum/maintenance pragmas ---
	case "auto_vacuum":
		return wrapPragmaResult(pragma.HandleAutoVacuum(db, stmt))
	case "incremental_vacuum":
		return wrapPragmaResult(pragma.HandleIncrementalVacuum(db, stmt))
	case "freelist_count":
		return wrapPragmaResult(pragma.HandleFreelistCount(db))
	case "page_count":
		return wrapPragmaResult(pragma.HandlePageCount(db))
	case "shrink_memory":
		return wrapPragmaResult(pragma.HandleShrinkMemory(db))
	case "optimize":
		return wrapPragmaResult(pragma.HandleOptimize(db))
	case "integrity_check":
		return wrapPragmaResult(pragma.HandleIntegrityCheck(db))
	case "quick_check":
		return wrapPragmaResult(pragma.HandleQuickCheck(db))
	case "journal_size_limit":
		return wrapPragmaResult(pragma.HandleJournalSizeLimit(db, stmt))

	// --- Transaction pragmas ---
	case "isolation_level":
		return wrapPragmaResult(pragma.HandleIsolationLevel(db, stmt))
	case "busy_timeout":
		return wrapPragmaResult(pragma.HandleBusyTimeout(db, stmt))
	case "query_timeout":
		return wrapPragmaResult(pragma.HandleQueryTimeout(db, stmt))
	case "max_memory":
		return wrapPragmaResult(pragma.HandleMaxMemory(db, stmt))
	case "query_cache_size":
		return wrapPragmaResult(pragma.HandleQueryCacheSize(db, stmt))

	// --- Compat pragmas ---
	case "locking_mode":
		return wrapPragmaResult(pragma.HandleLockingMode(db, stmt))
	case "synchronous":
		return wrapPragmaResult(pragma.HandleSynchronous(db, stmt))
	case "query_only":
		return wrapPragmaResult(pragma.HandleQueryOnly(db, stmt))
	case "temp_store":
		return wrapPragmaResult(pragma.HandleTempStore(db, stmt))
	case "read_uncommitted":
		return wrapPragmaResult(pragma.HandleReadUncommitted(db, stmt))
	case "max_rows":
		return wrapPragmaResult(pragma.HandleMaxRows(db, stmt))

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
	colOrderMap := make(map[string]int)
	if colOrder, ok := db.columnOrder[idx.Table]; ok {
		for i, c := range colOrder {
			colOrderMap[c] = i
		}
	}
	for seqno, colName := range idx.Columns {
		cid := int64(-1)
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
	return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
}

func (db *Database) pragmaSQLiteSequence() (*Rows, error) {
	columns := []string{"name", "seq"}
	data := make([][]interface{}, 0)
	for tableName, seq := range db.seqValues {
		data = append(data, []interface{}{tableName, seq})
	}
	return &Rows{Columns: columns, Data: data}, nil
}

// pragmaTableList returns a row for every table (and view) in the database.
// Columns: schema, name, type, ncol, wr, strict
func (db *Database) pragmaTableList() (*Rows, error) {
	columns := []string{"schema", "name", "type", "ncol", "wr", "strict"}
	data := make([][]interface{}, 0)
	for tbl, cols := range db.tables {
		data = append(data, []interface{}{"main", tbl, "table", int64(len(cols)), int64(0), int64(0)})
	}
	for viewName := range db.views {
		data = append(data, []interface{}{"main", viewName, "view", int64(0), int64(0), int64(0)})
	}
	return &Rows{Columns: columns, Data: data}, nil
}

// pragmaIndexXInfo returns extended info for an index.
// Columns: seqno, cid, name, desc, coll, key
func (db *Database) pragmaIndexXInfo(stmt *QP.PragmaStmt) (*Rows, error) {
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
	columns := []string{"seqno", "cid", "name", "desc", "coll", "key"}
	if idxName == "" {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	idx, exists := db.indexes[idxName]
	if !exists {
		return &Rows{Columns: columns, Data: [][]interface{}{}}, nil
	}
	colOrderMap := make(map[string]int)
	if colOrder, ok := db.columnOrder[idx.Table]; ok {
		for i, c := range colOrder {
			colOrderMap[c] = i
		}
	}
	data := make([][]interface{}, 0, len(idx.Columns))
	for seqno, colName := range idx.Columns {
		cid := int64(-1)
		if id, ok := colOrderMap[colName]; ok {
			cid = int64(id)
		}
		data = append(data, []interface{}{int64(seqno), cid, colName, int64(0), "BINARY", int64(1)})
	}
	return &Rows{Columns: columns, Data: data}, nil
}

// pragmaForeignKeyCheck verifies FK constraints and returns violations.
// Columns: table, rowid, parent, fkid
// If stmt.Value is set, only checks the specified table.
func (db *Database) pragmaForeignKeyCheck(stmt *QP.PragmaStmt) (*Rows, error) {
	columns := []string{"table", "rowid", "parent", "fkid"}
	data := make([][]interface{}, 0)

	var targetTable string
	if stmt != nil && stmt.Value != nil {
		switch v := stmt.Value.(type) {
		case *QP.Literal:
			if s, ok := v.Value.(string); ok {
				targetTable = s
			}
		case *QP.ColumnRef:
			targetTable = v.Name
		}
	}

	for tblName, fks := range db.foreignKeys {
		if targetTable != "" && tblName != targetTable {
			continue
		}
		rows := db.data[tblName]
		for fkid, fk := range fks {
			parentRows := db.data[fk.ParentTable]
			// Build parent key set using strings.Builder for efficiency.
			parentSet := make(map[string]bool, len(parentRows))
			var kb strings.Builder
			for _, prow := range parentRows {
				kb.Reset()
				nullFound := false
				for i, pcol := range fk.ParentColumns {
					if i > 0 {
						kb.WriteByte(0)
					}
					v := prow[pcol]
					if v == nil {
						nullFound = true
						break
					}
					fmt.Fprintf(&kb, "%v", v)
				}
				if !nullFound {
					parentSet[kb.String()] = true
				}
			}
			for rowid, row := range rows {
				kb.Reset()
				skip := false
				for i, ccol := range fk.ChildColumns {
					if i > 0 {
						kb.WriteByte(0)
					}
					val := row[ccol]
					if val == nil {
						// NULL child values don't violate FK
						skip = true
						break
					}
					fmt.Fprintf(&kb, "%v", val)
				}
				if skip {
					continue
				}
				if !parentSet[kb.String()] {
					data = append(data, []interface{}{tblName, int64(rowid), fk.ParentTable, int64(fkid)})
				}
			}
		}
	}
	return &Rows{Columns: columns, Data: data}, nil
}

// getPragmaInt and getPragmaStr are backward-compat shims for backup.go and
// other internal callers that predate the pragma.Ctx interface. New code should
// use GetPragmaInt / GetPragmaStr directly.
func (db *Database) getPragmaInt(name string, defaultVal int64) int64 {
	return db.GetPragmaInt(name, defaultVal)
}

func (db *Database) getPragmaStr(name string, defaultVal string) string {
	return db.GetPragmaStr(name, defaultVal)
}
