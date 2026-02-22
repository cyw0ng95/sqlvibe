package sqlvibe

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cyw0ng95/sqlvibe/internal/CG"
	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/IS"
	"github.com/cyw0ng95/sqlvibe/internal/PB"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
	"github.com/cyw0ng95/sqlvibe/internal/TM"
	"github.com/cyw0ng95/sqlvibe/internal/VM"

)

// queryResultCache is a thread-safe in-process cache for full SELECT query results.
// Entries are keyed by FNV-1a hash of the SQL string.
// All entries are invalidated on any write operation (INSERT/UPDATE/DELETE/DDL).
type queryResultCache struct {
	mu   sync.RWMutex
	data map[uint64]*queryResultEntry
	max  int
}

type queryResultEntry struct {
	columns []string
	rows    [][]interface{}
}

func newQueryResultCache(max int) *queryResultCache {
	return &queryResultCache{data: make(map[uint64]*queryResultEntry), max: max}
}

func (c *queryResultCache) Get(key uint64) ([]string, [][]interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if e, ok := c.data[key]; ok {
		return e.columns, e.rows, true
	}
	return nil, nil, false
}

func (c *queryResultCache) Set(key uint64, columns []string, rows [][]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max > 0 && len(c.data) >= c.max {
		// Evict an arbitrary entry to stay within limit.
		for k := range c.data {
			delete(c.data, k)
			break
		}
	}
	c.data[key] = &queryResultEntry{columns: columns, rows: rows}
}

func (c *queryResultCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[uint64]*queryResultEntry)
}

type Database struct {
	pm             *DS.PageManager
	cache          *DS.Cache
	engine         *VM.QueryEngine
	tx             *Transaction
	txMgr          *TM.TransactionManager
	activeTx       *TM.Transaction
	dbPath         string
	tables         map[string]map[string]string        // table name -> column name -> type
	primaryKeys    map[string][]string                 // table name -> primary key column names
	columnOrder    map[string][]string                 // table name -> ordered column names
	columnDefaults map[string]map[string]interface{}   // table name -> column name -> default value
	columnNotNull  map[string]map[string]bool          // table name -> column name -> NOT NULL
	columnChecks   map[string]map[string]QP.Expr       // table name -> column name -> CHECK expression
	data           map[string][]map[string]interface{} // table name -> rows -> column name -> value
	indexes        map[string]*IndexInfo               // index name -> index info
	views          map[string]string                   // view name -> SELECT SQL
	isRegistry     *IS.Registry                        // information_schema registry
	txSnapshot     *dbSnapshot                         // snapshot for transaction rollback
	tableBTrees    map[string]*DS.BTree                // table name -> B-Tree for storage
	journalMode    string                              // "delete" or "wal"
	wal            *TM.WAL                             // WAL instance when in WAL mode
	pkHashSet      map[string]map[interface{}]struct{} // table name -> PK value -> exists (single-col PK only)
	indexData      map[string]map[interface{}][]int    // index name -> col value -> []row indices
	planCache          *CG.PlanCache                       // compiled query plan cache
	queryCache         *queryResultCache                   // full query result cache (columns + rows)
	schemaCache        *IS.SchemaCache                     // information_schema result cache (DDL-invalidated)
	hybridStores       map[string]*DS.HybridStore     // table name -> columnar hybrid store (analytical fast path)
	hybridStoresDirty  map[string]bool                     // table name -> needs rebuild on next access
	isolationConfig    *TM.IsolationConfig                 // isolation level and busy_timeout settings
	compressionName    string                              // active compression algorithm (NONE/RLE/LZ4/ZSTD/GZIP)
	// v0.8.6 additions
	foreignKeys       map[string][]QP.ForeignKeyConstraint // table name -> FK constraints
	triggers          map[string][]*QP.CreateTriggerStmt   // table name -> triggers list
	autoincrement     map[string]string                    // table name -> pk col name (if AUTOINCREMENT)
	seqValues         map[string]int64                     // table name -> last used autoincrement value
	foreignKeysEnabled bool                               // PRAGMA foreign_keys = ON/OFF
	pragmaSettings     map[string]interface{}             // PRAGMA setting storage
	tableStats         map[string]int64                   // table name -> row count (for ANALYZE)
	queryMu            sync.RWMutex                       // guards concurrent read queries
}

type dbSnapshot struct {
	data           map[string][]map[string]interface{}
	tables         map[string]map[string]string
	primaryKeys    map[string][]string
	columnOrder    map[string][]string
	columnDefaults map[string]map[string]interface{}
	columnNotNull  map[string]map[string]bool
	columnChecks   map[string]map[string]QP.Expr
	indexes        map[string]*IndexInfo
}

func (db *Database) captureSnapshot() *dbSnapshot {
	snap := &dbSnapshot{
		data:           make(map[string][]map[string]interface{}),
		tables:         make(map[string]map[string]string),
		primaryKeys:    make(map[string][]string),
		columnOrder:    make(map[string][]string),
		columnDefaults: make(map[string]map[string]interface{}),
		columnNotNull:  make(map[string]map[string]bool),
		columnChecks:   make(map[string]map[string]QP.Expr),
		indexes:        make(map[string]*IndexInfo),
	}
	for tbl, rows := range db.data {
		rowsCopy := make([]map[string]interface{}, len(rows))
		for i, row := range rows {
			rowCopy := make(map[string]interface{}, len(row))
			for k, v := range row {
				// Deep copy byte slices to prevent shared state between snapshot and live data
				if b, ok := v.([]byte); ok {
					bCopy := make([]byte, len(b))
					copy(bCopy, b)
					rowCopy[k] = bCopy
				} else {
					rowCopy[k] = v
				}
			}
			rowsCopy[i] = rowCopy
		}
		snap.data[tbl] = rowsCopy
	}
	for k, v := range db.tables {
		colsCopy := make(map[string]string, len(v))
		for ck, cv := range v {
			colsCopy[ck] = cv
		}
		snap.tables[k] = colsCopy
	}
	for k, v := range db.primaryKeys {
		pkCopy := make([]string, len(v))
		copy(pkCopy, v)
		snap.primaryKeys[k] = pkCopy
	}
	for k, v := range db.columnOrder {
		colOrdCopy := make([]string, len(v))
		copy(colOrdCopy, v)
		snap.columnOrder[k] = colOrdCopy
	}
	for k, v := range db.columnDefaults {
		defCopy := make(map[string]interface{}, len(v))
		for dk, dv := range v {
			defCopy[dk] = dv
		}
		snap.columnDefaults[k] = defCopy
	}
	for k, v := range db.columnNotNull {
		nnCopy := make(map[string]bool, len(v))
		for nk, nv := range v {
			nnCopy[nk] = nv
		}
		snap.columnNotNull[k] = nnCopy
	}
	for k, v := range db.columnChecks {
		chkCopy := make(map[string]QP.Expr, len(v))
		for ck, cv := range v {
			chkCopy[ck] = cv
		}
		snap.columnChecks[k] = chkCopy
	}
	for k, v := range db.indexes {
		idxCopy := &IndexInfo{
			Name:    v.Name,
			Table:   v.Table,
			Unique:  v.Unique,
			Columns: make([]string, len(v.Columns)),
		}
		copy(idxCopy.Columns, v.Columns)
		snap.indexes[k] = idxCopy
	}
	return snap
}

func (db *Database) restoreSnapshot(snap *dbSnapshot) {
	db.data = snap.data
	db.tables = snap.tables
	db.primaryKeys = snap.primaryKeys
	db.columnOrder = snap.columnOrder
	db.columnDefaults = snap.columnDefaults
	db.columnNotNull = snap.columnNotNull
	db.columnChecks = snap.columnChecks
	db.indexes = snap.indexes
	db.rebuildAllIndexes()
}

type IndexInfo struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

type Conn interface {
	Exec(sql string) (Result, error)
	Query(sql string) (*Rows, error)
	Close() error
}

type Result struct {
	LastInsertRowID int64
	RowsAffected    int64
}

type Rows struct {
	Columns []string
	Data    [][]interface{}
	pos     int  // Current position, starts at 0
	started bool // Whether Next() has been called
}

func (r *Rows) Next() bool {
	if r.Data == nil {
		return false
	}
	// On first call, don't advance pos (it's already at 0)
	if !r.started {
		r.started = true
		return len(r.Data) > 0
	}
	// On subsequent calls, advance pos
	r.pos++
	return r.pos < len(r.Data)
}

func (r *Rows) Scan(dest ...interface{}) error {
	util.Assert(r.pos >= 0, "row position cannot be negative: %d", r.pos)

	if r.Data == nil || r.pos < 0 || r.pos >= len(r.Data) {
		return fmt.Errorf("no rows available")
	}
	row := r.Data[r.pos]
	for i, val := range dest {
		if i >= len(row) {
			break
		}
		switch d := val.(type) {
		case *int:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = v
				case int64:
					*d = int(v)
				case float64:
					*d = int(v)
				}
			}
		case *int64:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = int64(v)
				case int64:
					*d = v
				case float64:
					*d = int64(v)
				}
			}
		case *float64:
			if row[i] != nil {
				switch v := row[i].(type) {
				case int:
					*d = float64(v)
				case int64:
					*d = float64(v)
				case float64:
					*d = v
				}
			}
		case *string:
			if row[i] != nil {
				switch v := row[i].(type) {
				case string:
					*d = v
				default:
					*d = fmt.Sprintf("%v", v)
				}
			}
		case *interface{}:
			*d = row[i]
		}
	}
	return nil
}

type Statement struct {
	db     *Database
	sql    string
	parsed QP.ASTNode
}

type Transaction struct {
	db        *Database
	committed bool
	rollback  bool
}

func Open(path string) (*Database, error) {
	file, err := PB.OpenFile(path, PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		return nil, err
	}

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		file.Close()
		return nil, err
	}

	data := make(map[string][]map[string]interface{})
	engine := VM.NewQueryEngine(pm, data)
	txMgr := TM.NewTransactionManager(pm)

	return &Database{
		pm:             pm,
		cache:          DS.NewCache(-2000),
		engine:         engine,
		txMgr:          txMgr,
		activeTx:       nil,
		dbPath:         path,
		tables:         make(map[string]map[string]string),
		primaryKeys:    make(map[string][]string),
		columnOrder:    make(map[string][]string),
		columnDefaults: make(map[string]map[string]interface{}),
		columnNotNull:  make(map[string]map[string]bool),
		columnChecks:   make(map[string]map[string]QP.Expr),
		data:           data,
		indexes:        make(map[string]*IndexInfo),
		views:          make(map[string]string),
		tableBTrees:    make(map[string]*DS.BTree),
		journalMode:    "delete",
		wal:            nil,
		pkHashSet:      make(map[string]map[interface{}]struct{}),
		indexData:      make(map[string]map[interface{}][]int),
		planCache:          CG.NewPlanCache(256),
		queryCache:         newQueryResultCache(512),
		schemaCache:        IS.NewSchemaCache(),
		hybridStores:       make(map[string]*DS.HybridStore),
		hybridStoresDirty:  make(map[string]bool),
		isolationConfig:    TM.NewIsolationConfig(),
		compressionName:    "NONE",
		foreignKeys:        make(map[string][]QP.ForeignKeyConstraint),
		triggers:           make(map[string][]*QP.CreateTriggerStmt),
		autoincrement:      make(map[string]string),
		seqValues:          make(map[string]int64),
		foreignKeysEnabled: false,
		pragmaSettings:     make(map[string]interface{}),
		tableStats:         make(map[string]int64),
	}, nil
}

func (db *Database) getOrderedColumns(tableName string) []string {
	if cols, ok := db.columnOrder[tableName]; ok {
		return cols
	}
	if schema, ok := db.tables[tableName]; ok {
		var cols []string
		for col := range schema {
			cols = append(cols, col)
		}
		return cols
	}
	return nil
}

// resolveTableName returns the canonical table name (case-insensitive lookup).
// Returns the original key if found with exact match, otherwise finds a case-insensitive match.
func (db *Database) resolveTableName(name string) string {
	if _, ok := db.tables[name]; ok {
		return name
	}
	lower := strings.ToLower(name)
	for k := range db.tables {
		if strings.ToLower(k) == lower {
			return k
		}
	}
	return name
}

func (db *Database) evalConstantExpression(stmt *QP.SelectStmt) (*Rows, error) {
	if len(stmt.Columns) == 0 {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	var cols []string
	var row []interface{}
	emptyRow := make(map[string]interface{})

	for _, col := range stmt.Columns {
		var colName string
		var colValue interface{}

		switch c := col.(type) {
		case *QP.AliasExpr:
			colName = c.Alias
			if c.Expr != nil {
				colValue = db.engine.EvalExpr(emptyRow, c.Expr)
			}
		case *QP.BinaryExpr:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		case *QP.UnaryExpr:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		case *QP.Literal:
			colName = "expr"
			colValue = c.Value
		case *QP.FuncCall:
			// LOCALTIME/LOCALTIMESTAMP are not supported as standalone functions (SQLite behavior)
			upperName := strings.ToUpper(c.Name)
			if upperName == "LOCALTIME" || upperName == "LOCALTIMESTAMP" {
				return nil, fmt.Errorf("no such column: %s", c.Name)
			}
			colName = c.Name
			colValue = db.engine.EvalExpr(emptyRow, c)
		default:
			colName = "expr"
			colValue = db.engine.EvalExpr(emptyRow, c)
		}

		cols = append(cols, colName)
		row = append(row, colValue)
	}

	return &Rows{Columns: cols, Data: [][]interface{}{row}}, nil
}

func (db *Database) Exec(sql string) (Result, error) {
	// Handle multi-statement SQL (separated by semicolons)
	stmts := splitStatements(sql)
	if len(stmts) > 1 {
		var lastResult Result
		for _, s := range stmts {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			result, err := db.Exec(s)
			if err != nil {
				return Result{}, err
			}
			lastResult = result
		}
		return lastResult, nil
	}

	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return Result{}, err
	}
	if len(tokens) == 0 {
		return Result{}, nil
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return Result{}, err
	}
	if ast == nil {
		return Result{}, nil
	}

	switch ast.NodeType() {
	case "CreateTableStmt":
		stmt := ast.(*QP.CreateTableStmt)
		// Case-insensitive existence check
		resolved := db.resolveTableName(stmt.Name)
		if _, exists := db.tables[resolved]; exists {
			if stmt.IfNotExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("table %s already exists", stmt.Name)
		}

		// Handle CREATE TABLE ... AS SELECT
		if stmt.AsSelect != nil {
			return db.execCreateTableAsSelect(stmt)
		}

		schema := make(map[string]VM.ColumnType)
		colTypes := make(map[string]string)
		var pkCols []string
		db.columnDefaults[stmt.Name] = make(map[string]interface{})
		db.columnNotNull[stmt.Name] = make(map[string]bool)
		db.columnChecks[stmt.Name] = make(map[string]QP.Expr)
		seenCols := make(map[string]bool)
		if len(stmt.Columns) == 0 {
			return Result{}, fmt.Errorf("table must have at least one column")
		}
		for _, col := range stmt.Columns {
			if seenCols[col.Name] {
				return Result{}, fmt.Errorf("duplicate column name: %s", col.Name)
			}
			seenCols[col.Name] = true
			schema[col.Name] = VM.ColumnType{Name: col.Name, Type: col.Type}
			colTypes[col.Name] = col.Type
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
			if col.Default != nil {
				db.columnDefaults[stmt.Name][col.Name] = col.Default
			}
			if col.NotNull {
				db.columnNotNull[stmt.Name][col.Name] = true
			}
			if col.Check != nil {
				db.columnChecks[stmt.Name][col.Name] = col.Check
			}
		}
		// Store table-level CHECK constraints under special keys
		for i, tableCheck := range stmt.TableChecks {
			db.columnChecks[stmt.Name][fmt.Sprintf("__table_check_%d__", i)] = tableCheck
		}
		// Store FK constraints
		allFKs := make([]QP.ForeignKeyConstraint, 0)
		for _, col := range stmt.Columns {
			if col.ForeignKey != nil {
				allFKs = append(allFKs, *col.ForeignKey)
			}
			if col.IsAutoincrement && col.PrimaryKey {
				db.autoincrement[stmt.Name] = col.Name
			}
		}
		allFKs = append(allFKs, stmt.ForeignKeys...)
		if len(allFKs) > 0 {
			db.foreignKeys[stmt.Name] = allFKs
		}
		db.engine.RegisterTable(stmt.Name, schema)
		db.tables[stmt.Name] = colTypes
		var colOrder []string
		for _, col := range stmt.Columns {
			colOrder = append(colOrder, col.Name)
		}
		db.columnOrder[stmt.Name] = colOrder
		if len(pkCols) > 0 {
			db.primaryKeys[stmt.Name] = pkCols
		}

		// Initialise PK hash set for the new (empty) table.
		if len(pkCols) > 0 {
			db.pkHashSet[stmt.Name] = make(map[interface{}]struct{})
		}

		// Create BTree for table storage
		bt := DS.NewBTree(db.pm, 0, true)
		db.tableBTrees[stmt.Name] = bt
		// Initialise an empty HybridStore for the new table.
		db.hybridStores[stmt.Name] = db.buildHybridStore(stmt.Name)
		db.hybridStoresDirty[stmt.Name] = false
		if db.schemaCache != nil {
			db.schemaCache.Invalidate()
		}

		return Result{}, nil
	case "InsertStmt":
		stmt := ast.(*QP.InsertStmt)
		var result Result
		var err error
		if stmt.SelectQuery != nil {
			result, err = db.execInsertSelect(stmt)
		} else if stmt.OnConflict != nil {
			result, err = db.execInsertOnConflict(stmt)
		} else if res, batchErr, handled := db.execInsertBatch(stmt); handled {
			result, err = res, batchErr
		} else {
			result, err = db.execVMDML(sql, stmt.Table)
		}
		if err == nil {
			db.invalidateWriteCaches()
		}
		return result, err
	case "UpdateStmt":
		stmt := ast.(*QP.UpdateStmt)
		result, err := db.execVMDML(sql, stmt.Table)
		if err == nil {
			db.invalidateWriteCaches()
		}
		return result, err
	case "DeleteStmt":
		stmt := ast.(*QP.DeleteStmt)
		result, err := db.execVMDML(sql, stmt.Table)
		if err == nil {
			db.invalidateWriteCaches()
		}
		return result, err
	case "DropTableStmt":
		stmt := ast.(*QP.DropTableStmt)
		if _, exists := db.tables[stmt.Name]; !exists {
			if stmt.IfExists {
				return Result{}, nil
			}
		}
		delete(db.tables, stmt.Name)
		delete(db.data, stmt.Name)
		delete(db.primaryKeys, stmt.Name)
		delete(db.columnDefaults, stmt.Name)
		delete(db.columnOrder, stmt.Name)
		delete(db.columnChecks, stmt.Name)
		delete(db.columnNotNull, stmt.Name)
		delete(db.tableBTrees, stmt.Name)
		delete(db.pkHashSet, stmt.Name)
		delete(db.hybridStores, stmt.Name)
		delete(db.hybridStoresDirty, stmt.Name)
		// Drop all secondary indexes that cover this table.
		for idxName, idx := range db.indexes {
			if idx.Table == stmt.Name {
				delete(db.indexes, idxName)
				delete(db.indexData, idxName)
			}
		}
		db.invalidateSchemaCaches()
		return Result{}, nil
	case "CreateViewStmt":
		result, err := db.execCreateView(ast.(*QP.CreateViewStmt), sql)
		if err == nil && db.schemaCache != nil {
			db.schemaCache.Invalidate()
		}
		return result, err
	case "DropViewStmt":
		stmt := ast.(*QP.DropViewStmt)
		if _, exists := db.views[stmt.Name]; !exists {
			if stmt.IfExists {
				return Result{}, nil
			}
		}
		delete(db.views, stmt.Name)
		if db.schemaCache != nil {
			db.schemaCache.Invalidate()
		}
		return Result{}, nil
	case "AlterTableStmt":
		result, err := db.execAlterTable(ast.(*QP.AlterTableStmt))
		if err == nil && db.schemaCache != nil {
			db.schemaCache.Invalidate()
		}
		return result, err
	case "CreateIndexStmt":
		stmt := ast.(*QP.CreateIndexStmt)
		if _, exists := db.indexes[stmt.Name]; exists {
			if stmt.IfNotExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("index %s already exists", stmt.Name)
		}
		if _, exists := db.tables[stmt.Table]; !exists {
			return Result{}, fmt.Errorf("table %s does not exist", stmt.Table)
		}
		db.indexes[stmt.Name] = &IndexInfo{
			Name:    stmt.Name,
			Table:   stmt.Table,
			Columns: stmt.Columns,
			Unique:  stmt.Unique,
		}
		// Build hash index immediately from existing table data.
		db.buildIndexData(stmt.Name)
		db.schemaCache.Invalidate()
		return Result{}, nil
	case "DropIndexStmt":
		stmt := ast.(*QP.DropIndexStmt)
		delete(db.indexes, stmt.Name)
		delete(db.indexData, stmt.Name)
		db.schemaCache.Invalidate()
		return Result{}, nil
	case "CreateTriggerStmt":
		stmt := ast.(*QP.CreateTriggerStmt)
		if _, exists := db.triggers[stmt.TableName]; !exists {
			db.triggers[stmt.TableName] = nil
		}
		// Check if trigger already exists
		for _, t := range db.triggers[stmt.TableName] {
			if t.Name == stmt.Name {
				if stmt.IfNotExists {
					return Result{}, nil
				}
				return Result{}, fmt.Errorf("trigger %s already exists", stmt.Name)
			}
		}
		db.triggers[stmt.TableName] = append(db.triggers[stmt.TableName], stmt)
		db.invalidateSchemaCaches()
		return Result{}, nil
	case "DropTriggerStmt":
		stmt := ast.(*QP.DropTriggerStmt)
		dropped := false
		for tbl, trigs := range db.triggers {
			for i, t := range trigs {
				if t.Name == stmt.Name {
					db.triggers[tbl] = append(trigs[:i], trigs[i+1:]...)
					dropped = true
					break
				}
			}
			if dropped {
				break
			}
		}
		if !dropped && !stmt.IfExists {
			return Result{}, fmt.Errorf("no such trigger: %s", stmt.Name)
		}
		db.invalidateSchemaCaches()
		return Result{}, nil
	case "BeginStmt":
		stmt := ast.(*QP.BeginStmt)
		if db.activeTx != nil {
			return Result{}, fmt.Errorf("transaction already active")
		}
		txType := TM.TransactionDeferred
		if stmt.Type == "IMMEDIATE" {
			txType = TM.TransactionImmediate
		} else if stmt.Type == "EXCLUSIVE" {
			txType = TM.TransactionExclusive
		}
		tx, err := db.txMgr.Begin(db.dbPath, txType)
		if err != nil {
			return Result{}, fmt.Errorf("failed to begin transaction: %w", err)
		}
		db.activeTx = tx
		db.txSnapshot = db.captureSnapshot()
		return Result{}, nil
	case "CommitStmt":
		if db.activeTx == nil {
			return Result{}, fmt.Errorf("no transaction active")
		}
		err := db.txMgr.CommitTransaction(db.activeTx.ID)
		if err != nil {
			return Result{}, fmt.Errorf("failed to commit transaction: %w", err)
		}
		db.activeTx = nil
		db.txSnapshot = nil
		return Result{}, nil
	case "RollbackStmt":
		if db.activeTx == nil {
			return Result{}, fmt.Errorf("no transaction active")
		}
		err := db.txMgr.RollbackTransaction(db.activeTx.ID)
		if err != nil {
			return Result{}, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		if db.txSnapshot != nil {
			db.restoreSnapshot(db.txSnapshot)
			db.txSnapshot = nil
		}
		// activeTx is cleared after snapshot restore so the engine sees clean state
		db.activeTx = nil
		return Result{}, nil
	case "PragmaStmt":
		// PRAGMAs like PRAGMA foreign_keys = ON need to work via Exec too
		rows, err := db.handlePragma(ast.(*QP.PragmaStmt))
		if err != nil {
			return Result{}, err
		}
		_ = rows
		return Result{}, nil
	case "VacuumStmt":
		_, err := db.handleVacuum(ast.(*QP.VacuumStmt))
		return Result{}, err
	case "AnalyzeStmt":
		_, err := db.handleAnalyze(ast.(*QP.AnalyzeStmt))
		return Result{}, err
	}

	return Result{}, nil
}

func (db *Database) Query(sql string) (*Rows, error) {
	// Handle multi-statement SQL (separated by semicolons)
	// Execute all but the last as Exec, return results of last SELECT
	stmts := splitStatements(sql)
	if len(stmts) > 1 {
		var lastRows *Rows
		for i, s := range stmts {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if i == len(stmts)-1 {
				// Execute last statement as query
				rows, err := db.Query(s)
				if err != nil {
					return nil, err
				}
				lastRows = rows
			} else {
				// Execute intermediate statements as exec
				if _, err := db.Exec(s); err != nil {
					return nil, err
				}
			}
		}
		return lastRows, nil
	}

	// Result cache check: serve repeated identical SELECT queries without re-execution.
	// Only applies to single-statement queries. During an active transaction,
	// caching is skipped to ensure transaction isolation.
	sqlUpper := strings.TrimSpace(strings.ToUpper(sql))
	isCacheable := (strings.HasPrefix(sqlUpper, "SELECT") || strings.HasPrefix(sqlUpper, "WITH")) &&
		db.activeTx == nil
	var cacheKey uint64
	if isCacheable && db.queryCache != nil {
		cacheKey = sqlQueryHash(sql)
		if cachedCols, cachedRows, ok := db.queryCache.Get(cacheKey); ok {
			return &Rows{Columns: cachedCols, Data: cachedRows}, nil
		}
	}

	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, nil
	}

	if ast.NodeType() == "PragmaStmt" {
		return db.handlePragma(ast.(*QP.PragmaStmt))
	}

	if ast.NodeType() == "BackupStmt" {
		return db.handleBackup(ast.(*QP.BackupStmt))
	}

	if ast.NodeType() == "VacuumStmt" {
		return db.handleVacuum(ast.(*QP.VacuumStmt))
	}

	if ast.NodeType() == "AnalyzeStmt" {
		return db.handleAnalyze(ast.(*QP.AnalyzeStmt))
	}

	if ast.NodeType() == "ExplainStmt" {
		return db.handleExplain(ast.(*QP.ExplainStmt), sql)
	}

	if ast.NodeType() == "SelectStmt" {
		stmt := ast.(*QP.SelectStmt)

		// Handle CTE (WITH ... AS (...) SELECT ...) by materializing temp tables
		if len(stmt.CTEs) > 0 {
			var cteNames []string
			for _, cte := range stmt.CTEs {
				var cteRows *Rows
				var err error
				if cte.Recursive {
					cteRows, err = db.execRecursiveCTE(&cte)
				} else {
					cteRows, err = db.execSelectStmt(cte.Select)
				}
				if err != nil {
					return nil, err
				}
				if cteRows == nil {
					cteRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
				}
				cols := cteRows.Columns
				if len(cte.Columns) > 0 && len(cte.Columns) == len(cols) {
					cols = cte.Columns
				}
				colTypes := make(map[string]string)
				for _, col := range cols {
					colTypes[col] = "TEXT"
				}
				db.tables[cte.Name] = colTypes
				db.columnOrder[cte.Name] = cols
				rowMaps := make([]map[string]interface{}, len(cteRows.Data))
				for i, row := range cteRows.Data {
					rm := make(map[string]interface{})
					for j, col := range cols {
						if j < len(row) {
							rm[col] = row[j]
						}
					}
					rowMaps[i] = rm
				}
				db.data[cte.Name] = rowMaps
				cteNames = append(cteNames, cte.Name)
			}
			stmt.CTEs = nil
			rows, err := db.execSelectStmt(stmt)
			for _, name := range cteNames {
				delete(db.tables, name)
				delete(db.columnOrder, name)
				delete(db.data, name)
			}
			return rows, err
		}

		if stmt.From == nil {
			return db.evalConstantExpression(stmt)
		}

		// Resolve column aliases in WHERE, GROUP BY, HAVING, ORDER BY
		// This allows SQLite-style alias references like: SELECT a AS x ... WHERE x > 0
		resolveSelectAliases(stmt)

		// Extract and replace window function columns with NULL placeholders for VM execution
		// Window functions will be computed as a post-processing step
		windowFuncs, windowExtraCols := extractWindowFunctions(stmt)

		// Handle derived table in FROM clause: SELECT ... FROM (SELECT ...) AS alias
		if stmt.From.Subquery != nil {
			return db.execDerivedTableQuery(stmt)
		}

		// Handle VALUES table constructor: SELECT * FROM (VALUES (1,'a'), (2,'b')) AS t(x,y)
		if stmt.From.Values != nil {
			return db.execValuesTable(stmt)
		}

		var tableName string
		var schemaName string
		if stmt.From != nil {
			tableName = stmt.From.Name
			schemaName = stmt.From.Schema
		}
		if tableName == "" {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		// Resolve case-insensitive table name (unquoted identifiers are case-insensitive)
		tableName = db.resolveTableName(tableName)
		stmt.From.Name = tableName

		// Handle sqlite_master virtual table
		if tableName == "sqlite_master" {
			return db.querySqliteMaster(stmt)
		}

		// Handle sqlite_stat1 virtual table
		if tableName == "sqlite_stat1" {
			return db.querySqliteStat1()
		}

		// Handle sqlvibe_extensions virtual table
		if tableName == "sqlvibe_extensions" {
			return db.querySqlvibeExtensions(stmt)
		}

		// Handle views by substituting the view SQL (case-insensitive)
		if viewSQL, isView := db.views[tableName]; isView {
			return db.queryView(viewSQL, stmt, sql)
		}
		// Also try case-insensitive view lookup
		lowerTbl := strings.ToLower(tableName)
		for vn, vs := range db.views {
			if strings.ToLower(vn) == lowerTbl {
				return db.queryView(vs, stmt, sql)
			}
		}

		// Handle information_schema virtual tables
		if strings.ToLower(schemaName) == "information_schema" {
			fullName := schemaName + "." + tableName
			return db.queryInformationSchema(stmt, fullName)
		}

		// SetOp (UNION, EXCEPT, INTERSECT) - hybrid implementation
		// Execute left and right queries separately using VM, then combine
		if stmt.SetOp != "" && stmt.SetOpRight != nil {
			// Compile and execute left query
			leftStmt := *stmt
			leftStmt.SetOp = ""
			leftStmt.SetOpAll = false
			leftStmt.SetOpRight = nil

			leftRows, err := db.execSelectStmt(&leftStmt)
			if err != nil {
				return nil, fmt.Errorf("SetOp left query failed: %w", err)
			}

			// Compile and execute right query
			rightRows, err := db.execSelectStmt(stmt.SetOpRight)
			if err != nil {
				return nil, fmt.Errorf("SetOp right query failed: %w", err)
			}

			// Combine results using SetOp logic
			// Validate column count match (SQLite requires same number of columns)
			if len(leftRows.Columns) != len(rightRows.Columns) {
				return nil, fmt.Errorf("SELECTs to the left and right of %s do not have the same number of result columns", stmt.SetOp)
			}
			combinedData := db.applySetOp(leftRows.Data, rightRows.Data, stmt.SetOp, stmt.SetOpAll)

			// Apply ORDER BY and LIMIT if present
			rows := &Rows{Columns: leftRows.Columns, Data: combinedData}
			if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
				topK := extractLimitInt(stmt.Limit, stmt.Offset)
				rows, err = db.sortResultsTopK(rows, stmt.OrderBy, topK)
				if err != nil {
					return nil, err
				}
			}
			if stmt.Limit != nil {
				rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
				if err != nil {
					return nil, err
				}
			}

			return rows, nil
		}

		// VM execution for all SELECT queries
		rows, err := db.execVMQuery(sql, stmt)
		if err != nil {
			return nil, err
		}

		// VM can return empty results validly - don't treat as error
		if rows == nil {
			return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
		}

		// Apply window functions if any exist in SELECT columns
		if len(windowFuncs) > 0 {
			rows, err = applyWindowFunctionsToRows(rows, windowFuncs, windowExtraCols)
			if err != nil {
				return nil, err
			}
		}

		// Handle ORDER BY - sort results
		if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
			topK := extractLimitInt(stmt.Limit, stmt.Offset)
			rows, err = db.sortResultsTopK(rows, stmt.OrderBy, topK)
			if err != nil {
				return nil, err
			}
		}

		// Handle LIMIT
		if stmt.Limit != nil {
			rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
			if err != nil {
				return nil, err
			}
		}

		// Populate result cache for pure SELECT queries (no CTEs, no active transaction).
		if isCacheable && db.queryCache != nil && len(stmt.CTEs) == 0 {
			db.queryCache.Set(cacheKey, rows.Columns, rows.Data)
		}

		return rows, nil
	}

	// For non-SELECT DML statements (INSERT, UPDATE, DELETE) called via Query(),
	// execute them via Exec and return empty results (matching SQLite driver behavior).
	if isDMLStatement(ast.NodeType()) {
		_, err := db.Exec(sql)
		if err != nil {
			return nil, err
		}
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	return nil, nil
}

// isDMLStatement returns true if the AST node type represents a data/schema manipulation statement
// (as opposed to a query statement).
func isDMLStatement(nodeType string) bool {
	switch nodeType {
	case "InsertStmt", "UpdateStmt", "DeleteStmt",
		"CreateTableStmt", "DropTableStmt",
		"CreateViewStmt", "DropViewStmt",
		"AlterTableStmt",
		"CreateIndexStmt", "DropIndexStmt":
		return true
	}
	return false
}

func (db *Database) Close() error {
	if db.wal != nil {
		_ = db.wal.Close()
		db.wal = nil
	}
	return db.pm.Close()
}

// invalidateWriteCaches clears the result cache and plan cache after any
// write operation (INSERT, UPDATE, DELETE, DROP, etc.) so that subsequent
// reads see fresh data.
func (db *Database) invalidateWriteCaches() {
	if db.queryCache != nil {
		db.queryCache.Invalidate()
	}
	// Mark all hybrid stores as needing a rebuild on next access.
	for tbl := range db.tables {
		db.hybridStoresDirty[tbl] = true
	}
}

// invalidateSchemaCaches clears caches that depend on schema structure.
// Call this after DDL operations (CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX)
// so information_schema queries are re-evaluated with the updated schema.
func (db *Database) invalidateSchemaCaches() {
	db.invalidateWriteCaches()
	if db.schemaCache != nil {
		db.schemaCache.Invalidate()
	}
}

// sqlQueryHash returns an FNV-1a hash of the SQL string for use as a cache key.
func sqlQueryHash(sql string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return h.Sum64()
}

func (db *Database) Prepare(sql string) (*Statement, error) {
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty SQL")
	}

	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, fmt.Errorf("failed to parse SQL")
	}

	return &Statement{db: db, sql: sql, parsed: ast}, nil
}

func (s *Statement) Exec(params ...interface{}) (Result, error) {
	return s.db.ExecWithParams(s.sql, params)
}

func (s *Statement) Query(params ...interface{}) (*Rows, error) {
	return s.db.QueryWithParams(s.sql, params)
}

func (s *Statement) Close() error {
	return nil
}

func ordinalSuffix(n int) string {
	switch n {
	case 1:
		return "1st"
	case 2:
		return "2nd"
	case 3:
		return "3rd"
	default:
		return fmt.Sprintf("%dth", n)
	}
}

func (db *Database) sortResults(rows *Rows, orderBy []QP.OrderBy) (*Rows, error) {
	return db.sortResultsTopK(rows, orderBy, 0)
}

// sortResultsTopK sorts rows using ORDER BY with an optional top-K hint.
// topK > 0 enables the O(N log K) heap sort path when only the first K results are needed.
func (db *Database) sortResultsTopK(rows *Rows, orderBy []QP.OrderBy, topK int) (*Rows, error) {
	if len(orderBy) == 0 || rows == nil || len(rows.Data) == 0 {
		return rows, nil
	}

	// Validate integer ORDER BY column references
	numCols := len(rows.Columns)
	for i, ob := range orderBy {
		if lit, ok := ob.Expr.(*QP.Literal); ok {
			if n, ok := lit.Value.(int64); ok {
				// SQLite uses 1-based integer ORDER BY column references
				if n < 1 || int(n) > numCols {
					return nil, fmt.Errorf("%s ORDER BY term out of range - should be between 1 and %d", ordinalSuffix(i+1), numCols)
				}
				// Replace literal with column reference (1-based → 0-based)
				orderBy[i].Expr = &QP.ColumnRef{Name: rows.Columns[n-1]}
			}
		}
	}

	sorted := db.engine.SortRowsTopK(rows.Data, orderBy, rows.Columns, topK)
	return &Rows{Columns: rows.Columns, Data: sorted}, nil
}

func (db *Database) ExecWithParams(sql string, params []interface{}) (Result, error) {
	return db.Exec(sql)
}

func (db *Database) QueryWithParams(sql string, params []interface{}) (*Rows, error) {
	return db.Query(sql)
}

func (db *Database) Begin() (*Transaction, error) {
	if db.tx != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}
	db.tx = &Transaction{db: db, committed: false, rollback: false}
	return db.tx, nil
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rollback {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.committed = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	tx.rollback = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Exec(sql string) (Result, error) {
	return tx.db.Exec(sql)
}

func (tx *Transaction) Query(sql string) (*Rows, error) {
	return tx.db.Query(sql)
}

func (db *Database) extractValue(expr QP.Expr) interface{} {
	return db.engine.ExtractValue(expr)
}

func (db *Database) extractValueTyped(expr QP.Expr, colType string) interface{} {
	return db.engine.ExtractValueTyped(expr, colType)
}

func (db *Database) negateValue(val interface{}) interface{} {
	return db.engine.Negate(val)
}

func (db *Database) convertStringToType(val string, colType string) interface{} {
	return db.engine.ConvertStringToType(val, colType)
}

func (db *Database) MustExec(sql string, params ...interface{}) Result {
	res, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	return res
}

func (db *Database) tryUseIndex(tableName string, where QP.Expr) []map[string]interface{} {
	// Convert IndexInfo to VM.IndexInfo
	qeIndexes := make(map[string]*VM.IndexInfo)
	for name, idx := range db.indexes {
		qeIndexes[name] = &VM.IndexInfo{
			Name:    idx.Name,
			Table:   idx.Table,
			Columns: idx.Columns,
			Unique:  idx.Unique,
		}
	}
	return db.engine.TryUseIndex(tableName, where, qeIndexes)
}

func (db *Database) scanByIndexValue(tableName, colName string, value interface{}, unique bool) []map[string]interface{} {
	return db.engine.ScanByIndexValue(tableName, colName, value, unique)
}

// normalizeIndexKey converts a value to a comparable map key.
// []byte keys are converted to string so they are hashable.
func normalizeIndexKey(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// pkKey builds a comparable key for the primary key columns of a row.
// Single-col PK: the value itself (normalised). Multi-col PK: a pipe-separated string.
func pkKey(row map[string]interface{}, pkCols []string) interface{} {
	if len(pkCols) == 1 {
		return normalizeIndexKey(row[pkCols[0]])
	}
	var b strings.Builder
	for i, col := range pkCols {
		if i > 0 {
			b.WriteByte('|')
		}
		switch v := row[col].(type) {
		case int64:
			b.WriteString(strconv.FormatInt(v, 10))
		case float64:
			b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		case string:
			b.WriteString(v)
		case bool:
			if v {
				b.WriteString("true")
			} else {
				b.WriteString("false")
			}
		case []byte:
			b.Write(v)
		case nil:
			b.WriteString("<nil>")
		default:
			fmt.Fprintf(&b, "%v", v)
		}
	}
	return b.String()
}

// pkHashAdd adds a row's PK to the hash set for tableName.
func (db *Database) pkHashAdd(tableName string, row map[string]interface{}) {
	pkCols := db.primaryKeys[tableName]
	if len(pkCols) == 0 {
		return
	}
	set := db.pkHashSet[tableName]
	if set == nil {
		set = make(map[interface{}]struct{})
		db.pkHashSet[tableName] = set
	}
	set[pkKey(row, pkCols)] = struct{}{}
}

// pkHashRemove removes a row's PK from the hash set.
func (db *Database) pkHashRemove(tableName string, row map[string]interface{}) {
	pkCols := db.primaryKeys[tableName]
	if len(pkCols) == 0 {
		return
	}
	if set := db.pkHashSet[tableName]; set != nil {
		delete(set, pkKey(row, pkCols))
	}
}

// pkHashContains returns true if the PK value already exists (duplicate check).
func (db *Database) pkHashContains(tableName string, row map[string]interface{}) bool {
	pkCols := db.primaryKeys[tableName]
	if len(pkCols) == 0 {
		return false
	}
	set := db.pkHashSet[tableName]
	if set == nil {
		return false
	}
	_, exists := set[pkKey(row, pkCols)]
	return exists
}

// buildPKHashSet rebuilds the PK hash set for tableName from current data.
func (db *Database) buildPKHashSet(tableName string) {
	pkCols := db.primaryKeys[tableName]
	if len(pkCols) == 0 {
		return
	}
	rows := db.data[tableName]
	set := make(map[interface{}]struct{}, len(rows))
	for _, row := range rows {
		set[pkKey(row, pkCols)] = struct{}{}
	}
	db.pkHashSet[tableName] = set
}

// buildIndexData builds the hash index for a single secondary index from current data.
func (db *Database) buildIndexData(indexName string) {
	idx := db.indexes[indexName]
	if idx == nil || len(idx.Columns) == 0 {
		return
	}
	colName := idx.Columns[0]
	rows := db.data[idx.Table]
	hmap := make(map[interface{}][]int, len(rows))
	for i, row := range rows {
		key := normalizeIndexKey(row[colName])
		hmap[key] = append(hmap[key], i)
	}
	db.indexData[indexName] = hmap
}

// indexAdd adds a row's indexed column value to the secondary hash index.
func (db *Database) indexAdd(indexName string, rowIdx int, row map[string]interface{}) {
	idx := db.indexes[indexName]
	if idx == nil || len(idx.Columns) == 0 {
		return
	}
	hmap := db.indexData[indexName]
	if hmap == nil {
		return
	}
	key := normalizeIndexKey(row[idx.Columns[0]])
	hmap[key] = append(hmap[key], rowIdx)
}

// indexRemove removes a specific row index from the secondary hash index entry.
func (db *Database) indexRemove(indexName string, rowIdx int, colValue interface{}) {
	hmap := db.indexData[indexName]
	if hmap == nil {
		return
	}
	key := normalizeIndexKey(colValue)
	entries := hmap[key]
	for i, e := range entries {
		if e == rowIdx {
			hmap[key] = append(entries[:i], entries[i+1:]...)
			return
		}
	}
}

// indexShiftDown decrements all row indices > fromIdx by 1 in the secondary hash index.
// Called after a DELETE to keep indices in sync with the shrunk slice.
// The entry for fromIdx itself is already removed by indexRemove before this is called.
func (db *Database) indexShiftDown(indexName string, fromIdx int) {
	hmap := db.indexData[indexName]
	if hmap == nil {
		return
	}
	for key, entries := range hmap {
		for i, e := range entries {
			if e > fromIdx {
				entries[i] = e - 1
			}
		}
		hmap[key] = entries
	}
}

// addToIndexes updates all secondary indexes (and PK hash set) for a newly inserted row.
func (db *Database) addToIndexes(tableName string, row map[string]interface{}, rowIdx int) {
	db.pkHashAdd(tableName, row)
	for idxName, idx := range db.indexes {
		if idx.Table == tableName && len(idx.Columns) > 0 && db.indexData[idxName] != nil {
			db.indexAdd(idxName, rowIdx, row)
		}
	}
}

// removeFromIndexes updates secondary indexes after a row is deleted at rowIdx.
func (db *Database) removeFromIndexes(tableName string, row map[string]interface{}, rowIdx int) {
	db.pkHashRemove(tableName, row)
	for idxName, idx := range db.indexes {
		if idx.Table == tableName && len(idx.Columns) > 0 {
			if db.indexData[idxName] != nil {
				db.indexRemove(idxName, rowIdx, row[idx.Columns[0]])
				db.indexShiftDown(idxName, rowIdx)
			}
		}
	}
}

// updateIndexes updates secondary indexes after a row update at rowIdx.
func (db *Database) updateIndexes(tableName string, oldRow, newRow map[string]interface{}, rowIdx int) {
	if oldRow != nil {
		db.pkHashRemove(tableName, oldRow)
	}
	db.pkHashAdd(tableName, newRow)
	for idxName, idx := range db.indexes {
		if idx.Table == tableName && len(idx.Columns) > 0 && db.indexData[idxName] != nil {
			if oldRow != nil {
				db.indexRemove(idxName, rowIdx, oldRow[idx.Columns[0]])
			}
			db.indexAdd(idxName, rowIdx, newRow)
		}
	}
}

// rebuildAllIndexes rebuilds all PK hash sets and secondary index data from current db.data.
// Called after snapshot restore (transaction rollback).
func (db *Database) rebuildAllIndexes() {
	// Rebuild PK hash sets
	db.pkHashSet = make(map[string]map[interface{}]struct{})
	for tableName := range db.tables {
		db.buildPKHashSet(tableName)
	}
	// Rebuild secondary indexes
	db.indexData = make(map[string]map[interface{}][]int)
	for idxName := range db.indexes {
		db.buildIndexData(idxName)
	}
	// Mark all hybrid stores dirty after rollback.
	for tbl := range db.tables {
		db.hybridStoresDirty[tbl] = true
	}
}

// sqlTypeToStorageType converts a SQL column type declaration to a DS.ValueType.
func sqlTypeToStorageType(typStr string) DS.ValueType {
	upper := strings.ToUpper(strings.TrimSpace(typStr))
	switch {
	case strings.Contains(upper, "INT"):
		return DS.TypeInt
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOAT"),
		strings.Contains(upper, "DOUBLE"), strings.Contains(upper, "NUMERIC"),
		strings.Contains(upper, "DECIMAL"):
		return DS.TypeFloat
	case strings.Contains(upper, "BOOL"):
		return DS.TypeBool
	case strings.Contains(upper, "BLOB"), strings.Contains(upper, "BYTES"):
		return DS.TypeBytes
	default:
		return DS.TypeString
	}
}

// interfaceToStorageValue converts a Go interface{} row value to a DS.Value.
func interfaceToStorageValue(v interface{}, vt DS.ValueType) DS.Value {
	if v == nil {
		return DS.NullValue()
	}
	switch vt {
	case DS.TypeInt:
		switch n := v.(type) {
		case int64:
			return DS.IntValue(n)
		case int:
			return DS.IntValue(int64(n))
		case float64:
			return DS.IntValue(int64(n))
		case bool:
			if n {
				return DS.IntValue(1)
			}
			return DS.IntValue(0)
		case string:
			if i, err := strconv.ParseInt(n, 10, 64); err == nil {
				return DS.IntValue(i)
			}
		}
	case DS.TypeFloat:
		switch n := v.(type) {
		case float64:
			return DS.FloatValue(n)
		case int64:
			return DS.FloatValue(float64(n))
		case int:
			return DS.FloatValue(float64(n))
		case string:
			if f, err := strconv.ParseFloat(n, 64); err == nil {
				return DS.FloatValue(f)
			}
		}
	case DS.TypeBool:
		switch n := v.(type) {
		case bool:
			return DS.BoolValue(n)
		case int64:
			return DS.BoolValue(n != 0)
		case float64:
			return DS.BoolValue(n != 0)
		}
	case DS.TypeBytes:
		if b, ok := v.([]byte); ok {
			return DS.BytesValue(b)
		}
	}
	// Default: string representation.
	return DS.StringValue(fmt.Sprintf("%v", v))
}

// rowToStorageValues converts a row map to an ordered []DS.Value for tableName.
func (db *Database) rowToStorageValues(tableName string, row map[string]interface{}) []DS.Value {
	cols := db.columnOrder[tableName]
	if len(cols) == 0 {
		return nil
	}
	colTypes := db.tables[tableName]
	vals := make([]DS.Value, len(cols))
	for i, col := range cols {
		vt := sqlTypeToStorageType(colTypes[col])
		vals[i] = interfaceToStorageValue(row[col], vt)
	}
	return vals
}

// buildHybridStore constructs a fresh HybridStore for tableName from db.data.
func (db *Database) buildHybridStore(tableName string) *DS.HybridStore {
	cols := db.columnOrder[tableName]
	if len(cols) == 0 {
		return nil
	}
	colTypeStrs := db.tables[tableName]
	stTypes := make([]DS.ValueType, len(cols))
	for i, col := range cols {
		stTypes[i] = sqlTypeToStorageType(colTypeStrs[col])
	}
	hs := DS.NewHybridStore(cols, stTypes)
	for _, row := range db.data[tableName] {
		if row == nil {
			continue
		}
		vals := make([]DS.Value, len(cols))
		for i, col := range cols {
			vals[i] = interfaceToStorageValue(row[col], stTypes[i])
		}
		hs.Insert(vals)
	}
	return hs
}

// markHybridDirty flags the HybridStore for tableName as needing a rebuild.
func (db *Database) markHybridDirty(tableName string) {
	if _, ok := db.tables[tableName]; ok {
		db.hybridStoresDirty[tableName] = true
	}
}

// GetHybridStore returns the HybridStore for tableName, rebuilding it when necessary.
// Returns nil if the table does not exist.
func (db *Database) GetHybridStore(tableName string) *DS.HybridStore {
	tbl := db.resolveTableName(tableName)
	if _, ok := db.tables[tbl]; !ok {
		return nil
	}
	if db.hybridStoresDirty[tbl] || db.hybridStores[tbl] == nil {
		db.hybridStores[tbl] = db.buildHybridStore(tbl)
		db.hybridStoresDirty[tbl] = false
	}
	return db.hybridStores[tbl]
}

// compareIndexVals compares two index key values for ordering.
// Returns -1, 0, or 1.  NULL (nil) sorts before all other values.
func compareIndexVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int64:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	}
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// tryIndexLookup attempts to use a secondary hash index for WHERE clauses.
// Returns nil if no index can be used, otherwise returns the pre-filtered rows.
// Supported patterns:
//   - col = val              (exact match)
//   - col BETWEEN lo AND hi  (range scan over index keys)
//   - col IN (a, b, c)       (union of per-value index lookups)
//   - col LIKE 'prefix%'     (prefix range scan over index keys)
func (db *Database) tryIndexLookup(tableName string, where QP.Expr) []map[string]interface{} {
	if where == nil {
		return nil
	}
	bin, ok := where.(*QP.BinaryExpr)
	if !ok {
		return nil
	}

	switch bin.Op {
	case QP.TokenEq:
		var colName string
		var val interface{}
		if cr, ok := bin.Left.(*QP.ColumnRef); ok {
			if lit, ok := bin.Right.(*QP.Literal); ok {
				colName, val = cr.Name, lit.Value
			}
		} else if cr, ok := bin.Right.(*QP.ColumnRef); ok {
			if lit, ok := bin.Left.(*QP.Literal); ok {
				colName, val = cr.Name, lit.Value
			}
		}
		if colName == "" {
			return nil
		}
		for idxName, idx := range db.indexes {
			if idx.Table != tableName || len(idx.Columns) == 0 || idx.Columns[0] != colName {
				continue
			}
			hmap := db.indexData[idxName]
			if hmap == nil {
				return nil
			}
			key := normalizeIndexKey(val)
			rowIdxs := hmap[key]
			if len(rowIdxs) == 0 {
				return []map[string]interface{}{} // index hit: no matching rows
			}
			rows := db.data[tableName]
			result := make([]map[string]interface{}, 0, len(rowIdxs))
			for _, ri := range rowIdxs {
				if ri >= 0 && ri < len(rows) {
					result = append(result, rows[ri])
				}
			}
			return result
		}

	case QP.TokenBetween:
		// col BETWEEN lo AND hi
		cr, ok := bin.Left.(*QP.ColumnRef)
		if !ok {
			return nil
		}
		rangeExpr, ok := bin.Right.(*QP.BinaryExpr)
		if !ok || rangeExpr.Op != QP.TokenAnd {
			return nil
		}
		loLit, ok1 := rangeExpr.Left.(*QP.Literal)
		hiLit, ok2 := rangeExpr.Right.(*QP.Literal)
		if !ok1 || !ok2 {
			return nil
		}
		return db.tryIndexRangeScan(tableName, cr.Name, loLit.Value, hiLit.Value)

	case QP.TokenIn:
		// col IN (a, b, c)
		cr, ok := bin.Left.(*QP.ColumnRef)
		if !ok {
			return nil
		}
		lit, ok := bin.Right.(*QP.Literal)
		if !ok {
			return nil
		}
		vals, ok := lit.Value.([]interface{})
		if !ok {
			return nil
		}
		return db.tryIndexInLookup(tableName, cr.Name, vals)

	case QP.TokenLike:
		// col LIKE 'prefix%'
		cr, ok := bin.Left.(*QP.ColumnRef)
		if !ok {
			return nil
		}
		lit, ok := bin.Right.(*QP.Literal)
		if !ok {
			return nil
		}
		pattern, ok := lit.Value.(string)
		if !ok {
			return nil
		}
		return db.tryIndexLikePrefix(tableName, cr.Name, pattern)
	}
	return nil
}

// tryIndexRangeScan uses a secondary index to answer col BETWEEN lo AND hi.
// It iterates all keys in the index and collects rows whose key is in [lo, hi].
func (db *Database) tryIndexRangeScan(tableName, colName string, lo, hi interface{}) []map[string]interface{} {
	for idxName, idx := range db.indexes {
		if idx.Table != tableName || len(idx.Columns) == 0 || idx.Columns[0] != colName {
			continue
		}
		hmap := db.indexData[idxName]
		if hmap == nil {
			return nil
		}
		rows := db.data[tableName]
		var rowIdxs []int
		for k, idxs := range hmap {
			if compareIndexVals(k, lo) >= 0 && compareIndexVals(k, hi) <= 0 {
				rowIdxs = append(rowIdxs, idxs...)
			}
		}
		if len(rowIdxs) == 0 {
			return []map[string]interface{}{}
		}
		result := make([]map[string]interface{}, 0, len(rowIdxs))
		for _, ri := range rowIdxs {
			if ri >= 0 && ri < len(rows) {
				result = append(result, rows[ri])
			}
		}
		return result
	}
	return nil
}

// tryIndexInLookup uses a secondary index to answer col IN (a, b, c).
// It performs a hash lookup for each value and unions the results.
func (db *Database) tryIndexInLookup(tableName, colName string, vals []interface{}) []map[string]interface{} {
	for idxName, idx := range db.indexes {
		if idx.Table != tableName || len(idx.Columns) == 0 || idx.Columns[0] != colName {
			continue
		}
		hmap := db.indexData[idxName]
		if hmap == nil {
			return nil
		}
		rows := db.data[tableName]
		seen := make(map[int]struct{})
		var result []map[string]interface{}
		for _, val := range vals {
			key := normalizeIndexKey(val)
			for _, ri := range hmap[key] {
				if _, ok := seen[ri]; ok {
					continue
				}
				seen[ri] = struct{}{}
				if ri >= 0 && ri < len(rows) {
					result = append(result, rows[ri])
				}
			}
		}
		if result == nil {
			return []map[string]interface{}{}
		}
		return result
	}
	return nil
}

// tryIndexLikePrefix uses a secondary index to answer col LIKE 'prefix%'.
// The pattern must be a simple constant prefix followed by a single trailing '%'
// with no wildcards in the prefix itself.
func (db *Database) tryIndexLikePrefix(tableName, colName, pattern string) []map[string]interface{} {
	pctIdx := strings.Index(pattern, "%")
	if pctIdx < 0 {
		return nil // no wildcard — no help from this function
	}
	for i := 0; i < pctIdx; i++ {
		if pattern[i] == '_' {
			return nil // wildcard before %, can't do prefix scan
		}
	}
	if pattern[pctIdx+1:] != "" {
		return nil // not a pure trailing %, e.g. 'pre%fix'
	}
	prefix := pattern[:pctIdx]
	if prefix == "" {
		return nil // LIKE '%' matches everything — no index benefit
	}

	for idxName, idx := range db.indexes {
		if idx.Table != tableName || len(idx.Columns) == 0 || idx.Columns[0] != colName {
			continue
		}
		hmap := db.indexData[idxName]
		if hmap == nil {
			return nil
		}
		rows := db.data[tableName]
		var rowIdxs []int
		for k, idxs := range hmap {
			kStr, ok := k.(string)
			if !ok {
				continue
			}
			if strings.HasPrefix(kStr, prefix) {
				rowIdxs = append(rowIdxs, idxs...)
			}
		}
		if len(rowIdxs) == 0 {
			return []map[string]interface{}{}
		}
		result := make([]map[string]interface{}, 0, len(rowIdxs))
		for _, ri := range rowIdxs {
			if ri >= 0 && ri < len(rows) {
				result = append(result, rows[ri])
			}
		}
		return result
	}
	return nil
}

// execExistsSubquery checks whether a subquery returns at least one row,
// short-circuiting after the first match by applying LIMIT 1.
func (db *Database) execExistsSubquery(stmt *QP.SelectStmt, outerRow map[string]interface{}) (bool, error) {
	// Shallow-copy the stmt so we can override Limit without mutating the shared AST.
	stmtCopy := *stmt
	stmtCopy.Limit = &QP.Literal{Value: int64(1)}
	rows, err := db.execSelectStmtWithContext(&stmtCopy, outerRow)
	if err != nil {
		return false, err
	}
	return len(rows.Data) > 0, nil
}

func (db *Database) applyOrderBy(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	if len(orderBy) == 0 || len(data) == 0 {
		return data
	}
	return db.engine.SortRows(data, orderBy, cols)
}

func (db *Database) applyLimit(rows *Rows, limitExpr QP.Expr, offsetExpr QP.Expr) (*Rows, error) {
	if rows == nil || len(rows.Data) == 0 {
		return rows, nil
	}

	limit := len(rows.Data)
	offset := 0

	if limitExpr != nil {
		if lit, ok := limitExpr.(*QP.Literal); ok {
			if num, ok := lit.Value.(int64); ok {
				limit = int(num)
			}
		}
	}

	if offsetExpr != nil {
		if lit, ok := offsetExpr.(*QP.Literal); ok {
			if num, ok := lit.Value.(int64); ok {
				offset = int(num)
			}
		}
	}

	// Use QE's ApplyLimit
	rows.Data = db.engine.ApplyLimit(rows.Data, limit, offset)
	return rows, nil
}

func (db *Database) serializeRow(row map[string]interface{}) []byte {
	result := make([]byte, 0)
	for key, val := range row {
		result = append(result, []byte(key)...)
		result = append(result, '=')
		switch v := val.(type) {
		case int64:
			result = append(result, []byte(fmt.Sprintf("%d", v))...)
		case float64:
			result = append(result, []byte(fmt.Sprintf("%f", v))...)
		case string:
			result = append(result, []byte(v)...)
		case []byte:
			result = append(result, '[')
			for i, b := range v {
				if i > 0 {
					result = append(result, ',')
				}
				result = append(result, []byte(fmt.Sprintf("%d", b))...)
			}
			result = append(result, ']')
		case nil:
		default:
			result = append(result, []byte(fmt.Sprintf("%v", v))...)
		}
		result = append(result, ';')
	}

	return result
}

func (db *Database) querySqliteMaster(stmt *QP.SelectStmt) (*Rows, error) {
	allResults := make([]map[string]interface{}, 0)
	for tableName := range db.tables {
		sql := fmt.Sprintf("CREATE TABLE %s ()", tableName)
		allResults = append(allResults, map[string]interface{}{
			"type":     "table",
			"name":     tableName,
			"tbl_name": tableName,
			"rootpage": int64(0),
			"sql":      sql,
		})
	}
	// Include views
	for viewName, viewSQL := range db.views {
		allResults = append(allResults, map[string]interface{}{
			"type":     "view",
			"name":     viewName,
			"tbl_name": viewName,
			"rootpage": int64(0),
			"sql":      "CREATE VIEW " + viewName + " AS " + viewSQL,
		})
	}
	// Include indexes
	for idxName, idx := range db.indexes {
		allResults = append(allResults, map[string]interface{}{
			"type":     "index",
			"name":     idxName,
			"tbl_name": idx.Table,
			"rootpage": int64(0),
			"sql":      fmt.Sprintf("CREATE INDEX %s ON %s", idxName, idx.Table),
		})
	}

	filtered := make([]map[string]interface{}, 0)
	for _, row := range allResults {
		if db.evalWhereOnMap(stmt.Where, row) {
			filtered = append(filtered, row)
		}
	}

	sqliteMasterCols := []string{"type", "name", "tbl_name", "rootpage", "sql"}

	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			if cr.Name == "*" {
				cols = append(cols, sqliteMasterCols...)
			} else {
				cols = append(cols, cr.Name)
			}
		}
	}
	if len(cols) == 0 {
		cols = sqliteMasterCols
	}

	resultData := make([][]interface{}, 0)
	for _, row := range filtered {
		resultRow := make([]interface{}, 0)
		for _, colName := range cols {
			resultRow = append(resultRow, row[colName])
		}
		resultData = append(resultData, resultRow)
	}

	// Sort by name if ORDER BY name is requested
	if stmt.OrderBy != nil {
		for _, ob := range stmt.OrderBy {
			if cr, ok := ob.Expr.(*QP.ColumnRef); ok && cr.Name == "name" {
				sort.Slice(resultData, func(i, j int) bool {
					nameIdx := -1
					for k, c := range cols {
						if c == "name" {
							nameIdx = k
							break
						}
					}
					if nameIdx < 0 {
						return false
					}
					ni := fmt.Sprintf("%v", resultData[i][nameIdx])
					nj := fmt.Sprintf("%v", resultData[j][nameIdx])
					if ob.Desc {
						return ni > nj
					}
					return ni < nj
				})
			}
		}
	} else {
		// Default sort by name for deterministic output
		nameIdx := -1
		for k, c := range cols {
			if c == "name" {
				nameIdx = k
				break
			}
		}
		if nameIdx >= 0 {
			sort.Slice(resultData, func(i, j int) bool {
				ni := fmt.Sprintf("%v", resultData[i][nameIdx])
				nj := fmt.Sprintf("%v", resultData[j][nameIdx])
				return ni < nj
			})
		}
	}

	return &Rows{Columns: cols, Data: resultData}, nil
}

// queryInformationSchema handles queries to information_schema virtual tables
func (db *Database) queryInformationSchema(stmt *QP.SelectStmt, tableName string) (*Rows, error) {
	// Extract view name from "information_schema.viewname"
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid information_schema table name: %s", tableName)
	}
	viewName := strings.ToLower(parts[1])

	// Check schema cache for pre-built view data.
	// The cache is invalidated on DDL; DML never changes the schema.
	var allResults [][]interface{}
	var columnNames []string
	if db.schemaCache != nil {
		if cachedCols, cachedRows, ok := db.schemaCache.Get(viewName); ok {
			columnNames = cachedCols
			allResults = cachedRows
		}
	}

	if allResults == nil {
		// Generate data based on view type
		switch viewName {
	case "columns":
		columnNames = []string{"column_name", "table_name", "table_schema", "data_type", "is_nullable", "column_default"}
		// Extract columns from in-memory schema
		for tblName, colTypes := range db.tables {
			orderedCols := db.columnOrder[tblName]
			pkCols := db.primaryKeys[tblName]
			pkMap := make(map[string]bool)
			for _, pk := range pkCols {
				pkMap[pk] = true
			}
			for _, colName := range orderedCols {
				colType := colTypes[colName]
				isNullable := "YES"
				// PRIMARY KEY columns cannot be NULL
				if pkMap[colName] {
					isNullable = "NO"
				} else if strings.Contains(strings.ToUpper(colType), "NOT NULL") {
					isNullable = "NO"
				}
				allResults = append(allResults, []interface{}{
					colName,
					tblName,
					"main",
					colType,
					isNullable,
					nil, // column_default (not tracked yet)
				})
			}
		}

	case "tables":
		columnNames = []string{"table_name", "table_schema", "table_type"}
		// Extract tables from in-memory schema
		for tblName := range db.tables {
			allResults = append(allResults, []interface{}{
				tblName,
				"main",
				"BASE TABLE",
			})
		}

	case "views":
		columnNames = []string{"table_name", "table_schema", "view_definition"}
		// No views tracked yet, return empty

	case "table_constraints":
		columnNames = []string{"constraint_name", "table_name", "table_schema", "constraint_type"}
		// Extract PRIMARY KEY constraints from in-memory schema
		for tblName, pkCols := range db.primaryKeys {
			if len(pkCols) > 0 {
				constraintName := fmt.Sprintf("pk_%s", tblName)
				allResults = append(allResults, []interface{}{
					constraintName,
					tblName,
					"main",
					"PRIMARY KEY",
				})
			}
		}

	case "referential_constraints":
		columnNames = []string{"constraint_name", "unique_constraint_schema", "unique_constraint_name"}
		// No foreign keys tracked yet, return empty

	case "key_column_usage":
		columnNames = []string{"constraint_name", "table_name", "table_schema", "column_name", "ordinal_position"}
		// Extract PRIMARY KEY column usage from in-memory schema
		for tblName, pkCols := range db.primaryKeys {
			if len(pkCols) > 0 {
				constraintName := fmt.Sprintf("pk_%s", tblName)
				for i, colName := range pkCols {
					allResults = append(allResults, []interface{}{
						constraintName,
						tblName,
						"main",
						colName,
						int64(i + 1), // ordinal position starts at 1
					})
				}
			}
		}

	default:
		return nil, fmt.Errorf("unknown information_schema view: %s", viewName)
	}
		// Store built results in schema cache for subsequent calls.
		if db.schemaCache != nil {
			db.schemaCache.Set(viewName, columnNames, allResults)
		}
	} // end if allResults == nil

	// Filter based on WHERE clause (simple support)
	filtered := allResults
	if stmt.Where != nil {
		filtered = make([][]interface{}, 0)
		for _, row := range allResults {
			if db.matchesWhereClause(row, columnNames, stmt.Where) {
				filtered = append(filtered, row)
			}
		}
	}

	// Select specific columns or all
	var selectedCols []string
	var selectedData [][]interface{}

	// Check if SELECT *
	if len(stmt.Columns) == 1 {
		if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" {
			// SELECT * - return all columns
			selectedCols = columnNames
			selectedData = filtered
		}
	}

	if len(selectedCols) == 0 {
		// SELECT specific columns
		for _, col := range stmt.Columns {
			if cr, ok := col.(*QP.ColumnRef); ok {
				selectedCols = append(selectedCols, cr.Name)
			}
		}

		// Project columns
		for _, row := range filtered {
			projectedRow := make([]interface{}, 0)
			for _, selCol := range selectedCols {
				// Find column index
				colIdx := -1
				for i, cn := range columnNames {
					if cn == selCol {
						colIdx = i
						break
					}
				}
				if colIdx >= 0 && colIdx < len(row) {
					projectedRow = append(projectedRow, row[colIdx])
				} else {
					projectedRow = append(projectedRow, nil)
				}
			}
			selectedData = append(selectedData, projectedRow)
		}
	}

	// Apply ORDER BY if present
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		rows := &Rows{Columns: selectedCols, Data: selectedData}
		sortedRows, err := db.sortResults(rows, stmt.OrderBy)
		if err != nil {
			return nil, err
		}
		return sortedRows, nil
	}

	return &Rows{Columns: selectedCols, Data: selectedData}, nil
}

// matchesWhereClause checks if a row matches the WHERE clause
func (db *Database) matchesWhereClause(row []interface{}, columnNames []string, where QP.Expr) bool {
	// Simple WHERE filtering (only equality checks for now)
	if binExpr, ok := where.(*QP.BinaryExpr); ok && binExpr.Op == QP.TokenEq {
		if colRef, ok := binExpr.Left.(*QP.ColumnRef); ok {
			if lit, ok := binExpr.Right.(*QP.Literal); ok {
				// Find column index
				colIdx := -1
				for i, cn := range columnNames {
					if cn == colRef.Name {
						colIdx = i
						break
					}
				}
				if colIdx >= 0 && colIdx < len(row) {
					return row[colIdx] == lit.Value
				}
			}
		}
	}
	// Default to include if we can't evaluate
	return true
}

func (db *Database) combineRows(left, right map[string]interface{}, stmt *QP.SelectStmt) []interface{} {
	row := make([]interface{}, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			colName := cr.Name
			if idx := strings.Index(cr.Name, "."); idx > 0 {
				colName = cr.Name[idx+1:]
			}
			if val, ok := left[colName]; ok {
				row = append(row, val)
			} else if val, ok := right[colName]; ok {
				row = append(row, val)
			} else {
				row = append(row, nil)
			}
		}
	}
	return row
}

func (db *Database) getJoinColumns(stmt *QP.SelectStmt) []string {
	cols := make([]string, 0)
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			colName := cr.Name
			if idx := strings.Index(cr.Name, "."); idx > 0 {
				colName = cr.Name[idx+1:]
			}
			cols = append(cols, colName)
		}
	}
	return cols
}

func (db *Database) getRightColumns(right []map[string]interface{}) []string {
	if len(right) == 0 {
		return []string{}
	}
	cols := make([]string, 0)
	for k := range right[0] {
		cols = append(cols, k)
	}
	return cols
}

// execCreateView handles CREATE VIEW
func (db *Database) execCreateView(stmt *QP.CreateViewStmt, origSQL string) (Result, error) {
	util.AssertNotNil(stmt, "CreateViewStmt")
	util.Assert(stmt.Name != "", "CreateViewStmt.Name cannot be empty")
	if _, exists := db.views[stmt.Name]; exists {
		if stmt.IfNotExists {
			return Result{}, nil
		}
		return Result{}, fmt.Errorf("view %s already exists", stmt.Name)
	}
	// Also check tables
	if _, exists := db.tables[stmt.Name]; exists {
		return Result{}, fmt.Errorf("view %s already exists as a table", stmt.Name)
	}
	// Reconstruct the SELECT SQL from the statement - we need to store it
	// Find the AS keyword position in origSQL to extract SELECT part
	upper := strings.ToUpper(origSQL)
	asIdx := strings.Index(upper, " AS ")
	if asIdx < 0 {
		asIdx = strings.Index(upper, "\nAS\n")
	}
	var selectSQL string
	if asIdx >= 0 {
		selectSQL = strings.TrimSpace(origSQL[asIdx+4:])
	} else {
		// Fallback: rebuild from stmt
		selectSQL = "SELECT * FROM unknown"
	}
	db.views[stmt.Name] = selectSQL
	return Result{}, nil
}

// execDerivedTableQuery materializes a derived table (subquery in FROM) and executes the outer query.
// SELECT ... FROM (SELECT ...) AS alias ...
func (db *Database) execValuesTable(stmt *QP.SelectStmt) (*Rows, error) {
	fromRef := stmt.From
	cols := fromRef.ValueCols
	if len(cols) == 0 && len(fromRef.Values) > 0 {
		cols = make([]string, len(fromRef.Values[0]))
		for i := range cols {
			cols[i] = fmt.Sprintf("column%d", i+1)
		}
	}

	alias := fromRef.Alias
	if alias == "" {
		alias = "__values__"
	}

	colTypes := make(map[string]string)
	for _, col := range cols {
		colTypes[col] = "TEXT"
	}
	db.tables[alias] = colTypes
	db.columnOrder[alias] = cols
	db.data[alias] = make([]map[string]interface{}, len(fromRef.Values))
	for i, row := range fromRef.Values {
		rm := make(map[string]interface{})
		for j, col := range cols {
			if j < len(row) {
				rm[col] = evalLiteralExpr(row[j])
			}
		}
		db.data[alias][i] = rm
	}

	origFrom := stmt.From
	stmt.From = &QP.TableRef{Name: alias, Alias: alias}
	rows, err := db.execSelectStmt(stmt)
	stmt.From = origFrom

	delete(db.tables, alias)
	delete(db.columnOrder, alias)
	delete(db.data, alias)

	return rows, err
}

func (db *Database) execRecursiveCTE(cte *QP.CTEClause) (*Rows, error) {
	sel := cte.Select
	if sel.SetOp != "UNION" && sel.SetOp != "UNION ALL" {
		return db.execSelectStmt(sel)
	}

	// Execute anchor (left side)
	anchorStmt := *sel
	anchorStmt.SetOp = ""
	anchorStmt.SetOpAll = false
	anchorStmt.SetOpRight = nil
	anchorRows, err := db.execSelectStmt(&anchorStmt)
	if err != nil {
		return nil, err
	}
	if anchorRows == nil {
		anchorRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}

	cols := anchorRows.Columns
	if len(cte.Columns) > 0 && len(cte.Columns) == len(cols) {
		cols = cte.Columns
	}

	allRows := make([][]interface{}, len(anchorRows.Data))
	copy(allRows, anchorRows.Data)

	maxIter := 1000
	current := anchorRows
	for i := 0; i < maxIter; i++ {
		db.tables[cte.Name] = make(map[string]string)
		db.columnOrder[cte.Name] = cols
		db.data[cte.Name] = make([]map[string]interface{}, len(current.Data))
		for _, col := range cols {
			db.tables[cte.Name][col] = "TEXT"
		}
		for j, row := range current.Data {
			rm := make(map[string]interface{})
			for k, col := range cols {
				if k < len(row) {
					rm[col] = row[k]
				}
			}
			db.data[cte.Name][j] = rm
		}

		recursiveRows, rerr := db.execSelectStmt(sel.SetOpRight)

		delete(db.tables, cte.Name)
		delete(db.columnOrder, cte.Name)
		delete(db.data, cte.Name)

		if rerr != nil || recursiveRows == nil || len(recursiveRows.Data) == 0 {
			break
		}

		allRows = append(allRows, recursiveRows.Data...)
		current = recursiveRows
	}

	return &Rows{Columns: cols, Data: allRows}, nil
}

func (db *Database) execDerivedTableQuery(stmt *QP.SelectStmt) (*Rows, error) {
	subq := stmt.From.Subquery
	alias := stmt.From.Alias
	if alias == "" {
		alias = "__subq__"
	}

	// Execute the subquery to materialize its rows
	// For UNION/EXCEPT/INTERSECT subqueries, use the SetOp path to avoid VM cursor conflicts
	var subRows *Rows
	var err error
	if subq.SetOp != "" && subq.SetOpRight != nil {
		leftStmt := *subq
		leftStmt.SetOp = ""
		leftStmt.SetOpAll = false
		leftStmt.SetOpRight = nil
		leftRows, lerr := db.execSelectStmt(&leftStmt)
		if lerr != nil {
			return nil, lerr
		}
		rightRows, rerr := db.execSelectStmt(subq.SetOpRight)
		if rerr != nil {
			return nil, rerr
		}
		combined := db.applySetOp(leftRows.Data, rightRows.Data, subq.SetOp, subq.SetOpAll)
		subRows = &Rows{Columns: leftRows.Columns, Data: combined}
	} else {
		subRows, err = db.execSelectStmt(subq)
		if err != nil {
			return nil, err
		}
	}
	if subRows == nil {
		subRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}

	// Register temp table
	tempName := alias
	colTypes := make(map[string]string)
	for _, col := range subRows.Columns {
		colTypes[col] = "TEXT"
	}
	db.tables[tempName] = colTypes
	db.columnOrder[tempName] = subRows.Columns
	rowMaps := make([]map[string]interface{}, len(subRows.Data))
	for i, row := range subRows.Data {
		rm := make(map[string]interface{})
		for j, col := range subRows.Columns {
			if j < len(row) {
				rm[col] = row[j]
			}
		}
		rowMaps[i] = rm
	}
	db.data[tempName] = rowMaps

	// Rewrite stmt.From to use temp table name instead of subquery.
	// Preserve any Join from the original From (e.g., comma join of two derived tables).
	origFrom := stmt.From
	newFrom := &QP.TableRef{Name: tempName, Alias: alias, Join: origFrom.Join}

	// If the Join's right side is also a derived table, materialize it too
	var joinRightTempName string
	var joinRightOrigRef *QP.TableRef
	if origFrom.Join != nil && origFrom.Join.Right != nil && origFrom.Join.Right.Subquery != nil {
		joinRightOrigRef = origFrom.Join.Right
		rightSubq := joinRightOrigRef.Subquery
		rightAlias := joinRightOrigRef.Alias
		if rightAlias == "" {
			rightAlias = "__subq_right__"
		}
		rightRows, rerr := db.execSelectStmt(rightSubq)
		if rerr != nil {
			delete(db.tables, tempName)
			delete(db.columnOrder, tempName)
			delete(db.data, tempName)
			stmt.From = origFrom
			return nil, rerr
		}
		if rightRows == nil {
			rightRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
		}
		joinRightTempName = rightAlias
		rightColTypes := make(map[string]string)
		for _, col := range rightRows.Columns {
			rightColTypes[col] = "TEXT"
		}
		db.tables[joinRightTempName] = rightColTypes
		db.columnOrder[joinRightTempName] = rightRows.Columns
		rightRowMaps := make([]map[string]interface{}, len(rightRows.Data))
		for i, row := range rightRows.Data {
			rm := make(map[string]interface{})
			for j, col := range rightRows.Columns {
				if j < len(row) {
					rm[col] = row[j]
				}
			}
			rightRowMaps[i] = rm
		}
		db.data[joinRightTempName] = rightRowMaps
		// Update the join right ref to point to the materialized temp table
		newFrom.Join = &QP.Join{
			Type:  origFrom.Join.Type,
			Left:  origFrom.Join.Left,
			Right: &QP.TableRef{Name: joinRightTempName, Alias: rightAlias},
			Cond:  origFrom.Join.Cond,
		}
	}

	stmt.From = newFrom

	// Execute outer query
	rows, err := db.execVMQuery("", stmt)

	// Restore and clean up
	stmt.From = origFrom
	delete(db.tables, tempName)
	delete(db.columnOrder, tempName)
	delete(db.data, tempName)
	if joinRightTempName != "" {
		delete(db.tables, joinRightTempName)
		delete(db.columnOrder, joinRightTempName)
		delete(db.data, joinRightTempName)
	}

	if err != nil {
		return nil, err
	}
	if rows == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	// Apply ORDER BY and LIMIT if present
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		topK := extractLimitInt(stmt.Limit, stmt.Offset)
		rows, err = db.sortResultsTopK(rows, stmt.OrderBy, topK)
		if err != nil {
			return nil, err
		}
	}
	if stmt.Limit != nil {
		rows, err = db.applyLimit(rows, stmt.Limit, stmt.Offset)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// queryView executes a query against a view by executing the underlying SELECT
func (db *Database) queryView(viewSQL string, outerStmt *QP.SelectStmt, origSQL string) (*Rows, error) {
	// Execute the underlying view SELECT
	rows, err := db.Query(viewSQL)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return &Rows{Columns: []string{}, Data: [][]interface{}{}}, nil
	}

	// Handle UNION/EXCEPT/INTERSECT with view on left side
	if outerStmt.SetOp != "" && outerStmt.SetOpRight != nil {
		rightRows, err := db.execSelectStmt(outerStmt.SetOpRight)
		if err != nil {
			return nil, err
		}
		if len(rows.Columns) != len(rightRows.Columns) {
			return nil, fmt.Errorf("SELECTs to the left and right of %s do not have the same number of result columns", outerStmt.SetOp)
		}
		combined := db.applySetOp(rows.Data, rightRows.Data, outerStmt.SetOp, outerStmt.SetOpAll)
		result := &Rows{Columns: rows.Columns, Data: combined}
		if len(outerStmt.OrderBy) > 0 {
			result, err = db.sortResults(result, outerStmt.OrderBy)
			if err != nil {
				return nil, err
			}
		}
		if outerStmt.Limit != nil {
			result, err = db.applyLimit(result, outerStmt.Limit, outerStmt.Offset)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Apply outer SELECT column filtering if needed
	if len(outerStmt.Columns) > 0 {
		if cr, ok := outerStmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" {
			// SELECT * - return all
		} else {
			// Filter to requested columns
			reqCols := make([]string, 0)
			colIdx := make(map[string]int)
			for i, col := range rows.Columns {
				colIdx[col] = i
			}
			for _, c := range outerStmt.Columns {
				if cr, ok := c.(*QP.ColumnRef); ok {
					reqCols = append(reqCols, cr.Name)
				} else if ae, ok := c.(*QP.AliasExpr); ok {
					if cr, ok := ae.Expr.(*QP.ColumnRef); ok {
						reqCols = append(reqCols, cr.Name)
					}
				}
			}
			if len(reqCols) > 0 {
				newData := make([][]interface{}, len(rows.Data))
				for i, row := range rows.Data {
					newRow := make([]interface{}, len(reqCols))
					for j, col := range reqCols {
						if idx, ok := colIdx[col]; ok {
							newRow[j] = row[idx]
						}
					}
					newData[i] = newRow
				}
				rows = &Rows{Columns: reqCols, Data: newData}
			}
		}
	}

	// Apply outer WHERE
	if outerStmt.Where != nil {
		colIdx := make(map[string]int)
		for i, col := range rows.Columns {
			colIdx[col] = i
		}
		filtered := make([][]interface{}, 0)
		for _, row := range rows.Data {
			rowMap := make(map[string]interface{})
			for col, idx := range colIdx {
				rowMap[col] = row[idx]
			}
			if db.evalWhereOnMap(outerStmt.Where, rowMap) {
				filtered = append(filtered, row)
			}
		}
		rows.Data = filtered
	}

	// Apply outer ORDER BY
	if len(outerStmt.OrderBy) > 0 {
		var err error
		topK := extractLimitInt(outerStmt.Limit, outerStmt.Offset)
		rows, err = db.sortResultsTopK(rows, outerStmt.OrderBy, topK)
		if err != nil {
			return nil, err
		}
	}

	// Apply outer LIMIT
	if outerStmt.Limit != nil {
		var err error
		rows, err = db.applyLimit(rows, outerStmt.Limit, outerStmt.Offset)
		if err != nil {
			return nil, err
		}
	}

	return rows, nil
}

// evalWhereOnMap evaluates a WHERE expression against a map row
func (db *Database) evalWhereOnMap(expr QP.Expr, row map[string]interface{}) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case *QP.BinaryExpr:
		switch e.Op {
		case QP.TokenAnd:
			return db.evalWhereOnMap(e.Left, row) && db.evalWhereOnMap(e.Right, row)
		case QP.TokenOr:
			return db.evalWhereOnMap(e.Left, row) || db.evalWhereOnMap(e.Right, row)
		default:
			lv := db.evalExprOnMap(e.Left, row)
			rv := db.evalExprOnMap(e.Right, row)
			return db.engine.CompareVals(lv, rv) == 0
		}
	case *QP.UnaryExpr:
		if e.Op == QP.TokenNot {
			return !db.evalWhereOnMap(e.Expr, row)
		}
	}
	return true
}

func (db *Database) evalExprOnMap(expr QP.Expr, row map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.ColumnRef:
		if v, ok := row[e.Name]; ok {
			return v
		}
		if idx := strings.LastIndex(e.Name, "."); idx >= 0 {
			return row[e.Name[idx+1:]]
		}
		return nil
	case *QP.AnyAllExpr:
		subRows, err := db.execSelectStmt(e.Subquery)
		if err != nil || subRows == nil || len(subRows.Data) == 0 {
			if e.Quantifier == "ALL" {
				return int64(1) // vacuously true
			}
			return nil
		}
		leftVal := db.evalExprOnMap(e.Left, row)
		for _, subRow := range subRows.Data {
			if len(subRow) == 0 {
				continue
			}
			rightVal := subRow[0]
			cmp := compareForAnyAll(leftVal, rightVal)
			match := false
			switch e.Op {
			case QP.TokenEq:
				match = cmp == 0
			case QP.TokenNe:
				match = cmp != 0
			case QP.TokenLt:
				match = cmp < 0
			case QP.TokenLe:
				match = cmp <= 0
			case QP.TokenGt:
				match = cmp > 0
			case QP.TokenGe:
				match = cmp >= 0
			}
			if e.Quantifier == "ALL" && !match {
				return nil
			}
			if (e.Quantifier == "ANY" || e.Quantifier == "SOME") && match {
				return int64(1)
			}
		}
		if e.Quantifier == "ALL" {
			return int64(1)
		}
		return nil
	}
	return nil
}

// compareForAnyAll compares two values for ANY/ALL predicates.
func compareForAnyAll(a, b interface{}) int {
if a == nil && b == nil {
return 0
}
if a == nil {
return -1
}
if b == nil {
return 1
}
switch av := a.(type) {
case int64:
switch bv := b.(type) {
case int64:
if av < bv {
return -1
}
if av > bv {
return 1
}
return 0
case float64:
fa := float64(av)
if fa < bv {
return -1
}
if fa > bv {
return 1
}
return 0
}
case float64:
switch bv := b.(type) {
case float64:
if av < bv {
return -1
}
if av > bv {
return 1
}
return 0
case int64:
fb := float64(bv)
if av < fb {
return -1
}
if av > fb {
return 1
}
return 0
}
case string:
if bv, ok := b.(string); ok {
if av < bv {
return -1
}
if av > bv {
return 1
}
return 0
}
}
as := fmt.Sprintf("%v", a)
bs := fmt.Sprintf("%v", b)
if as < bs {
return -1
}
if as > bs {
return 1
}
return 0
}

// execCreateTableAsSelect handles CREATE TABLE ... AS SELECT
func (db *Database) execCreateTableAsSelect(stmt *QP.CreateTableStmt) (Result, error) {
	// Execute the SELECT
	// Build SQL for the inner SELECT
	rows, err := db.execSelectStmt(stmt.AsSelect)
	if err != nil {
		return Result{}, err
	}
	if rows == nil {
		rows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}

	// Create the table with columns from SELECT result
	schema := make(map[string]VM.ColumnType)
	colTypes := make(map[string]string)
	db.columnDefaults[stmt.Name] = make(map[string]interface{})
	db.columnNotNull[stmt.Name] = make(map[string]bool)
	db.columnChecks[stmt.Name] = make(map[string]QP.Expr)

	for _, col := range rows.Columns {
		// Try to infer column type from the source table schema
		colType := db.inferColumnTypeFromSelect(col, stmt.AsSelect)
		schema[col] = VM.ColumnType{Name: col, Type: colType}
		colTypes[col] = colType
	}
	db.engine.RegisterTable(stmt.Name, schema)
	db.tables[stmt.Name] = colTypes
	db.columnOrder[stmt.Name] = rows.Columns

	bt := DS.NewBTree(db.pm, 0, true)
	db.tableBTrees[stmt.Name] = bt

	// Insert rows
	db.data[stmt.Name] = make([]map[string]interface{}, 0, len(rows.Data))
	for _, row := range rows.Data {
		rowMap := make(map[string]interface{})
		for i, col := range rows.Columns {
			if i < len(row) {
				rowMap[col] = row[i]
			}
		}
		db.data[stmt.Name] = append(db.data[stmt.Name], rowMap)
	}
	// Build HybridStore populated with the newly inserted rows.
	db.hybridStores[stmt.Name] = db.buildHybridStore(stmt.Name)
	db.hybridStoresDirty[stmt.Name] = false

	return Result{}, nil
}

// inferColumnTypeFromSelect tries to determine a column's type from its source in a SELECT statement.
// Falls back to "TEXT" if the type cannot be determined.
func (db *Database) inferColumnTypeFromSelect(colName string, sel *QP.SelectStmt) string {
	if sel == nil || sel.From == nil {
		return "TEXT"
	}
	srcTable := sel.From.Name
	if srcTable == "" {
		return "TEXT"
	}
	srcTypes, hasSrc := db.tables[srcTable]
	// Scan SELECT columns to find matching expressions
	for _, expr := range sel.Columns {
		switch e := expr.(type) {
		case *QP.ColumnRef:
			// Direct column reference: SELECT a FROM t1 → column 'a' gets t1's type
			if e.Name == colName && hasSrc {
				if typ, ok := srcTypes[e.Name]; ok {
					return normalizeTypeForPragma(typ)
				}
			}
		case *QP.AliasExpr:
			alias := e.Alias
			if alias == colName {
				// Check if inner is a simple column ref
				if cr, ok := e.Expr.(*QP.ColumnRef); ok && hasSrc {
					if typ, ok := srcTypes[cr.Name]; ok {
						return normalizeTypeForPragma(typ)
					}
				}
				// Expression alias (e.g. a * 2 AS double_a): SQLite uses empty type
				return ""
			}
		}
	}
	// Direct column lookup in source table
	if hasSrc {
		if typ, ok := srcTypes[colName]; ok {
			return normalizeTypeForPragma(typ)
		}
	}
	return "TEXT"
}

// normalizeTypeForPragma maps declared types to the type SQLite uses in PRAGMA table_info for CTAS.
func normalizeTypeForPragma(typ string) string {
	switch strings.ToUpper(typ) {
	case "INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return "INT"
	case "REAL", "FLOAT", "DOUBLE":
		return "REAL"
	case "BLOB":
		return "BLOB"
	default:
		return typ
	}
}
func (db *Database) execAlterTable(stmt *QP.AlterTableStmt) (Result, error) {
	util.AssertNotNil(stmt, "AlterTableStmt")
	util.Assert(stmt.Table != "", "AlterTableStmt.Table cannot be empty")
	switch stmt.Action {
	case "ADD_COLUMN":
		if _, exists := db.tables[stmt.Table]; !exists {
			return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
		}
		col := stmt.Column
		db.tables[stmt.Table][col.Name] = col.Type
		db.columnOrder[stmt.Table] = append(db.columnOrder[stmt.Table], col.Name)
		db.engine.RegisterTable(stmt.Table, db.buildSchema(stmt.Table))

		// Add default value for existing rows
		var defaultVal interface{}
		if col.Default != nil {
			defaultVal = db.evalConstExpr(col.Default)
		}

		// Update existing rows with the new column
		for i := range db.data[stmt.Table] {
			if db.data[stmt.Table][i] == nil {
				db.data[stmt.Table][i] = make(map[string]interface{})
			}
			db.data[stmt.Table][i][col.Name] = defaultVal
		}

		if col.Default != nil {
			if db.columnDefaults[stmt.Table] == nil {
				db.columnDefaults[stmt.Table] = make(map[string]interface{})
			}
			db.columnDefaults[stmt.Table][col.Name] = col.Default
		}
		if col.NotNull {
			if db.columnNotNull[stmt.Table] == nil {
				db.columnNotNull[stmt.Table] = make(map[string]bool)
			}
			db.columnNotNull[stmt.Table][col.Name] = true
		}
		return Result{}, nil

	case "RENAME_TO":
		if _, exists := db.tables[stmt.Table]; !exists {
			return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
		}
		// Rename table
		db.tables[stmt.NewName] = db.tables[stmt.Table]
		db.data[stmt.NewName] = db.data[stmt.Table]
		db.columnOrder[stmt.NewName] = db.columnOrder[stmt.Table]
		db.primaryKeys[stmt.NewName] = db.primaryKeys[stmt.Table]
		db.columnDefaults[stmt.NewName] = db.columnDefaults[stmt.Table]
		db.columnNotNull[stmt.NewName] = db.columnNotNull[stmt.Table]
		db.columnChecks[stmt.NewName] = db.columnChecks[stmt.Table]
		db.tableBTrees[stmt.NewName] = db.tableBTrees[stmt.Table]

		delete(db.tables, stmt.Table)
		delete(db.data, stmt.Table)
		delete(db.columnOrder, stmt.Table)
		delete(db.primaryKeys, stmt.Table)
		delete(db.columnDefaults, stmt.Table)
		delete(db.columnNotNull, stmt.Table)
		delete(db.columnChecks, stmt.Table)
		delete(db.tableBTrees, stmt.Table)
		// Move hybrid store to new name.
		if hs, ok := db.hybridStores[stmt.Table]; ok {
			db.hybridStores[stmt.NewName] = hs
			delete(db.hybridStores, stmt.Table)
		}
		db.hybridStoresDirty[stmt.NewName] = true
		delete(db.hybridStoresDirty, stmt.Table)
		return Result{}, nil
	}
	return Result{}, nil
}

func (db *Database) buildSchema(tableName string) map[string]VM.ColumnType {
	schema := make(map[string]VM.ColumnType)
	for col, typ := range db.tables[tableName] {
		schema[col] = VM.ColumnType{Name: col, Type: typ}
	}
	return schema
}

// evalConstExpr evaluates a constant expression (literal, etc.)
func (db *Database) evalConstExpr(expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}
	if lit, ok := expr.(*QP.Literal); ok {
		return lit.Value
	}
	return nil
}

// isAllLiteralValues reports whether all values in a multi-row INSERT are constant
// literals (no column refs, function calls, or subqueries).  Only *QP.Literal and
// negated-literal *QP.UnaryExpr nodes are accepted.
func isAllLiteralValues(stmt *QP.InsertStmt) bool {
	if len(stmt.Values) == 0 {
		return false
	}
	for _, row := range stmt.Values {
		for _, val := range row {
			if !isLiteralExpr(val) {
				return false
			}
		}
	}
	return true
}

// isLiteralExpr reports whether expr is a constant literal safe to evaluate
// without the VM (covers *QP.Literal and negated literals like -5 or -3.14).
func isLiteralExpr(expr QP.Expr) bool {
	switch e := expr.(type) {
	case *QP.Literal:
		return true
	case *QP.UnaryExpr:
		if e.Op == QP.TokenMinus {
			_, ok := e.Expr.(*QP.Literal)
			return ok
		}
	case nil:
		return true
	}
	return false
}

// evalLiteralExpr evaluates a constant literal expression to its Go value.
// Handles *QP.Literal and negated literals (*QP.UnaryExpr with TokenMinus).
func evalLiteralExpr(expr QP.Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value
	case *QP.UnaryExpr:
		if e.Op == QP.TokenMinus {
			if lit, ok := e.Expr.(*QP.Literal); ok {
				switch v := lit.Value.(type) {
				case int64:
					return -v
				case float64:
					return -v
				case int:
					return -int64(v)
				}
			}
		}
	}
	return nil
}

// execInsertBatch is a fast-path for simple INSERT statements whose VALUES rows
// all contain constant literals.  It bypasses tokenize/compile/VM and directly
// calls InsertRow for each row, reducing overhead for bulk inserts.
// Returns (result, error, true) when handled; (Result{}, nil, false) to fall through.
func (db *Database) execInsertBatch(stmt *QP.InsertStmt) (Result, error, bool) {
	util.AssertNotNil(stmt, "InsertStmt")
	// Only handle simple literal-value inserts (no SELECT, ON CONFLICT, DEFAULT VALUES).
	if stmt.SelectQuery != nil || stmt.OnConflict != nil || stmt.UseDefaults {
		return Result{}, nil, false
	}
	if !isAllLiteralValues(stmt) {
		return Result{}, nil, false
	}

	tableName := db.resolveTableName(stmt.Table)
	tableCols := db.columnOrder[tableName]
	if _, exists := db.tables[tableName]; !exists {
		return Result{}, fmt.Errorf("no such table: %s", tableName), true
	}

	// Validate that explicitly specified columns exist in the table.
	if len(stmt.Columns) > 0 {
		validCols := make(map[string]bool, len(tableCols))
		for _, col := range tableCols {
			validCols[strings.ToLower(col)] = true
		}
		for _, col := range stmt.Columns {
			if !validCols[strings.ToLower(col)] {
				return Result{}, fmt.Errorf("table %s has no column named %s", tableName, col), true
			}
		}
		// Validate row value counts.
		for _, row := range stmt.Values {
			if len(row) != len(stmt.Columns) {
				return Result{}, fmt.Errorf("%d values for %d columns", len(row), len(stmt.Columns)), true
			}
		}
	} else if len(tableCols) > 0 {
		for _, row := range stmt.Values {
			if len(row) != len(tableCols) {
				return Result{}, fmt.Errorf("table %s has %d columns but %d values were supplied",
					tableName, len(tableCols), len(row)), true
			}
		}
	}

	// Check whether any non-literal default needs applying (e.g. DEFAULT (1+1)).
	// If so, fall through to VM which can evaluate arbitrary expressions.
	tableDefaults := db.columnDefaults[tableName]
	if len(stmt.Columns) > 0 && len(tableDefaults) > 0 {
		specifiedCols := make(map[string]bool, len(stmt.Columns))
		for _, col := range stmt.Columns {
			specifiedCols[strings.ToLower(col)] = true
		}
		for colName, defaultVal := range tableDefaults {
			if specifiedCols[strings.ToLower(colName)] {
				continue // column explicitly provided — default not needed
			}
			// Default needed for this column. Only accept literal defaults.
			if _, isLit := defaultVal.(*QP.Literal); !isLit {
				return Result{}, nil, false // non-literal default — fall through to VM
			}
		}
	}

	ctx := newDsVmContext(db)
	var rowsAffected int64
	for _, rowVals := range stmt.Values {
		row := make(map[string]interface{}, len(tableCols))
		if len(stmt.Columns) > 0 {
			for i, val := range rowVals {
				if i < len(stmt.Columns) {
					row[stmt.Columns[i]] = evalLiteralExpr(val)
				}
			}
		} else {
			for i, val := range rowVals {
				if i < len(tableCols) {
					row[tableCols[i]] = evalLiteralExpr(val)
				}
			}
		}
		// Apply literal defaults only for columns that are entirely absent from the row.
		// Explicit NULLs (column present but nil) are left as-is.
		for colName, defaultVal := range tableDefaults {
			if _, exists := row[colName]; !exists {
				if lit, ok := defaultVal.(*QP.Literal); ok {
					row[colName] = lit.Value
				}
			}
		}
		if err := ctx.InsertRow(tableName, row); err != nil {
			return Result{}, err, true
		}
		rowsAffected++
	}
	return Result{RowsAffected: rowsAffected}, nil, true
}

// execInsertSelect handles INSERT INTO table SELECT ...
func (db *Database) execInsertSelect(stmt *QP.InsertStmt) (Result, error) {
	util.AssertNotNil(stmt, "InsertStmt")
	util.Assert(stmt.Table != "", "InsertStmt.Table cannot be empty")
	util.Assert(stmt.SelectQuery != nil, "InsertStmt.SelectQuery cannot be nil for INSERT...SELECT")
	if _, exists := db.tables[stmt.Table]; !exists {
		return Result{}, fmt.Errorf("no such table: %s", stmt.Table)
	}

	// Execute the SELECT query to get rows to insert
	rows, err := db.execSelectStmt(stmt.SelectQuery)
	if err != nil {
		return Result{}, fmt.Errorf("INSERT SELECT: %w", err)
	}
	if rows == nil || len(rows.Data) == 0 {
		return Result{}, nil
	}

	tableCols := db.columnOrder[stmt.Table]
	insertCols := stmt.Columns
	if len(insertCols) == 0 {
		insertCols = tableCols
	}

	var affected int64
	for _, rowData := range rows.Data {
		// Build INSERT for this row
		colParts := make([]string, 0, len(insertCols))
		valParts := make([]string, 0, len(insertCols))
		for i, col := range insertCols {
			colParts = append(colParts, col)
			var val interface{}
			if i < len(rowData) {
				val = rowData[i]
			}
			valParts = append(valParts, literalToString(val))
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			stmt.Table,
			strings.Join(colParts, ", "),
			strings.Join(valParts, ", "))
		res, err := db.execVMDML(insertSQL, stmt.Table)
		if err != nil {
			return Result{}, err
		}
		affected += res.RowsAffected
	}
	return Result{RowsAffected: affected}, nil
}

// execInsertOnConflict handles INSERT ... ON CONFLICT DO NOTHING/UPDATE.
func (db *Database) execInsertOnConflict(stmt *QP.InsertStmt) (Result, error) {
	util.AssertNotNil(stmt, "InsertStmt")
	util.Assert(stmt.Table != "", "InsertStmt.Table cannot be empty")
	util.AssertNotNil(stmt.OnConflict, "OnConflict cannot be nil")

	tableName := db.resolveTableName(stmt.Table)
	if _, exists := db.tables[tableName]; !exists {
		return Result{}, fmt.Errorf("no such table: %s", tableName)
	}

	tableCols := db.columnOrder[tableName]
	if tableCols == nil {
		tableCols = db.getOrderedColumns(tableName)
	}

	oc := stmt.OnConflict
	var affected int64

	for _, row := range stmt.Values {
		// Determine column-to-value mapping for this row.
		insertCols := stmt.Columns
		if len(insertCols) == 0 {
			insertCols = tableCols
		}

		// Build a map of column → SQL value string for this row.
		colVals := make(map[string]string, len(insertCols))
		for i, col := range insertCols {
			if i < len(row) {
				colVals[col] = exprToSQL(row[i])
			} else {
				colVals[col] = "NULL"
			}
		}

		// Build plain INSERT SQL for this single row.
		colParts := make([]string, 0, len(insertCols))
		valParts := make([]string, 0, len(insertCols))
		for _, col := range insertCols {
			colParts = append(colParts, col)
			valParts = append(valParts, colVals[col])
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(colParts, ", "),
			strings.Join(valParts, ", "))

		res, err := db.execVMDML(insertSQL, tableName)
		if err == nil {
			affected += res.RowsAffected
			continue
		}

		// Check if this is a UNIQUE/PK constraint violation.
		if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return Result{}, err
		}

		// Constraint violation — apply conflict resolution.
		if oc.DoNothing {
			// Skip this row silently.
			continue
		}

		// DO UPDATE SET: build UPDATE SQL replacing excluded.col with actual values.
		if len(oc.Updates) == 0 {
			continue
		}

		// Build WHERE clause from conflict target columns.
		// If no explicit target columns, use primary key columns.
		conflictCols := oc.Columns
		if len(conflictCols) == 0 {
			conflictCols = db.primaryKeys[tableName]
		}

		whereParts := make([]string, 0, len(conflictCols))
		for _, col := range conflictCols {
			val, ok := colVals[col]
			if !ok {
				val = "NULL"
			}
			whereParts = append(whereParts, fmt.Sprintf("%s = %s", col, val))
		}
		if len(whereParts) == 0 {
			continue
		}

		// Build SET clause, resolving excluded.col references.
		setParts := make([]string, 0, len(oc.Updates))
		for _, sc := range oc.Updates {
			colRef, ok := sc.Column.(*QP.ColumnRef)
			if !ok {
				continue
			}
			valSQL := resolveExcluded(sc.Value, colVals)
			setParts = append(setParts, fmt.Sprintf("%s = %s", colRef.Name, valSQL))
		}
		if len(setParts) == 0 {
			continue
		}

		updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			strings.Join(setParts, ", "),
			strings.Join(whereParts, " AND "))

		upRes, upErr := db.execVMDML(updateSQL, tableName)
		if upErr != nil {
			return Result{}, upErr
		}
		affected += upRes.RowsAffected
	}

	return Result{RowsAffected: affected}, nil
}

// resolveExcluded converts an expression to SQL, replacing excluded.col references
// with the actual literal values from the attempted INSERT row.
func resolveExcluded(expr QP.Expr, colVals map[string]string) string {
	if expr == nil {
		return "NULL"
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if strings.EqualFold(e.Table, "excluded") {
			if val, ok := colVals[e.Name]; ok {
				return val
			}
			return "NULL"
		}
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	default:
		return exprToSQL(expr)
	}
}

// resolveSelectAliases resolves column aliases defined in SELECT in WHERE, GROUP BY, HAVING, ORDER BY.
// This implements SQLite's extension allowing aliases like: SELECT a AS x ... GROUP BY x
func resolveSelectAliases(stmt *QP.SelectStmt) {
	// Build alias map: alias_name -> original expression
	aliasMap := make(map[string]QP.Expr)
	for _, col := range stmt.Columns {
		if alias, ok := col.(*QP.AliasExpr); ok {
			aliasMap[alias.Alias] = alias.Expr
		}
	}
	if len(aliasMap) == 0 {
		return
	}

	// Substitute in WHERE
	stmt.Where = substituteAliasExpr(stmt.Where, aliasMap)

	// Substitute in GROUP BY
	for i, expr := range stmt.GroupBy {
		stmt.GroupBy[i] = substituteAliasExpr(expr, aliasMap)
	}

	// Substitute in HAVING
	stmt.Having = substituteAliasExpr(stmt.Having, aliasMap)
}

// substituteAliasExpr recursively substitutes alias references in an expression.
func substituteAliasExpr(expr QP.Expr, aliasMap map[string]QP.Expr) QP.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Table == "" {
			if orig, ok := aliasMap[e.Name]; ok {
				return orig
			}
		}
		return expr
	case *QP.BinaryExpr:
		return &QP.BinaryExpr{Op: e.Op, Left: substituteAliasExpr(e.Left, aliasMap), Right: substituteAliasExpr(e.Right, aliasMap)}
	case *QP.UnaryExpr:
		return &QP.UnaryExpr{Op: e.Op, Expr: substituteAliasExpr(e.Expr, aliasMap)}
	case *QP.AliasExpr:
		return &QP.AliasExpr{Expr: substituteAliasExpr(e.Expr, aliasMap), Alias: e.Alias}
	default:
		return expr
	}
}

// splitStatements splits SQL on top-level semicolons (not inside strings/parens).
func splitStatements(sql string) []string {
	var stmts []string
	var cur strings.Builder
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if inSingle {
			cur.WriteByte(c)
			if c == '\'' && i+1 < len(sql) && sql[i+1] == '\'' {
				cur.WriteByte(sql[i+1])
				i++
			} else if c == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			cur.WriteByte(c)
			if c == '"' {
				inDouble = false
			}
			continue
		}
		switch c {
		case '\'':
			inSingle = true
			cur.WriteByte(c)
		case '"':
			inDouble = true
			cur.WriteByte(c)
		case '(':
			depth++
			cur.WriteByte(c)
		case ')':
			depth--
			cur.WriteByte(c)
		case ';':
			if depth == 0 {
				s := strings.TrimSpace(cur.String())
				if s != "" {
					stmts = append(stmts, s)
				}
				cur.Reset()
			} else {
				cur.WriteByte(c)
			}
		default:
			cur.WriteByte(c)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}
