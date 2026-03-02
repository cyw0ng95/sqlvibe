// Package F873 tests v0.9.1 optimization features:
// covering indexes, statement pool, slab allocator,
// expression bytecode, column projection, direct compiler
// fast-path detection, and dispatch table.
package F873

import (
	"fmt"
	"testing"

	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// openDB opens an in-memory sqlvibe database.
func openDB(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	return db
}

// -----------------------------------------------------------------
// 1. Covering Index: IndexMetaQP.CoversColumns
// -----------------------------------------------------------------

// TestSQL1999_F873_CoversColumns_L1 verifies that CoversColumns correctly
// reports whether an index covers a given set of required columns.
func TestSQL1999_F873_CoversColumns_L1(t *testing.T) {
	idx := &QP.IndexMetaQP{
		Name:      "idx_name_age",
		TableName: "users",
		Columns:   []string{"name", "age"},
	}

	if !idx.CoversColumns([]string{"name"}) {
		t.Error("Expected CoversColumns(['name']) = true")
	}
	if !idx.CoversColumns([]string{"age"}) {
		t.Error("Expected CoversColumns(['age']) = true")
	}
	if !idx.CoversColumns([]string{"name", "age"}) {
		t.Error("Expected CoversColumns(['name','age']) = true")
	}
	if idx.CoversColumns([]string{"salary"}) {
		t.Error("Expected CoversColumns(['salary']) = false")
	}
	if idx.CoversColumns([]string{"name", "salary"}) {
		t.Error("Expected CoversColumns(['name','salary']) = false")
	}
	// Empty required list is always covered.
	if !idx.CoversColumns([]string{}) {
		t.Error("Expected CoversColumns([]) = true")
	}
}

// TestSQL1999_F873_FindCoveringIndex_L1 verifies FindCoveringIndex returns
// the first index that covers all required columns, or nil.
func TestSQL1999_F873_FindCoveringIndex_L1(t *testing.T) {
	idxA := &QP.IndexMetaQP{Name: "idx_id", TableName: "t", Columns: []string{"id"}}
	idxB := &QP.IndexMetaQP{Name: "idx_name_dept", TableName: "t", Columns: []string{"name", "dept"}}
	indexes := []*QP.IndexMetaQP{idxA, idxB}

	got := QP.FindCoveringIndex(indexes, []string{"id"})
	if got != idxA {
		t.Errorf("Expected idxA, got %v", got)
	}

	got = QP.FindCoveringIndex(indexes, []string{"name", "dept"})
	if got != idxB {
		t.Errorf("Expected idxB, got %v", got)
	}

	got = QP.FindCoveringIndex(indexes, []string{"salary"})
	if got != nil {
		t.Errorf("Expected nil for uncovered column, got %v", got)
	}
}

// TestSQL1999_F873_SelectBestIndex_L1 verifies SelectBestIndex prefers the
// index whose leading column matches the filter column.
func TestSQL1999_F873_SelectBestIndex_L1(t *testing.T) {
	idxID := &QP.IndexMetaQP{Name: "idx_id", TableName: "t", Columns: []string{"id", "val"}}
	idxName := &QP.IndexMetaQP{Name: "idx_name", TableName: "t", Columns: []string{"name"}}
	indexes := []*QP.IndexMetaQP{idxID, idxName}

	got := QP.SelectBestIndex(indexes, "id", []string{"id", "val"})
	if got != idxID {
		t.Errorf("Expected idxID as best for filter on id, got %v", got)
	}

	got = QP.SelectBestIndex(indexes, "name", []string{"name"})
	if got != idxName {
		t.Errorf("Expected idxName as best for filter on name, got %v", got)
	}
}

// TestSQL1999_F873_CanSkipScan_L1 verifies CanSkipScan heuristics.
func TestSQL1999_F873_CanSkipScan_L1(t *testing.T) {
	// Skip scan is worthwhile when leading cardinality is low relative to rowCount.
	// indexCols has a leading col not in filterCols, suffix matches filterCols.
	ok := QP.CanSkipScan([]string{"dept", "id"}, []string{"id"}, 5, 10000)
	if !ok {
		t.Error("Expected CanSkipScan=true for low leading cardinality")
	}

	// Not worthwhile when leading cardinality >= rowCount/10 and >= 100.
	ok = QP.CanSkipScan([]string{"dept", "id"}, []string{"id"}, 5000, 10000)
	if ok {
		t.Error("Expected CanSkipScan=false for high leading cardinality")
	}

	// filterCols don't match suffix of indexCols.
	ok = QP.CanSkipScan([]string{"dept", "id"}, []string{"name"}, 5, 10000)
	if ok {
		t.Error("Expected CanSkipScan=false when filterCols don't match index suffix")
	}
}

// -----------------------------------------------------------------
// 2. Statement Pool: Get / Clear / Len
// -----------------------------------------------------------------

// TestSQL1999_F873_StatementPool_L1 tests that the StatementPool caches
// prepared statements and returns correct query results.
func TestSQL1999_F873_StatementPool_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE nums (n INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO nums VALUES (%d)", i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	pool := sqlvibe.NewStatementPool(db, 10)

	// First Get compiles the statement.
	stmt, err := pool.Get("SELECT n FROM nums ORDER BY n")
	if err != nil {
		t.Fatalf("pool.Get error: %v", err)
	}
	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("stmt.Query error: %v", err)
	}
	if len(rows.Data) != 5 {
		t.Errorf("Expected 5 rows, got %d", len(rows.Data))
	}

	if pool.Len() != 1 {
		t.Errorf("Expected pool.Len()=1, got %d", pool.Len())
	}

	// Second Get returns cached statement.
	stmt2, err := pool.Get("SELECT n FROM nums ORDER BY n")
	if err != nil {
		t.Fatalf("pool.Get (cached) error: %v", err)
	}
	if stmt2 != stmt {
		t.Error("Expected cached statement to be the same pointer")
	}
	if pool.Len() != 1 {
		t.Errorf("Expected pool.Len()=1 after re-Get, got %d", pool.Len())
	}

	// Add a second statement.
	if _, err := pool.Get("SELECT COUNT(*) FROM nums"); err != nil {
		t.Fatalf("pool.Get second stmt: %v", err)
	}
	if pool.Len() != 2 {
		t.Errorf("Expected pool.Len()=2, got %d", pool.Len())
	}

	pool.Clear()
	if pool.Len() != 0 {
		t.Errorf("Expected pool.Len()=0 after Clear, got %d", pool.Len())
	}
}

// TestSQL1999_F873_StatementPoolLRUEviction_L1 verifies LRU eviction when
// the pool exceeds its capacity.
func TestSQL1999_F873_StatementPoolLRUEviction_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE x (v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	pool := sqlvibe.NewStatementPool(db, 3)

	queries := []string{
		"SELECT v FROM x",
		"SELECT v FROM x WHERE v > 0",
		"SELECT v FROM x WHERE v > 1",
		"SELECT v FROM x WHERE v > 2", // triggers eviction
	}
	for _, q := range queries {
		if _, err := pool.Get(q); err != nil {
			t.Fatalf("pool.Get(%q): %v", q, err)
		}
	}

	if pool.Len() != 3 {
		t.Errorf("Expected pool.Len()=3 after eviction, got %d", pool.Len())
	}
}

// -----------------------------------------------------------------
// 3. Slab Allocator: Alloc / Reset
// -----------------------------------------------------------------

// TestSQL1999_F873_SlabAllocator_L1 verifies basic slab allocator functionality.
func TestSQL1999_F873_SlabAllocator_L1(t *testing.T) {
	sa := DS.NewSlabAllocator()

	// Allocate a small buffer (goes through pool path).
	buf := sa.Alloc(64)
	if len(buf) != 64 {
		t.Errorf("Expected buf len=64, got %d", len(buf))
	}

	// Allocate a medium buffer (goes through slab path).
	buf2 := sa.Alloc(1024)
	if len(buf2) != 1024 {
		t.Errorf("Expected buf2 len=1024, got %d", len(buf2))
	}

	if sa.Stats.TotalAllocs < 2 {
		t.Errorf("Expected TotalAllocs >= 2, got %d", sa.Stats.TotalAllocs)
	}
	if sa.Stats.BytesAllocated < 1088 {
		t.Errorf("Expected BytesAllocated >= 1088, got %d", sa.Stats.BytesAllocated)
	}

	// Reset clears state.
	sa.Reset()
	if sa.Stats.TotalAllocs < 2 {
		// Stats are not cleared by Reset — that is intentional.
	}

	// After Reset, allocations should still work.
	buf3 := sa.Alloc(512)
	if len(buf3) != 512 {
		t.Errorf("Expected buf3 len=512 after Reset, got %d", len(buf3))
	}
}

// TestSQL1999_F873_SlabAllocatorIntSlice_L1 tests AllocIntSlice helper.
func TestSQL1999_F873_SlabAllocatorIntSlice_L1(t *testing.T) {
	sa := DS.NewSlabAllocator()
	s := sa.AllocIntSlice(8)
	if len(s) != 8 {
		t.Errorf("Expected AllocIntSlice(8) len=8, got %d", len(s))
	}
	s[0] = 42
	if s[0] != 42 {
		t.Error("Expected to write/read int slice from slab")
	}
}

// -----------------------------------------------------------------
// 4. Expression Bytecode: Eval on simple expressions
// -----------------------------------------------------------------

// TestSQL1999_F873_ExprBytecode_Add_L1 tests ExprBytecode evaluation of
// a simple addition: const(2) + const(3) = 5.
func TestSQL1999_F873_ExprBytecode_Add_L1(t *testing.T) {
	eb := VM.NewExprBytecode()
	ci0 := eb.AddConst(int64(2))
	ci1 := eb.AddConst(int64(3))
	eb.Emit(VM.EOpLoadConst, ci0)
	eb.Emit(VM.EOpLoadConst, ci1)
	eb.Emit(VM.EOpAdd)

	result := eb.Eval(nil)
	if result != int64(5) {
		t.Errorf("Expected int64(5), got %v (%T)", result, result)
	}
}

// TestSQL1999_F873_ExprBytecode_Column_L1 tests loading a column value
// from a row.
func TestSQL1999_F873_ExprBytecode_Column_L1(t *testing.T) {
	eb := VM.NewExprBytecode()
	eb.Emit(VM.EOpLoadColumn, 0)

	row := []interface{}{int64(99)}
	result := eb.Eval(row)
	if result != int64(99) {
		t.Errorf("Expected int64(99) from column 0, got %v (%T)", result, result)
	}
}

// TestSQL1999_F873_ExprBytecode_Comparison_L1 tests a comparison expression:
// 10 > 5 should yield 1.
func TestSQL1999_F873_ExprBytecode_Comparison_L1(t *testing.T) {
	eb := VM.NewExprBytecode()
	ci0 := eb.AddConst(int64(10))
	ci1 := eb.AddConst(int64(5))
	eb.Emit(VM.EOpLoadConst, ci0)
	eb.Emit(VM.EOpLoadConst, ci1)
	eb.Emit(VM.EOpGt)

	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Expected int64(1) for 10>5, got %v (%T)", result, result)
	}
}

// TestSQL1999_F873_ExprBytecodeOps_L1 verifies that Ops() returns the
// emitted operations.
func TestSQL1999_F873_ExprBytecodeOps_L1(t *testing.T) {
	eb := VM.NewExprBytecode()
	eb.Emit(VM.EOpLoadConst, 0)
	eb.Emit(VM.EOpLoadConst, 1)
	eb.Emit(VM.EOpAdd)

	ops := eb.Ops()
	if len(ops) != 3 {
		t.Errorf("Expected 3 ops, got %d", len(ops))
	}
	if ops[2] != VM.EOpAdd {
		t.Errorf("Expected ops[2]=EOpAdd, got %v", ops[2])
	}
}

// -----------------------------------------------------------------
// 5. Column Projection: RequiredColumns extracts column names
// -----------------------------------------------------------------

// TestSQL1999_F873_RequiredColumns_L1 verifies that RequiredColumns extracts
// all column references from a parsed SELECT statement.
func TestSQL1999_F873_RequiredColumns_L1(t *testing.T) {
	sql := "SELECT name, age FROM employees WHERE dept = 'eng' ORDER BY age"
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}
	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	selectStmt, ok := ast.(*QP.SelectStmt)
	if !ok {
		t.Fatalf("Expected *QP.SelectStmt, got %T", ast)
	}

	cols := QP.RequiredColumns(selectStmt)

	colSet := make(map[string]bool)
	for _, c := range cols {
		colSet[c] = true
	}

	for _, expected := range []string{"name", "age", "dept"} {
		if !colSet[expected] {
			t.Errorf("Expected column %q in RequiredColumns, got %v", expected, cols)
		}
	}
}

// -----------------------------------------------------------------
// 6. DirectCompiler: IsFastPath detection
// -----------------------------------------------------------------

// TestSQL1999_F873_IsFastPath_L1 verifies IsFastPath correctly classifies
// simple vs complex SQL queries.
func TestSQL1999_F873_IsFastPath_L1(t *testing.T) {
	fastCases := []string{
		"SELECT id, name FROM users",
		"SELECT * FROM t WHERE id = 1",
		"SELECT COUNT(*) FROM orders GROUP BY status",
		"SELECT id FROM t ORDER BY id LIMIT 10",
	}
	for _, sql := range fastCases {
		if !CG.IsFastPath(sql) {
			t.Errorf("Expected IsFastPath=true for: %q", sql)
		}
	}

	slowCases := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET v = 1 WHERE id = 1",
		"SELECT a.id FROM a JOIN b ON a.id = b.id",
		"SELECT id FROM a UNION SELECT id FROM b",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
	}
	for _, sql := range slowCases {
		if CG.IsFastPath(sql) {
			t.Errorf("Expected IsFastPath=false for: %q", sql)
		}
	}
}

// -----------------------------------------------------------------
// 7. Dispatch table: HasDispatchHandler
// -----------------------------------------------------------------

// TestSQL1999_F873_HasDispatchHandler_L1 verifies that known arithmetic
// opcodes have dispatch handlers registered.
func TestSQL1999_F873_HasDispatchHandler_L1(t *testing.T) {
	// These opcodes are registered in dispatch.go init().
	registered := []VM.OpCode{
		VM.OpAdd,
		VM.OpSubtract,
		VM.OpMultiply,
		VM.OpDivide,
		VM.OpNull,
		VM.OpLoadConst,
		VM.OpMove,
		VM.OpCopy,
	}
	for _, op := range registered {
		if !VM.HasDispatchHandler(op) {
			t.Errorf("Expected HasDispatchHandler=true for OpCode %v", op)
		}
	}
}
