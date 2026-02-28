package vtab_test

import (
	"testing"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
	_ "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe" // ensure init() registers the series module
	sqlvibe "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// ---------------------------------------------------------------------------
// Registry tests
// ---------------------------------------------------------------------------

// TestVTabRegistry_RegisterAndGet verifies that modules can be registered and retrieved.
func TestVTabRegistry_RegisterAndGet(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found in registry")
	}
	if mod == nil {
		t.Fatal("series module is nil")
	}
}

// TestVTabRegistry_UnknownModule verifies that looking up an unregistered name returns false.
func TestVTabRegistry_UnknownModule(t *testing.T) {
	_, ok := IS.GetVTabModule("__no_such_module__")
	if ok {
		t.Fatal("expected GetVTabModule to return false for unknown module")
	}
}

// TestVTabRegistry_Overwrite verifies that registering under the same name replaces the old module.
func TestVTabRegistry_Overwrite(t *testing.T) {
	// Register a dummy module, then overwrite with another.
	dummy1 := &dummyModule{id: "first"}
	dummy2 := &dummyModule{id: "second"}
	IS.RegisterVTabModule("__test_overwrite__", dummy1)
	IS.RegisterVTabModule("__test_overwrite__", dummy2)
	mod, ok := IS.GetVTabModule("__test_overwrite__")
	if !ok {
		t.Fatal("module not found after overwrite")
	}
	if m, ok := mod.(*dummyModule); !ok || m.id != "second" {
		t.Errorf("expected 'second' module after overwrite, got %v", mod)
	}
}

// TestVTabRegistry_ListModules verifies that ListVTabModules returns at least "series".
func TestVTabRegistry_ListModules(t *testing.T) {
	names := IS.ListVTabModules()
	found := false
	for _, n := range names {
		if n == "series" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ListVTabModules did not include 'series', got: %v", names)
	}
}

// ---------------------------------------------------------------------------
// RowStoreCursor tests
// ---------------------------------------------------------------------------

// TestVTabCursorRowStore_Basic verifies RowStoreCursor scans all live rows.
func TestVTabCursorRowStore_Basic(t *testing.T) {
	rs := DS.NewRowStore([]string{"id", "name"}, []DS.ValueType{DS.TypeInt, DS.TypeString})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(1), DS.StringValue("alice")}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(2), DS.StringValue("bob")}))

	cursor := DS.NewRowStoreCursor(rs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	var count int
	for !cursor.Eof() {
		v, err := cursor.Column(0)
		if err != nil {
			t.Fatalf("Column: %v", err)
		}
		if v == nil {
			t.Error("expected non-nil value for column 0")
		}
		count++
		if err := cursor.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	cursor.Close()
}

// TestVTabCursorRowStore_Empty verifies RowStoreCursor on empty store returns 0 rows.
func TestVTabCursorRowStore_Empty(t *testing.T) {
	rs := DS.NewRowStore([]string{"id"}, []DS.ValueType{DS.TypeInt})
	cursor := DS.NewRowStoreCursor(rs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if !cursor.Eof() {
		t.Error("cursor on empty store should be Eof immediately")
	}
	cursor.Close()
}

// TestVTabCursorRowStore_WithDeleted verifies tombstoned rows are skipped.
func TestVTabCursorRowStore_WithDeleted(t *testing.T) {
	rs := DS.NewRowStore([]string{"id"}, []DS.ValueType{DS.TypeInt})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(1)}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(2)})) // will be deleted
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(3)}))
	rs.Delete(1) // delete row at index 1

	cursor := DS.NewRowStoreCursor(rs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	var ids []int64
	for !cursor.Eof() {
		v, err := cursor.Column(0)
		if err != nil {
			t.Fatalf("Column: %v", err)
		}
		ids = append(ids, v.(int64))
		_ = cursor.Next()
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Errorf("expected [1,3] after delete, got %v", ids)
	}
	cursor.Close()
}

// TestVTabCursorRowStore_AllColumns verifies all columns are accessible.
func TestVTabCursorRowStore_AllColumns(t *testing.T) {
	rs := DS.NewRowStore([]string{"a", "b", "c"}, []DS.ValueType{DS.TypeInt, DS.TypeFloat, DS.TypeString})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(42), DS.FloatValue(3.14), DS.StringValue("hello")}))

	cursor := DS.NewRowStoreCursor(rs)
	_ = cursor.Filter(0, "", nil)

	v0, _ := cursor.Column(0)
	v1, _ := cursor.Column(1)
	v2, _ := cursor.Column(2)
	if v0.(int64) != 42 {
		t.Errorf("col0: expected 42, got %v", v0)
	}
	if v1.(float64) != 3.14 {
		t.Errorf("col1: expected 3.14, got %v", v1)
	}
	if v2.(string) != "hello" {
		t.Errorf("col2: expected hello, got %v", v2)
	}
	cursor.Close()
}

// TestVTabCursorRowStore_RowID verifies RowID() returns sequential 0-based positions.
func TestVTabCursorRowStore_RowID(t *testing.T) {
	rs := DS.NewRowStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(10)}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(20)}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(30)}))

	cursor := DS.NewRowStoreCursor(rs)
	_ = cursor.Filter(0, "", nil)

	for i := 0; !cursor.Eof(); i++ {
		id, err := cursor.RowID()
		if err != nil {
			t.Fatalf("RowID: %v", err)
		}
		if id != int64(i) {
			t.Errorf("position %d: expected RowID %d, got %d", i, i, id)
		}
		_ = cursor.Next()
	}
	cursor.Close()
}

// TestVTabCursorRowStore_ReFilter verifies that Filter() resets the cursor.
func TestVTabCursorRowStore_ReFilter(t *testing.T) {
	rs := DS.NewRowStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(1)}))
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(2)}))

	cursor := DS.NewRowStoreCursor(rs)

	// First pass: consume all rows.
	_ = cursor.Filter(0, "", nil)
	for !cursor.Eof() {
		_ = cursor.Next()
	}

	// Second pass after re-Filter: should see rows again.
	_ = cursor.Filter(0, "", nil)
	var count int
	for !cursor.Eof() {
		count++
		_ = cursor.Next()
	}
	if count != 2 {
		t.Errorf("after re-Filter expected 2 rows, got %d", count)
	}
	cursor.Close()
}

// ---------------------------------------------------------------------------
// HybridStoreCursor tests
// ---------------------------------------------------------------------------

// TestVTabCursorHybridStore_Basic verifies HybridStoreCursor scans all live rows.
func TestVTabCursorHybridStore_Basic(t *testing.T) {
	hs := DS.NewHybridStore([]string{"x", "y"}, []DS.ValueType{DS.TypeInt, DS.TypeString})
	hs.Insert([]DS.Value{DS.IntValue(10), DS.StringValue("foo")})
	hs.Insert([]DS.Value{DS.IntValue(20), DS.StringValue("bar")})

	cursor := DS.NewHybridStoreCursor(hs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	var count int
	for !cursor.Eof() {
		v, err := cursor.Column(0)
		if err != nil {
			t.Fatalf("Column(0): %v", err)
		}
		if v == nil {
			t.Error("expected non-nil value")
		}
		count++
		_ = cursor.Next()
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	cursor.Close()
}

// TestVTabCursorHybridStore_Empty verifies HybridStoreCursor on empty store.
func TestVTabCursorHybridStore_Empty(t *testing.T) {
	hs := DS.NewHybridStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	cursor := DS.NewHybridStoreCursor(hs)
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if !cursor.Eof() {
		t.Error("cursor on empty HybridStore should be Eof immediately")
	}
	cursor.Close()
}

// TestVTabCursorHybridStore_ColumnOutOfRange verifies out-of-range column returns an error.
func TestVTabCursorHybridStore_ColumnOutOfRange(t *testing.T) {
	hs := DS.NewHybridStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	hs.Insert([]DS.Value{DS.IntValue(1)})

	cursor := DS.NewHybridStoreCursor(hs)
	_ = cursor.Filter(0, "", nil)

	_, err := cursor.Column(99)
	if err == nil {
		t.Error("expected error for out-of-range column, got nil")
	}
	cursor.Close()
}

// ---------------------------------------------------------------------------
// Series SQL integration tests
// ---------------------------------------------------------------------------

// TestSeriesVTab_BasicRange tests SELECT * FROM series(1,5) returns values 1..5.
func TestSeriesVTab_BasicRange(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 5)")
	if len(rows.Data) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows.Data))
	}
	for i, row := range rows.Data {
		want := int64(i + 1)
		got, ok := row[0].(int64)
		if !ok {
			t.Errorf("row %d: expected int64, got %T (%v)", i, row[0], row[0])
			continue
		}
		if got != want {
			t.Errorf("row %d: expected %d, got %d", i, want, got)
		}
	}
}

// TestSeriesVTab_StepTwo tests series(1,10,2) returns odd numbers 1..9.
func TestSeriesVTab_StepTwo(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 10, 2)")
	expected := []int64{1, 3, 5, 7, 9}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows.Data))
	}
	for i, row := range rows.Data {
		got := row[0].(int64)
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_NegativeStep tests descending series(10, 1, -1).
func TestSeriesVTab_NegativeStep(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(10, 1, -1)")
	expected := []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d: %v", len(expected), len(rows.Data), rows.Data)
	}
	for i, row := range rows.Data {
		got := row[0].(int64)
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_SingleElement tests series(5,5) returns exactly 1 row.
func TestSeriesVTab_SingleElement(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(5, 5)")
	if len(rows.Data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Data))
	}
	if got := rows.Data[0][0].(int64); got != 5 {
		t.Errorf("expected 5, got %d", got)
	}
}

// TestSeriesVTab_EmptyRange tests that start > stop with positive step returns 0 rows.
func TestSeriesVTab_EmptyRange(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(10, 1)")
	if len(rows.Data) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_ColumnName verifies the result column is named "value".
func TestSeriesVTab_ColumnName(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 1)")
	if len(rows.Columns) != 1 || rows.Columns[0] != "value" {
		t.Errorf("expected column named 'value', got %v", rows.Columns)
	}
}

// TestSeriesVTab_ExplicitColumn tests SELECT value FROM series(...) works.
func TestSeriesVTab_ExplicitColumn(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT value FROM series(1, 3)")
	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_WithWhere tests WHERE filtering on virtual table.
func TestSeriesVTab_WithWhere(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 10) WHERE value > 7")
	expected := []int64{8, 9, 10}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows.Data))
	}
	for i, row := range rows.Data {
		got := row[0].(int64)
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_WithLimit tests LIMIT on virtual table.
func TestSeriesVTab_WithLimit(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 100) LIMIT 5")
	if len(rows.Data) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_WithAggregate tests COUNT(*) and SUM on virtual table.
func TestSeriesVTab_WithAggregate(t *testing.T) {
	db := mustOpen(t)

	// COUNT
	rows := mustQuery(t, db, "SELECT COUNT(*) FROM series(1, 10)")
	if len(rows.Data) != 1 {
		t.Fatalf("COUNT: expected 1 row, got %d", len(rows.Data))
	}
	cnt := toInt64(rows.Data[0][0])
	if cnt != 10 {
		t.Errorf("COUNT: expected 10, got %d", cnt)
	}

	// SUM(value) for 1..10 = 55
	rows2 := mustQuery(t, db, "SELECT SUM(value) FROM series(1, 10)")
	if len(rows2.Data) != 1 {
		t.Fatalf("SUM: expected 1 row, got %d", len(rows2.Data))
	}
	sum := toInt64(rows2.Data[0][0])
	if sum != 55 {
		t.Errorf("SUM: expected 55, got %d", sum)
	}
}

// TestSeriesVTab_OrderByDesc tests ORDER BY value DESC on virtual table.
func TestSeriesVTab_OrderByDesc(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 5) ORDER BY value DESC")
	expected := []int64{5, 4, 3, 2, 1}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows.Data))
	}
	for i, row := range rows.Data {
		got := row[0].(int64)
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_CreateVirtualTable tests CREATE VIRTUAL TABLE then SELECT.
func TestSeriesVTab_CreateVirtualTable(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE t1 USING series(1, 10)")
	rows := mustQuery(t, db, "SELECT * FROM t1")
	if len(rows.Data) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_IfNotExists tests CREATE VIRTUAL TABLE IF NOT EXISTS is idempotent.
func TestSeriesVTab_IfNotExists(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE IF NOT EXISTS t2 USING series(1, 3)")
	mustExec(t, db, "CREATE VIRTUAL TABLE IF NOT EXISTS t2 USING series(1, 5)")
	rows := mustQuery(t, db, "SELECT * FROM t2")
	// Should still have the original 3 rows.
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows from first create, got %d", len(rows.Data))
	}
}

// TestSeriesVTab_DropVirtualTable tests DROP TABLE removes the virtual table.
func TestSeriesVTab_DropVirtualTable(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE drop_me USING series(1, 5)")
	// Verify it's queryable.
	rows := mustQuery(t, db, "SELECT * FROM drop_me")
	if len(rows.Data) != 5 {
		t.Fatalf("expected 5 rows before drop, got %d", len(rows.Data))
	}
	// Drop it.
	mustExec(t, db, "DROP TABLE drop_me")
	// After drop, it should not be found.
	_, err := db.Query("SELECT * FROM drop_me")
	if err == nil {
		t.Error("expected error querying dropped virtual table, got nil")
	}
}

// TestSeriesVTab_DropVirtualTableIfExists tests DROP TABLE IF EXISTS on virtual and nonexistent tables.
func TestSeriesVTab_DropVirtualTableIfExists(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE drop_ie USING series(1, 3)")
	mustExec(t, db, "DROP TABLE IF EXISTS drop_ie")
	// Dropping nonexistent with IF EXISTS should not error.
	mustExec(t, db, "DROP TABLE IF EXISTS __no_such_vtab__")
}

// TestSeriesVTab_MultipleVTabs tests two virtual tables can coexist.
func TestSeriesVTab_MultipleVTabs(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE evens USING series(0, 10, 2)")
	mustExec(t, db, "CREATE VIRTUAL TABLE odds  USING series(1, 10, 2)")

	evens := mustQuery(t, db, "SELECT * FROM evens")
	odds := mustQuery(t, db, "SELECT * FROM odds")

	if len(evens.Data) != 6 { // 0,2,4,6,8,10
		t.Errorf("evens: expected 6 rows, got %d", len(evens.Data))
	}
	if len(odds.Data) != 5 { // 1,3,5,7,9
		t.Errorf("odds: expected 5 rows, got %d", len(odds.Data))
	}
}

// TestSeriesVTab_LimitOffset tests LIMIT + OFFSET on virtual table.
func TestSeriesVTab_LimitOffset(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT * FROM series(1, 10) LIMIT 3 OFFSET 4")
	expected := []int64{5, 6, 7}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d: %v", len(expected), len(rows.Data), rows.Data)
	}
	for i, row := range rows.Data {
		got := row[0].(int64)
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// TestSeriesVTab_ExpressionOnColumn tests arithmetic expression on virtual table column.
func TestSeriesVTab_ExpressionOnColumn(t *testing.T) {
	db := mustOpen(t)
	rows := mustQuery(t, db, "SELECT value * 2 FROM series(1, 5)")
	expected := []int64{2, 4, 6, 8, 10}
	if len(rows.Data) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(rows.Data))
	}
	for i, row := range rows.Data {
		got := toInt64(row[0])
		if got != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

// ---------------------------------------------------------------------------
// Error / validation tests
// ---------------------------------------------------------------------------

// TestSeriesVTab_ErrorTooFewArgs tests that series() with too few args returns an error.
func TestSeriesVTab_ErrorTooFewArgs(t *testing.T) {
	db := mustOpen(t)
	// series with only 1 arg via CREATE VIRTUAL TABLE
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING series(1)")
	if err == nil {
		t.Error("expected error for series with 1 arg, got nil")
	}
}

// TestSeriesVTab_ErrorZeroStep tests that step=0 is rejected.
func TestSeriesVTab_ErrorZeroStep(t *testing.T) {
	db := mustOpen(t)
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING series(1, 10, 0)")
	if err == nil {
		t.Error("expected error for step=0, got nil")
	}
}

// TestSeriesVTab_ErrorUnknownModule tests CREATE VIRTUAL TABLE with an unknown module.
func TestSeriesVTab_ErrorUnknownModule(t *testing.T) {
	db := mustOpen(t)
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING __no_such_module__(1, 2)")
	if err == nil {
		t.Error("expected error for unknown module, got nil")
	}
}

// TestSeriesVTab_ErrorDuplicateCreate tests that duplicate CREATE without IF NOT EXISTS errors.
func TestSeriesVTab_ErrorDuplicateCreate(t *testing.T) {
	db := mustOpen(t)
	mustExec(t, db, "CREATE VIRTUAL TABLE dup USING series(1, 5)")
	_, err := db.Exec("CREATE VIRTUAL TABLE dup USING series(1, 5)")
	if err == nil {
		t.Error("expected error for duplicate CREATE VIRTUAL TABLE, got nil")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustOpen(t *testing.T) *sqlvibe.Database {
	t.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustQuery(t *testing.T, db *sqlvibe.Database, sql string) *sqlvibe.Rows {
	t.Helper()
	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query %q: %v", sql, err)
	}
	return rows
}

func mustExec(t *testing.T, db *sqlvibe.Database, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

// dummyModule is a minimal VTabModule used in registry override tests.
type dummyModule struct {
	id string
	DS.TableModule
}

func (m *dummyModule) Create(args []string) (DS.VTab, error) { return nil, nil }
func (m *dummyModule) Connect(args []string) (DS.VTab, error) { return nil, nil }

// ---------------------------------------------------------------------------
// Coverage gap tests
// ---------------------------------------------------------------------------

// TestVTabCursorHybridStore_RowID verifies that HybridStoreCursor.RowID returns
// a sequential position.
func TestVTabCursorHybridStore_RowID(t *testing.T) {
	hs := DS.NewHybridStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	hs.Insert([]DS.Value{DS.IntValue(10)})
	hs.Insert([]DS.Value{DS.IntValue(20)})

	cursor := DS.NewHybridStoreCursor(hs)
	_ = cursor.Filter(0, "", nil)
	for i := 0; !cursor.Eof(); i++ {
		id, err := cursor.RowID()
		if err != nil {
			t.Fatalf("RowID: %v", err)
		}
		if id != int64(i) {
			t.Errorf("position %d: expected RowID %d, got %d", i, i, id)
		}
		_ = cursor.Next()
	}
}

// TestSeriesVTab_CursorRowID verifies seriesCursor.RowID returns correct values.
func TestSeriesVTab_CursorRowID(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found")
	}
	vt, err := mod.Create([]string{"1", "5"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cursor, err := vt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cursor.Filter(0, "", nil)
	for i := 0; !cursor.Eof(); i++ {
		id, err := cursor.RowID()
		if err != nil {
			t.Fatalf("RowID: %v", err)
		}
		if id != int64(i) {
			t.Errorf("step %d: expected RowID %d, got %d", i, i, id)
		}
		_ = cursor.Next()
	}
	_ = cursor.Close()
}

// TestSeriesVTab_CursorColumnOutOfRange verifies seriesCursor.Column returns
// an error for invalid column indices.
func TestSeriesVTab_CursorColumnOutOfRange(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found")
	}
	vt, err := mod.Create([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cursor, err := vt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cursor.Filter(0, "", nil)
	_, err = cursor.Column(1) // only col 0 exists
	if err == nil {
		t.Error("expected error for Column(1) on series cursor, got nil")
	}
	_ = cursor.Close()
}

// TestSeriesVTab_BestIndex verifies seriesVTab.BestIndex is callable (no-op).
func TestSeriesVTab_BestIndex(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found")
	}
	vt, err := mod.Create([]string{"1", "10"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := vt.BestIndex(&DS.IndexInfo{}); err != nil {
		t.Errorf("BestIndex returned unexpected error: %v", err)
	}
}

// TestSeriesVTab_Connect verifies Connect produces a working vtab (same as Create).
func TestSeriesVTab_Connect(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found")
	}
	vt, err := mod.Connect([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cols := vt.Columns()
	if len(cols) != 1 || cols[0] != "value" {
		t.Errorf("expected [value], got %v", cols)
	}
	cursor, err := vt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cursor.Filter(0, "", nil)
	var count int
	for !cursor.Eof() {
		count++
		_ = cursor.Next()
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
	_ = cursor.Close()
}

// TestSeriesVTab_Disconnect verifies Disconnect does not error.
func TestSeriesVTab_Disconnect(t *testing.T) {
	mod, ok := IS.GetVTabModule("series")
	if !ok {
		t.Fatal("series module not found")
	}
	vt, err := mod.Create([]string{"1", "5"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := vt.Disconnect(); err != nil {
		t.Errorf("Disconnect returned unexpected error: %v", err)
	}
}

// TestTableModule_BestIndex verifies the embedded TableModule.BestIndex no-op.
func TestTableModule_BestIndex(t *testing.T) {
	var m DS.TableModule
	if err := m.BestIndex(&DS.IndexInfo{}); err != nil {
		t.Errorf("TableModule.BestIndex returned unexpected error: %v", err)
	}
}

// TestVTabCursorRowStore_ColumnOutOfPos verifies Column returns an error when the
// cursor position is out of range (exhausted cursor).
func TestVTabCursorRowStore_ColumnOutOfPos(t *testing.T) {
	rs := DS.NewRowStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	rs.Insert(DS.NewRow([]DS.Value{DS.IntValue(1)}))

	cursor := DS.NewRowStoreCursor(rs)
	_ = cursor.Filter(0, "", nil)
	// Advance past end.
	for !cursor.Eof() {
		_ = cursor.Next()
	}
	// Now cursor is at len(rows) — out of range.
	_, err := cursor.Column(0)
	if err == nil {
		t.Error("expected error when Column called past Eof, got nil")
	}
	cursor.Close()
}

// TestVTabCursorRowStore_BytesAndBool verifies dsValueToInterface handles TypeBytes
// and TypeBool values via RowStoreCursor.
func TestVTabCursorRowStore_BytesAndBool(t *testing.T) {
	rs := DS.NewRowStore(
		[]string{"b", "f"},
		[]DS.ValueType{DS.TypeBytes, DS.TypeBool},
	)
	rs.Insert(DS.NewRow([]DS.Value{
		DS.BytesValue([]byte("hello")),
		DS.BoolValue(true),
	}))

	cursor := DS.NewRowStoreCursor(rs)
	_ = cursor.Filter(0, "", nil)

	vBytes, err := cursor.Column(0)
	if err != nil {
		t.Fatalf("Column(0): %v", err)
	}
	if b, ok := vBytes.([]byte); !ok || string(b) != "hello" {
		t.Errorf("expected []byte('hello'), got %T %v", vBytes, vBytes)
	}

	vBool, err := cursor.Column(1)
	if err != nil {
		t.Fatalf("Column(1): %v", err)
	}
	if b, ok := vBool.(bool); !ok || !b {
		t.Errorf("expected bool(true), got %T %v", vBool, vBool)
	}
	cursor.Close()
}

// TestSeriesVTab_ParseArgsInvalidStart verifies that a non-integer start arg errors.
func TestSeriesVTab_ParseArgsInvalidStart(t *testing.T) {
	db := mustOpen(t)
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING series(abc, 10)")
	if err == nil {
		t.Error("expected error for non-integer start, got nil")
	}
}

// TestSeriesVTab_ParseArgsInvalidStop verifies that a non-integer stop arg errors.
func TestSeriesVTab_ParseArgsInvalidStop(t *testing.T) {
	db := mustOpen(t)
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING series(1, xyz)")
	if err == nil {
		t.Error("expected error for non-integer stop, got nil")
	}
}

// TestSeriesVTab_ParseArgsInvalidStep verifies that a non-integer step arg errors.
func TestSeriesVTab_ParseArgsInvalidStep(t *testing.T) {
	db := mustOpen(t)
	_, err := db.Exec("CREATE VIRTUAL TABLE bad USING series(1, 10, bad)")
	if err == nil {
		t.Error("expected error for non-integer step, got nil")
	}
}

// TestVTabCursorHybridStore_ColumnPastEof verifies HybridStoreCursor.Column returns
// an error when the cursor is past Eof.
func TestVTabCursorHybridStore_ColumnPastEof(t *testing.T) {
	hs := DS.NewHybridStore([]string{"v"}, []DS.ValueType{DS.TypeInt})
	hs.Insert([]DS.Value{DS.IntValue(1)})

	cursor := DS.NewHybridStoreCursor(hs)
	_ = cursor.Filter(0, "", nil)
	for !cursor.Eof() {
		_ = cursor.Next()
	}
	_, err := cursor.Column(0)
	if err == nil {
		t.Error("expected error when Column called past Eof on HybridStoreCursor, got nil")
	}
	cursor.Close()
}

// TestVTabCursorRowStore_NullValue verifies dsValueToInterface handles TypeNull (nil).
func TestVTabCursorRowStore_NullValue(t *testing.T) {
	rs := DS.NewRowStore([]string{"n"}, []DS.ValueType{DS.TypeNull})
	rs.Insert(DS.NewRow([]DS.Value{DS.NullValue()}))

	cursor := DS.NewRowStoreCursor(rs)
	_ = cursor.Filter(0, "", nil)
	v, err := cursor.Column(0)
	if err != nil {
		t.Fatalf("Column(0): %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for TypeNull, got %v", v)
	}
	cursor.Close()
}

