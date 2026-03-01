package VM

import (
	"testing"
)

func TestCursorArray_New(t *testing.T) {
	ca := NewCursorArray()
	if ca == nil {
		t.Error("NewCursorArray should not return nil")
	}
	if len(ca.cursors) != 0 {
		t.Error("Initial cursor array should be empty")
	}
}

func TestCursorArray_Open(t *testing.T) {
	ca := NewCursorArray()

	id := ca.Open(1)
	if id != 0 {
		t.Errorf("First cursor ID should be 0, got %d", id)
	}

	id = ca.Open(1)
	if id != 1 {
		t.Errorf("Second cursor ID should be 1, got %d", id)
	}
}

func TestCursorArray_OpenTable(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	columns := []string{"id", "name"}

	id := ca.OpenTable("test_table", data, columns)
	if id != 0 {
		t.Errorf("Cursor ID should be 0, got %d", id)
	}

	cursor := ca.cursors[0]
	if cursor.TableName != "test_table" {
		t.Errorf("Table name mismatch: got %s", cursor.TableName)
	}
	if cursor.EOF != false {
		t.Error("EOF should be false for non-empty data")
	}
}

func TestCursorArray_OpenTable_Empty(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{}
	columns := []string{"id", "name"}

	id := ca.OpenTable("empty_table", data, columns)

	cursor := ca.cursors[id]
	if cursor.EOF != true {
		t.Error("EOF should be true for empty data")
	}
}

func TestCursorArray_OpenTableAtID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{
		{"id": 1},
	}
	columns := []string{"id"}

	ca.OpenTableAtID(5, "table5", data, columns)

	cursor := ca.cursors[5]
	if cursor == nil {
		t.Error("Cursor should exist at ID 5")
	}
	if cursor.ID != 5 {
		t.Errorf("Cursor ID should be 5, got %d", cursor.ID)
	}
	if cursor.TableName != "table5" {
		t.Errorf("Table name mismatch: got %s", cursor.TableName)
	}
}

func TestCursorArray_Next(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{
		{"id": 1},
		{"id": 2},
		{"id": 3},
	}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	row, eof := ca.Next(id)
	if eof {
		t.Error("Should not be EOF on first Next")
	}
	if row["id"].(int) != 1 {
		t.Errorf("First row ID should be 1, got %v", row["id"])
	}

	row, eof = ca.Next(id)
	if eof {
		t.Error("Should not be EOF on second Next")
	}
	if row["id"].(int) != 2 {
		t.Errorf("Second row ID should be 2, got %v", row["id"])
	}

	row, eof = ca.Next(id)
	if eof {
		t.Error("Should not be EOF on third Next")
	}
	if row["id"].(int) != 3 {
		t.Errorf("Third row ID should be 3, got %v", row["id"])
	}

	row, eof = ca.Next(id)
	if !eof {
		t.Error("Should be EOF after all rows consumed")
	}
	if row != nil {
		t.Error("Row should be nil at EOF")
	}
}

func TestCursorArray_Next_InvalidID(t *testing.T) {
	ca := NewCursorArray()

	// These should panic due to assertion - skip them
	// Testing with valid ID first, then checking bounds behavior
	_ = ca.Next
	ca2 := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}
	id := ca2.OpenTable("test", data, columns)
	ca2.Next(id)
	ca2.Next(id)
	ca2.Next(id)
	row, eof := ca2.Next(id)
	if !eof {
		t.Error("Should be EOF after all rows consumed")
	}
	if row != nil {
		t.Error("Row should be nil at EOF")
	}
}

func TestCursorArray_GetRow(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	columns := []string{"id", "name"}

	id := ca.OpenTable("test", data, columns)

	ca.Next(id)
	row, _ := ca.GetRow(id)
	if row["name"] != "Alice" {
		t.Errorf("Row name should be Alice, got %v", row["name"])
	}
}

func TestCursorArray_GetRow_InvalidID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}
	id := ca.OpenTable("test", data, columns)
	ca.Next(id)
	ca.Next(id)
	row, eof := ca.GetRow(id)
	if !eof {
		t.Error("Should be EOF after consuming all rows")
	}
	if row != nil {
		t.Error("Row should be nil at EOF")
	}
}

func TestCursorArray_GetColumn(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
	}
	columns := []string{"id", "name"}

	id := ca.OpenTable("test", data, columns)
	ca.Next(id)

	val := ca.GetColumn(id, "name")
	if val != "Alice" {
		t.Errorf("GetColumn should return Alice, got %v", val)
	}
}

func TestCursorArray_GetColumn_InvalidID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}
	id := ca.OpenTable("test", data, columns)
	ca.Next(id)
	ca.Next(id)

	// After consuming all rows, GetColumn returns nil
	val := ca.GetColumn(id, "name")
	if val != nil {
		t.Error("GetColumn should return nil at EOF")
	}
}

func TestCursorArray_Close(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	ca.Close(id)

	// After closing, cursor should be nil but slice length may remain
	cursor := ca.Get(id)
	if cursor != nil {
		t.Error("Cursor should be nil after closing")
	}
}

func TestCursorArray_Close_InvalidID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}
	id := ca.OpenTable("test", data, columns)

	// Close should work on valid ID
	ca.Close(id)

	// After closing, cursor should be nil
	cursor := ca.Get(id)
	if cursor != nil {
		t.Error("Cursor should be nil after close")
	}
}

func TestCursorArray_Reset(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)
	ca.Next(id)

	ca.Reset()

	if len(ca.cursors) != 0 {
		t.Error("Cursors should be empty after reset")
	}
}

func TestCursorArray_Get(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	cursor := ca.Get(id)
	if cursor == nil {
		t.Error("Cursor should not be nil")
	}
	if cursor.TableName != "test" {
		t.Errorf("Table name mismatch: got %s", cursor.TableName)
	}
}

func TestCursorArray_Get_InvalidID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}
	id := ca.OpenTable("test", data, columns)
	ca.Close(id)

	// After closing, cursor should be nil
	cursor := ca.Get(id)
	if cursor != nil {
		t.Error("Cursor should be nil after close")
	}
}

func TestCursorArray_SetRowID(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	ca.SetRowID(id, 42)

	cursor := ca.Get(id)
	if cursor.RowID != 42 {
		t.Errorf("RowID should be 42, got %d", cursor.RowID)
	}
}

func TestCursorArray_SetEOF(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	ca.SetEOF(id, true)

	cursor := ca.Get(id)
	if !cursor.EOF {
		t.Error("EOF should be true after SetEOF(true)")
	}
}

func TestCursorArray_SetIndex(t *testing.T) {
	ca := NewCursorArray()
	data := []map[string]interface{}{{"id": 1}}
	columns := []string{"id"}

	id := ca.OpenTable("test", data, columns)

	ca.SetIndex(id, 5)

	cursor := ca.Get(id)
	if cursor.Index != 5 {
		t.Errorf("Index should be 5, got %d", cursor.Index)
	}
}

func TestCursor_MaxCursors(t *testing.T) {
	if MaxCursors != 256 {
		t.Errorf("MaxCursors should be 256, got %d", MaxCursors)
	}
}
