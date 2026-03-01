package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "cursor.h"
#include <stdlib.h>
*/
import "C"
import (
"runtime"
"unsafe"

"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

const MaxCursors = 256

type Cursor struct {
ID        int
TableID   int
TableName string
RowID     int64
EOF       bool
Index     int
IndexKey  int64
Data      []map[string]interface{}
Columns   []string
}

type CursorArray struct {
cursors []*Cursor
cArray  C.svdb_cursor_array_t
}

func NewCursorArray() *CursorArray {
ca := &CursorArray{
cursors: make([]*Cursor, 0),
cArray:  C.svdb_cursor_array_create(),
}
runtime.SetFinalizer(ca, func(c *CursorArray) {
if c.cArray != nil {
C.svdb_cursor_array_destroy(c.cArray)
c.cArray = nil
}
})
return ca
}

func (ca *CursorArray) Open(tableID int) int {
id := len(ca.cursors)
cursor := &Cursor{ID: id, TableID: tableID, RowID: 0, EOF: false, Index: -1}
ca.cursors = append(ca.cursors, cursor)
C.svdb_cursor_array_open(ca.cArray, C.int(tableID))
return id
}

func (ca *CursorArray) OpenTable(tableName string, data []map[string]interface{}, columns []string) int {
util.Assert(tableName != "", "table name cannot be empty")
util.AssertNotNil(data, "data")
util.AssertNotNil(columns, "columns")
id := len(ca.cursors)
cursor := &Cursor{ID: id, TableName: tableName, RowID: 0, EOF: len(data) == 0, Index: -1, Data: data, Columns: columns}
ca.cursors = append(ca.cursors, cursor)
cs := C.CString(tableName)
defer C.free(unsafe.Pointer(cs))
C.svdb_cursor_array_open_table(ca.cArray, cs, C.int(len(data)))
return id
}

func (ca *CursorArray) OpenTableAtID(cursorID int, tableName string, data []map[string]interface{}, columns []string) {
util.Assert(cursorID >= 0 && cursorID < MaxCursors, "cursor ID %d out of bounds [0, %d)", cursorID, MaxCursors)
util.Assert(tableName != "", "table name cannot be empty")
util.AssertNotNil(data, "data")
util.AssertNotNil(columns, "columns")
for len(ca.cursors) <= cursorID {
ca.cursors = append(ca.cursors, nil)
}
ca.cursors[cursorID] = &Cursor{ID: cursorID, TableName: tableName, RowID: 0, EOF: len(data) == 0, Index: -1, Data: data, Columns: columns}
cs := C.CString(tableName)
defer C.free(unsafe.Pointer(cs))
C.svdb_cursor_array_open_at_id(ca.cArray, C.int(cursorID), cs, C.int(len(data)))
}

func (ca *CursorArray) Next(id int) (map[string]interface{}, bool) {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < 0 || id >= len(ca.cursors) || ca.cursors[id] == nil {
return nil, true
}
cursor := ca.cursors[id]
cursor.Index++
if cursor.Index >= len(cursor.Data) {
cursor.EOF = true
C.svdb_cursor_set_eof(ca.cArray, C.int(id), 1)
C.svdb_cursor_set_index(ca.cArray, C.int(id), C.int(cursor.Index))
return nil, true
}
C.svdb_cursor_set_index(ca.cArray, C.int(id), C.int(cursor.Index))
return cursor.Data[cursor.Index], false
}

func (ca *CursorArray) GetRow(id int) (map[string]interface{}, bool) {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < 0 || id >= len(ca.cursors) || ca.cursors[id] == nil {
return nil, true
}
cursor := ca.cursors[id]
if cursor.Index < 0 || cursor.Index >= len(cursor.Data) {
return nil, true
}
return cursor.Data[cursor.Index], false
}

func (ca *CursorArray) GetColumn(id int, colName string) interface{} {
	row, eof := ca.GetRow(id)
	if eof {
		return nil
	}
	return row[colName]
}

func (ca *CursorArray) Close(id int) error {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < len(ca.cursors) {
ca.cursors[id] = nil
C.svdb_cursor_array_close(ca.cArray, C.int(id))
}
return nil
}

func (ca *CursorArray) Get(id int) *Cursor {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id >= 0 && id < len(ca.cursors) {
return ca.cursors[id]
}
return nil
}

func (ca *CursorArray) SetRowID(id int, rowID int64) {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < len(ca.cursors) && ca.cursors[id] != nil {
ca.cursors[id].RowID = rowID
C.svdb_cursor_set_rowid(ca.cArray, C.int(id), C.int64_t(rowID))
}
}

func (ca *CursorArray) SetEOF(id int, eof bool) {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < len(ca.cursors) && ca.cursors[id] != nil {
ca.cursors[id].EOF = eof
eofInt := 0
if eof {
eofInt = 1
}
C.svdb_cursor_set_eof(ca.cArray, C.int(id), C.int(eofInt))
}
}

func (ca *CursorArray) SetIndex(id int, idx int) {
util.Assert(id >= 0 && id < MaxCursors, "cursor ID %d out of bounds [0, %d)", id, MaxCursors)
if id < len(ca.cursors) && ca.cursors[id] != nil {
ca.cursors[id].Index = idx
C.svdb_cursor_set_index(ca.cArray, C.int(id), C.int(idx))
}
}

func (ca *CursorArray) Reset() {
for i := range ca.cursors {
ca.cursors[i] = nil
}
ca.cursors = ca.cursors[:0]
C.svdb_cursor_array_reset(ca.cArray)
}
