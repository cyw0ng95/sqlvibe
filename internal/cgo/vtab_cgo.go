// Package svdbcgo provides CGO wrappers for the SVDB C API
package svdbcgo

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/svdb -I${SRCDIR}/../../src/core/IS
#include "svdb.h"
#include "vtab_api.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

// VTabModule represents a virtual table module handle
type VTabModule struct {
	h *C.svdb_vtab_module_t
}

// VTab represents a virtual table handle
type VTab struct {
	h *C.svdb_vtab_t
}

// VTabCursor represents a virtual table cursor handle
type VTabCursor struct {
	h *C.svdb_vtab_cursor_t
}

// VTabType represents a virtual table value type
type VTabType int

const (
	VTabTypeNull VTabType = C.SVDB_VTAB_TYPE_NULL
	VTabTypeInt  VTabType = C.SVDB_VTAB_TYPE_INT
	VTabTypeReal VTabType = C.SVDB_VTAB_TYPE_REAL
	VTabTypeText VTabType = C.SVDB_VTAB_TYPE_TEXT
	VTabTypeBlob VTabType = C.SVDB_VTAB_TYPE_BLOB
)

// RegisterVTabModule registers a virtual table module
func RegisterVTabModule(name string, module *VTabModule) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	
	code := C.svdb_register_vtab_module(cName, module.h)
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}

// HasVTabModule checks if a module is registered
func HasVTabModule(name string) bool {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	
	return C.svdb_has_vtab_module(cName) != 0
}

// GetVTabModuleCount returns the number of registered modules
func GetVTabModuleCount() int {
	return int(C.svdb_get_vtab_module_count())
}

// GetVTabModuleName returns the name of a module by index
func GetVTabModuleName(index int) (string, error) {
	buf := make([]C.char, 256)
	code := C.svdb_get_vtab_module_name(C.int(index), &buf[0], C.size_t(len(buf)))
	if code != C.SVDB_OK {
		return "", errors.New("vtab module name lookup failed")
	}
	return C.GoString(&buf[0]), nil
}

// VTabCreate creates a permanent virtual table
func VTabCreate(module *VTabModule, args []string) (*VTab, error) {
	cArgs := make([]*C.char, len(args))
	for i, arg := range args {
		cArgs[i] = C.CString(arg)
		defer C.free(unsafe.Pointer(cArgs[i]))
	}
	
	var cArgsPtr **C.char
	if len(cArgs) > 0 {
		cArgsPtr = &cArgs[0]
	}
	
	h := C.svdb_vtab_create(module.h, cArgsPtr, C.int(len(args)))
	if h == nil {
		return nil, errors.New("failed to create virtual table")
	}
	
	return &VTab{h: h}, nil
}

// VTabConnect connects to a transient virtual table (table function)
func VTabConnect(module *VTabModule, args []string) (*VTab, error) {
	cArgs := make([]*C.char, len(args))
	for i, arg := range args {
		cArgs[i] = C.CString(arg)
		defer C.free(unsafe.Pointer(cArgs[i]))
	}
	
	var cArgsPtr **C.char
	if len(cArgs) > 0 {
		cArgsPtr = &cArgs[0]
	}
	
	h := C.svdb_vtab_connect(module.h, cArgsPtr, C.int(len(args)))
	if h == nil {
		return nil, errors.New("failed to connect to virtual table")
	}
	
	return &VTab{h: h}, nil
}

// VTabColumnCount returns the number of columns in a virtual table
func (v *VTab) ColumnCount() int {
	if v.h == nil {
		return 0
	}
	return int(C.svdb_vtab_column_count(v.h))
}

// VTabColumnName returns the name of a column
func (v *VTab) ColumnName(col int) string {
	if v.h == nil {
		return ""
	}
	return C.GoString(C.svdb_vtab_column_name(v.h, C.int(col)))
}

// VTabCursorOpen opens a cursor on a virtual table
func (v *VTab) CursorOpen() (*VTabCursor, error) {
	if v.h == nil {
		return nil, errors.New("nil virtual table")
	}
	
	h := C.svdb_vtab_cursor_open(v.h)
	if h == nil {
		return nil, errors.New("failed to open virtual table cursor")
	}
	
	return &VTabCursor{h: h}, nil
}

// VTabClose closes a virtual table
func (v *VTab) Close(destroy bool) error {
	if v.h == nil {
		return nil
	}
	
	cDestroy := 0
	if destroy {
		cDestroy = 1
	}
	
	code := C.svdb_vtab_close(v.h, C.int(cDestroy))
	v.h = nil
	
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}

// VTabCursorFilter filters the cursor based on constraints
func (c *VTabCursor) Filter(idxNum int, idxStr string, args []string) error {
	if c.h == nil {
		return errors.New("nil cursor")
	}
	
	cIdxStr := C.CString(idxStr)
	defer C.free(unsafe.Pointer(cIdxStr))
	
	cArgs := make([]*C.char, len(args))
	for i, arg := range args {
		cArgs[i] = C.CString(arg)
		defer C.free(unsafe.Pointer(cArgs[i]))
	}
	
	var cArgsPtr **C.char
	if len(cArgs) > 0 {
		cArgsPtr = &cArgs[0]
	}
	
	code := C.svdb_vtab_cursor_filter(
		c.h,
		C.int(idxNum),
		cIdxStr,
		cArgsPtr,
		C.int(len(args)),
	)
	
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}

// VTabCursorNext advances the cursor to the next row
func (c *VTabCursor) Next() error {
	if c.h == nil {
		return errors.New("nil cursor")
	}
	
	code := C.svdb_vtab_cursor_next(c.h)
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}

// VTabCursorEOF checks if the cursor is at end
func (c *VTabCursor) EOF() bool {
	if c.h == nil {
		return true
	}
	return C.svdb_vtab_cursor_eof(c.h) != 0
}

// VTabCursorColumn gets a column value from the current row
func (c *VTabCursor) Column(col int) (interface{}, error) {
	if c.h == nil {
		return nil, errors.New("nil cursor")
	}
	
	var outType C.int
	var outIval C.int64_t
	var outRval C.double
	var outSval *C.char
	var outSlen C.size_t
	
	code := C.svdb_vtab_cursor_column(
		c.h,
		C.int(col),
		&outType,
		&outIval,
		&outRval,
		&outSval,
		&outSlen,
	)
	
	if code != C.SVDB_OK {
		return nil, svdbErr(nil, code)
	}
	
	switch VTabType(outType) {
	case VTabTypeNull:
		return nil, nil
	case VTabTypeInt:
		return int64(outIval), nil
	case VTabTypeReal:
		return float64(outRval), nil
	case VTabTypeText:
		if outSval == nil {
			return "", nil
		}
		return C.GoStringN(outSval, C.int(outSlen)), nil
	case VTabTypeBlob:
		if outSval == nil {
			return []byte(nil), nil
		}
		return C.GoBytes(unsafe.Pointer(outSval), C.int(outSlen)), nil
	default:
		return nil, errors.New("unknown vtab column type")
	}
}

// VTabCursorRowID gets the current row ID
func (c *VTabCursor) RowID() (int64, error) {
	if c.h == nil {
		return 0, errors.New("nil cursor")
	}
	
	var outRowid C.int64_t
	code := C.svdb_vtab_cursor_rowid(c.h, &outRowid)
	if code != C.SVDB_OK {
		return 0, svdbErr(nil, code)
	}
	
	return int64(outRowid), nil
}

// VTabCursorClose closes the cursor
func (c *VTabCursor) Close() error {
	if c.h == nil {
		return nil
	}
	
	code := C.svdb_vtab_cursor_close(c.h)
	c.h = nil
	
	if code != C.SVDB_OK {
		return svdbErr(nil, code)
	}
	return nil
}
