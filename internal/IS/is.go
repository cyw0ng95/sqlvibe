package is

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/IS
#include "is_registry.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"runtime"
	
	"unsafe"
)

// ErrISQueryFailed is returned when an IS query fails
var ErrISQueryFailed = errors.New("information schema query failed")

// TableInfo represents information about a table or view
type TableInfo struct {
	TableName   string
	TableSchema string
	TableType   string
}

// ColumnInfo represents information about a column
type ColumnInfo struct {
	ColumnName    string
	TableName     string
	TableSchema   string
	DataType      string
	IsNullable    string
	ColumnDefault string
}

// ViewInfo represents information about a view
type ViewInfo struct {
	TableName      string
	TableSchema    string
	ViewDefinition string
}

// ConstraintInfo represents information about a constraint
type ConstraintInfo struct {
	ConstraintName string
	TableName      string
	TableSchema    string
	ConstraintType string
}

// ReferentialConstraint represents FK relationship
type ReferentialConstraint struct {
	ConstraintName         string
	UniqueConstraintSchema string
	UniqueConstraintName   string
}

// Registry wraps C++ IS registry
type Registry struct {
	ptr *C.svdb_is_registry_t
}

// NewRegistry creates a new IS registry
func NewRegistry(btreeHandle unsafe.Pointer) *Registry {
	ptr := C.svdb_is_registry_create(btreeHandle)
	r := &Registry{ptr: ptr}
	runtime.SetFinalizer(r, func(r *Registry) {
		if r.ptr != nil {
			C.svdb_is_registry_destroy(r.ptr)
			r.ptr = nil
		}
	})
	return r
}

// IsInformationSchemaTable checks if table is an IS table
func IsInformationSchemaTable(tableName string) bool {
	cs := C.CString(tableName)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_is_information_schema_table(cs) != 0
}

// QueryColumns queries information_schema.columns
func (r *Registry) QueryColumns(schema, tableName string) ([]ColumnInfo, error) {
	if r.ptr == nil {
		return nil, errors.New("registry not initialized")
	}
	
	var result C.svdb_is_result_t
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	cTable := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTable))

	ret := C.svdb_is_query_columns(r.ptr, cSchema, cTable, &result)
	if ret < 0 {
		return nil, ErrISQueryFailed
	}
	defer C.svdb_is_result_free(&result)

	if result.num_columns == 0 {
		return nil, nil
	}

	cols := (*[1 << 30]C.svdb_is_column_info_t)(unsafe.Pointer(result.columns))[:int(result.num_columns):int(result.num_columns)]
	goCols := make([]ColumnInfo, int(result.num_columns))
	for i := 0; i < int(result.num_columns); i++ {
		goCols[i] = ColumnInfo{
			ColumnName:    C.GoString(cols[i].column_name),
			TableName:     C.GoString(cols[i].table_name),
			TableSchema:   C.GoString(cols[i].table_schema),
			DataType:      C.GoString(cols[i].data_type),
			IsNullable:    intToYesNo(int(cols[i].is_nullable)),
			ColumnDefault: goStringOrNull(cols[i].column_default),
		}
	}
	return goCols, nil
}

// QueryTables queries information_schema.tables
func (r *Registry) QueryTables(schema, tableName string) ([]TableInfo, error) {
	if r.ptr == nil {
		return nil, errors.New("registry not initialized")
	}
	
	var result C.svdb_is_result_t
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	cTable := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTable))

	ret := C.svdb_is_query_tables(r.ptr, cSchema, cTable, &result)
	if ret < 0 {
		return nil, ErrISQueryFailed
	}
	defer C.svdb_is_result_free(&result)

	if result.num_tables == 0 {
		return nil, nil
	}

	tables := (*[1 << 30]C.svdb_is_table_info_t)(unsafe.Pointer(result.tables))[:int(result.num_tables):int(result.num_tables)]
	goTables := make([]TableInfo, int(result.num_tables))
	for i := 0; i < int(result.num_tables); i++ {
		goTables[i] = TableInfo{
			TableName:   C.GoString(tables[i].table_name),
			TableSchema: C.GoString(tables[i].table_schema),
			TableType:   C.GoString(tables[i].table_type),
		}
	}
	return goTables, nil
}

// QueryViews queries information_schema.views
func (r *Registry) QueryViews(schema, tableName string) ([]ViewInfo, error) {
	if r.ptr == nil {
		return nil, errors.New("registry not initialized")
	}
	
	var result C.svdb_is_result_t
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	cTable := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTable))

	ret := C.svdb_is_query_views(r.ptr, cSchema, cTable, &result)
	if ret < 0 {
		return nil, ErrISQueryFailed
	}
	defer C.svdb_is_result_free(&result)

	if result.num_views == 0 {
		return nil, nil
	}

	views := (*[1 << 30]C.svdb_is_view_info_t)(unsafe.Pointer(result.views))[:int(result.num_views):int(result.num_views)]
	goViews := make([]ViewInfo, int(result.num_views))
	for i := 0; i < int(result.num_views); i++ {
		goViews[i] = ViewInfo{
			TableName:      C.GoString(views[i].table_name),
			TableSchema:    C.GoString(views[i].table_schema),
			ViewDefinition: C.GoString(views[i].view_definition),
		}
	}
	return goViews, nil
}

// QueryConstraints queries information_schema.table_constraints
func (r *Registry) QueryConstraints(schema, tableName string) ([]ConstraintInfo, error) {
	// TODO: Implement in C++
	return nil, nil
}

// QueryReferential queries information_schema.referential_constraints
func (r *Registry) QueryReferential(schema, tableName string) ([]ReferentialConstraint, error) {
	// TODO: Implement in C++
	return nil, nil
}

func intToYesNo(i int) string {
	if i != 0 {
		return "YES"
	}
	return "NO"
}

func goStringOrNull(s *C.char) string {
	if s == nil {
		return ""
	}
	return C.GoString(s)
}
