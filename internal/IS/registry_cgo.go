package IS

import "errors"

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/IS
#include "is_registry.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// ErrISQueryFailed is returned when an IS query fails.
var ErrISQueryFailed = errors.New("information schema query failed")

// ISRegistry wraps the C++ information schema registry.
type ISRegistry struct {
	ptr *C.svdb_is_registry_t
}

// NewISRegistry creates a new C++ IS registry.
func NewISRegistry(btreeHandle unsafe.Pointer) *ISRegistry {
	reg := &ISRegistry{
		ptr: C.svdb_is_registry_create(btreeHandle),
	}
	runtime.SetFinalizer(reg, func(r *ISRegistry) {
		if r.ptr != nil {
			C.svdb_is_registry_destroy(r.ptr)
			r.ptr = nil
		}
	})
	return reg
}

// Destroy frees the C++ IS registry.
func (r *ISRegistry) Destroy() {
	if r.ptr != nil {
		C.svdb_is_registry_destroy(r.ptr)
		r.ptr = nil
	}
	runtime.SetFinalizer(r, nil)
}

// IsInformationSchemaTable checks if a table name is an information_schema table.
func IsInformationSchemaTable(tableName string) bool {
	cs := C.CString(tableName)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_is_information_schema_table(cs) != 0
}

// QueryColumns queries the columns view.
func (r *ISRegistry) QueryColumns(schema, tableName string) ([]ColumnInfo, error) {
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

// QueryTables queries the tables view.
func (r *ISRegistry) QueryTables(schema, tableName string) ([]TableInfo, error) {
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

// QueryViews queries the views view.
func (r *ISRegistry) QueryViews(schema, tableName string) ([]ViewInfo, error) {
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

// QueryConstraints queries the constraints view.
func (r *ISRegistry) QueryConstraints(schema, tableName string) ([]ConstraintInfo, error) {
	var result C.svdb_is_result_t
	
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	
	cTable := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTable))
	
	ret := C.svdb_is_query_constraints(r.ptr, cSchema, cTable, &result)
	if ret < 0 {
		return nil, ErrISQueryFailed
	}
	defer C.svdb_is_result_free(&result)
	
	if result.num_columns == 0 {
		return nil, nil
	}
	
	/* TODO: Implement constraint query */
	
	return nil, nil
}

// QueryReferential queries the referential constraints view.
func (r *ISRegistry) QueryReferential(schema, tableName string) ([]ReferentialConstraint, error) {
	var result C.svdb_is_result_t
	
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	
	cTable := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTable))
	
	ret := C.svdb_is_query_referential(r.ptr, cSchema, cTable, &result)
	if ret < 0 {
		return nil, ErrISQueryFailed
	}
	defer C.svdb_is_result_free(&result)
	
	/* TODO: Implement referential query */
	
	return nil, nil
}

// Helper: convert int to "YES"/"NO"
func intToYesNo(i int) string {
	if i != 0 {
		return "YES"
	}
	return "NO"
}

// Helper: convert C string to Go string (handling NULL)
func goStringOrNull(s *C.char) string {
	if s == nil {
		return ""
	}
	return C.GoString(s)
}

// ToSQL converts column info to database/sql compatible format.
func (ci ColumnInfo) ToSQL() []any {
	return []any{
		ci.ColumnName,
		ci.TableName,
		ci.TableSchema,
		ci.DataType,
		ci.IsNullable,
		ci.ColumnDefault,
	}
}

// ToSQL converts table info to database/sql compatible format.
func (ti TableInfo) ToSQL() []any {
	return []any{
		ti.TableName,
		ti.TableSchema,
		ti.TableType,
	}
}

// ToSQL converts view info to database/sql compatible format.
func (vi ViewInfo) ToSQL() []any {
	return []any{
		vi.TableName,
		vi.TableSchema,
		vi.ViewDefinition,
	}
}

// ToSQL converts constraint info to database/sql compatible format.
func (ci ConstraintInfo) ToSQL() []any {
	return []any{
		ci.ConstraintName,
		ci.TableName,
		ci.TableSchema,
		ci.ConstraintType,
	}
}

// ToSQL converts referential constraint to database/sql compatible format.
func (rc ReferentialConstraint) ToSQL() []any {
	return []any{
		rc.ConstraintName,
		rc.UniqueConstraintSchema,
		rc.UniqueConstraintName,
	}
}
