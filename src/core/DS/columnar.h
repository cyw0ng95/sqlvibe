#ifndef SVDB_DS_COLUMNAR_H
#define SVDB_DS_COLUMNAR_H

#include <stdint.h>
#include <stddef.h>
#include "value.h"
#include "manager.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Column type constants (match internal/DS value types). */
#define SVDB_TYPE_NULL  0
#define SVDB_TYPE_INT   1
#define SVDB_TYPE_REAL  2
#define SVDB_TYPE_TEXT  3
#define SVDB_TYPE_BLOB  4

/* Opaque columnar store handle. */
typedef struct svdb_column_store_t svdb_column_store_t;

/*
 * Create a column store with num_cols columns.
 * col_names[i] is a null-terminated column name.
 * col_types[i] is one of the SVDB_TYPE_* constants.
 * Returns NULL on failure.
 */
svdb_column_store_t* svdb_column_store_create(const char* const* col_names,
                                               const int* col_types,
                                               int num_cols);

/*
 * Create a column store with embedded PageManager for persistence.
 * This version stores data directly to disk using the C++ PageManager.
 */
svdb_column_store_t* svdb_column_store_create_embedded(const char* const* col_names,
                                                       const int* col_types,
                                                       int num_cols,
                                                       svdb_page_manager* pm,
                                                       uint32_t root_page);

/* Destroy the store and free all memory. */
void svdb_column_store_destroy(svdb_column_store_t* store);

/* Destroy an embedded column store (with persistence metadata). */
void svdb_column_store_destroy_embedded(svdb_column_store_t* store);

/*
 * Persist the column store to disk using embedded PageManager.
 * Returns 1 on success, 0 on error.
 */
int svdb_column_store_persist(svdb_column_store_t* store, svdb_page_manager* pm, uint32_t* out_root_page);

/*
 * Append a row of num_values values.
 * If num_values < num_cols, the remaining columns receive NULL.
 */
void svdb_column_store_append_row(svdb_column_store_t* store,
                                   const svdb_value_t* values,
                                   int num_values);

/*
 * Read row at idx (including deleted rows) into out_values[0..num_cols-1].
 * out_count is set to the number of columns written.
 * Returns 1 on success, 0 if idx is out of range.
 * Caller must provide an out_values array of at least num_cols elements.
 * Note: str_data/bytes_data pointers in returned values are valid only
 * until the next mutation of the store.
 */
int svdb_column_store_get_row(svdb_column_store_t* store, int idx,
                                svdb_value_t* out_values, int* out_count);

/* Mark row at idx as deleted (idempotent). */
void svdb_column_store_delete_row(svdb_column_store_t* store, int idx);

/* Return 1 if the row at idx is deleted, 0 otherwise. Out of range returns 1. */
int svdb_column_store_is_deleted(svdb_column_store_t* store, int idx);

/*
 * Update all column values for the existing row at idx.
 * If num_values < num_cols, remaining columns become NULL.
 * If idx is out of range, does nothing.
 */
void svdb_column_store_update_row(svdb_column_store_t* store, int idx,
                                   const svdb_value_t* values, int num_values);

/* Return total row count (including deleted rows). */
int svdb_column_store_row_count(svdb_column_store_t* store);

/* Return non-deleted row count. */
int svdb_column_store_live_count(svdb_column_store_t* store);

/* ==========================================================================
 * SIMD-Optimized Column Scan Operations
 * ========================================================================== */

/* Comparison operators for SIMD scan */
#define SVDB_COL_CMP_EQ   0  /* == */
#define SVDB_COL_CMP_NE   1  /* != */
#define SVDB_COL_CMP_GT   2  /* >  */
#define SVDB_COL_CMP_GE   3  /* >= */
#define SVDB_COL_CMP_LT   4  /* <  */
#define SVDB_COL_CMP_LE   5  /* <= */

/*
 * SIMD-optimized column scan for INT64 columns.
 * Produces a bitmap of matching rows.
 *
 * col_idx: column index to scan
 * op: comparison operator (SVDB_COL_CMP_*)
 * val: comparison value
 * result_bitmap: output bitmap (must be pre-allocated with (row_count+63)/64 uint64_t)
 *                Bit i is set if row i matches the condition and is not deleted.
 *
 * Returns number of matching rows.
 */
size_t svdb_column_store_scan_int64(svdb_column_store_t* store,
                                     int col_idx,
                                     int op,
                                     int64_t val,
                                     uint64_t* result_bitmap);

/*
 * SIMD-optimized column scan for DOUBLE columns.
 */
size_t svdb_column_store_scan_double(svdb_column_store_t* store,
                                      int col_idx,
                                      int op,
                                      double val,
                                      uint64_t* result_bitmap);

/*
 * Combine two bitmaps with AND operation (for multi-condition WHERE).
 * result[i] = a[i] & b[i]
 * Returns number of set bits in result.
 */
size_t svdb_column_store_bitmap_and(uint64_t* result,
                                     const uint64_t* a,
                                     const uint64_t* b,
                                     size_t bitmap_size);

/*
 * Combine two bitmaps with OR operation.
 */
size_t svdb_column_store_bitmap_or(uint64_t* result,
                                    const uint64_t* a,
                                    const uint64_t* b,
                                    size_t bitmap_size);

/*
 * Extract row indices from a bitmap.
 * out_indices: pre-allocated array of at least bitmap_size * 64 elements
 * Returns the number of indices extracted.
 */
size_t svdb_column_store_bitmap_to_indices(const uint64_t* bitmap,
                                            size_t bitmap_size,
                                            int* out_indices);

/*
 * SIMD aggregation with bitmap filter.
 * Only rows where bitmap bit is set are included.
 */

/* Sum of INT64 column */
int64_t svdb_column_store_sum_int64(svdb_column_store_t* store,
                                     int col_idx,
                                     const uint64_t* bitmap);

/* Sum of DOUBLE column */
double svdb_column_store_sum_double(svdb_column_store_t* store,
                                     int col_idx,
                                     const uint64_t* bitmap);

/* Min of INT64 column */
int64_t svdb_column_store_min_int64(svdb_column_store_t* store,
                                     int col_idx,
                                     const uint64_t* bitmap);

/* Max of INT64 column */
int64_t svdb_column_store_max_int64(svdb_column_store_t* store,
                                     int col_idx,
                                     const uint64_t* bitmap);

/* Count of rows in bitmap */
size_t svdb_column_store_count(const uint64_t* bitmap, size_t bitmap_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_COLUMNAR_H */
