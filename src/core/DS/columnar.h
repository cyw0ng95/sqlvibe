#ifndef SVDB_DS_COLUMNAR_H
#define SVDB_DS_COLUMNAR_H

#include <stdint.h>
#include <stddef.h>
#include "value.h"

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

/* Destroy the store and free all memory. */
void svdb_column_store_destroy(svdb_column_store_t* store);

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

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_COLUMNAR_H */
