#ifndef SVDB_DS_ROW_STORE_H
#define SVDB_DS_ROW_STORE_H

#include <stdint.h>
#include <stddef.h>
#include "value.h"
#include "manager.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque row store handle. */
typedef struct svdb_row_store_t svdb_row_store_t;

/*
 * Create a row store with num_cols columns.
 * col_names[i] is a null-terminated column name.
 * col_types[i] is one of the SVDB_TYPE_* constants (defined in columnar.h).
 * Returns NULL on failure.
 */
svdb_row_store_t* svdb_row_store_create(const char* const* col_names,
                                         const int* col_types,
                                         int num_cols);

/*
 * Create a row store with embedded PageManager for persistence.
 */
svdb_row_store_t* svdb_row_store_create_embedded(const char* const* col_names,
                                                  const int* col_types,
                                                  int num_cols,
                                                  svdb_page_manager* pm,
                                                  uint32_t root_page);

/* Destroy the store and free all memory. */
void svdb_row_store_destroy(svdb_row_store_t* store);

/* Destroy an embedded row store (with persistence metadata). */
void svdb_row_store_destroy_embedded(svdb_row_store_t* store);

/*
 * Persist the row store to disk using embedded PageManager.
 */
int svdb_row_store_persist(svdb_row_store_t* store, svdb_page_manager* pm, uint32_t* out_root_page);

/*
 * Append a row of num_values values and return its row index.
 * If num_values < num_cols, remaining columns receive NULL.
 * Returns -1 on failure.
 */
int svdb_row_store_insert(svdb_row_store_t* store,
                           const svdb_value_t* values,
                           int num_values);

/*
 * Read the row at idx into out_values[0..num_cols-1].
 * *out_count is set to the number of columns written.
 * Returns 1 on success, 0 if idx is out of range.
 * str_data/bytes_data pointers are valid until the next mutation.
 */
int svdb_row_store_get(svdb_row_store_t* store, int idx,
                        svdb_value_t* out_values, int* out_count);

/*
 * Replace the row at idx with num_values new values.
 * If idx is out of range, does nothing.
 */
void svdb_row_store_update(svdb_row_store_t* store, int idx,
                            const svdb_value_t* values, int num_values);

/* Mark the row at idx as deleted (tombstone; idempotent). */
void svdb_row_store_delete(svdb_row_store_t* store, int idx);

/* Return 1 if the row at idx is deleted, 0 otherwise. Out of range returns 1. */
int svdb_row_store_is_deleted(svdb_row_store_t* store, int idx);

/* Return total row count (including deleted rows). */
int svdb_row_store_row_count(svdb_row_store_t* store);

/* Return non-deleted row count. */
int svdb_row_store_live_count(svdb_row_store_t* store);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_ROW_STORE_H */
