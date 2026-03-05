#ifndef SVDB_DS_HYBRID_STORE_API_H
#define SVDB_DS_HYBRID_STORE_API_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Result structure for scan operations */
typedef struct {
    svdb_value_t* values;     /* Flat array: [row0_col0, row0_col1, ..., row1_col0, ...] */
    int32_t*      row_indices;/* Original row indices for each result row */
    int32_t       num_rows;
    int32_t       num_cols;
} svdb_scan_result_t;

/* Index types */
typedef struct svdb_bitmap_index svdb_bitmap_index_t;
typedef struct svdb_skiplist_index svdb_skiplist_index_t;
typedef struct svdb_index_engine svdb_index_engine_t;

/* Allocation helpers */
svdb_scan_result_t* svdb_scan_result_alloc(int32_t num_rows, int32_t num_cols);
void                svdb_scan_result_free(svdb_scan_result_t* result);

/* Index Engine operations */
svdb_index_engine_t* svdb_index_engine_create(void);
void                 svdb_index_engine_destroy(svdb_index_engine_t* ie);

void                 svdb_index_engine_add_bitmap(svdb_index_engine_t* ie, const char* col_name);
void                 svdb_index_engine_add_skiplist(svdb_index_engine_t* ie, const char* col_name);
int                  svdb_index_engine_has_bitmap(svdb_index_engine_t* ie, const char* col_name);
int                  svdb_index_engine_has_skiplist(svdb_index_engine_t* ie, const char* col_name);

void                 svdb_index_engine_index_row(svdb_index_engine_t* ie, uint32_t row_idx,
                                                  const char* col_name, const svdb_value_t* val);
void                 svdb_index_engine_unindex_row(svdb_index_engine_t* ie, uint32_t row_idx,
                                                    const char* col_name, const svdb_value_t* val);

/* Scan operations using C++ index engine */
svdb_scan_result_t* svdb_index_lookup_equal(svdb_index_engine_t* ie, const char* col_name,
                                             const svdb_value_t* val, int32_t num_cols);

svdb_scan_result_t* svdb_index_lookup_range(svdb_index_engine_t* ie, const char* col_name,
                                             const svdb_value_t* lo, const svdb_value_t* hi,
                                             int32_t num_cols, int inclusive);

/* Go callback types for row materialization */
typedef svdb_value_t (*svdb_get_row_value_fn)(uint32_t row_idx, int32_t col_idx, void* user_data);
typedef int32_t      (*svdb_compare_values_fn)(const svdb_value_t* a, const svdb_value_t* b);

/* Scan with filter using C++ comparison */
svdb_scan_result_t* svdb_scan_with_filter(
    void* row_store,                    /* Go RowStore pointer */
    svdb_get_row_value_fn get_value,    /* Callback to get row value */
    int32_t* row_indices,               /* Array of valid row indices */
    int32_t num_indices,                /* Number of valid indices */
    int32_t num_cols,                   /* Number of columns */
    int32_t filter_col_idx,             /* Column index to filter on */
    const svdb_value_t* filter_val,     /* Filter value */
    const char* op,                     /* Comparison op: "=", "!=", "<", "<=", ">", ">=" */
    svdb_compare_values_fn cmp          /* Compare callback */
);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_HYBRID_STORE_API_H */
