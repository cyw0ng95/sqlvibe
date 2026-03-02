#ifndef SVDB_ENGINE_API_H
#define SVDB_ENGINE_API_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A Row is an array of (column_name, value) pairs */
typedef struct {
    char**         col_names;  /* null-terminated C strings */
    svdb_value_t*  vals;       /* parallel value array */
    int32_t        num_cols;
} svdb_engine_row_t;

/* A Rows result — rows is an inline array of num_rows row structs */
typedef struct {
    svdb_engine_row_t* rows;
    int32_t            num_rows;
} svdb_engine_rows_t;

/* Sort key */
typedef struct {
    const char* col_name;
    int32_t     order;       /* 0=ASC, 1=DESC */
    int32_t     null_order;  /* 0=NULLS FIRST, 1=NULLS LAST */
} svdb_engine_sort_key_t;

/* Allocation helpers */
svdb_engine_rows_t* svdb_engine_rows_alloc(int32_t num_rows);
void                svdb_engine_rows_free(svdb_engine_rows_t* rows);
svdb_engine_row_t*  svdb_engine_row_alloc(int32_t num_cols);
void                svdb_engine_row_free(svdb_engine_row_t* row);

/* SELECT operations */
svdb_engine_rows_t* svdb_engine_apply_limit_offset(
    const svdb_engine_rows_t* rows, int32_t limit, int32_t offset);

int32_t svdb_engine_col_names(
    const svdb_engine_rows_t* rows, char** out_names, int32_t max_names);

/* JOIN operations */
svdb_engine_row_t* svdb_engine_merge_rows(
    const svdb_engine_row_t* a, const svdb_engine_row_t* b);

svdb_engine_row_t* svdb_engine_merge_rows_alias(
    const svdb_engine_row_t* a, const char* alias_a,
    const svdb_engine_row_t* b, const char* alias_b);

svdb_engine_rows_t* svdb_engine_cross_join(
    const svdb_engine_rows_t* left, const svdb_engine_rows_t* right);

/* Aggregate operations */
int64_t svdb_engine_count_rows(
    const svdb_engine_rows_t* rows, const char* col_name);

/* Sort / reverse */
svdb_engine_rows_t* svdb_engine_sort_rows(
    const svdb_engine_rows_t* rows,
    const svdb_engine_sort_key_t* sort_keys, int32_t num_keys);

svdb_engine_rows_t* svdb_engine_reverse_rows(const svdb_engine_rows_t* rows);

/* Subquery operations */
int32_t svdb_engine_exists_rows(const svdb_engine_rows_t* rows);

int32_t svdb_engine_in_rows(
    const svdb_value_t* value,
    const svdb_engine_rows_t*  rows,
    const char*                col_name);

int32_t svdb_engine_not_in_rows(
    const svdb_value_t* value,
    const svdb_engine_rows_t*  rows,
    const char*                col_name);

/* Window operations — caller must free() the returned arrays */
int64_t* svdb_engine_row_numbers(int32_t n);
int64_t* svdb_engine_ranks(
    const svdb_engine_rows_t* rows, const char* col_name);
int64_t* svdb_engine_dense_ranks(
    const svdb_engine_rows_t* rows, const char* col_name);

/* Filter operations — predicate callback receives row, returns 1 to keep */
typedef int32_t (*svdb_row_predicate_fn)(const svdb_engine_row_t* row, void* user_data);

svdb_engine_rows_t* svdb_engine_filter_rows(
    const svdb_engine_rows_t* rows,
    svdb_row_predicate_fn pred,
    void* user_data);

/* Distinct operation — key function returns string key for deduplication */
typedef const char* (*svdb_row_key_fn)(const svdb_engine_row_t* row, void* user_data);

svdb_engine_rows_t* svdb_engine_apply_distinct(
    const svdb_engine_rows_t* rows,
    svdb_row_key_fn key_fn,
    void* user_data);

/* JOIN operations with predicates */
typedef int32_t (*svdb_join_predicate_fn)(const svdb_engine_row_t* merged, void* user_data);

svdb_engine_rows_t* svdb_engine_inner_join(
    const svdb_engine_rows_t* left,
    const svdb_engine_rows_t* right,
    svdb_join_predicate_fn pred,
    void* user_data);

svdb_engine_rows_t* svdb_engine_left_outer_join(
    const svdb_engine_rows_t* left,
    const svdb_engine_rows_t* right,
    svdb_join_predicate_fn pred,
    void* user_data,
    const char** right_cols,
    int32_t num_right_cols);

/* Aggregate operations */
svdb_engine_rows_t* svdb_engine_group_rows(
    const svdb_engine_rows_t* rows,
    svdb_row_key_fn key_fn,
    void* user_data);

svdb_value_t svdb_engine_sum_rows(
    const svdb_engine_rows_t* rows, const char* col_name);

double svdb_engine_avg_rows(
    const svdb_engine_rows_t* rows, const char* col_name);

svdb_value_t svdb_engine_min_rows(
    const svdb_engine_rows_t* rows, const char* col_name);

svdb_value_t svdb_engine_max_rows(
    const svdb_engine_rows_t* rows, const char* col_name);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_ENGINE_API_H */
