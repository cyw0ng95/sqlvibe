/* row.h — Row structure C API */
#pragma once
#ifndef SVDB_DS_ROW_H
#define SVDB_DS_ROW_H

#include <stdint.h>
#include <stddef.h>
#include "value.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Row holds up to 64 columns with NULL bitmap */
typedef struct {
    svdb_value_t* cols;  /* Array of values */
    size_t num_cols;
    uint64_t bitmap;     /* Bit i set → col i is NULL */
} svdb_row_t;

/* Create row from values array */
svdb_row_t* svdb_row_create(const svdb_value_t* values, size_t num_cols);

/* Free row */
void svdb_row_destroy(svdb_row_t* row);

/* Check if column is NULL */
int svdb_row_is_null(const svdb_row_t* row, size_t idx);

/* Set column as NULL */
void svdb_row_set_null(svdb_row_t* row, size_t idx);

/* Clear NULL mark */
void svdb_row_clear_null(svdb_row_t* row, size_t idx);

/* Get column value */
const svdb_value_t* svdb_row_get(const svdb_row_t* row, size_t idx);

/* Set column value */
void svdb_row_set(svdb_row_t* row, size_t idx, const svdb_value_t* value);

/* Get number of columns */
size_t svdb_row_len(const svdb_row_t* row);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_ROW_H */
