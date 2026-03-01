#ifndef SVDB_VM_EXPR_EVAL_H
#define SVDB_VM_EXPR_EVAL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Batch compare int64 values; results[i] = -1/0/1 */
void svdb_compare_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int*           results,
    size_t         count
);

/* Batch compare float64 values; results[i] = -1/0/1 */
void svdb_compare_float64_batch(
    const double* a,
    const double* b,
    int*          results,
    size_t        count
);

/* Batch add int64 values */
void svdb_add_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
);

/* Batch add float64 values */
void svdb_add_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
);

/* Batch subtract int64 values */
void svdb_sub_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
);

/* Batch subtract float64 values */
void svdb_sub_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
);

/* Batch multiply int64 values */
void svdb_mul_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
);

/* Batch multiply float64 values */
void svdb_mul_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
);

/* Batch apply mask filter: out[j] = indices[i] for each true mask[i] */
size_t svdb_filter_mask(
    const int8_t*   mask,
    const int64_t*  indices,
    int64_t*        out,
    size_t          count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_EXPR_EVAL_H */
