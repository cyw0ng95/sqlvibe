#ifndef SVDB_VM_AGGREGATE_H
#define SVDB_VM_AGGREGATE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Batch SUM for int64 (null_mask[i]==1 means NULL/skip).
 * ok is set to 0 if all values are NULL.
 */
int64_t svdb_agg_sum_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
);

/* Batch SUM for float64. */
double svdb_agg_sum_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
);

/* Batch MIN for int64. */
int64_t svdb_agg_min_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
);

/* Batch MAX for int64. */
int64_t svdb_agg_max_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
);

/* Batch MIN for float64. */
double svdb_agg_min_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
);

/* Batch MAX for float64. */
double svdb_agg_max_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
);

/* Count non-null entries (null_mask[i]==1 means NULL). */
int64_t svdb_agg_count_notnull(
    const int8_t* null_mask,
    size_t        count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_AGGREGATE_H */
