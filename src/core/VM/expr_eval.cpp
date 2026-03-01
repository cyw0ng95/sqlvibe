#include "expr_eval.h"
#include <cstring>
#include <algorithm>

extern "C" {

void svdb_compare_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int*           results,
    size_t         count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = (a[i] < b[i]) ? -1 : (a[i] > b[i]) ? 1 : 0;
    }
}

void svdb_compare_float64_batch(
    const double* a,
    const double* b,
    int*          results,
    size_t        count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = (a[i] < b[i]) ? -1 : (a[i] > b[i]) ? 1 : 0;
    }
}

void svdb_add_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] + b[i];
    }
}

void svdb_add_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] + b[i];
    }
}

void svdb_sub_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] - b[i];
    }
}

void svdb_sub_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] - b[i];
    }
}

void svdb_mul_int64_batch(
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] * b[i];
    }
}

void svdb_mul_float64_batch(
    const double* a,
    const double* b,
    double*       results,
    size_t        count
) {
    for (size_t i = 0; i < count; ++i) {
        results[i] = a[i] * b[i];
    }
}

size_t svdb_filter_mask(
    const int8_t*   mask,
    const int64_t*  indices,
    int64_t*        out,
    size_t          count
) {
    size_t j = 0;
    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            out[j++] = indices[i];
        }
    }
    return j;
}

} // extern "C"
