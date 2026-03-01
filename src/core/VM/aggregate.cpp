#include "aggregate.h"
#include <climits>
#include <cfloat>

extern "C" {

int64_t svdb_agg_sum_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
) {
    int64_t sum = 0;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        sum += values[i];
        any = 1;
    }
    *ok = any;
    return sum;
}

double svdb_agg_sum_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
) {
    double sum = 0.0;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        sum += values[i];
        any = 1;
    }
    *ok = any;
    return sum;
}

int64_t svdb_agg_min_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
) {
    int64_t mn = INT64_MAX;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        if (!any || values[i] < mn) { mn = values[i]; any = 1; }
    }
    *ok = any;
    return mn;
}

int64_t svdb_agg_max_int64(
    const int64_t* values,
    const int8_t*  null_mask,
    size_t         count,
    int*           ok
) {
    int64_t mx = INT64_MIN;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        if (!any || values[i] > mx) { mx = values[i]; any = 1; }
    }
    *ok = any;
    return mx;
}

double svdb_agg_min_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
) {
    double mn = DBL_MAX;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        if (!any || values[i] < mn) { mn = values[i]; any = 1; }
    }
    *ok = any;
    return mn;
}

double svdb_agg_max_float64(
    const double* values,
    const int8_t* null_mask,
    size_t        count,
    int*          ok
) {
    double mx = -DBL_MAX;
    int any = 0;
    for (size_t i = 0; i < count; ++i) {
        if (null_mask && null_mask[i]) continue;
        if (!any || values[i] > mx) { mx = values[i]; any = 1; }
    }
    *ok = any;
    return mx;
}

int64_t svdb_agg_count_notnull(
    const int8_t* null_mask,
    size_t        count
) {
    if (!null_mask) return (int64_t)count;
    int64_t n = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!null_mask[i]) ++n;
    }
    return n;
}

} // extern "C"
