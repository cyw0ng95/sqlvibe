#ifndef SVDB_SF_OPT_H
#define SVDB_SF_OPT_H

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <algorithm>

namespace svdb {
namespace sf {
namespace opt {

// Vector operations with 4-way loop unrolling
inline void VectorAddInt64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] + b[i];
        dst[i+1] = a[i+1] + b[i+1];
        dst[i+2] = a[i+2] + b[i+2];
        dst[i+3] = a[i+3] + b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] + b[i];
    }
}

inline void VectorSubInt64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] - b[i];
        dst[i+1] = a[i+1] - b[i+1];
        dst[i+2] = a[i+2] - b[i+2];
        dst[i+3] = a[i+3] - b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] - b[i];
    }
}

inline void VectorMulInt64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] * b[i];
        dst[i+1] = a[i+1] * b[i+1];
        dst[i+2] = a[i+2] * b[i+2];
        dst[i+3] = a[i+3] * b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] * b[i];
    }
}

inline int64_t VectorSumInt64(const int64_t* a, size_t n) {
    int64_t s0 = 0, s1 = 0, s2 = 0, s3 = 0;
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        s0 += a[i];
        s1 += a[i+1];
        s2 += a[i+2];
        s3 += a[i+3];
    }
    int64_t sum = s0 + s1 + s2 + s3;
    for (; i < n; i++) {
        sum += a[i];
    }
    return sum;
}

inline void VectorAddFloat64(double* dst, const double* a, const double* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] + b[i];
        dst[i+1] = a[i+1] + b[i+1];
        dst[i+2] = a[i+2] + b[i+2];
        dst[i+3] = a[i+3] + b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] + b[i];
    }
}

inline void VectorSubFloat64(double* dst, const double* a, const double* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] - b[i];
        dst[i+1] = a[i+1] - b[i+1];
        dst[i+2] = a[i+2] - b[i+2];
        dst[i+3] = a[i+3] - b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] - b[i];
    }
}

inline void VectorMulFloat64(double* dst, const double* a, const double* b, size_t n) {
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        dst[i] = a[i] * b[i];
        dst[i+1] = a[i+1] * b[i+1];
        dst[i+2] = a[i+2] * b[i+2];
        dst[i+3] = a[i+3] * b[i+3];
    }
    for (; i < n; i++) {
        dst[i] = a[i] * b[i];
    }
}

inline double VectorSumFloat64(const double* a, size_t n) {
    double s0 = 0.0, s1 = 0.0, s2 = 0.0, s3 = 0.0;
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        s0 += a[i];
        s1 += a[i+1];
        s2 += a[i+2];
        s3 += a[i+3];
    }
    double sum = s0 + s1 + s2 + s3;
    for (; i < n; i++) {
        sum += a[i];
    }
    return sum;
}

inline int64_t VectorMinInt64(const int64_t* a, size_t n) {
    if (n == 0) return 0;
    int64_t min = a[0];
    for (size_t i = 1; i < n; i++) {
        if (a[i] < min) min = a[i];
    }
    return min;
}

inline int64_t VectorMaxInt64(const int64_t* a, size_t n) {
    if (n == 0) return 0;
    int64_t max = a[0];
    for (size_t i = 1; i < n; i++) {
        if (a[i] > max) max = a[i];
    }
    return max;
}

inline double VectorMinFloat64(const double* a, size_t n) {
    if (n == 0) return 0.0;
    double min = a[0];
    for (size_t i = 1; i < n; i++) {
        if (a[i] < min) min = a[i];
    }
    return min;
}

inline double VectorMaxFloat64(const double* a, size_t n) {
    if (n == 0) return 0.0;
    double max = a[0];
    for (size_t i = 1; i < n; i++) {
        if (a[i] > max) max = a[i];
    }
    return max;
}

} // namespace opt
} // namespace sf
} // namespace svdb

// C-compatible wrapper functions for CGO
extern "C" {

void svdb_sf_vector_add_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n);
void svdb_sf_vector_sub_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n);
void svdb_sf_vector_mul_int64(int64_t* dst, const int64_t* a, const int64_t* b, size_t n);
int64_t svdb_sf_vector_sum_int64(const int64_t* a, size_t n);

void svdb_sf_vector_add_float64(double* dst, const double* a, const double* b, size_t n);
void svdb_sf_vector_sub_float64(double* dst, const double* a, const double* b, size_t n);
void svdb_sf_vector_mul_float64(double* dst, const double* a, const double* b, size_t n);
double svdb_sf_vector_sum_float64(const double* a, size_t n);

int64_t svdb_sf_vector_min_int64(const int64_t* a, size_t n);
int64_t svdb_sf_vector_max_int64(const int64_t* a, size_t n);
double svdb_sf_vector_min_float64(const double* a, size_t n);
double svdb_sf_vector_max_float64(const double* a, size_t n);

} // extern "C"

#endif // SVDB_SF_OPT_H
