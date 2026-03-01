#ifndef SVDB_DS_SIMD_H
#define SVDB_DS_SIMD_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Vector operations (SIMD-accelerated when available)

// Vector addition: out[i] = a[i] + b[i]
void svdb_vector_add_double(const double* a, const double* b, double* out, size_t n);
void svdb_vector_add_int64(const int64_t* a, const int64_t* b, int64_t* out, size_t n);

// Vector subtraction: out[i] = a[i] - b[i]
void svdb_vector_sub_double(const double* a, const double* b, double* out, size_t n);
void svdb_vector_sub_int64(const int64_t* a, const int64_t* b, int64_t* out, size_t n);

// Vector multiplication: out[i] = a[i] * b[i]
void svdb_vector_mul_double(const double* a, const double* b, double* out, size_t n);

// Vector comparison: returns count of elements matching condition
size_t svdb_vector_eq_int64(const int64_t* a, const int64_t* b, size_t n);
size_t svdb_vector_gt_int64(const int64_t* a, const int64_t* b, size_t n);  // a > b
size_t svdb_vector_lt_int64(const int64_t* a, const int64_t* b, size_t n);  // a < b

// Vector aggregation
double svdb_vector_sum_double(const double* a, size_t n);
int64_t svdb_vector_sum_int64(const int64_t* a, size_t n);
double svdb_vector_avg_double(const double* a, size_t n);
int64_t svdb_vector_min_int64(const int64_t* a, size_t n);
int64_t svdb_vector_max_int64(const int64_t* a, size_t n);

// Vector fill
void svdb_vector_fill_double(double* a, double value, size_t n);
void svdb_vector_fill_int64(int64_t* a, int64_t value, size_t n);

// Bitmap operations (for filtering)
// AND: out[i] = a[i] & b[i]
void svdb_bitmap_and(uint64_t* a, const uint64_t* b, size_t n);

// OR: out[i] = a[i] | b[i]
void svdb_bitmap_or(uint64_t* a, const uint64_t* b, size_t n);

// XOR: out[i] = a[i] ^ b[i]
void svdb_bitmap_xor(uint64_t* a, const uint64_t* b, size_t n);

// NOT: a[i] = ~a[i]
void svdb_bitmap_not(uint64_t* a, size_t n);

// Population count (count set bits)
size_t svdb_bitmap_popcount(const uint64_t* a, size_t n);

// Find first set bit
int svdb_bitmap_find_first(const uint64_t* a, size_t n);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_SIMD_H
