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
double svdb_vector_min_double(const double* a, size_t n);
double svdb_vector_max_double(const double* a, size_t n);

// Batch filter: write indices of elements where a[i] == val into out[]; returns count
size_t svdb_vector_filter_eq_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices);
size_t svdb_vector_filter_gt_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices);
size_t svdb_vector_filter_lt_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices);

// CRC32 hash (hardware-accelerated when SSE4.2 available)
uint32_t svdb_crc32_u64(uint32_t crc, uint64_t val);
uint32_t svdb_crc32_bytes(const void* data, size_t len, uint32_t seed);

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

/* ============================================================================
 * SIMD Column Scan Operations (for columnar query execution)
 * ============================================================================ */

/* Comparison operators for SIMD scan */
#define SVDB_CMP_EQ   0  /* == */
#define SVDB_CMP_NE   1  /* != */
#define SVDB_CMP_GT   2  /* >  */
#define SVDB_CMP_GE   3  /* >= */
#define SVDB_CMP_LT   4  /* <  */
#define SVDB_CMP_LE   5  /* <= */

/*
 * SIMD-optimized column scan for INT64 columns.
 * Scans column values and produces a bitmap of matching rows.
 *
 * values: array of int64 values (column data)
 * n: number of values
 * op: comparison operator (SVDB_CMP_*)
 * val: comparison value
 * result_bitmap: output bitmap (must be pre-allocated with (n+63)/64 uint64_t)
 *                Bit i is set if values[i] matches the condition.
 *
 * Returns number of matching rows.
 */
size_t svdb_simd_scan_int64(const int64_t* values, size_t n,
                             int op, int64_t val,
                             uint64_t* result_bitmap);

/*
 * SIMD-optimized column scan for DOUBLE columns.
 */
size_t svdb_simd_scan_double(const double* values, size_t n,
                              int op, double val,
                              uint64_t* result_bitmap);

/*
 * Combine two bitmaps with AND operation.
 * result[i] = a[i] & b[i]
 */
void svdb_simd_bitmap_combine_and(uint64_t* result,
                                   const uint64_t* a,
                                   const uint64_t* b,
                                   size_t n);

/*
 * Combine two bitmaps with OR operation.
 * result[i] = a[i] | b[i]
 */
void svdb_simd_bitmap_combine_or(uint64_t* result,
                                  const uint64_t* a,
                                  const uint64_t* b,
                                  size_t n);

/*
 * Extract row indices from a bitmap.
 * Returns the number of indices extracted.
 */
size_t svdb_simd_bitmap_to_indices(const uint64_t* bitmap, size_t n,
                                    size_t* out_indices);

/*
 * SIMD-optimized aggregation with bitmap filter.
 * Only rows where bitmap bit is set are included in aggregation.
 */

/* Sum of int64 values where bitmap bit is set */
int64_t svdb_simd_sum_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap);

/* Sum of double values where bitmap bit is set */
double svdb_simd_sum_double_filtered(const double* values, size_t n,
                                      const uint64_t* bitmap);

/* Min of int64 values where bitmap bit is set */
int64_t svdb_simd_min_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap);

/* Max of int64 values where bitmap bit is set */
int64_t svdb_simd_max_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap);

/* Count of set bits in bitmap (filtered row count) */
size_t svdb_simd_count_filtered(const uint64_t* bitmap, size_t n);

/* Prefetch memory for sequential access */
void svdb_simd_prefetch_read(const void* addr);
void svdb_simd_prefetch_write(void* addr);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_SIMD_H
