#include "simd.h"
#include <cstring>
#include <cfloat>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#ifdef __SSE4_1__
#include <smmintrin.h>
#endif

extern "C" {

// Vector addition - double
void svdb_vector_add_double(const double* a, const double* b, double* out, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_add_pd(va, vb);
        _mm256_storeu_pd(&out[i], vr);
    }
    for (; i < n; i++) {
        out[i] = a[i] + b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        out[i] = a[i] + b[i];
    }
#endif
}

// Vector addition - int64
void svdb_vector_add_int64(const int64_t* a, const int64_t* b, int64_t* out, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vr = _mm256_add_epi64(va, vb);
        _mm256_storeu_si256((__m256i*)&out[i], vr);
    }
    for (; i < n; i++) {
        out[i] = a[i] + b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        out[i] = a[i] + b[i];
    }
#endif
}

// Vector subtraction - double
void svdb_vector_sub_double(const double* a, const double* b, double* out, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_sub_pd(va, vb);
        _mm256_storeu_pd(&out[i], vr);
    }
    for (; i < n; i++) {
        out[i] = a[i] - b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        out[i] = a[i] - b[i];
    }
#endif
}

// Vector subtraction - int64
void svdb_vector_sub_int64(const int64_t* a, const int64_t* b, int64_t* out, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vr = _mm256_sub_epi64(va, vb);
        _mm256_storeu_si256((__m256i*)&out[i], vr);
    }
    for (; i < n; i++) {
        out[i] = a[i] - b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        out[i] = a[i] - b[i];
    }
#endif
}

// Vector multiplication - double
void svdb_vector_mul_double(const double* a, const double* b, double* out, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_mul_pd(va, vb);
        _mm256_storeu_pd(&out[i], vr);
    }
    for (; i < n; i++) {
        out[i] = a[i] * b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        out[i] = a[i] * b[i];
    }
#endif
}

// Vector comparison - equality
size_t svdb_vector_eq_int64(const int64_t* a, const int64_t* b, size_t n) {
    size_t count = 0;
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vcmp = _mm256_cmpeq_epi64(va, vb);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
        count += __builtin_popcount(mask);
    }
    for (; i < n; i++) {
        count += (a[i] == b[i] ? 1 : 0);
    }
#else
    for (size_t i = 0; i < n; i++) {
        count += (a[i] == b[i] ? 1 : 0);
    }
#endif
    return count;
}

// Vector comparison - greater than
size_t svdb_vector_gt_int64(const int64_t* a, const int64_t* b, size_t n) {
    size_t count = 0;
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vcmp = _mm256_cmpgt_epi64(va, vb);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
        count += __builtin_popcount(mask);
    }
    for (; i < n; i++) {
        count += (a[i] > b[i] ? 1 : 0);
    }
#else
    for (size_t i = 0; i < n; i++) {
        count += (a[i] > b[i] ? 1 : 0);
    }
#endif
    return count;
}

// Vector comparison - less than
size_t svdb_vector_lt_int64(const int64_t* a, const int64_t* b, size_t n) {
    return svdb_vector_gt_int64(b, a, n);
}

// Vector sum - double
double svdb_vector_sum_double(const double* a, size_t n) {
    double sum = 0.0;
#ifdef __AVX2__
    __m256d vsum = _mm256_setzero_pd();
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        vsum = _mm256_add_pd(vsum, va);
    }
    // Horizontal sum
    __m128d vlow = _mm256_castpd256_pd128(vsum);
    __m128d vhigh = _mm256_extractf128_pd(vsum, 1);
    vlow = _mm_add_pd(vlow, vhigh);
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    sum = _mm_cvtsd_f64(_mm_add_sd(vlow, high64));
    for (; i < n; i++) {
        sum += a[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        sum += a[i];
    }
#endif
    return sum;
}

// Vector sum - int64
int64_t svdb_vector_sum_int64(const int64_t* a, size_t n) {
    int64_t sum = 0;
#ifdef __AVX2__
    __m256i vsum = _mm256_setzero_si256();
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        vsum = _mm256_add_epi64(vsum, va);
    }
    // Horizontal sum
    alignas(32) int64_t tmp[4];
    _mm256_store_si256((__m256i*)tmp, vsum);
    sum = tmp[0] + tmp[1] + tmp[2] + tmp[3];
    for (; i < n; i++) {
        sum += a[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        sum += a[i];
    }
#endif
    return sum;
}

// Vector average - double
double svdb_vector_avg_double(const double* a, size_t n) {
    if (n == 0) return 0.0;
    return svdb_vector_sum_double(a, n) / static_cast<double>(n);
}

// Vector min - int64
int64_t svdb_vector_min_int64(const int64_t* a, size_t n) {
    if (n == 0) return 0;
    int64_t min_val = a[0];
    // Fallback to scalar for min/max due to AVX-512 requirement
    for (size_t i = 1; i < n; i++) {
        if (a[i] < min_val) min_val = a[i];
    }
    return min_val;
}

// Vector max - int64
int64_t svdb_vector_max_int64(const int64_t* a, size_t n) {
    if (n == 0) return 0;
    int64_t max_val = a[0];
    // Fallback to scalar for min/max due to AVX-512 requirement
    for (size_t i = 1; i < n; i++) {
        if (a[i] > max_val) max_val = a[i];
    }
    return max_val;
}

// Vector fill - double
void svdb_vector_fill_double(double* a, double value, size_t n) {
#ifdef __AVX2__
    __m256d vval = _mm256_set1_pd(value);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        _mm256_storeu_pd(&a[i], vval);
    }
    for (; i < n; i++) {
        a[i] = value;
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] = value;
    }
#endif
}

// Vector fill - int64
void svdb_vector_fill_int64(int64_t* a, int64_t value, size_t n) {
#ifdef __AVX2__
    __m256i vval = _mm256_set1_epi64x(value);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        _mm256_storeu_si256((__m256i*)&a[i], vval);
    }
    for (; i < n; i++) {
        a[i] = value;
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] = value;
    }
#endif
}

// Bitmap AND
void svdb_bitmap_and(uint64_t* a, const uint64_t* b, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vr = _mm256_and_si256(va, vb);
        _mm256_storeu_si256((__m256i*)&a[i], vr);
    }
    for (; i < n; i++) {
        a[i] &= b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] &= b[i];
    }
#endif
}

// Bitmap OR
void svdb_bitmap_or(uint64_t* a, const uint64_t* b, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vr = _mm256_or_si256(va, vb);
        _mm256_storeu_si256((__m256i*)&a[i], vr);
    }
    for (; i < n; i++) {
        a[i] |= b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] |= b[i];
    }
#endif
}

// Bitmap XOR
void svdb_bitmap_xor(uint64_t* a, const uint64_t* b, size_t n) {
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vb = _mm256_loadu_si256((const __m256i*)&b[i]);
        __m256i vr = _mm256_xor_si256(va, vb);
        _mm256_storeu_si256((__m256i*)&a[i], vr);
    }
    for (; i < n; i++) {
        a[i] ^= b[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] ^= b[i];
    }
#endif
}

// Bitmap NOT
void svdb_bitmap_not(uint64_t* a, size_t n) {
#ifdef __AVX2__
    __m256i vones = _mm256_set1_epi64x(-1LL);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vr = _mm256_xor_si256(va, vones);
        _mm256_storeu_si256((__m256i*)&a[i], vr);
    }
    for (; i < n; i++) {
        a[i] = ~a[i];
    }
#else
    for (size_t i = 0; i < n; i++) {
        a[i] = ~a[i];
    }
#endif
}

// Bitmap population count
size_t svdb_bitmap_popcount(const uint64_t* a, size_t n) {
    size_t count = 0;
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        // Use _mm256_popcnt if available, otherwise fallback
        for (int j = 0; j < 4; j++) {
            count += __builtin_popcountll(a[i + j]);
        }
    }
    for (; i < n; i++) {
        count += __builtin_popcountll(a[i]);
    }
#else
    for (size_t i = 0; i < n; i++) {
        count += __builtin_popcountll(a[i]);
    }
#endif
    return count;
}

// Bitmap find first set bit
int svdb_bitmap_find_first(const uint64_t* a, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (a[i] != 0) {
            return static_cast<int>(i * 64 + __builtin_ctzll(a[i]));
        }
    }
    return -1;
}

// Vector min - double
double svdb_vector_min_double(const double* a, size_t n) {
    if (n == 0) return 0.0;
    double min_val = a[0];
#ifdef __AVX2__
    __m256d vmin = _mm256_set1_pd(DBL_MAX);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        vmin = _mm256_min_pd(vmin, va);
    }
    alignas(32) double tmp[4];
    _mm256_store_pd(tmp, vmin);
    min_val = tmp[0];
    for (int j = 1; j < 4; j++) if (tmp[j] < min_val) min_val = tmp[j];
    for (; i < n; i++) if (a[i] < min_val) min_val = a[i];
#else
    for (size_t i = 1; i < n; i++) if (a[i] < min_val) min_val = a[i];
#endif
    return min_val;
}

// Vector max - double
double svdb_vector_max_double(const double* a, size_t n) {
    if (n == 0) return 0.0;
    double max_val = a[0];
#ifdef __AVX2__
    __m256d vmax = _mm256_set1_pd(-DBL_MAX);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        vmax = _mm256_max_pd(vmax, va);
    }
    alignas(32) double tmp[4];
    _mm256_store_pd(tmp, vmax);
    max_val = tmp[0];
    for (int j = 1; j < 4; j++) if (tmp[j] > max_val) max_val = tmp[j];
    for (; i < n; i++) if (a[i] > max_val) max_val = a[i];
#else
    for (size_t i = 1; i < n; i++) if (a[i] > max_val) max_val = a[i];
#endif
    return max_val;
}

// Batch filter: collect indices where a[i] == val
size_t svdb_vector_filter_eq_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices) {
    size_t count = 0;
#ifdef __AVX2__
    __m256i vval = _mm256_set1_epi64x(val);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vcmp = _mm256_cmpeq_epi64(va, vval);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
        if (mask & 1) out_indices[count++] = i;
        if (mask & 2) out_indices[count++] = i + 1;
        if (mask & 4) out_indices[count++] = i + 2;
        if (mask & 8) out_indices[count++] = i + 3;
    }
    for (; i < n; i++) if (a[i] == val) out_indices[count++] = i;
#else
    for (size_t i = 0; i < n; i++) if (a[i] == val) out_indices[count++] = i;
#endif
    return count;
}

// Batch filter: collect indices where a[i] > val
size_t svdb_vector_filter_gt_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices) {
    size_t count = 0;
#ifdef __AVX2__
    __m256i vval = _mm256_set1_epi64x(val);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        __m256i vcmp = _mm256_cmpgt_epi64(va, vval);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
        if (mask & 1) out_indices[count++] = i;
        if (mask & 2) out_indices[count++] = i + 1;
        if (mask & 4) out_indices[count++] = i + 2;
        if (mask & 8) out_indices[count++] = i + 3;
    }
    for (; i < n; i++) if (a[i] > val) out_indices[count++] = i;
#else
    for (size_t i = 0; i < n; i++) if (a[i] > val) out_indices[count++] = i;
#endif
    return count;
}

// Batch filter: collect indices where a[i] < val
size_t svdb_vector_filter_lt_int64(const int64_t* a, int64_t val, size_t n,
                                    size_t* out_indices) {
    size_t count = 0;
#ifdef __AVX2__
    __m256i vval = _mm256_set1_epi64x(val);
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&a[i]);
        /* a[i] < val ↔ val > a[i] */
        __m256i vcmp = _mm256_cmpgt_epi64(vval, va);
        int mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
        if (mask & 1) out_indices[count++] = i;
        if (mask & 2) out_indices[count++] = i + 1;
        if (mask & 4) out_indices[count++] = i + 2;
        if (mask & 8) out_indices[count++] = i + 3;
    }
    for (; i < n; i++) if (a[i] < val) out_indices[count++] = i;
#else
    for (size_t i = 0; i < n; i++) if (a[i] < val) out_indices[count++] = i;
#endif
    return count;
}

// CRC32 hardware-accelerated hash
uint32_t svdb_crc32_u64(uint32_t crc, uint64_t val) {
#ifdef __SSE4_2__
    return (uint32_t)_mm_crc32_u64((uint64_t)crc, val);
#else
    /* Software CRC32 fallback */
    crc ^= (uint32_t)(val & 0xFFFFFFFF);
    for (int i = 0; i < 32; i++) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
    crc ^= (uint32_t)(val >> 32);
    for (int i = 0; i < 32; i++) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
    return crc;
#endif
}

uint32_t svdb_crc32_bytes(const void* data, size_t len, uint32_t seed) {
    const uint8_t* p = (const uint8_t*)data;
    uint32_t crc = seed;
#ifdef __SSE4_2__
    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        uint64_t v;
        memcpy(&v, p + i, 8);
        crc = (uint32_t)_mm_crc32_u64((uint64_t)crc, v);
    }
    for (; i < len; i++) crc = (uint32_t)_mm_crc32_u8(crc, p[i]);
#else
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int j = 0; j < 8; j++) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
    }
#endif
    return crc;
}

/* ============================================================================
 * SIMD Column Scan Operations
 * ============================================================================ */

size_t svdb_simd_scan_int64(const int64_t* values, size_t n,
                             int op, int64_t val,
                             uint64_t* result_bitmap) {
    if (!values || !result_bitmap || n == 0) return 0;

    /* Clear the bitmap */
    size_t bitmap_size = (n + 63) / 64;
    memset(result_bitmap, 0, bitmap_size * sizeof(uint64_t));

    size_t match_count = 0;

#ifdef __AVX2__
    __m256i vval = _mm256_set1_epi64x(val);

    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)&values[i]);
        __m256i vcmp;
        int mask;

        switch (op) {
            case SVDB_CMP_EQ:
                vcmp = _mm256_cmpeq_epi64(va, vval);
                mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
                break;
            case SVDB_CMP_NE:
                vcmp = _mm256_cmpeq_epi64(va, vval);
                mask = ~_mm256_movemask_pd(_mm256_castsi256_pd(vcmp)) & 0xF;
                break;
            case SVDB_CMP_GT:
                vcmp = _mm256_cmpgt_epi64(va, vval);
                mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
                break;
            case SVDB_CMP_GE:
                /* a >= b iff !(a < b) iff !(b > a) */
                vcmp = _mm256_cmpgt_epi64(vval, va);
                mask = ~_mm256_movemask_pd(_mm256_castsi256_pd(vcmp)) & 0xF;
                break;
            case SVDB_CMP_LT:
                /* a < b iff b > a */
                vcmp = _mm256_cmpgt_epi64(vval, va);
                mask = _mm256_movemask_pd(_mm256_castsi256_pd(vcmp));
                break;
            case SVDB_CMP_LE:
                /* a <= b iff !(a > b) */
                vcmp = _mm256_cmpgt_epi64(va, vval);
                mask = ~_mm256_movemask_pd(_mm256_castsi256_pd(vcmp)) & 0xF;
                break;
            default:
                mask = 0;
        }

        /* Set bitmap bits */
        size_t word_idx = i / 64;
        size_t bit_offset = i % 64;

        if (bit_offset + 4 <= 64) {
            result_bitmap[word_idx] |= ((uint64_t)mask) << bit_offset;
        } else {
            /* Handle crossing word boundary */
            result_bitmap[word_idx] |= ((uint64_t)mask) << bit_offset;
            result_bitmap[word_idx + 1] |= ((uint64_t)mask) >> (64 - bit_offset);
        }

        match_count += __builtin_popcount(mask);
    }

    /* Handle remaining elements */
    for (; i < n; i++) {
        bool match = false;
        switch (op) {
            case SVDB_CMP_EQ: match = (values[i] == val); break;
            case SVDB_CMP_NE: match = (values[i] != val); break;
            case SVDB_CMP_GT: match = (values[i] > val); break;
            case SVDB_CMP_GE: match = (values[i] >= val); break;
            case SVDB_CMP_LT: match = (values[i] < val); break;
            case SVDB_CMP_LE: match = (values[i] <= val); break;
        }
        if (match) {
            result_bitmap[i / 64] |= (1ULL << (i % 64));
            match_count++;
        }
    }
#else
    for (size_t i = 0; i < n; i++) {
        bool match = false;
        switch (op) {
            case SVDB_CMP_EQ: match = (values[i] == val); break;
            case SVDB_CMP_NE: match = (values[i] != val); break;
            case SVDB_CMP_GT: match = (values[i] > val); break;
            case SVDB_CMP_GE: match = (values[i] >= val); break;
            case SVDB_CMP_LT: match = (values[i] < val); break;
            case SVDB_CMP_LE: match = (values[i] <= val); break;
        }
        if (match) {
            result_bitmap[i / 64] |= (1ULL << (i % 64));
            match_count++;
        }
    }
#endif

    return match_count;
}

size_t svdb_simd_scan_double(const double* values, size_t n,
                              int op, double val,
                              uint64_t* result_bitmap) {
    if (!values || !result_bitmap || n == 0) return 0;

    size_t bitmap_size = (n + 63) / 64;
    memset(result_bitmap, 0, bitmap_size * sizeof(uint64_t));

    size_t match_count = 0;

#ifdef __AVX2__
    __m256d vval = _mm256_set1_pd(val);

    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256d va = _mm256_loadu_pd(&values[i]);
        __m256d vcmp;
        int mask;

        switch (op) {
            case SVDB_CMP_EQ:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_EQ_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            case SVDB_CMP_NE:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_NEQ_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            case SVDB_CMP_GT:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_GT_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            case SVDB_CMP_GE:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_GE_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            case SVDB_CMP_LT:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_LT_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            case SVDB_CMP_LE:
                vcmp = _mm256_cmp_pd(va, vval, _CMP_LE_OQ);
                mask = _mm256_movemask_pd(vcmp);
                break;
            default:
                mask = 0;
        }

        size_t word_idx = i / 64;
        size_t bit_offset = i % 64;

        if (bit_offset + 4 <= 64) {
            result_bitmap[word_idx] |= ((uint64_t)mask) << bit_offset;
        } else {
            result_bitmap[word_idx] |= ((uint64_t)mask) << bit_offset;
            result_bitmap[word_idx + 1] |= ((uint64_t)mask) >> (64 - bit_offset);
        }

        match_count += __builtin_popcount(mask);
    }

    for (; i < n; i++) {
        bool match = false;
        switch (op) {
            case SVDB_CMP_EQ: match = (values[i] == val); break;
            case SVDB_CMP_NE: match = (values[i] != val); break;
            case SVDB_CMP_GT: match = (values[i] > val); break;
            case SVDB_CMP_GE: match = (values[i] >= val); break;
            case SVDB_CMP_LT: match = (values[i] < val); break;
            case SVDB_CMP_LE: match = (values[i] <= val); break;
        }
        if (match) {
            result_bitmap[i / 64] |= (1ULL << (i % 64));
            match_count++;
        }
    }
#else
    for (size_t i = 0; i < n; i++) {
        bool match = false;
        switch (op) {
            case SVDB_CMP_EQ: match = (values[i] == val); break;
            case SVDB_CMP_NE: match = (values[i] != val); break;
            case SVDB_CMP_GT: match = (values[i] > val); break;
            case SVDB_CMP_GE: match = (values[i] >= val); break;
            case SVDB_CMP_LT: match = (values[i] < val); break;
            case SVDB_CMP_LE: match = (values[i] <= val); break;
        }
        if (match) {
            result_bitmap[i / 64] |= (1ULL << (i % 64));
            match_count++;
        }
    }
#endif

    return match_count;
}

void svdb_simd_bitmap_combine_and(uint64_t* result,
                                   const uint64_t* a,
                                   const uint64_t* b,
                                   size_t n) {
    svdb_bitmap_and(result, b, n); /* Use existing AND, then AND with a */
    svdb_bitmap_and(result, a, n);
}

void svdb_simd_bitmap_combine_or(uint64_t* result,
                                  const uint64_t* a,
                                  const uint64_t* b,
                                  size_t n) {
    memcpy(result, a, n * sizeof(uint64_t));
    svdb_bitmap_or(result, b, n);
}

size_t svdb_simd_bitmap_to_indices(const uint64_t* bitmap, size_t n,
                                    size_t* out_indices) {
    size_t count = 0;
    for (size_t i = 0; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            out_indices[count++] = i;
        }
    }
    return count;
}

int64_t svdb_simd_sum_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap) {
    if (!values || !bitmap || n == 0) return 0;

    int64_t sum = 0;

#ifdef __AVX2__
    __m256i vsum = _mm256_setzero_si256();
    size_t i = 0;

    for (; i + 4 <= n; i += 4) {
        size_t word_idx = i / 64;
        size_t bit_offset = i % 64;

        uint64_t mask_bits;
        if (bit_offset + 4 <= 64) {
            mask_bits = (bitmap[word_idx] >> bit_offset) & 0xF;
        } else {
            mask_bits = ((bitmap[word_idx] >> bit_offset) |
                        (bitmap[word_idx + 1] << (64 - bit_offset))) & 0xF;
        }

        if (mask_bits == 0) continue;  /* No matches in this batch */
        if (mask_bits == 0xF) {
            /* All match - full SIMD */
            __m256i va = _mm256_loadu_si256((const __m256i*)&values[i]);
            vsum = _mm256_add_epi64(vsum, va);
        } else {
            /* Partial match - scalar fallback */
            for (int j = 0; j < 4; j++) {
                if (mask_bits & (1 << j)) {
                    sum += values[i + j];
                }
            }
        }
    }

    /* Horizontal sum of vsum */
    alignas(32) int64_t tmp[4];
    _mm256_store_si256((__m256i*)tmp, vsum);
    sum += tmp[0] + tmp[1] + tmp[2] + tmp[3];

    /* Handle remaining */
    for (; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            sum += values[i];
        }
    }
#else
    for (size_t i = 0; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            sum += values[i];
        }
    }
#endif

    return sum;
}

double svdb_simd_sum_double_filtered(const double* values, size_t n,
                                      const uint64_t* bitmap) {
    if (!values || !bitmap || n == 0) return 0.0;

    double sum = 0.0;

#ifdef __AVX2__
    __m256d vsum = _mm256_setzero_pd();
    size_t i = 0;

    for (; i + 4 <= n; i += 4) {
        size_t word_idx = i / 64;
        size_t bit_offset = i % 64;

        uint64_t mask_bits;
        if (bit_offset + 4 <= 64) {
            mask_bits = (bitmap[word_idx] >> bit_offset) & 0xF;
        } else {
            mask_bits = ((bitmap[word_idx] >> bit_offset) |
                        (bitmap[word_idx + 1] << (64 - bit_offset))) & 0xF;
        }

        if (mask_bits == 0) continue;
        if (mask_bits == 0xF) {
            __m256d va = _mm256_loadu_pd(&values[i]);
            vsum = _mm256_add_pd(vsum, va);
        } else {
            for (int j = 0; j < 4; j++) {
                if (mask_bits & (1 << j)) {
                    sum += values[i + j];
                }
            }
        }
    }

    /* Horizontal sum */
    __m128d vlow = _mm256_castpd256_pd128(vsum);
    __m128d vhigh = _mm256_extractf128_pd(vsum, 1);
    vlow = _mm_add_pd(vlow, vhigh);
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    sum += _mm_cvtsd_f64(_mm_add_sd(vlow, high64));

    for (; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            sum += values[i];
        }
    }
#else
    for (size_t i = 0; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            sum += values[i];
        }
    }
#endif

    return sum;
}

int64_t svdb_simd_min_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap) {
    if (!values || !bitmap || n == 0) return 0;

    int64_t min_val = INT64_MAX;
    bool found = false;

    for (size_t i = 0; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            if (!found || values[i] < min_val) {
                min_val = values[i];
                found = true;
            }
        }
    }

    return found ? min_val : 0;
}

int64_t svdb_simd_max_int64_filtered(const int64_t* values, size_t n,
                                      const uint64_t* bitmap) {
    if (!values || !bitmap || n == 0) return 0;

    int64_t max_val = INT64_MIN;
    bool found = false;

    for (size_t i = 0; i < n; i++) {
        if (bitmap[i / 64] & (1ULL << (i % 64))) {
            if (!found || values[i] > max_val) {
                max_val = values[i];
                found = true;
            }
        }
    }

    return found ? max_val : 0;
}

size_t svdb_simd_count_filtered(const uint64_t* bitmap, size_t n) {
    if (!bitmap || n == 0) return 0;
    return svdb_bitmap_popcount(bitmap, (n + 63) / 64);
}

void svdb_simd_prefetch_read(const void* addr) {
#ifdef __SSE__
    _mm_prefetch(static_cast<const char*>(addr), _MM_HINT_T0);
#endif
}

void svdb_simd_prefetch_write(void* addr) {
#ifdef __SSE__
    _mm_prefetch(static_cast<const char*>(addr), _MM_HINT_T0);
#endif
}

} // extern "C"
