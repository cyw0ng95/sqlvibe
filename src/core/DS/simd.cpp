#include "simd.h"
#include <cstring>

#ifdef __AVX2__
#include <immintrin.h>
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

} // extern "C"
