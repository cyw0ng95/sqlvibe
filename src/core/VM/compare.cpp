#include "compare.h"
#include <cstring>

#ifdef __AVX2__
#include <immintrin.h>
#endif

extern "C" {

int svdb_compare(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len) {
    size_t min_len = (a_len < b_len) ? a_len : b_len;
    
#ifdef __AVX2__
    // SIMD comparison for longer strings
    size_t i = 0;
    for (; i + 32 <= min_len; i += 32) {
        __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
        
        // Compare unsigned bytes
        int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb));
        if (mask != 0xFFFFFFFF) {
            // Found difference - find position
            for (int j = 0; j < 32; j++) {
                if (a[i + j] != b[i + j]) {
                    return (a[i + j] < b[i + j]) ? -1 : 1;
                }
            }
        }
    }
    
    // Handle remainder
    for (; i < min_len; i++) {
        if (a[i] != b[i]) {
            return (a[i] < b[i]) ? -1 : 1;
        }
    }
#else
    // Scalar comparison
    for (size_t i = 0; i < min_len; i++) {
        if (a[i] != b[i]) {
            return (a[i] < b[i]) ? -1 : 1;
        }
    }
#endif
    
    // All compared bytes are equal - check lengths
    if (a_len == b_len) {
        return 0;
    }
    return (a_len < b_len) ? -1 : 1;
}

void svdb_compare_batch(
    const uint8_t** a_ptrs,
    const size_t* a_lens,
    const uint8_t** b_ptrs,
    const size_t* b_lens,
    int* results,
    size_t count
) {
    for (size_t i = 0; i < count; i++) {
        results[i] = svdb_compare(a_ptrs[i], a_lens[i], b_ptrs[i], b_lens[i]);
    }
}

void svdb_equal_batch(
    const uint8_t** a_ptrs,
    const size_t* a_lens,
    const uint8_t** b_ptrs,
    const size_t* b_lens,
    uint8_t* results,
    size_t count
) {
    for (size_t i = 0; i < count; i++) {
        // Quick length check first
        if (a_lens[i] != b_lens[i]) {
            results[i] = 0;
            continue;
        }
        
        // Compare content
        int cmp = svdb_compare(a_ptrs[i], a_lens[i], b_ptrs[i], b_lens[i]);
        results[i] = (cmp == 0) ? 1 : 0;
    }
}

size_t svdb_find_diff(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len) {
    size_t min_len = (a_len < b_len) ? a_len : b_len;
    
#ifdef __AVX2__
    size_t i = 0;
    for (; i + 32 <= min_len; i += 32) {
        __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
        
        int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb));
        if (mask != 0xFFFFFFFF) {
            // Find first different byte
            for (int j = 0; j < 32; j++) {
                if (a[i + j] != b[i + j]) {
                    return i + j;
                }
            }
        }
    }
    
    for (; i < min_len; i++) {
        if (a[i] != b[i]) {
            return i;
        }
    }
#else
    for (size_t i = 0; i < min_len; i++) {
        if (a[i] != b[i]) {
            return i;
        }
    }
#endif
    
    return min_len;
}

int svdb_has_prefix(const uint8_t* a, size_t a_len, const uint8_t* prefix, size_t prefix_len) {
    if (prefix_len > a_len) {
        return 0;
    }
    
#ifdef __AVX2__
    // Use SIMD for longer prefixes
    if (prefix_len >= 32) {
        size_t i = 0;
        for (; i + 32 <= prefix_len; i += 32) {
            __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
            __m256i vp = _mm256_loadu_si256((const __m256i*)(prefix + i));
            
            int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vp));
            if (mask != 0xFFFFFFFF) {
                return 0;
            }
        }
        
        // Check remainder
        for (; i < prefix_len; i++) {
            if (a[i] != prefix[i]) {
                return 0;
            }
        }
        return 1;
    }
#endif
    
    // Scalar comparison for short prefixes
    return (std::memcmp(a, prefix, prefix_len) == 0) ? 1 : 0;
}

} // extern "C"
