/*
 * invoke_chain_wrapper.cpp
 *
 * Phase 4 invoke chain wrappers. Each function sequences multiple C-level
 * subsystem calls so the caller needs only a single CGO boundary crossing.
 *
 * Principle: thin wrappers only — no business logic, just call sequencing.
 */

#include "invoke_chain_wrapper.h"

#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <limits>

/* ── local helpers ──────────────────────────────────────────────── */

/* xxHash64 inline (no dependency on hash.cpp for portability) */
static inline uint64_t rotl64(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}
static uint64_t xxhash64_inline(const void* data, size_t len, uint64_t seed) {
    static const uint64_t P1 = 0x9E3779B185EBCA87ULL;
    static const uint64_t P2 = 0xC2B2AE3D27D4EB4FULL;
    static const uint64_t P3 = 0x165667B19E3779F9ULL;
    static const uint64_t P4 = 0x85EBCA77C2B2AE63ULL;
    static const uint64_t P5 = 0x27D4EB2F165667C5ULL;

    const uint8_t* p   = static_cast<const uint8_t*>(data);
    const uint8_t* end = p + len;
    uint64_t h;

    if (len >= 32) {
        uint64_t v1 = seed + P1 + P2;
        uint64_t v2 = seed + P2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - P1;
        while (p <= end - 32) {
            uint64_t k; memcpy(&k, p, 8); v1 = rotl64(v1 + k * P2, 31) * P1; p += 8;
            memcpy(&k, p, 8); v2 = rotl64(v2 + k * P2, 31) * P1; p += 8;
            memcpy(&k, p, 8); v3 = rotl64(v3 + k * P2, 31) * P1; p += 8;
            memcpy(&k, p, 8); v4 = rotl64(v4 + k * P2, 31) * P1; p += 8;
        }
        h = rotl64(v1,  1) + rotl64(v2,  7) + rotl64(v3, 12) + rotl64(v4, 18);
        h = (h ^ (rotl64(v1 * P2, 31) * P1)) * P1 + P4;
        h = (h ^ (rotl64(v2 * P2, 31) * P1)) * P1 + P4;
        h = (h ^ (rotl64(v3 * P2, 31) * P1)) * P1 + P4;
        h = (h ^ (rotl64(v4 * P2, 31) * P1)) * P1 + P4;
    } else {
        h = seed + P5;
    }
    h += static_cast<uint64_t>(len);
    while (p <= end - 8) {
        uint64_t k; memcpy(&k, p, 8);
        h ^= rotl64(k * P2, 31) * P1;
        h = rotl64(h, 27) * P1 + P4; p += 8;
    }
    if (p <= end - 4) {
        uint32_t k; memcpy(&k, p, 4);
        h ^= static_cast<uint64_t>(k) * P1;
        h = rotl64(h, 23) * P2 + P3; p += 4;
    }
    while (p < end) {
        h ^= static_cast<uint64_t>(*p) * P5;
        h = rotl64(h, 11) * P1; ++p;
    }
    h ^= h >> 33; h *= P2; h ^= h >> 29; h *= P3; h ^= h >> 32;
    return h;
}

/* Apply integer comparison operator; returns 1 if predicate is true. */
static inline int cmp_int64(int64_t a, int64_t b, int op) {
    switch (op) {
        case 0: return a == b;
        case 1: return a != b;
        case 2: return a <  b;
        case 3: return a <= b;
        case 4: return a >  b;
        case 5: return a >= b;
        default: return 0;
    }
}

static inline int cmp_f64(double a, double b, int op) {
    switch (op) {
        case 0: return a == b;
        case 1: return a != b;
        case 2: return a <  b;
        case 3: return a <= b;
        case 4: return a >  b;
        case 5: return a >= b;
        default: return 0;
    }
}

/* ── extern-C implementations ───────────────────────────────────── */

extern "C" {

/* Phase 4.2 ─────────────────────────────────────────────────────── */

size_t svdb_pipeline_hash_filter(
    const uint8_t** keys,
    const size_t*   key_lens,
    size_t          count,
    uint64_t        seed,
    uint64_t        bucket_count,
    uint64_t        target_bucket,
    size_t*         out_indices
) {
    if (!keys || !key_lens || !out_indices || count == 0 || bucket_count == 0) {
        return 0;
    }
    size_t out_count = 0;
    for (size_t i = 0; i < count; ++i) {
        uint64_t h = xxhash64_inline(keys[i], key_lens[i], seed);
        if (h % bucket_count == target_bucket) {
            out_indices[out_count++] = i;
        }
    }
    return out_count;
}

/* Phase 4.3 ─────────────────────────────────────────────────────── */

size_t svdb_batch_eval_compare_int64(
    const int64_t* a,
    const int64_t* b,
    size_t         count,
    int            op,
    uint8_t*       out_mask
) {
    if (!a || !b || !out_mask || count == 0) return 0;
    size_t pass = 0;
    for (size_t i = 0; i < count; ++i) {
        uint8_t v = static_cast<uint8_t>(cmp_int64(a[i], b[i], op));
        out_mask[i] = v;
        pass += v;
    }
    return pass;
}

size_t svdb_batch_eval_compare_float64(
    const double* a,
    const double* b,
    size_t        count,
    int           op,
    uint8_t*      out_mask
) {
    if (!a || !b || !out_mask || count == 0) return 0;
    size_t pass = 0;
    for (size_t i = 0; i < count; ++i) {
        uint8_t v = static_cast<uint8_t>(cmp_f64(a[i], b[i], op));
        out_mask[i] = v;
        pass += v;
    }
    return pass;
}

size_t svdb_batch_arith_and_compare_int64(
    const int64_t* a,
    const int64_t* b,
    size_t         count,
    int            arith_op,
    int64_t        threshold,
    int            cmp_op,
    uint8_t*       out_mask
) {
    if (!a || !b || !out_mask || count == 0) return 0;
    size_t pass = 0;
    for (size_t i = 0; i < count; ++i) {
        int64_t tmp;
        switch (arith_op) {
            case 1:  tmp = a[i] - b[i]; break;
            case 2:  tmp = a[i] * b[i]; break;
            default: tmp = a[i] + b[i]; break;  /* 0 = add */
        }
        uint8_t v = static_cast<uint8_t>(cmp_int64(tmp, threshold, cmp_op));
        out_mask[i] = v;
        pass += v;
    }
    return pass;
}

/* Phase 4.4 ─────────────────────────────────────────────────────── */

size_t svdb_scan_filter_int64(
    const int64_t* column,
    size_t         row_count,
    int            op,
    int64_t        threshold,
    size_t*        out_indices
) {
    if (!column || !out_indices || row_count == 0) return 0;
    size_t out_count = 0;
    for (size_t i = 0; i < row_count; ++i) {
        if (cmp_int64(column[i], threshold, op)) {
            out_indices[out_count++] = i;
        }
    }
    return out_count;
}

size_t svdb_scan_filter_float64(
    const double* column,
    size_t        row_count,
    int           op,
    double        threshold,
    size_t*       out_indices
) {
    if (!column || !out_indices || row_count == 0) return 0;
    size_t out_count = 0;
    for (size_t i = 0; i < row_count; ++i) {
        if (cmp_f64(column[i], threshold, op)) {
            out_indices[out_count++] = i;
        }
    }
    return out_count;
}

size_t svdb_scan_aggregate_int64(
    const int64_t* column,
    size_t         row_count,
    int            filter_op,
    int64_t        filter_threshold,
    int            agg_op,
    int64_t*       out_agg
) {
    if (!column || !out_agg || row_count == 0) return 0;

    int64_t sum   = 0;
    int64_t mn    = std::numeric_limits<int64_t>::max();
    int64_t mx    = std::numeric_limits<int64_t>::min();
    size_t  count = 0;

    /* filter_op < 0 means no filter */
    for (size_t i = 0; i < row_count; ++i) {
        if (filter_op >= 0 && !cmp_int64(column[i], filter_threshold, filter_op)) {
            continue;
        }
        int64_t v = column[i];
        sum += v;
        if (v < mn) mn = v;
        if (v > mx) mx = v;
        ++count;
    }

    switch (agg_op) {
        case 1:  *out_agg = (count > 0) ? mn : 0; break;
        case 2:  *out_agg = (count > 0) ? mx : 0; break;
        case 3:  *out_agg = static_cast<int64_t>(count); break;
        default: *out_agg = sum; break;  /* 0 = SUM */
    }
    return count;
}

} /* extern "C" */
