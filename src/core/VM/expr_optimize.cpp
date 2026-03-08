#include "expr_optimize.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* NULL sentinel for fast comparisons */
const uint8_t SVDB_NULL_SENTINEL = 0;

/* ── CASE expression optimization ──────────────────────────────────────── */

/* Check if CASE can use optimized jump table */
int svdb_case_can_optimize(const int64_t* when_values, size_t count) {
    if (count == 0) return 0;

    /* All WHEN values must be constant (non-zero pointer) */
    return (when_values != NULL) ? 1 : 0;
}

/* Evaluate CASE using binary search (requires sorted when_values) */
int64_t svdb_case_evaluate_binary(
    const int64_t* when_values,
    const int64_t* results,
    int64_t case_input,
    size_t count,
    int64_t else_result
) {
    if (EXPR_LIKELY(count > 0 && when_values != NULL && results != NULL)) {
        /* Binary search for matching WHEN clause */
        size_t lo = 0, hi = count;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (when_values[mid] < case_input) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        /* Check if we found a match */
        if (lo < count && when_values[lo] == case_input) {
            return results[lo];
        }
    }

    /* No match - return ELSE result */
    return else_result;
}

/* ── Expression cache implementation ─────────────────────────────────── */

#define EXPR_CACHE_LOAD_FACTOR 0.75

static uint64_t svdb_hash_uint64(uint64_t x) {
    /* Simple hash: splitmix64 */
    x += 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

SvdbExprCache* svdb_expr_cache_create(size_t capacity) {
    if (capacity == 0) capacity = 64;

    SvdbExprCache* cache = (SvdbExprCache*)calloc(1, sizeof(SvdbExprCache));
    if (!cache) return NULL;

    cache->entries = (SvdbExprCacheEntry*)calloc(capacity, sizeof(SvdbExprCacheEntry));
    if (!cache->entries) {
        free(cache);
        return NULL;
    }

    cache->capacity = capacity;
    cache->size = 0;
    cache->hits = 0;
    cache->misses = 0;

    return cache;
}

void svdb_expr_cache_destroy(SvdbExprCache* cache) {
    if (!cache) return;
    if (cache->entries) free(cache->entries);
    free(cache);
}

int svdb_expr_cache_lookup(SvdbExprCache* cache, uint64_t key, int64_t* result, uint8_t* val_type) {
    if (!cache || !cache->entries) return 0;

    uint64_t hash = svdb_hash_uint64(key);
    size_t idx = hash & (cache->capacity - 1);

    /* Linear probe for matching entry */
    for (size_t i = 0; i < cache->capacity; i++) {
        size_t probe_idx = (idx + i) & (cache->capacity - 1);
        SvdbExprCacheEntry* entry = &cache->entries[probe_idx];

        if (!entry->is_valid) {
            cache->misses++;
            return 0;  /* Not found */
        }

        if (entry->key == key) {
            *result = entry->result;
            *val_type = entry->val_type;
            cache->hits++;
            return 1;  /* Found */
        }
    }

    cache->misses++;
    return 0;  /* Not found (full) */
}

void svdb_expr_cache_store(SvdbExprCache* cache, uint64_t key, int64_t result, uint8_t val_type) {
    if (!cache || !cache->entries) return;

    /* Check if we need to resize */
    if ((cache->size + 1) > (size_t)(cache->capacity * EXPR_CACHE_LOAD_FACTOR)) {
        /* Simple: just clear old cache on overflow */
        /* TODO: implement proper resize */
        if (cache->size >= cache->capacity) {
            svdb_expr_cache_clear(cache);
        }
    }

    uint64_t hash = svdb_hash_uint64(key);
    size_t idx = hash & (cache->capacity - 1);

    /* Linear probe for empty slot */
    for (size_t i = 0; i < cache->capacity; i++) {
        size_t probe_idx = (idx + i) & (cache->capacity - 1);
        SvdbExprCacheEntry* entry = &cache->entries[probe_idx];

        if (!entry->is_valid) {
            entry->key = key;
            entry->result = result;
            entry->val_type = val_type;
            entry->is_valid = 1;
            cache->size++;
            return;
        }

        /* Update existing entry */
        if (entry->key == key) {
            entry->result = result;
            entry->val_type = val_type;
            return;
        }
    }

    /* Cache full - evict first entry */
    cache->entries[idx].key = key;
    cache->entries[idx].result = result;
    cache->entries[idx].val_type = val_type;
    cache->entries[idx].is_valid = 1;
}

void svdb_expr_cache_clear(SvdbExprCache* cache) {
    if (!cache || !cache->entries) return;

    memset(cache->entries, 0, cache->capacity * sizeof(SvdbExprCacheEntry));
    cache->size = 0;
    cache->hits = 0;
    cache->misses = 0;
}

void svdb_expr_cache_stats(SvdbExprCache* cache, uint64_t* hits, uint64_t* misses, float* hit_rate) {
    if (!cache) {
        *hits = 0;
        *misses = 0;
        *hit_rate = 0.0f;
        return;
    }

    *hits = cache->hits;
    *misses = cache->misses;
    uint64_t total = cache->hits + cache->misses;
    *hit_rate = (total > 0) ? ((float)cache->hits / (float)total) : 0.0f;
}

/* ── Cache key generation ───────────────────────────────────────────────── */

uint64_t svdb_expr_cache_key_int64(int64_t a, int64_t b) {
    /* Simple combination of two int64 values */
    return ((uint64_t)a << 32) ^ (uint64_t)(b & 0xFFFFFFFF);
}

uint64_t svdb_expr_cache_key_double(double a, double b) {
    /* Convert doubles to int64 bits */
    int64_t ia, ib;
    memcpy(&ia, &a, sizeof(double));
    memcpy(&ib, &b, sizeof(double));
    return svdb_expr_cache_key_int64(ia, ib);
}

uint64_t svdb_expr_cache_key_mixed(uint8_t type, int64_t i1, int64_t i2, double f1, double f2) {
    uint64_t key = type;
    key = (key << 56) ^ ((uint64_t)i1 & 0x00FFFFFFFFFFFFFFULL);
    key = (key << 56) ^ ((uint64_t)i2 & 0x00FFFFFFFFFFFFFFULL);
    return key;
}

/* ── Coalesce/IFNULL optimization ───────────────────────────────────────── */

int svdb_coalesce_fast(const uint8_t* v1, const uint8_t* v2, uint8_t* result) {
    /* Fast path: check if first value is non-NULL */
    if (EXPR_LIKELY(v1 != NULL && v1[0] != SVDB_VAL_NULL)) {
        memcpy(result, v1, sizeof(svdb_value_t));
        return 1;
    }

    /* Second value is NULL or first value is NULL */
    if (v2 != NULL && v2[0] != SVDB_VAL_NULL) {
        memcpy(result, v2, sizeof(svdb_value_t));
        return 1;
    }

    /* Both are NULL */
    result[0] = SVDB_VAL_NULL;
    return 0;
}

/* svdb_value_t structure (must match internal definition) */
typedef struct {
    uint8_t  val_type;
    uint8_t  reserved[3];
    int64_t  int_val;
    double   float_val;
    void*    str_data;
    uint32_t str_len;
} svdb_value_t;

int svdb_coalesce_fast_value(const svdb_value_t* v1, const svdb_value_t* v2, svdb_value_t* result) {
    /* Fast path: check if first value is non-NULL */
    if (EXPR_LIKELY(v1 != NULL && v1->val_type != SVDB_VAL_NULL)) {
        *result = *v1;
        return 1;
    }

    /* Check second value */
    if (v2 != NULL && v2->val_type != SVDB_VAL_NULL) {
        *result = *v2;
        return 1;
    }

    /* Both are NULL */
    result->val_type = SVDB_VAL_NULL;
    return 0;
}