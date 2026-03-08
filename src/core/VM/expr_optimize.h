#ifndef SVDB_VM_EXPR_OPTIMIZE_H
#define SVDB_VM_EXPR_OPTIMIZE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Value type constants (must match internal types) ───────────────────── */
#define SVDB_VAL_NULL   0
#define SVDB_VAL_INT    1
#define SVDB_VAL_FLOAT  2
#define SVDB_VAL_TEXT   3
#define SVDB_VAL_BLOB   4
#define SVDB_VAL_BOOL   5

/* ── Branch prediction hints ────────────────────────────────────────────── */
#if defined(__GNUC__) || defined(__clang__)
#define EXPR_LIKELY(x)   __builtin_expect(!!(x), 1)
#define EXPR_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define EXPR_LIKELY(x)   (x)
#define EXPR_UNLIKELY(x) (x)
#endif

/* ── NULL value sentinel (cached for fast comparisons) ────────────────── */
extern const uint8_t SVDB_NULL_SENTINEL;

/* ── Fast NULL check macros (inline for performance) ────────────────────── */
#define SVDB_IS_NULL(v)        ((v) == NULL || (v)->val_type == SVDB_VAL_NULL)
#define SVDB_IS_NOT_NULL(v)    ((v) != NULL && (v)->val_type != SVDB_VAL_NULL)

/* ── NULL check optimization functions ─────────────────────────────────── */

/* Fast path: check if value is NULL without full type dispatch */
static inline bool svdb_fast_is_null(uint8_t val_type) {
    return EXPR_UNLIKELY(val_type == SVDB_VAL_NULL);
}

/* Fast path: check if value is not NULL without full type dispatch */
static inline bool svdb_fast_is_not_null(uint8_t val_type) {
    return EXPR_LIKELY(val_type != SVDB_VAL_NULL);
}

/* Fast NULL propagation for binary operations */
/* Returns 1 if result should be NULL, 0 otherwise */
static inline int svdb_null_propagate_binary(uint8_t type1, uint8_t type2) {
    return EXPR_UNLIKELY(type1 == SVDB_VAL_NULL || type2 == SVDB_VAL_NULL);
}

/* Fast NULL propagation for unary operations */
static inline int svdb_null_propagate_unary(uint8_t type1) {
    return EXPR_UNLIKELY(type1 == SVDB_VAL_NULL);
}

/* ── CASE expression optimization ──────────────────────────────────────── */

/* Optimized CASE evaluation: check if we can use jump table */
typedef struct {
    int64_t when_val;     /* constant value for WHEN clause */
    int32_t jump_offset;  /* offset to jump to if match */
} SvdbCaseEntry;

/* Check if CASE can use optimized jump table (all WHEN values are constants) */
int svdb_case_can_optimize(const int64_t* when_values, size_t count);

/* Evaluate CASE using optimized search (binary search for sorted constants) */
int64_t svdb_case_evaluate_binary(
    const int64_t* when_values,
    const int64_t* results,
    int64_t case_input,
    size_t count,
    int64_t else_result
);

/* ── Expression result cache ────────────────────────────────────────────── */

/* Cache entry for expression results */
typedef struct SvdbExprCacheEntry {
    uint64_t key;           /* hash of input values */
    int64_t result;         /* cached result */
    uint8_t val_type;       /* result type */
    uint8_t is_valid;        /* 1 if entry is valid */
} SvdbExprCacheEntry;

/* Expression cache structure */
typedef struct SvdbExprCache {
    SvdbExprCacheEntry* entries;
    size_t capacity;        /* number of entries */
    size_t size;            /* current number of cached entries */
    uint64_t hits;          /* cache hits */
    uint64_t misses;        /* cache misses */
} SvdbExprCache;

/* Create expression cache with given capacity */
SvdbExprCache* svdb_expr_cache_create(size_t capacity);

/* Destroy expression cache */
void svdb_expr_cache_destroy(SvdbExprCache* cache);

/* Look up cached expression result */
int svdb_expr_cache_lookup(SvdbExprCache* cache, uint64_t key, int64_t* result, uint8_t* val_type);

/* Store expression result in cache */
void svdb_expr_cache_store(SvdbExprCache* cache, uint64_t key, int64_t result, uint8_t val_type);

/* Clear expression cache */
void svdb_expr_cache_clear(SvdbExprCache* cache);

/* Get cache statistics */
void svdb_expr_cache_stats(SvdbExprCache* cache, uint64_t* hits, uint64_t* misses, float* hit_rate);

/* Generate cache key from expression inputs */
uint64_t svdb_expr_cache_key_int64(int64_t a, int64_t b);
uint64_t svdb_expr_cache_key_double(double a, double b);
uint64_t svdb_expr_cache_key_mixed(uint8_t type, int64_t i1, int64_t i2, double f1, double f2);

/* ── Coalesce/IFNULL optimization ───────────────────────────────────────── */

/* Fast coalesce: return first non-NULL value */
/* Returns 1 if result is valid, 0 if both are NULL */
int svdb_coalesce_fast(const uint8_t* v1, const uint8_t* v2, uint8_t* result);

/* Fast IFNULL: return v2 if v1 is NULL, otherwise return v1 */
static inline int svdb_ifnull_fast(uint8_t type1, const uint8_t* v1, const uint8_t* v2, uint8_t* result) {
    if (EXPR_UNLIKELY(type1 == SVDB_VAL_NULL)) {
        *result = *v2;
        return 1;
    }
    *result = *v1;
    return 1;
}

/* ── Type coercion helpers ─────────────────────────────────────────────── */

static inline int svdb_type_is_numeric(uint8_t t) {
    return t == SVDB_VAL_INT || t == SVDB_VAL_FLOAT;
}

static inline int svdb_type_is_comparable(uint8_t t) {
    return t != SVDB_VAL_BLOB && t != SVDB_VAL_NULL;
}

/* Get type priority for comparison (NULL < INT < FLOAT < TEXT < BLOB) */
static inline int svdb_type_priority(uint8_t t) {
    switch (t) {
        case SVDB_VAL_NULL:  return 0;
        case SVDB_VAL_INT:   return 1;
        case SVDB_VAL_FLOAT: return 2;
        case SVDB_VAL_BOOL:  return 3;
        case SVDB_VAL_TEXT:  return 4;
        case SVDB_VAL_BLOB:  return 5;
        default:             return -1;
    }
}

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_EXPR_OPTIMIZE_H */