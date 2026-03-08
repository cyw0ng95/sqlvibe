#ifndef SVDB_VM_VECTOR_COMPARE_H
#define SVDB_VM_VECTOR_COMPARE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── SIMD comparison support ──────────────────────────────────────────── */

#if defined(__AVX2__)
#define SVDB_HAVE_AVX2 1
#include <immintrin.h>
#elif defined(__SSE4_1__)
#define SVDB_HAVE_SSE4 1
#include <smmintrin.h>
#else
#define SVDB_HAVE_SIMD 0
#endif

/* ── Vector comparison results ──────────────────────────────────────────── */

/* Comparison result masks */
#define CMP_LT -1
#define CMP_EQ  0
#define CMP_GT  1

/* ── Vectorized integer comparison (AVX2) ──────────────────────────────── */

#if SVDB_HAVE_AVX2

/* Compare 4 x int64 values, return mask of equal positions */
static inline __m256i svdb_vec_cmp_eq_i64(__m256i a, __m256i b) {
    return _mm256_cmpeq_epi64(a, b);
}

/* Compare 4 x int64 values, return mask of less-than positions */
static inline __m256i svdb_vec_cmp_lt_i64(__m256i a, __m256i b) {
    return _mm256_cmpgt_epi64(b, a);
}

/* Compare 4 x int64 values, return mask of greater-than positions */
static inline __m256i svdb_vec_cmp_gt_i64(__m256i a, __m256i b) {
    return _mm256_cmpgt_epi64(a, b);
}

/* Compare 4 x int64 values, return -1/0/1 for each element */
static inline void svdb_vec_cmp_i64(__m256i a, __m256i b, int32_t* results) {
    __m256i eq = _mm256_cmpeq_epi64(a, b);
    __m256i lt = _mm256_cmpgt_epi64(b, a);

    /* eq = 0xFFFFFFFF where equal, lt = 0xFFFFFFFF where a < b */
    /* Result: lt = -1, eq = 0, else 1 */
    __m256i sign = _mm256_set1_epi32(-1);
    __m256i r = _mm256_blendv_epi8(_mm256_set1_epi32(1), sign, lt);
    r = _mm256_blendv_epi8(r, _mm256_setzero_si256(), eq);
    _mm256_storeu_si256((__m256i*)results, r);
}

#endif

/* ── Vectorized double comparison (AVX2) ───────────────────────────────── */

#if SVDB_HAVE_AVX2

/* Compare 4 x double values, return mask of equal positions */
static inline __m256d svdb_vec_cmp_eq_f64(__m256d a, __m256d b) {
    return _mm256_cmp_pd(a, b, _CMP_EQ_OQ);
}

/* Compare 4 x double values, return mask of less-than positions */
static inline __m256d svdb_vec_cmp_lt_f64(__m256d a, __m256d b) {
    return _mm256_cmp_pd(a, b, _CMP_LT_OQ);
}

/* Compare 4 x double values, return mask of greater-than positions */
static inline __m256d svdb_vec_cmp_gt_f64(__m256d a, __m256d b) {
    return _mm256_cmp_pd(a, b, _CMP_GT_OQ);
}

#endif

/* ── Batch vectorized comparison entry points ─────────────────────────── */

#if SVDB_HAVE_AVX2

/* Vectorized batch int64 comparison with multiple operations */
void svdb_vec_batch_cmp_i64(
    const int64_t* a,
    const int64_t* b,
    int32_t*       results,
    size_t         count
);

/* Vectorized batch double comparison with multiple operations */
void svdb_vec_batch_cmp_f64(
    const double* a,
    const double* b,
    int32_t*      results,
    size_t        count
);

/* Vectorized equality check - returns 1 if all equal, 0 otherwise */
int svdb_vec_all_eq_i64(const int64_t* a, const int64_t* b, size_t count);
int svdb_vec_all_eq_f64(const double* a, const double* b, size_t count);

#else

/* Fallback scalar implementations */
static inline int svdb_vec_all_eq_i64(const int64_t* a, const int64_t* b, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (a[i] != b[i]) return 0;
    }
    return 1;
}

static inline int svdb_vec_all_eq_f64(const double* a, const double* b, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (a[i] != b[i]) return 0;
    }
    return 1;
}

#endif

/* ── IN子查询优化 ──────────────────────────────────────────────────────── */

/* IN 查询结果结构 */
typedef struct {
    int64_t* values;     /* 值数组 */
    size_t   count;      /* 值数量 */
    size_t   capacity;   /* 容量 */
    int      sorted;      /* 是否已排序 */
} SvdbInList;

/* 创建 IN 列表 */
SvdbInList* svdb_in_list_create(size_t capacity);

/* 销毁 IN 列表 */
void svdb_in_list_destroy(SvdbInList* list);

/* 添加值到 IN 列表 */
int svdb_in_list_add(SvdbInList* list, int64_t value);

/* 排序 IN 列表（二分查找需要） */
void svdb_in_list_sort(SvdbInList* list);

/* 二分查找：在排序列表中查找值，返回1 if found */
int svdb_in_list_contains_sorted(const SvdbInList* list, int64_t value);

/* 线性查找：检查值是否在列表中 */
int svdb_in_list_contains(const SvdbInList* list, int64_t value);

/* 批量 IN 查询：检查多个值是否在列表中 */
void svdb_in_list_batch(
    const SvdbInList* list,
    const int64_t*    check_values,
    uint8_t*          results,
    size_t            count
);

/* ── BETWEEN 优化 ──────────────────────────────────────────────────────── */

/* BETWEEN 查询结果 */
typedef struct {
    int64_t low;   /* 下界 */
    int64_t high;  /* 上界 */
    int inclusive; /* 是否包含边界 */
} SvdbBetween;

/* 检查值是否在 BETWEEN 范围内（优化版） */
static inline int svdb_between_contains_i64(int64_t value, int64_t low, int64_t high, int inclusive) {
    if (inclusive) {
        return (value >= low && value <= high) ? 1 : 0;
    }
    return (value > low && value < high) ? 1 : 0;
}

/* 检查值是否在 BETWEEN 范围内（浮点数） */
static inline int svdb_between_contains_f64(double value, double low, double high, int inclusive) {
    if (inclusive) {
        return (value >= low && value <= high) ? 1 : 0;
    }
    return (value > low && value < high) ? 1 : 0;
}

/* 批量 BETWEEN 查询：检查多个值是否在范围内 */
void svdb_between_batch_i64(
    const int64_t* values,
    int64_t        low,
    int64_t        high,
    int            inclusive,
    uint8_t*       results,
    size_t         count
);

void svdb_between_batch_f64(
    const double* values,
    double        low,
    double        high,
    int           inclusive,
    uint8_t*      results,
    size_t        count
);

/* ── LIKE/Glob 模式匹配优化 ───────────────────────────────────────────── */

#define LIKE_WILDCARD '%'
#define LIKE_SINGLE   '_'

/* 检查字符是否为通配符 */
static inline int svdb_like_is_wildcard(char c) {
    return (c == LIKE_WILDCARD || c == LIKE_SINGLE);
}

/* 优化版 LIKE：前置编译模式以加速匹配 */
typedef struct {
    char*   pattern;       /* 模式字符串 */
    size_t  pattern_len;   /* 模式长度 */
    int     has_wildcard;  /* 是否有通配符 */
    int     prefix_len;    /* 非通配符前缀长度 */
    char    prefix[64];    /* 前缀内容 */
} SvdbLikePattern;

/* 创建 LIKE 模式（预处理） */
SvdbLikePattern* svdb_like_compile(const char* pattern, size_t len);

/* 销毁 LIKE 模式 */
void svdb_like_destroy(SvdbLikePattern* pat);

/* 使用预处理模式匹配 */
int svdb_like_match(const SvdbLikePattern* pat, const char* text, size_t text_len);

/* 批量 LIKE 匹配 */
void svdb_like_batch(
    const SvdbLikePattern* pat,
    const char**           texts,
    const size_t*          text_lens,
    uint8_t*               results,
    size_t                count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_VECTOR_COMPARE_H */