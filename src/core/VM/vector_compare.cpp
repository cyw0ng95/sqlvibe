#include "vector_compare.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <ctype.h>

/* ── SIMD comparison implementations ──────────────────────────────────── */

#if SVDB_HAVE_AVX2

void svdb_vec_batch_cmp_i64(
    const int64_t* a,
    const int64_t* b,
    int32_t*       results,
    size_t         count
) {
    size_t i = 0;

    /* Process 4 elements at a time with AVX2 */
    for (; i + 4 <= count; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));

        __m256i eq = _mm256_cmpeq_epi64(va, vb);
        __m256i lt = _mm256_cmpgt_epi64(vb, va);

        /* Build result: -1 if lt, 1 if gt, 0 if eq */
        __m256i minus1 = _mm256_set1_epi32(-1);
        __m256i one = _mm256_set1_epi32(1);
        __m256i zero = _mm256_setzero_si256();

        /* r = blend(1, -1, lt) -> 1 if not lt, -1 if lt */
        __m256i r = _mm256_blendv_epi8(one, minus1, lt);
        /* r = blend(r, 0, eq) -> r if not eq, 0 if eq */
        r = _mm256_blendv_epi8(r, zero, eq);

        _mm256_storeu_si256((__m256i*)(results + i), r);
    }

    /* Handle remaining elements */
    for (; i < count; i++) {
        results[i] = (a[i] < b[i]) ? -1 : (a[i] > b[i]) ? 1 : 0;
    }
}

void svdb_vec_batch_cmp_f64(
    const double* a,
    const double* b,
    int32_t*      results,
    size_t        count
) {
    size_t i = 0;

    /* Process 4 elements at a time with AVX2 */
    for (; i + 4 <= count; i += 4) {
        __m256d va = _mm256_loadu_pd(a + i);
        __m256d vb = _mm256_loadu_pd(b + i);

        __m256d eq = _mm256_cmp_pd(va, vb, _CMP_EQ_OQ);
        __m256d lt = _mm256_cmp_pd(va, vb, _CMP_LT_OQ);

        /* Convert masks to int results */
        int eq_mask = _mm256_movemask_pd(eq);
        int lt_mask = _mm256_movemask_pd(lt);
        int gt_mask = (~lt_mask) & (~eq_mask) & 0xF;

        /* Build result array */
        for (int j = 0; j < 4; j++) {
            if (eq_mask & (1 << j)) {
                results[i + j] = 0;
            } else if (lt_mask & (1 << j)) {
                results[i + j] = -1;
            } else {
                results[i + j] = 1;
            }
        }
    }

    /* Handle remaining elements */
    for (; i < count; i++) {
        results[i] = (a[i] < b[i]) ? -1 : (a[i] > b[i]) ? 1 : 0;
    }
}

int svdb_vec_all_eq_i64(const int64_t* a, const int64_t* b, size_t count) {
    size_t i = 0;

    /* Process 4 elements at a time with AVX2 */
    for (; i + 4 <= count; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));

        __m256i eq = _mm256_cmpeq_epi64(va, vb);
        int mask = _mm256_movemask_epi8(eq);

        if (mask != 0xFFFFFFFF) {
            return 0;  /* Not all equal */
        }
    }

    /* Handle remaining elements */
    for (; i < count; i++) {
        if (a[i] != b[i]) return 0;
    }

    return 1;  /* All equal */
}

int svdb_vec_all_eq_f64(const double* a, const double* b, size_t count) {
    size_t i = 0;

    /* Process 4 elements at a time with AVX2 */
    for (; i + 4 <= count; i += 4) {
        __m256d va = _mm256_loadu_pd(a + i);
        __m256d vb = _mm256_loadu_pd(b + i);

        __m256d eq = _mm256_cmp_pd(va, vb, _CMP_EQ_OQ);
        int mask = _mm256_movemask_pd(eq);

        if (mask != 0xF) {
            return 0;  /* Not all equal */
        }
    }

    /* Handle remaining elements */
    for (; i < count; i++) {
        if (a[i] != b[i]) return 0;
    }

    return 1;  /* All equal */
}

#endif /* SVDB_HAVE_AVX2 */

/* ── IN list implementation ───────────────────────────────────────────── */

SvdbInList* svdb_in_list_create(size_t capacity) {
    if (capacity == 0) capacity = 16;

    SvdbInList* list = (SvdbInList*)calloc(1, sizeof(SvdbInList));
    if (!list) return NULL;

    list->values = (int64_t*)malloc(capacity * sizeof(int64_t));
    if (!list->values) {
        free(list);
        return NULL;
    }

    list->capacity = capacity;
    list->count = 0;
    list->sorted = 1;  /* Empty list is considered sorted */

    return list;
}

void svdb_in_list_destroy(SvdbInList* list) {
    if (!list) return;
    if (list->values) free(list->values);
    free(list);
}

int svdb_in_list_add(SvdbInList* list, int64_t value) {
    if (!list) return 0;

    /* Resize if needed */
    if (list->count >= list->capacity) {
        size_t new_cap = list->capacity * 2;
        int64_t* new_values = (int64_t*)realloc(list->values, new_cap * sizeof(int64_t));
        if (!new_values) return 0;
        list->values = new_values;
        list->capacity = new_cap;
    }

    list->values[list->count++] = value;
    list->sorted = 0;  /* Need to re-sort after adding */

    return 1;
}

/* Simple quicksort for int64 values */
static void svdb_quicksort_i64(int64_t* arr, size_t left, size_t right) {
    if (left >= right) return;

    /* Choose pivot (middle element) */
    size_t mid = left + (right - left) / 2;
    int64_t pivot = arr[mid];

    size_t i = left, j = right;
    while (i <= j) {
        while (arr[i] < pivot) i++;
        while (arr[j] > pivot) j--;
        if (i <= j) {
            int64_t tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
            i++;
            j--;
        }
    }

    if (left < j) svdb_quicksort_i64(arr, left, j);
    if (i < right) svdb_quicksort_i64(arr, i, right);
}

void svdb_in_list_sort(SvdbInList* list) {
    if (!list || list->sorted) return;

    svdb_quicksort_i64(list->values, 0, list->count - 1);
    list->sorted = 1;
}

/* Binary search in sorted list */
int svdb_in_list_contains_sorted(const SvdbInList* list, int64_t value) {
    if (!list || list->count == 0 || !list->sorted) {
        return svdb_in_list_contains(list, value);
    }

    size_t lo = 0, hi = list->count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (list->values[mid] < value) {
            lo = mid + 1;
        } else if (list->values[mid] > value) {
            hi = mid;
        } else {
            return 1;  /* Found */
        }
    }

    return 0;  /* Not found */
}

/* Linear search (for unsorted lists) */
int svdb_in_list_contains(const SvdbInList* list, int64_t value) {
    if (!list) return 0;

    /* If sorted, use binary search */
    if (list->sorted && list->count > 4) {
        return svdb_in_list_contains_sorted(list, value);
    }

    /* Linear search for small lists */
    for (size_t i = 0; i < list->count; i++) {
        if (list->values[i] == value) return 1;
    }

    return 0;
}

/* Batch IN query */
void svdb_in_list_batch(
    const SvdbInList* list,
    const int64_t*    check_values,
    uint8_t*          results,
    size_t            count
) {
    if (!list || !results) return;

    /* Sort the list if not already sorted (for faster lookup) */
    if (!list->sorted && list->count > 4) {
        /* Note: In practice, sort before calling batch */
    }

    for (size_t i = 0; i < count; i++) {
        results[i] = svdb_in_list_contains(list, check_values[i]) ? 1 : 0;
    }
}

/* ── BETWEEN implementation ──────────────────────────────────────────── */

void svdb_between_batch_i64(
    const int64_t* values,
    int64_t        low,
    int64_t        high,
    int            inclusive,
    uint8_t*       results,
    size_t         count
) {
    if (inclusive) {
        /* value >= low && value <= high */
        for (size_t i = 0; i < count; i++) {
            results[i] = (values[i] >= low && values[i] <= high) ? 1 : 0;
        }
    } else {
        /* value > low && value < high */
        for (size_t i = 0; i < count; i++) {
            results[i] = (values[i] > low && values[i] < high) ? 1 : 0;
        }
    }
}

void svdb_between_batch_f64(
    const double* values,
    double        low,
    double        high,
    int           inclusive,
    uint8_t*      results,
    size_t        count
) {
    if (inclusive) {
        /* value >= low && value <= high */
        for (size_t i = 0; i < count; i++) {
            results[i] = (values[i] >= low && values[i] <= high) ? 1 : 0;
        }
    } else {
        /* value > low && value < high */
        for (size_t i = 0; i < count; i++) {
            results[i] = (values[i] > low && values[i] < high) ? 1 : 0;
        }
    }
}

/* ── LIKE pattern matching implementation ──────────────────────────────── */

SvdbLikePattern* svdb_like_compile(const char* pattern, size_t len) {
    if (!pattern || len == 0) return NULL;

    SvdbLikePattern* pat = (SvdbLikePattern*)calloc(1, sizeof(SvdbLikePattern));
    if (!pat) return NULL;

    pat->pattern = (char*)malloc(len + 1);
    if (!pat->pattern) {
        free(pat);
        return NULL;
    }

    memcpy(pat->pattern, pattern, len);
    pat->pattern[len] = '\0';
    pat->pattern_len = len;

    /* Extract non-wildcard prefix for fast matching */
    pat->prefix_len = 0;
    for (size_t i = 0; i < len && i < 63; i++) {
        if (pattern[i] == LIKE_WILDCARD || pattern[i] == LIKE_SINGLE) {
            break;
        }
        pat->prefix[i] = pattern[i];
        pat->prefix_len++;
    }

    /* Check if pattern contains any wildcards */
    pat->has_wildcard = 0;
    for (size_t i = 0; i < len; i++) {
        if (pattern[i] == LIKE_WILDCARD || pattern[i] == LIKE_SINGLE) {
            pat->has_wildcard = 1;
            break;
        }
    }

    return pat;
}

void svdb_like_destroy(SvdbLikePattern* pat) {
    if (!pat) return;
    if (pat->pattern) free(pat->pattern);
    free(pat);
}

/* Simple LIKE match implementation */
int svdb_like_match(const SvdbLikePattern* pat, const char* text, size_t text_len) {
    if (!pat || !text) return 0;

    /* Fast path: if no wildcards, direct comparison */
    if (!pat->has_wildcard) {
        if (pat->pattern_len != text_len) return 0;
        return (memcmp(pat->pattern, text, text_len) == 0) ? 1 : 0;
    }

    /* Fast path: check prefix first */
    if (pat->prefix_len > 0) {
        if (text_len < pat->prefix_len) return 0;
        if (memcmp(pat->prefix, text, pat->prefix_len) != 0) return 0;
    }

    /* Simple wildcard matching: treat % as .* and _ as . */
    /* This is a simplified implementation */
    const char* p = pat->pattern;
    const char* t = text;
    size_t      p_len = pat->pattern_len;

    /* Skip past prefix (already matched) */
    p += pat->prefix_len;
    t += pat->prefix_len;
    p_len -= pat->prefix_len;

    while (p_len > 0 && t < text + text_len) {
        if (*p == LIKE_WILDCARD) {
            /* % matches any sequence - try matching rest */
            if (p_len == 1) {
                return 1;  /* % at end matches everything remaining */
            }
            /* Try matching after each position */
            while (t < text + text_len) {
                if (svdb_like_match(pat, t, text_len - (t - text))) {
                    return 1;
                }
                t++;
            }
            return 0;
        } else if (*p == LIKE_SINGLE) {
            /* _ matches any single character */
            p++;
            t++;
            p_len--;
        } else {
            /* Literal character match */
            if (*p != *t) return 0;
            p++;
            t++;
            p_len--;
        }
    }

    /* Check if pattern exhausted */
    return (p_len == 0) ? 1 : 0;
}

/* Batch LIKE matching */
void svdb_like_batch(
    const SvdbLikePattern* pat,
    const char**           texts,
    const size_t*          text_lens,
    uint8_t*               results,
    size_t                count
) {
    if (!pat || !results) return;

    for (size_t i = 0; i < count; i++) {
        results[i] = svdb_like_match(pat, texts[i], text_lens[i]) ? 1 : 0;
    }
}