#ifndef SVDB_VM_EXEC_H
#define SVDB_VM_EXEC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Returns 1 if a SELECT query has no non-deterministic side effects
 * (no RANDOM(), no volatile datetime functions that change between calls).
 */
int svdb_exec_is_result_cache_eligible(const char* sql, size_t sql_len);

/*
 * Estimate memory in bytes needed for a result set:
 *   num_cols * num_rows * 8  (average value size)
 */
size_t svdb_exec_estimate_result_size(int num_cols, int num_rows);

/*
 * Returns 1 if columnar storage would be more efficient:
 *   num_cols >= 4 && num_rows >= 1000
 */
int svdb_exec_should_use_columnar(int num_cols, int num_rows);

/* Threshold for inline vs heap row storage */
int svdb_exec_max_inline_rows(void);

/*
 * Compute a fast FNV-1a hash of the SQL string for cache key lookup.
 */
uint64_t svdb_exec_compute_hash(const char* sql, size_t sql_len);

/*
 * Collapse runs of whitespace (space/tab/newline) to a single space and
 * trim leading/trailing whitespace.
 * Returns output length, or -1 if buffer too small.
 */
int svdb_exec_normalize_whitespace(const char* sql, size_t sql_len,
                                    char* out_buf, int out_buf_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_EXEC_H */
