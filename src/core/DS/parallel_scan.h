/* parallel_scan.h — Parallel table scan for query execution
 *
 * Uses worker pool and lock-free data structures to parallelize:
 *   - Table scanning with WHERE filter
 *   - Hash join build/probe
 *   - Aggregation
 */
#ifndef SVDB_DS_PARALLEL_SCAN_H
#define SVDB_DS_PARALLEL_SCAN_H

#include <stdint.h>
#include <stddef.h>
#include "lockfree_queue.h"
#include "worker_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum parallel workers */
#define SVDB_PARALLEL_MAX_WORKERS  64

/* Result row for parallel scan */
typedef struct {
    int64_t rowid;
    const void* data;    /* Pointer to row data (owned by caller) */
    size_t data_len;
} svdb_parallel_row_t;

/* Scan result buffer (per-thread to avoid contention) */
typedef struct {
    svdb_parallel_row_t* rows;
    size_t count;
    size_t capacity;
} svdb_scan_result_buffer_t;

/* Parallel scan configuration */
typedef struct {
    /* Table scan callback - called for each row
     * Returns 1 to include row, 0 to skip */
    int (*filter_fn)(void* user_data, int64_t rowid, const void* data, size_t data_len);

    /* Result callback - called with batch of results */
    void (*result_fn)(void* user_data, const svdb_parallel_row_t* rows, size_t count);

    /* User data passed to callbacks */
    void* user_data;

    /* Number of worker threads */
    int num_workers;

    /* Chunk size for parallel processing (rows per chunk) */
    int chunk_size;
} svdb_parallel_scan_config_t;

/* Parallel scan context (opaque) */
typedef struct svdb_parallel_scan_s svdb_parallel_scan_t;

/* ============================================================================
 * Parallel Table Scan
 * ============================================================================ */

/*
 * Create a parallel scan context.
 * The scan will use the worker pool for parallel execution.
 */
svdb_parallel_scan_t* svdb_parallel_scan_create(svdb_worker_pool_t* pool);

/*
 * Destroy the parallel scan context.
 */
void svdb_parallel_scan_destroy(svdb_parallel_scan_t* scan);

/*
 * Execute a parallel table scan.
 * The table is divided into chunks, each processed by a worker thread.
 * Results are collected via the result callback.
 *
 * row_count: total number of rows to scan
 * get_row_fn: callback to get row data by index
 * config: scan configuration
 *
 * Returns 0 on success, negative on error.
 */
int svdb_parallel_scan_execute(svdb_parallel_scan_t* scan,
                                int64_t row_count,
                                int (*get_row_fn)(void* user_data, int64_t idx,
                                                   int64_t* out_rowid,
                                                   const void** out_data,
                                                   size_t* out_len),
                                const svdb_parallel_scan_config_t* config);

/*
 * Wait for parallel scan to complete.
 */
void svdb_parallel_scan_wait(svdb_parallel_scan_t* scan);

/* ============================================================================
 * Parallel Hash Join
 * ============================================================================ */

/* Hash join context (opaque) */
typedef struct svdb_hash_join_s svdb_hash_join_t;

/*
 * Create a hash join context.
 */
svdb_hash_join_t* svdb_hash_join_create(int num_workers);

/*
 * Destroy the hash join context.
 */
void svdb_hash_join_destroy(svdb_hash_join_t* hj);

/*
 * Build phase: insert rows into hash table.
 * Can be called from multiple threads in parallel.
 */
int svdb_hash_join_build(svdb_hash_join_t* hj,
                          int64_t rowid,
                          const void* key,
                          size_t key_len);

/*
 * Probe phase: find matching rows.
 * Returns the number of matching rowids found.
 * matching_rowids must be pre-allocated with max_matches capacity.
 */
int svdb_hash_join_probe(svdb_hash_join_t* hj,
                          const void* key,
                          size_t key_len,
                          int64_t* matching_rowids,
                          int max_matches);

/*
 * Get total number of entries in the hash table.
 */
int64_t svdb_hash_join_entry_count(svdb_hash_join_t* hj);

/* ============================================================================
 * Parallel Aggregation
 * ============================================================================ */

/* Aggregate types */
typedef enum {
    SVDB_AGG_COUNT,
    SVDB_AGG_SUM,
    SVDB_AGG_MIN,
    SVDB_AGG_MAX,
    SVDB_AGG_AVG
} svdb_agg_type_t;

/* Partial aggregate result (one per worker thread) */
typedef struct {
    int64_t count;
    union {
        int64_t int_val;
        double dbl_val;
    };
} svdb_partial_agg_t;

/* Aggregate context (opaque) */
typedef struct svdb_agg_s svdb_agg_t;

/*
 * Create an aggregate context.
 */
svdb_agg_t* svdb_agg_create(svdb_agg_type_t type, int is_int);

/*
 * Destroy the aggregate context.
 */
void svdb_agg_destroy(svdb_agg_t* agg);

/*
 * Update partial aggregate with a new value.
 * Thread-safe for concurrent updates.
 */
void svdb_agg_update_int(svdb_agg_t* agg, int64_t value);
void svdb_agg_update_dbl(svdb_agg_t* agg, double value);

/*
 * Merge partial results from multiple workers.
 */
void svdb_agg_merge(svdb_agg_t* dest, const svdb_agg_t* src);

/*
 * Get final aggregate result.
 */
int64_t svdb_agg_result_int(svdb_agg_t* agg);
double svdb_agg_result_dbl(svdb_agg_t* agg);

/* ============================================================================
 * Parallel Result Collection
 * ============================================================================ */

/*
 * Create a result buffer for collecting scan results.
 */
svdb_scan_result_buffer_t* svdb_scan_result_buffer_create(size_t initial_capacity);

/*
 * Destroy a result buffer.
 */
void svdb_scan_result_buffer_destroy(svdb_scan_result_buffer_t* buf);

/*
 * Add a row to the result buffer.
 * Returns 0 on success, -1 if out of memory.
 */
int svdb_scan_result_buffer_add(svdb_scan_result_buffer_t* buf,
                                 int64_t rowid,
                                 const void* data,
                                 size_t data_len);

/*
 * Clear the result buffer for reuse.
 */
void svdb_scan_result_buffer_clear(svdb_scan_result_buffer_t* buf);

/*
 * Merge multiple result buffers into one.
 */
int svdb_scan_result_buffer_merge(svdb_scan_result_buffer_t* dest,
                                   const svdb_scan_result_buffer_t* const* srcs,
                                   size_t num_srcs);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_PARALLEL_SCAN_H