#ifndef SVDB_CG_PLAN_CACHE_H
#define SVDB_CG_PLAN_CACHE_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct svdb_cg_cache svdb_cg_cache_t;

/*
 * Create a plan cache with default max size (100 entries).
 */
svdb_cg_cache_t* svdb_cg_cache_create(void);

/*
 * Create a plan cache with a custom max size.
 * When the cache reaches max_size entries, the least-recently-used
 * entry is evicted on the next put.
 */
svdb_cg_cache_t* svdb_cg_cache_create_with_size(size_t max_size);

void             svdb_cg_cache_free(svdb_cg_cache_t* cache);

/*
 * Store a JSON plan for the given SQL statement.
 * Thread-safe. Evicts LRU entry if cache is full.
 */
void        svdb_cg_cache_put_json(svdb_cg_cache_t* cache, const char* sql, const char* json_data, size_t json_len);

/*
 * Retrieve a JSON plan for the given SQL statement.
 * Thread-safe. Updates LRU order on hit.
 * Returns pointer to internal data (valid until next modification).
 * Returns nullptr and sets *out_len=0 on miss.
 */
const char* svdb_cg_cache_get_json(svdb_cg_cache_t* cache, const char* sql, size_t* out_len);

/*
 * Clear all entries from the cache.
 * Thread-safe.
 */
void        svdb_cg_cache_erase(svdb_cg_cache_t* cache);

/*
 * Thread-safe copy variant: copies the JSON string into a caller-provided
 * buffer under a shared lock.  Returns true on hit, false on miss.
 * On hit with buf_size > json_len the buffer is NUL-terminated.
 *
 * Note: Uses shared lock for concurrent read access. Does NOT update
 * LRU order. For LRU updates on read, use svdb_cg_cache_get_json.
 */
bool svdb_cg_cache_copy_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    char*            buf,
    size_t           buf_size,
    size_t*          out_len
);

/*
 * Get current number of cached entries.
 * Thread-safe (uses shared lock).
 */
size_t svdb_cg_cache_count(svdb_cg_cache_t* cache);

/*
 * Get the maximum cache size.
 * Thread-safe (uses shared lock).
 */
size_t svdb_cg_cache_max_size(svdb_cg_cache_t* cache);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_PLAN_CACHE_H */