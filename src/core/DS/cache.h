#ifndef SVDB_DS_CACHE_H
#define SVDB_DS_CACHE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque LRU page cache handle. */
typedef struct svdb_cache_t svdb_cache_t;

/*
 * Create an LRU cache.
 * capacity > 0: max number of pages.
 * capacity < 0: cache size in KiB (converted using 4096-byte default page size).
 * capacity == 0: defaults to 2000 pages.
 */
svdb_cache_t* svdb_cache_create(int capacity);

/* Destroy the cache and free all memory. */
void svdb_cache_destroy(svdb_cache_t* cache);

/*
 * Look up page_num in the cache.
 * On hit, *out_page_data points into internally-owned memory and *out_page_size
 * is set; returns 1.  Does NOT transfer ownership.
 * Returns 0 on miss.
 */
int svdb_cache_get(svdb_cache_t* cache, uint32_t page_num,
                   const uint8_t** out_page_data, size_t* out_page_size);

/*
 * Insert or update a page in the cache.
 * A copy of page_data[0..page_size) is stored internally.
 * Evicts the LRU entry if the cache is full.
 */
void svdb_cache_set(svdb_cache_t* cache, uint32_t page_num,
                    const uint8_t* page_data, size_t page_size);

/* Remove page_num from the cache (no-op if not present). */
void svdb_cache_remove(svdb_cache_t* cache, uint32_t page_num);

/* Clear all entries and reset hit/miss statistics. */
void svdb_cache_clear(svdb_cache_t* cache);

/* Return the number of pages currently in the cache. */
int svdb_cache_size(svdb_cache_t* cache);

/* Return cumulative hit and miss counts. */
void svdb_cache_stats(svdb_cache_t* cache, int* out_hits, int* out_misses);

/*
 * Resize the cache, evicting LRU entries if the current size exceeds
 * the new capacity.  Follows the same sign convention as svdb_cache_create.
 */
void svdb_cache_set_capacity(svdb_cache_t* cache, int capacity);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_CACHE_H */
