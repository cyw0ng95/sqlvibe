#ifndef SVDB_DS_SKIP_LIST_H
#define SVDB_DS_SKIP_LIST_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Skip list providing O(log n) key → int64 index mapping.
 * Supports both int64 and variable-length string keys.
 * Maximum level: 16.  Promotion probability: 50%.
 * RNG: std::mt19937 seeded at creation time.
 */

typedef void* svdb_skiplist_t;

/* Create an empty skip list.  Returns NULL on allocation failure. */
svdb_skiplist_t svdb_skiplist_create(void);

/* Destroy the skip list and free all memory. */
void svdb_skiplist_destroy(svdb_skiplist_t sl);

/* Insert an int64 key mapped to row_idx.
 * Duplicate (key, row_idx) pairs are silently ignored. */
void svdb_skiplist_insert_int(svdb_skiplist_t sl, int64_t key, int64_t row_idx);

/* Insert a string key of key_len bytes mapped to row_idx.
 * Duplicate (key, row_idx) pairs are silently ignored. */
void svdb_skiplist_insert_str(svdb_skiplist_t sl,
                               const uint8_t* key, size_t key_len,
                               int64_t row_idx);

/* Remove the (key, row_idx) pair for an int64 key.
 * No-op if the pair does not exist. */
void svdb_skiplist_delete_int(svdb_skiplist_t sl, int64_t key, int64_t row_idx);

/* Remove the (key, row_idx) pair for a string key. */
void svdb_skiplist_delete_str(svdb_skiplist_t sl,
                               const uint8_t* key, size_t key_len,
                               int64_t row_idx);

/* Find all row indices associated with an int64 key.
 * Writes up to max_out values into out_indices.
 * Returns the number of matches written. */
int svdb_skiplist_find_int(svdb_skiplist_t sl, int64_t key,
                            int64_t* out_indices, int max_out);

/* Find all row indices associated with a string key. */
int svdb_skiplist_find_str(svdb_skiplist_t sl,
                            const uint8_t* key, size_t key_len,
                            int64_t* out_indices, int max_out);

/* Range query over int64 keys in [lo, hi].
 * If inclusive == 0, the range is (lo, hi) (exclusive on both ends).
 * Writes up to max_out row indices into out_indices.
 * Returns the number of matches written. */
int svdb_skiplist_range_int(svdb_skiplist_t sl, int64_t lo, int64_t hi, int inclusive,
                             int64_t* out_indices, int max_out);

/* Return the number of unique keys in the skip list. */
int svdb_skiplist_len(svdb_skiplist_t sl);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_SKIP_LIST_H */
