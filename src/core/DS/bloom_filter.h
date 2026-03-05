#ifndef SVDB_DS_BLOOM_FILTER_H
#define SVDB_DS_BLOOM_FILTER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct svdb_bloom_filter svdb_bloom_filter_t;

/* Create a new bloom filter
 * expected_items: expected number of items to be added
 * false_positive_rate: desired false positive rate (0.0-1.0)
 * Returns NULL on error */
svdb_bloom_filter_t* svdb_bloom_filter_create(int expected_items, double false_positive_rate);

/* Destroy a bloom filter */
void svdb_bloom_filter_destroy(svdb_bloom_filter_t* filter);

/* Add a key to the filter */
void svdb_bloom_filter_add(svdb_bloom_filter_t* filter, const uint8_t* key, size_t key_len);

/* Check if key might be in the filter
 * Returns 1 if possibly in filter, 0 if definitely not */
int svdb_bloom_filter_might_contain(svdb_bloom_filter_t* filter, const uint8_t* key, size_t key_len);

/* Get the number of bits in the filter */
int svdb_bloom_filter_size(const svdb_bloom_filter_t* filter);

/* Get the number of hash functions */
int svdb_bloom_filter_hash_count(const svdb_bloom_filter_t* filter);

/* Clear all bits in the filter */
void svdb_bloom_filter_clear(svdb_bloom_filter_t* filter);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_BLOOM_FILTER_H */
