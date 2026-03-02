#include "bloom_filter.h"
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>

/* FNV-1a hash constants */
static const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
static const uint64_t FNV_PRIME = 1099511628211ULL;

struct svdb_bloom_filter {
    std::vector<uint64_t> bits;
    int k;  /* number of hash functions */
    int m;  /* total number of bits */
};

/* FNV-1a hash function */
static uint64_t fnv1a_hash(const uint8_t* data, size_t len, uint64_t seed) {
    uint64_t hash = seed;
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

/* Get two independent hashes for bloom filter */
static void get_hashes(const uint8_t* key, size_t key_len, uint64_t* h1, uint64_t* h2) {
    *h1 = fnv1a_hash(key, key_len, FNV_OFFSET_BASIS);
    /* Generate second hash from first */
    *h2 = fnv1a_hash((const uint8_t*)h1, sizeof(uint64_t), FNV_OFFSET_BASIS ^ 0x9e3779b97f4a7c15ULL);
}

extern "C" {

svdb_bloom_filter_t* svdb_bloom_filter_create(int expected_items, double false_positive_rate) {
    if (expected_items <= 0) expected_items = 1;
    if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) false_positive_rate = 0.01;
    
    /* m = -n * ln(p) / (ln(2)^2) */
    double ln2 = 0.693147180559945; /* log(2) */
    double ln2_sq = ln2 * ln2;
    double m_double = -expected_items * log(false_positive_rate) / ln2_sq;
    int m = (int)ceil(m_double);
    if (m < 64) m = 64;
    /* Round up to multiple of 64 */
    m = ((m + 63) / 64) * 64;
    
    int k = 2;
    
    auto* filter = (svdb_bloom_filter_t*)malloc(sizeof(svdb_bloom_filter));
    if (!filter) return nullptr;
    
    filter->bits.resize(m / 64, 0);
    filter->k = k;
    filter->m = m;
    
    return filter;
}

void svdb_bloom_filter_destroy(svdb_bloom_filter_t* filter) {
    if (filter) free(filter);
}

void svdb_bloom_filter_add(svdb_bloom_filter_t* filter, const uint8_t* key, size_t key_len) {
    if (!filter || !key || key_len == 0) return;
    
    uint64_t h1, h2;
    get_hashes(key, key_len, &h1, &h2);
    
    for (int i = 0; i < filter->k; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2) % (uint64_t)filter->m;
        filter->bits[pos / 64] |= (1ULL << (pos % 64));
    }
}

int svdb_bloom_filter_might_contain(svdb_bloom_filter_t* filter, const uint8_t* key, size_t key_len) {
    if (!filter || !key || key_len == 0) return 0;
    
    uint64_t h1, h2;
    get_hashes(key, key_len, &h1, &h2);
    
    for (int i = 0; i < filter->k; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2) % (uint64_t)filter->m;
        if ((filter->bits[pos / 64] >> (pos % 64)) & 1ULL == 0) {
            return 0;
        }
    }
    return 1;
}

int svdb_bloom_filter_size(const svdb_bloom_filter_t* filter) {
    if (!filter) return 0;
    return filter->m;
}

int svdb_bloom_filter_hash_count(const svdb_bloom_filter_t* filter) {
    if (!filter) return 0;
    return filter->k;
}

void svdb_bloom_filter_clear(svdb_bloom_filter_t* filter) {
    if (!filter) return;
    std::fill(filter->bits.begin(), filter->bits.end(), 0);
}

} /* extern "C" */
