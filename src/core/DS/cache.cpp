#include "cache.h"
#include <cstdlib>
#include <cstring>
#include <list>
#include <unordered_map>
#include <mutex>
#include <vector>

/* Default page size used when converting KiB-based capacity (mirrors Go). */
static const int SVDB_CACHE_DEFAULT_PAGE_SIZE = 4096;
static const int SVDB_CACHE_DEFAULT_CAPACITY  = 2000;

/* Normalize a raw capacity value (same logic as Go NewCache). */
static int normalize_capacity(int cap) {
    if (cap < 0) {
        /* Use int64 arithmetic to avoid overflow for extreme values. */
        int64_t kib_bytes = (int64_t)(-cap) * 1024;
        cap = (int)(kib_bytes / SVDB_CACHE_DEFAULT_PAGE_SIZE);
    }
    if (cap <= 0) {
        cap = SVDB_CACHE_DEFAULT_CAPACITY;
    }
    return cap;
}

struct CacheEntry {
    uint32_t           page_num;
    std::vector<uint8_t> data;
};

struct svdb_cache_t {
    std::mutex                                              mu;
    int                                                     capacity;
    std::list<CacheEntry>                                   lru;
    std::unordered_map<uint32_t, std::list<CacheEntry>::iterator> index;
    int                                                     hits;
    int                                                     misses;

    explicit svdb_cache_t(int cap)
        : capacity(cap), hits(0), misses(0) {}
};

extern "C" {

svdb_cache_t* svdb_cache_create(int capacity) {
    return new svdb_cache_t(normalize_capacity(capacity));
}

void svdb_cache_destroy(svdb_cache_t* cache) {
    delete cache;
}

int svdb_cache_get(svdb_cache_t* cache, uint32_t page_num,
                   const uint8_t** out_page_data, size_t* out_page_size) {
    if (!cache) return 0;
    std::lock_guard<std::mutex> lock(cache->mu);
    auto it = cache->index.find(page_num);
    if (it == cache->index.end()) {
        ++cache->misses;
        return 0;
    }
    cache->lru.splice(cache->lru.begin(), cache->lru, it->second);
    ++cache->hits;
    if (out_page_data) *out_page_data = it->second->data.data();
    if (out_page_size) *out_page_size = it->second->data.size();
    return 1;
}

void svdb_cache_set(svdb_cache_t* cache, uint32_t page_num,
                    const uint8_t* page_data, size_t page_size) {
    if (!cache || !page_data) return;
    std::lock_guard<std::mutex> lock(cache->mu);
    auto it = cache->index.find(page_num);
    if (it != cache->index.end()) {
        it->second->data.assign(page_data, page_data + page_size);
        cache->lru.splice(cache->lru.begin(), cache->lru, it->second);
        return;
    }
    /* Evict LRU if at capacity. */
    if ((int)cache->lru.size() >= cache->capacity) {
        auto back = std::prev(cache->lru.end());
        cache->index.erase(back->page_num);
        cache->lru.erase(back);
    }
    cache->lru.push_front(CacheEntry{page_num,
        std::vector<uint8_t>(page_data, page_data + page_size)});
    cache->index[page_num] = cache->lru.begin();
}

void svdb_cache_remove(svdb_cache_t* cache, uint32_t page_num) {
    if (!cache) return;
    std::lock_guard<std::mutex> lock(cache->mu);
    auto it = cache->index.find(page_num);
    if (it != cache->index.end()) {
        cache->lru.erase(it->second);
        cache->index.erase(it);
    }
}

void svdb_cache_clear(svdb_cache_t* cache) {
    if (!cache) return;
    std::lock_guard<std::mutex> lock(cache->mu);
    cache->lru.clear();
    cache->index.clear();
    cache->hits   = 0;
    cache->misses = 0;
}

int svdb_cache_size(svdb_cache_t* cache) {
    if (!cache) return 0;
    std::lock_guard<std::mutex> lock(cache->mu);
    return (int)cache->lru.size();
}

void svdb_cache_stats(svdb_cache_t* cache, int* out_hits, int* out_misses) {
    if (!cache) return;
    std::lock_guard<std::mutex> lock(cache->mu);
    if (out_hits)   *out_hits   = cache->hits;
    if (out_misses) *out_misses = cache->misses;
}

void svdb_cache_set_capacity(svdb_cache_t* cache, int capacity) {
    if (!cache) return;
    int cap = normalize_capacity(capacity);
    std::lock_guard<std::mutex> lock(cache->mu);
    cache->capacity = cap;
    while ((int)cache->lru.size() > cache->capacity) {
        auto back = std::prev(cache->lru.end());
        cache->index.erase(back->page_num);
        cache->lru.erase(back);
    }
}

} /* extern "C" */
