// CacheV2 Implementation
#include "cache_v2.h"
#include <cstring>

namespace svdb::ds {

// ============================================================================
// C API Implementation
// ============================================================================

extern "C" {

struct svdb_cache_v2_t {
    LRUCacheV2* ptr;
};

svdb_cache_v2_t* svdb_cache_v2_create(int capacity) {
    try {
        auto* cache = new LRUCacheV2(capacity);
        auto* handle = new svdb_cache_v2_t{cache};
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void svdb_cache_v2_destroy(svdb_cache_v2_t* cache) {
    if (cache) {
        delete cache->ptr;
        delete cache;
    }
}

uint8_t* svdb_cache_v2_get(svdb_cache_v2_t* cache, uint32_t page_num, size_t* out_size) {
    if (!cache || !out_size) return nullptr;
    try {
        return cache->ptr->Get(page_num, out_size);
    } catch (...) {
        return nullptr;
    }
}

void svdb_cache_v2_free_page(uint8_t* page) {
    LRUCacheV2::FreePage(page);
}

void svdb_cache_v2_put(svdb_cache_v2_t* cache, uint32_t page_num,
                       const uint8_t* data, size_t size) {
    if (!cache || !data || size == 0) return;
    try {
        cache->ptr->Put(page_num, data, size);
    } catch (...) {
        // Silently ignore
    }
}

void svdb_cache_v2_remove(svdb_cache_v2_t* cache, uint32_t page_num) {
    if (cache) cache->ptr->Remove(page_num);
}

void svdb_cache_v2_clear(svdb_cache_v2_t* cache) {
    if (cache) cache->ptr->Clear();
}

size_t svdb_cache_v2_size(svdb_cache_v2_t* cache) {
    return cache ? cache->ptr->Size() : 0;
}

size_t svdb_cache_v2_hits(svdb_cache_v2_t* cache) {
    return cache ? cache->ptr->Hits() : 0;
}

size_t svdb_cache_v2_misses(svdb_cache_v2_t* cache) {
    return cache ? cache->ptr->Misses() : 0;
}

}  // extern "C"

// ============================================================================
// LRUCacheV2 Implementation
// ============================================================================

LRUCacheV2::LRUCacheV2(int capacity)
    : hits_(0)
    , misses_(0)
{
    cache_ = svdb_cache_create(capacity);
    if (!cache_) {
        throw std::bad_alloc();
    }
}

LRUCacheV2::~LRUCacheV2() {
    svdb_cache_destroy(cache_);
}

uint8_t* LRUCacheV2::Get(uint32_t page_num, size_t* out_size) {
    const uint8_t* data = nullptr;
    size_t size = 0;
    
    if (svdb_cache_get(cache_, page_num, &data, &size)) {
        hits_++;
        // Make a copy since the cache owns the memory
        uint8_t* result = new uint8_t[size];
        std::memcpy(result, data, size);
        if (out_size) *out_size = size;
        return result;
    }
    
    misses_++;
    if (out_size) *out_size = 0;
    return nullptr;
}

void LRUCacheV2::FreePage(uint8_t* page) {
    delete[] page;
}

void LRUCacheV2::Put(uint32_t page_num, const uint8_t* data, size_t size) {
    svdb_cache_set(cache_, page_num, data, size);
}

void LRUCacheV2::Remove(uint32_t page_num) {
    svdb_cache_remove(cache_, page_num);
}

void LRUCacheV2::Clear() {
    svdb_cache_clear(cache_);
    hits_ = 0;
    misses_ = 0;
}

size_t LRUCacheV2::Size() const {
    return static_cast<size_t>(svdb_cache_size(cache_));
}

size_t LRUCacheV2::Hits() const {
    return hits_;
}

size_t LRUCacheV2::Misses() const {
    return misses_;
}

double LRUCacheV2::HitRate() const {
    size_t total = hits_ + misses_;
    return total > 0 ? static_cast<double>(hits_) / total : 0.0;
}

}  // namespace svdb::ds
