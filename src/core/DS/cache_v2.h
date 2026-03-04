// CacheV2 - C++ wrapper around svdb_cache_t
// v0.11.3: C++ owned cache with direct memory management
#ifndef SVDB_DS_CACHE_V2_H
#define SVDB_DS_CACHE_V2_H

#include "cache.h"
#include "page.h"
#include <cstdint>
#include <optional>

namespace svdb::ds {

class LRUCacheV2 {
public:
    // capacity: number of pages (positive) or KB (negative)
    explicit LRUCacheV2(int capacity = -1024);  // Default 1MB (~256 pages @ 4KB)
    ~LRUCacheV2();
    
    // Non-copyable
    LRUCacheV2(const LRUCacheV2&) = delete;
    LRUCacheV2& operator=(const LRUCacheV2&) = delete;
    
    // Get page from cache (returns allocated buffer, caller must free)
    uint8_t* Get(uint32_t page_num, size_t* out_size);
    
    // Free a page buffer returned by Get
    static void FreePage(uint8_t* page);
    
    // Put page into cache (makes internal copy)
    void Put(uint32_t page_num, const uint8_t* data, size_t size);
    
    // Remove page from cache
    void Remove(uint32_t page_num);
    
    // Clear all entries
    void Clear();
    
    // Statistics
    size_t Size() const;
    size_t Hits() const;
    size_t Misses() const;
    double HitRate() const;
    
private:
    svdb_cache_t* cache_;
    mutable size_t hits_;
    mutable size_t misses_;
};

// C API for Go bindings
extern "C" {
    struct svdb_cache_v2_t;
    
    svdb_cache_v2_t* svdb_cache_v2_create(int capacity);
    void svdb_cache_v2_destroy(svdb_cache_v2_t* cache);
    
    // Returns nullptr if not found, caller must free with svdb_cache_v2_free_page
    uint8_t* svdb_cache_v2_get(svdb_cache_v2_t* cache, uint32_t page_num, size_t* out_size);
    void svdb_cache_v2_free_page(uint8_t* page);
    
    void svdb_cache_v2_put(svdb_cache_v2_t* cache, uint32_t page_num, 
                           const uint8_t* data, size_t size);
    void svdb_cache_v2_remove(svdb_cache_v2_t* cache, uint32_t page_num);
    void svdb_cache_v2_clear(svdb_cache_v2_t* cache);
    
    size_t svdb_cache_v2_size(svdb_cache_v2_t* cache);
    size_t svdb_cache_v2_hits(svdb_cache_v2_t* cache);
    size_t svdb_cache_v2_misses(svdb_cache_v2_t* cache);
}

}  // namespace svdb::ds

#endif  // SVDB_DS_CACHE_V2_H
