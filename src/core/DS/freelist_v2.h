// FreeListV2 - C++ wrapper around svdb_freelist functions
// v0.11.3: C++ owned freelist with direct memory management
#ifndef SVDB_DS_FREELIST_V2_H
#define SVDB_DS_FREELIST_V2_H

#include "freelist.h"
#include <cstdint>
#include <vector>

namespace svdb::ds {

class FreeListV2 {
public:
    FreeListV2();
    ~FreeListV2();
    
    // Non-copyable
    FreeListV2(const FreeListV2&) = delete;
    FreeListV2& operator=(const FreeListV2&) = delete;
    
    // Add page to freelist
    void Add(uint32_t page_num);
    
    // Allocate page from freelist (returns 0 if empty)
    uint32_t Allocate();
    
    // Get count of free pages
    size_t Count() const { return free_pages_.size(); }
    
    // Clear freelist
    void Clear();
    
    // Get all free pages (for debugging)
    std::vector<uint32_t> GetPages() const;
    
private:
    std::vector<uint32_t> free_pages_;
};

// C API for Go bindings
extern "C" {
    struct svdb_freelist_v2_t;
    
    svdb_freelist_v2_t* svdb_freelist_v2_create();
    void svdb_freelist_v2_destroy(svdb_freelist_v2_t* fl);
    
    void svdb_freelist_v2_add(svdb_freelist_v2_t* fl, uint32_t page_num);
    uint32_t svdb_freelist_v2_allocate(svdb_freelist_v2_t* fl);
    size_t svdb_freelist_v2_count(svdb_freelist_v2_t* fl);
    void svdb_freelist_v2_clear(svdb_freelist_v2_t* fl);
}

}  // namespace svdb::ds

#endif  // SVDB_DS_FREELIST_V2_H
