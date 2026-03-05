// FreeListV2 Implementation
#include "freelist_v2.h"
#include <algorithm>

namespace svdb::ds {

// ============================================================================
// C API Implementation
// ============================================================================

extern "C" {

struct svdb_freelist_v2_t {
    FreeListV2* ptr;
};

svdb_freelist_v2_t* svdb_freelist_v2_create() {
    try {
        auto* fl = new FreeListV2();
        auto* handle = new svdb_freelist_v2_t{fl};
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void svdb_freelist_v2_destroy(svdb_freelist_v2_t* fl) {
    if (fl) {
        delete fl->ptr;
        delete fl;
    }
}

void svdb_freelist_v2_add(svdb_freelist_v2_t* fl, uint32_t page_num) {
    if (fl && page_num != 0) fl->ptr->Add(page_num);
}

uint32_t svdb_freelist_v2_allocate(svdb_freelist_v2_t* fl) {
    return fl ? fl->ptr->Allocate() : 0;
}

size_t svdb_freelist_v2_count(svdb_freelist_v2_t* fl) {
    return fl ? fl->ptr->Count() : 0;
}

void svdb_freelist_v2_clear(svdb_freelist_v2_t* fl) {
    if (fl) fl->ptr->Clear();
}

}  // extern "C"

// ============================================================================
// FreeListV2 Implementation
// ============================================================================

FreeListV2::FreeListV2() {
    // Reserve some initial capacity to avoid reallocations
    free_pages_.reserve(256);
}

FreeListV2::~FreeListV2() {
    // Vector automatically cleaned up
}

void FreeListV2::Add(uint32_t page_num) {
    if (page_num != 0) {
        free_pages_.push_back(page_num);
    }
}

uint32_t FreeListV2::Allocate() {
    if (free_pages_.empty()) {
        return 0;
    }
    
    // Pop from back (O(1))
    uint32_t page_num = free_pages_.back();
    free_pages_.pop_back();
    return page_num;
}

void FreeListV2::Clear() {
    free_pages_.clear();
}

std::vector<uint32_t> FreeListV2::GetPages() const {
    return free_pages_;
}

}  // namespace svdb::ds
