// ArenaV2 Implementation
#include "arena_v2.h"
#include <cstring>
#include <stdexcept>

namespace svdb::ds {

// ============================================================================
// C API Implementation
// ============================================================================

extern "C" {

struct svdb_arena_v2_t {
    ArenaV2* ptr;
};

svdb_arena_v2_t* svdb_arena_v2_create(size_t chunk_size) {
    try {
        auto* arena = new ArenaV2(chunk_size);
        auto* handle = new svdb_arena_v2_t{arena};
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void svdb_arena_v2_destroy(svdb_arena_v2_t* arena) {
    if (arena) {
        delete arena->ptr;
        delete arena;
    }
}

void* svdb_arena_v2_alloc(svdb_arena_v2_t* arena, size_t size) {
    if (!arena || size == 0) return nullptr;
    try {
        return arena->ptr->Alloc(size);
    } catch (...) {
        return nullptr;
    }
}

void* svdb_arena_v2_calloc(svdb_arena_v2_t* arena, size_t count, size_t size) {
    if (!arena || size == 0 || count == 0) return nullptr;
    try {
        return arena->ptr->Calloc(count, size);
    } catch (...) {
        return nullptr;
    }
}

void svdb_arena_v2_reset(svdb_arena_v2_t* arena) {
    if (arena) arena->ptr->Reset();
}

size_t svdb_arena_v2_bytes_used(svdb_arena_v2_t* arena) {
    return arena ? arena->ptr->BytesUsed() : 0;
}

size_t svdb_arena_v2_bytes_allocated(svdb_arena_v2_t* arena) {
    return arena ? arena->ptr->BytesAllocated() : 0;
}

size_t svdb_arena_v2_chunk_count(svdb_arena_v2_t* arena) {
    return arena ? arena->ptr->ChunkCount() : 0;
}

// WS6: SafeArena C API implementation
struct svdb_safe_arena_t {
    SafeArena* ptr;
};

svdb_safe_arena_t* svdb_safe_arena_create(size_t max_bytes, size_t chunk_size) {
    try {
        // max_bytes=0 → use a generous 64 MB default suitable for most queries.
        // Callers that know their memory budget should always pass an explicit value.
        if (max_bytes == 0) max_bytes = 64 * 1024 * 1024;
        if (chunk_size == 0) chunk_size = 64 * 1024;        // 64KB chunks
        auto* arena = new SafeArena(max_bytes, chunk_size);
        auto* handle = new svdb_safe_arena_t{arena};
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void svdb_safe_arena_destroy(svdb_safe_arena_t* arena) {
    if (arena) {
        delete arena->ptr;
        delete arena;
    }
}

void* svdb_safe_arena_alloc(svdb_safe_arena_t* arena, size_t size) {
    if (!arena || size == 0) return nullptr;
    try {
        return arena->ptr->Alloc(size);
    } catch (...) {
        return nullptr;
    }
}

void* svdb_safe_arena_calloc(svdb_safe_arena_t* arena, size_t count, size_t size) {
    if (!arena || count == 0 || size == 0) return nullptr;
    try {
        return arena->ptr->Calloc(count, size);
    } catch (...) {
        return nullptr;
    }
}

void svdb_safe_arena_reset(svdb_safe_arena_t* arena) {
    if (arena) arena->ptr->Reset();
}

size_t svdb_safe_arena_bytes_used(svdb_safe_arena_t* arena) {
    return arena ? arena->ptr->BytesUsed() : 0;
}

size_t svdb_safe_arena_bytes_remaining(svdb_safe_arena_t* arena) {
    return arena ? arena->ptr->BytesRemaining() : 0;
}

}  // extern "C"

// ============================================================================
// ArenaV2 Implementation
// ============================================================================

ArenaV2::ArenaV2(size_t chunk_size)
    : current_(nullptr)
    , offset_(0)
    , used_(0)
    , allocated_(0)
    , chunk_size_(chunk_size)
{
    // Allocate initial chunk
    Grow(chunk_size);
}

ArenaV2::~ArenaV2() {
    // Chunks are automatically freed by unique_ptr
}

void* ArenaV2::Alloc(size_t size) {
    // Align to 8 bytes
    size = (size + 7) & ~7;
    
    // Check if current chunk has space
    if (offset_ + size > chunk_size_) {
        Grow(size);
    }
    
    void* result = current_ + offset_;
    offset_ += size;
    used_ += size;
    
    return result;
}

void* ArenaV2::Calloc(size_t count, size_t size) {
    void* ptr = Alloc(count * size);
    if (ptr) {
        std::memset(ptr, 0, count * size);
    }
    return ptr;
}

void ArenaV2::Reset() {
    // Keep chunks but reset offset
    if (!chunks_.empty()) {
        current_ = chunks_[0].get();
        offset_ = 0;
        used_ = 0;
    }
}

void ArenaV2::Grow(size_t min_size) {
    // Allocate new chunk (at least min_size, but at least chunk_size_)
    size_t new_chunk_size = std::max(min_size, chunk_size_);
    auto new_chunk = std::make_unique<char[]>(new_chunk_size);
    
    current_ = new_chunk.get();
    offset_ = 0;
    allocated_ += new_chunk_size;
    
    chunks_.push_back(std::move(new_chunk));
}

}  // namespace svdb::ds
