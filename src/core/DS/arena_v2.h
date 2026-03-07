// ArenaV2 - C++ arena allocator for zero-GC memory management
// v0.11.3: C++ owned memory, Go just holds references
#ifndef SVDB_DS_ARENA_V2_H
#define SVDB_DS_ARENA_V2_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <memory>
#include <stdexcept>

namespace svdb::ds {

class ArenaV2 {
public:
    explicit ArenaV2(size_t chunk_size = 256 * 1024);  // 256KB default
    ~ArenaV2();
    
    // Non-copyable
    ArenaV2(const ArenaV2&) = delete;
    ArenaV2& operator=(const ArenaV2&) = delete;
    
    // Allocate memory from arena
    void* Alloc(size_t size);
    
    // Allocate and zero-initialize
    void* Calloc(size_t count, size_t size);
    
    // Reset arena (free all allocations, keep capacity)
    void Reset();
    
    // Statistics
    size_t BytesUsed() const { return used_; }
    size_t BytesAllocated() const { return allocated_; }
    size_t ChunkCount() const { return chunks_.size(); }
    
    // C API handle
    struct Handle {
        ArenaV2* arena;
        void* ptr;
        size_t size;
    };
    
private:
    // Allocate a new chunk
    void Grow(size_t min_size);
    
    // Members
    std::vector<std::unique_ptr<char[]>> chunks_;
    char* current_;
    size_t offset_;
    size_t used_;
    size_t allocated_;
    size_t chunk_size_;
};

// WS6: SafeArena — ArenaV2 with a hard allocation limit.
// Throws std::bad_alloc if max_bytes would be exceeded.
// Designed for per-query use: construct at query start, destroy at end.
class SafeArena : public ArenaV2 {
public:
    explicit SafeArena(size_t max_bytes, size_t chunk_size = 64 * 1024)
        : ArenaV2(chunk_size), max_bytes_(max_bytes) {}

    // Allocate with limit check
    void* Alloc(size_t size) {
        if (BytesUsed() + size > max_bytes_) {
            throw std::bad_alloc();
        }
        return ArenaV2::Alloc(size);
    }

    void* Calloc(size_t count, size_t size) {
        size_t total = count * size;
        if (BytesUsed() + total > max_bytes_) {
            throw std::bad_alloc();
        }
        return ArenaV2::Calloc(count, size);
    }

    size_t MaxBytes() const { return max_bytes_; }
    size_t BytesRemaining() const {
        size_t used = BytesUsed();
        return (used < max_bytes_) ? max_bytes_ - used : 0;
    }

private:
    size_t max_bytes_;
};

// C API for Go bindings
extern "C" {
    struct svdb_arena_v2_t;
    
    svdb_arena_v2_t* svdb_arena_v2_create(size_t chunk_size);
    void svdb_arena_v2_destroy(svdb_arena_v2_t* arena);
    
    void* svdb_arena_v2_alloc(svdb_arena_v2_t* arena, size_t size);
    void* svdb_arena_v2_calloc(svdb_arena_v2_t* arena, size_t count, size_t size);
    void svdb_arena_v2_reset(svdb_arena_v2_t* arena);
    
    size_t svdb_arena_v2_bytes_used(svdb_arena_v2_t* arena);
    size_t svdb_arena_v2_bytes_allocated(svdb_arena_v2_t* arena);
    size_t svdb_arena_v2_chunk_count(svdb_arena_v2_t* arena);

    // WS6: SafeArena C API
    // max_bytes: hard allocation limit; pass 0 to use the 64 MB default.
    // chunk_size: arena chunk size; pass 0 to use the 64 KB default.
    struct svdb_safe_arena_t;

    svdb_safe_arena_t* svdb_safe_arena_create(size_t max_bytes, size_t chunk_size);
    void svdb_safe_arena_destroy(svdb_safe_arena_t* arena);
    void* svdb_safe_arena_alloc(svdb_safe_arena_t* arena, size_t size);
    void* svdb_safe_arena_calloc(svdb_safe_arena_t* arena, size_t count, size_t size);
    void svdb_safe_arena_reset(svdb_safe_arena_t* arena);
    size_t svdb_safe_arena_bytes_used(svdb_safe_arena_t* arena);
    size_t svdb_safe_arena_bytes_remaining(svdb_safe_arena_t* arena);
}

}  // namespace svdb::ds

#endif  // SVDB_DS_ARENA_V2_H
