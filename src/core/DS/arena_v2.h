// ArenaV2 - C++ arena allocator for zero-GC memory management
// v0.11.3: C++ owned memory, Go just holds references
#ifndef SVDB_DS_ARENA_V2_H
#define SVDB_DS_ARENA_V2_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <memory>

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
}

}  // namespace svdb::ds

#endif  // SVDB_DS_ARENA_V2_H
