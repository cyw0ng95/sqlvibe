/* query_arena.h — Arena allocator for query-ephemeral TEXT values
 *
 * Purpose: Eliminate per-value malloc calls during query execution.
 * For 10,000 rows with 5 text columns, this reduces 50,000 malloc calls to 1.
 *
 * Usage:
 *   QueryArena arena(256 * 1024);  // 256KB pre-allocated
 *   char* text = arena.AllocText(str, len);  // Bump allocation
 *   arena.Reset();  // O(1) free all at query end
 *
 * Thread Safety: One arena per query execution (per VM instance).
 */
#ifndef SVDB_DS_QUERY_ARENA_H
#define SVDB_DS_QUERY_ARENA_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>

namespace svdb::ds {

/* QueryArena — Bump allocator for ephemeral query values
 *
 * Design:
 * - Pre-allocates chunks (64KB-256KB default)
 * - Bump pointer allocation (O(1), cache-friendly)
 * - No individual frees — Reset() frees everything
 * - Fallback to malloc for oversized allocations
 *
 * Memory Layout:
 *   [Chunk 0: 64KB] -> [Chunk 1: 64KB] -> ... -> [Oversize: malloc'd individually]
 */
class QueryArena {
public:
    // Default sizes tuned for typical SELECT queries
    static constexpr size_t kDefaultChunkSize = 64 * 1024;   // 64KB per chunk
    static constexpr size_t kDefaultMaxSize = 16 * 1024 * 1024; // 16MB max per query
    static constexpr size_t kMaxOversize = 1024 * 1024;  // Values >1MB use malloc

    explicit QueryArena(size_t chunk_size = kDefaultChunkSize,
                        size_t max_size = kDefaultMaxSize)
        : chunk_size_(chunk_size)
        , max_size_(max_size)
        , current_(nullptr)
        , offset_(0)
        , remaining_(0)
        , total_used_(0)
        , alloc_count_(0)
    {
        // Pre-allocate first chunk
        Grow();
    }

    ~QueryArena() {
        // Free all chunks
        for (auto& chunk : chunks_) {
            delete[] chunk;
        }
        // Free oversize allocations
        for (auto& ptr : oversize_) {
            delete[] ptr;
        }
    }

    // Non-copyable, non-movable
    QueryArena(const QueryArena&) = delete;
    QueryArena& operator=(const QueryArena&) = delete;
    QueryArena(QueryArena&&) = delete;
    QueryArena& operator=(QueryArena&&) = delete;

    /* Allocate bytes from arena
     * Returns pointer to aligned memory, or nullptr on failure.
     * For allocations > kMaxOversize, falls back to malloc.
     */
    void* Alloc(size_t size) {
        if (size == 0) return nullptr;

        // Align to 8 bytes for performance
        size = Align8(size);

        // Check max limit
        if (total_used_ + size > max_size_) {
            return nullptr;  // Memory limit exceeded
        }

        // Large allocations go to separate malloc'd blocks
        if (size > kMaxOversize) {
            char* ptr = new char[size];
            if (ptr) {
                oversize_.push_back(ptr);
                total_used_ += size;
                alloc_count_++;
            }
            return ptr;
        }

        // Need a new chunk?
        if (size > remaining_) {
            Grow();
            if (remaining_ < size) {
                return nullptr;  // Allocation failed
            }
        }

        // Bump allocation
        char* ptr = current_ + offset_;
        offset_ += size;
        remaining_ -= size;
        total_used_ += size;
        alloc_count_++;

        return ptr;
    }

    /* Allocate and copy a text string (null-terminated)
     * This is the primary function for TEXT value allocation.
     */
    char* AllocText(const char* str, size_t len) {
        if (!str || len == 0) return nullptr;

        // Allocate len + 1 for null terminator
        char* dst = (char*)Alloc(len + 1);
        if (!dst) return nullptr;

        // Copy string data
        memcpy(dst, str, len);
        dst[len] = '\0';

        return dst;
    }

    /* Allocate and copy a text string (C-string version) */
    char* AllocText(const char* str) {
        if (!str) return nullptr;
        return AllocText(str, strlen(str));
    }

    /* Allocate zeroed memory */
    void* Calloc(size_t size) {
        void* ptr = Alloc(size);
        if (ptr) {
            memset(ptr, 0, size);
        }
        return ptr;
    }

    /* Reset arena for reuse
     * Keeps allocated chunks for next query.
     * O(1) operation - just resets pointers.
     */
    void Reset() {
        // Reset to start of first chunk
        if (!chunks_.empty()) {
            current_ = chunks_[0];
            offset_ = 0;
            remaining_ = chunk_size_;
        }

        // Free oversize allocations
        for (auto& ptr : oversize_) {
            delete[] ptr;
        }
        oversize_.clear();

        total_used_ = 0;
        alloc_count_ = 0;
    }

    /* Shrink: release all but the first chunk */
    void Shrink() {
        while (chunks_.size() > 1) {
            delete[] chunks_.back();
            chunks_.pop_back();
        }
        Reset();
    }

    // Statistics
    size_t BytesUsed() const { return total_used_; }
    size_t ChunkCount() const { return chunks_.size(); }
    size_t AllocCount() const { return alloc_count_; }
    size_t Capacity() const {
        size_t cap = chunks_.size() * chunk_size_;
        for (auto& ptr : oversize_) {
            // We don't track individual oversize sizes, so estimate
            cap += kMaxOversize;  // Lower bound
        }
        return cap;
    }

private:
    void Grow() {
        char* new_chunk = new char[chunk_size_];
        if (!new_chunk) return;

        chunks_.push_back(new_chunk);
        current_ = new_chunk;
        offset_ = 0;
        remaining_ = chunk_size_;
    }

    static constexpr size_t Align8(size_t n) {
        return (n + 7) & ~((size_t)7);
    }

    // Configuration
    size_t chunk_size_;
    size_t max_size_;

    // Current chunk state
    char* current_;
    size_t offset_;
    size_t remaining_;

    // All chunks (owned)
    std::vector<char*> chunks_;

    // Oversize allocations (owned, individually malloc'd)
    std::vector<char*> oversize_;

    // Statistics
    size_t total_used_;
    size_t alloc_count_;
};

/* ThreadSafeQueryArena — QueryArena with atomic allocation
 * For use in parallel query execution contexts.
 */
class ThreadSafeQueryArena {
public:
    explicit ThreadSafeQueryArena(size_t chunk_size = QueryArena::kDefaultChunkSize,
                                   size_t max_size = QueryArena::kDefaultMaxSize)
        : arena_(chunk_size, max_size)
    {}

    void* Alloc(size_t size) {
        std::lock_guard<std::mutex> lock(mutex_);
        return arena_.Alloc(size);
    }

    char* AllocText(const char* str, size_t len) {
        std::lock_guard<std::mutex> lock(mutex_);
        return arena_.AllocText(str, len);
    }

    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        arena_.Reset();
    }

    size_t BytesUsed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return arena_.BytesUsed();
    }

private:
    QueryArena arena_;
    mutable std::mutex mutex_;
};

}  // namespace svdb::ds

/* ── C API for Go bindings ───────────────────────────────────────── */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct svdb_query_arena_s svdb_query_arena_t;

/* Create a query arena with default sizes (64KB chunks, 16MB max) */
svdb_query_arena_t* svdb_query_arena_create(void);

/* Create a query arena with custom sizes */
svdb_query_arena_t* svdb_query_arena_create_ex(size_t chunk_size, size_t max_size);

/* Destroy arena and free all memory */
void svdb_query_arena_destroy(svdb_query_arena_t* arena);

/* Allocate bytes from arena */
void* svdb_query_arena_alloc(svdb_query_arena_t* arena, size_t size);

/* Allocate and copy text string (null-terminated) */
char* svdb_query_arena_alloc_text(svdb_query_arena_t* arena, const char* str, size_t len);

/* Allocate zeroed memory */
void* svdb_query_arena_calloc(svdb_query_arena_t* arena, size_t size);

/* Reset arena for reuse (O(1)) */
void svdb_query_arena_reset(svdb_query_arena_t* arena);

/* Statistics */
size_t svdb_query_arena_bytes_used(svdb_query_arena_t* arena);
size_t svdb_query_arena_chunk_count(svdb_query_arena_t* arena);
size_t svdb_query_arena_alloc_count(svdb_query_arena_t* arena);

#ifdef __cplusplus
}
#endif

#endif  // SVDB_DS_QUERY_ARENA_H