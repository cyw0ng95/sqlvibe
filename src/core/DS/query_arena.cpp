/* query_arena.cpp — QueryArena C API implementation */
#include "query_arena.h"
#include <new>

/* ── C API wrapper ───────────────────────────────────────────── */

struct svdb_query_arena_s {
    svdb::ds::QueryArena arena;
};

extern "C" {

svdb_query_arena_t* svdb_query_arena_create(void) {
    return svdb_query_arena_create_ex(
        svdb::ds::QueryArena::kDefaultChunkSize,
        svdb::ds::QueryArena::kDefaultMaxSize
    );
}

svdb_query_arena_t* svdb_query_arena_create_ex(size_t chunk_size, size_t max_size) {
    svdb_query_arena_t* wrapper = new (std::nothrow) svdb_query_arena_t;
    if (!wrapper) return nullptr;

    // Placement new to construct arena with custom sizes
    new (&wrapper->arena) svdb::ds::QueryArena(chunk_size, max_size);
    return wrapper;
}

void svdb_query_arena_destroy(svdb_query_arena_t* arena) {
    if (arena) {
        arena->arena.~QueryArena();
        delete arena;
    }
}

void* svdb_query_arena_alloc(svdb_query_arena_t* arena, size_t size) {
    if (!arena) return nullptr;
    return arena->arena.Alloc(size);
}

char* svdb_query_arena_alloc_text(svdb_query_arena_t* arena, const char* str, size_t len) {
    if (!arena) return nullptr;
    return arena->arena.AllocText(str, len);
}

void* svdb_query_arena_calloc(svdb_query_arena_t* arena, size_t size) {
    if (!arena) return nullptr;
    return arena->arena.Calloc(size);
}

void svdb_query_arena_reset(svdb_query_arena_t* arena) {
    if (arena) {
        arena->arena.Reset();
    }
}

size_t svdb_query_arena_bytes_used(svdb_query_arena_t* arena) {
    return arena ? arena->arena.BytesUsed() : 0;
}

size_t svdb_query_arena_chunk_count(svdb_query_arena_t* arena) {
    return arena ? arena->arena.ChunkCount() : 0;
}

size_t svdb_query_arena_alloc_count(svdb_query_arena_t* arena) {
    return arena ? arena->arena.AllocCount() : 0;
}

}  // extern "C"