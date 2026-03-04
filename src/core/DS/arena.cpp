/* arena.cpp — Arena allocator implementation */
#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <vector>

/* Default chunk size: 256KB */
#define SVDB_ARENA_CHUNK_SIZE (256 * 1024)

struct svdb_arena_s {
    std::vector<char*> chunks;
    char* current;
    size_t offset;
    size_t used;
    size_t total_used;
    size_t num_allocs;
};

svdb_arena_t* svdb_arena_create(size_t initial_size) {
    if (initial_size == 0) {
        initial_size = SVDB_ARENA_CHUNK_SIZE;
    }
    
    svdb_arena_t* arena = (svdb_arena_t*)calloc(1, sizeof(svdb_arena_t));
    if (!arena) return nullptr;
    
    char* chunk = (char*)malloc(initial_size);
    if (!chunk) {
        free(arena);
        return nullptr;
    }
    
    arena->chunks.push_back(chunk);
    arena->current = chunk;
    arena->offset = 0;
    arena->used = 0;
    arena->total_used = 0;
    arena->num_allocs = 0;
    
    return arena;
}

void svdb_arena_destroy(svdb_arena_t* arena) {
    if (!arena) return;
    
    for (size_t i = 0; i < arena->chunks.size(); i++) {
        free(arena->chunks[i]);
    }
    
    free(arena);
}

void* svdb_arena_alloc(svdb_arena_t* arena, size_t size) {
    if (!arena || size == 0) return nullptr;
    
    /* Check if current chunk has enough space */
    if (arena->offset + size > SVDB_ARENA_CHUNK_SIZE) {
        /* Need new chunk */
        size_t chunk_size = (size > SVDB_ARENA_CHUNK_SIZE) ? size : SVDB_ARENA_CHUNK_SIZE;
        char* chunk = (char*)malloc(chunk_size);
        if (!chunk) return nullptr;
        
        arena->chunks.push_back(chunk);
        arena->current = chunk;
        arena->offset = 0;
    }
    
    void* ptr = arena->current + arena->offset;
    arena->offset += size;
    arena->used += size;
    arena->total_used += size;
    arena->num_allocs++;
    
    return ptr;
}

void* svdb_arena_calloc(svdb_arena_t* arena, size_t size) {
    void* ptr = svdb_arena_alloc(arena, size);
    if (ptr) {
        memset(ptr, 0, size);
    }
    return ptr;
}

void svdb_arena_reset(svdb_arena_t* arena) {
    if (!arena) return;
    
    /* Keep chunks but reset offset */
    if (!arena->chunks.empty()) {
        arena->current = arena->chunks[0];
    }
    arena->offset = 0;
    arena->used = 0;
    /* total_used and num_allocs are NOT reset */
}

size_t svdb_arena_total_used(svdb_arena_t* arena) {
    return arena ? arena->total_used : 0;
}

size_t svdb_arena_used(svdb_arena_t* arena) {
    return arena ? arena->used : 0;
}

size_t svdb_arena_num_allocs(svdb_arena_t* arena) {
    return arena ? arena->num_allocs : 0;
}

const char* svdb_arena_strdup(svdb_arena_t* arena, const char* str) {
    if (!arena || !str) return nullptr;
    size_t len = strlen(str);
    char* dup = (char*)svdb_arena_alloc(arena, len + 1);
    if (dup) {
        memcpy(dup, str, len + 1);
    }
    return dup;
}

void* svdb_arena_memdup(svdb_arena_t* arena, const void* ptr, size_t size) {
    if (!arena || !ptr || size == 0) return nullptr;
    void* dup = svdb_arena_alloc(arena, size);
    if (dup) {
        memcpy(dup, ptr, size);
    }
    return dup;
}
