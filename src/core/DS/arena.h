/* arena.h — Arena allocator C API */
#pragma once
#ifndef SVDB_DS_ARENA_H
#define SVDB_DS_ARENA_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque arena handle */
typedef struct svdb_arena_s svdb_arena_t;

/* Create arena with initial size (0 = default 256KB) */
svdb_arena_t* svdb_arena_create(size_t initial_size);

/* Destroy arena and free all memory */
void svdb_arena_destroy(svdb_arena_t* arena);

/* Allocate size bytes from arena */
void* svdb_arena_alloc(svdb_arena_t* arena, size_t size);

/* Allocate and zero size bytes from arena */
void* svdb_arena_calloc(svdb_arena_t* arena, size_t size);

/* Reset arena (free all allocations, keep chunks for reuse) */
void svdb_arena_reset(svdb_arena_t* arena);

/* Get total bytes allocated (including reset allocations) */
size_t svdb_arena_total_used(svdb_arena_t* arena);

/* Get current used bytes (since last reset) */
size_t svdb_arena_used(svdb_arena_t* arena);

/* Get number of allocations (since last reset) */
size_t svdb_arena_num_allocs(svdb_arena_t* arena);

/* Allocate string (null-terminated copy) */
const char* svdb_arena_strdup(svdb_arena_t* arena, const char* str);

/* Allocate and copy memory */
void* svdb_arena_memdup(svdb_arena_t* arena, const void* ptr, size_t size);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_ARENA_H */
