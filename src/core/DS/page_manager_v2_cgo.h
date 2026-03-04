// C-compatible wrapper for PageManagerV2
// This header exposes only C types to CGO
#ifndef SVDB_DS_PAGE_MANAGER_V2_CGO_H
#define SVDB_DS_PAGE_MANAGER_V2_CGO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle
typedef struct svdb_page_manager_v2_t svdb_page_manager_v2_t;
typedef struct svdb_arena_v2_t svdb_arena_v2_t;
typedef struct svdb_cache_v2_t svdb_cache_v2_t;
typedef struct svdb_freelist_v2_t svdb_freelist_v2_t;

// PageManagerV2 C API
svdb_page_manager_v2_t* svdb_pm_v2_create(const char* path, uint32_t page_size);
void svdb_pm_v2_destroy(svdb_page_manager_v2_t* pm);

uint8_t* svdb_pm_v2_read_page(svdb_page_manager_v2_t* pm, uint32_t page_num, size_t* out_size);
void svdb_pm_v2_write_page(svdb_page_manager_v2_t* pm, uint32_t page_num, const uint8_t* data, size_t size);

uint32_t svdb_pm_v2_allocate_page(svdb_page_manager_v2_t* pm);
void svdb_pm_v2_free_page(svdb_page_manager_v2_t* pm, uint32_t page_num);

uint32_t svdb_pm_v2_get_page_size(svdb_page_manager_v2_t* pm);
uint32_t svdb_pm_v2_get_page_count(svdb_page_manager_v2_t* pm);
uint64_t svdb_pm_v2_get_file_size(svdb_page_manager_v2_t* pm);

void svdb_pm_v2_sync(svdb_page_manager_v2_t* pm);
void svdb_pm_v2_close(svdb_page_manager_v2_t* pm);
int svdb_pm_v2_is_open(svdb_page_manager_v2_t* pm);

void svdb_pm_v2_clear_cache(svdb_page_manager_v2_t* pm);
size_t svdb_pm_v2_get_cache_size(svdb_page_manager_v2_t* pm);

// ArenaV2 C API
svdb_arena_v2_t* svdb_arena_v2_create(size_t chunk_size);
void svdb_arena_v2_destroy(svdb_arena_v2_t* arena);

void* svdb_arena_v2_alloc(svdb_arena_v2_t* arena, size_t size);
void* svdb_arena_v2_calloc(svdb_arena_v2_t* arena, size_t count, size_t size);
void svdb_arena_v2_reset(svdb_arena_v2_t* arena);

size_t svdb_arena_v2_bytes_used(svdb_arena_v2_t* arena);
size_t svdb_arena_v2_bytes_allocated(svdb_arena_v2_t* arena);
size_t svdb_arena_v2_chunk_count(svdb_arena_v2_t* arena);

// CacheV2 C API
svdb_cache_v2_t* svdb_cache_v2_create(int capacity);
void svdb_cache_v2_destroy(svdb_cache_v2_t* cache);

uint8_t* svdb_cache_v2_get(svdb_cache_v2_t* cache, uint32_t page_num, size_t* out_size);
void svdb_cache_v2_free_page(uint8_t* page);

void svdb_cache_v2_put(svdb_cache_v2_t* cache, uint32_t page_num, const uint8_t* data, size_t size);
void svdb_cache_v2_remove(svdb_cache_v2_t* cache, uint32_t page_num);
void svdb_cache_v2_clear(svdb_cache_v2_t* cache);

size_t svdb_cache_v2_size(svdb_cache_v2_t* cache);
size_t svdb_cache_v2_hits(svdb_cache_v2_t* cache);
size_t svdb_cache_v2_misses(svdb_cache_v2_t* cache);

// FreeListV2 C API
svdb_freelist_v2_t* svdb_freelist_v2_create(void);
void svdb_freelist_v2_destroy(svdb_freelist_v2_t* fl);

void svdb_freelist_v2_add(svdb_freelist_v2_t* fl, uint32_t page_num);
uint32_t svdb_freelist_v2_allocate(svdb_freelist_v2_t* fl);
size_t svdb_freelist_v2_count(svdb_freelist_v2_t* fl);
void svdb_freelist_v2_clear(svdb_freelist_v2_t* fl);

#ifdef __cplusplus
}
#endif

#endif  // SVDB_DS_PAGE_MANAGER_V2_CGO_H
