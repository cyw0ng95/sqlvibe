/* slab.h — Slab allocator C API */
#pragma once
#ifndef SVDB_DS_SLAB_H
#define SVDB_DS_SLAB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque slab allocator handle */
typedef struct svdb_slab_alloc_s svdb_slab_alloc_t;

/* Statistics */
typedef struct {
    int64_t total_allocs;
    int64_t slab_hits;
    int64_t pool_hits;
    int64_t misses;
    int64_t bytes_allocated;
} svdb_slab_stats_t;

/* Create slab allocator */
svdb_slab_alloc_t* svdb_slab_alloc_create(void);

/* Destroy slab allocator */
void svdb_slab_alloc_destroy(svdb_slab_alloc_t* sa);

/* Allocate size bytes from slab */
void* svdb_slab_alloc(svdb_slab_alloc_t* sa, size_t size);

/* Reset allocator (keep slabs for reuse) */
void svdb_slab_alloc_reset(svdb_slab_alloc_t* sa);

/* Get statistics */
svdb_slab_stats_t svdb_slab_alloc_stats(svdb_slab_alloc_t* sa);

/* Allocate array of int64_t */
int64_t* svdb_slab_alloc_int64_array(svdb_slab_alloc_t* sa, size_t n);

/* Allocate array of double */
double* svdb_slab_alloc_double_array(svdb_slab_alloc_t* sa, size_t n);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_SLAB_H */
