/* slab.cpp — Slab allocator implementation */
#include "slab.h"
#include <stdlib.h>
#include <string.h>
#include <vector>

#define SVDB_SLAB_SIZE (64 * 1024)
#define SVDB_MAX_SLABS 16
#define SVDB_SMALL_SLAB (4 * 1024)

struct svdb_slab_alloc_s {
    std::vector<char*> slabs;
    char* current;
    size_t offset;
    char* small_pool[64];  /* Simple fixed-size pool */
    int small_pool_count;
    svdb_slab_stats_t stats;
};

svdb_slab_alloc_t* svdb_slab_alloc_create(void) {
    svdb_slab_alloc_t* sa = (svdb_slab_alloc_t*)calloc(1, sizeof(svdb_slab_alloc_t));
    if (!sa) return nullptr;
    
    sa->current = (char*)malloc(SVDB_SLAB_SIZE);
    if (!sa->current) {
        free(sa);
        return nullptr;
    }
    
    sa->offset = 0;
    sa->small_pool_count = 0;
    sa->stats = {};
    
    return sa;
}

void svdb_slab_alloc_destroy(svdb_slab_alloc_t* sa) {
    if (!sa) return;
    
    /* Free current slab */
    if (sa->current) free(sa->current);
    
    /* Free stored slabs */
    for (size_t i = 0; i < sa->slabs.size(); i++) {
        free(sa->slabs[i]);
    }
    
    /* Free small pool */
    for (int i = 0; i < sa->small_pool_count; i++) {
        if (sa->small_pool[i]) free(sa->small_pool[i]);
    }
    
    free(sa);
}

void* svdb_slab_alloc(svdb_slab_alloc_t* sa, size_t size) {
    if (!sa || size == 0) return nullptr;
    
    sa->stats.total_allocs++;
    sa->stats.bytes_allocated += (int64_t)size;
    
    /* Small allocations from pool */
    if (size <= SVDB_SMALL_SLAB / 4) {
        if (sa->small_pool_count > 0) {
            sa->small_pool_count--;
            sa->stats.pool_hits++;
            return sa->small_pool[sa->small_pool_count];
        }
        /* Allocate new small buffer */
        char* buf = (char*)malloc(SVDB_SMALL_SLAB);
        if (!buf) return nullptr;
        sa->stats.pool_hits++;
        return buf;
    }
    
    /* Try current slab */
    if (sa->offset + size <= SVDB_SLAB_SIZE) {
        void* ptr = sa->current + sa->offset;
        sa->offset += size;
        sa->stats.slab_hits++;
        return ptr;
    }
    
    /* Need new slab */
    if (size > SVDB_SLAB_SIZE) {
        sa->stats.misses++;
        return malloc(size);
    }
    
    if (sa->slabs.size() >= SVDB_MAX_SLABS - 1) {
        sa->stats.misses++;
        return malloc(size);
    }
    
    /* Store current and allocate new */
    sa->slabs.push_back(sa->current);
    sa->current = (char*)malloc(SVDB_SLAB_SIZE);
    if (!sa->current) return nullptr;
    
    sa->offset = size;
    sa->stats.slab_hits++;
    return sa->current;
}

void svdb_slab_alloc_reset(svdb_slab_alloc_t* sa) {
    if (!sa) return;
    sa->offset = 0;
    /* Keep slabs for reuse */
}

svdb_slab_stats_t svdb_slab_alloc_stats(svdb_slab_alloc_t* sa) {
    if (!sa) return {};
    return sa->stats;
}

int64_t* svdb_slab_alloc_int64_array(svdb_slab_alloc_t* sa, size_t n) {
    if (n == 0) return nullptr;
    return (int64_t*)svdb_slab_alloc(sa, n * sizeof(int64_t));
}

double* svdb_slab_alloc_double_array(svdb_slab_alloc_t* sa, size_t n) {
    if (n == 0) return nullptr;
    return (double*)svdb_slab_alloc(sa, n * sizeof(double));
}
