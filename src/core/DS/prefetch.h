/* prefetch.h — Async prefetch C API */
#pragma once
#ifndef SVDB_DS_PREFETCH_H
#define SVDB_DS_PREFETCH_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque prefetcher handle */
typedef struct svdb_prefetcher_s svdb_prefetcher_t;

/* Create prefetcher with given degree (0 = default 64) */
svdb_prefetcher_t* svdb_prefetcher_create(int degree);

/* Destroy prefetcher */
void svdb_prefetcher_destroy(svdb_prefetcher_t* prefetcher);

/* Prefetch page asynchronously (non-blocking) */
void svdb_prefetcher_prefetch(svdb_prefetcher_t* prefetcher, uint32_t page_num);

/* Wait for all pending prefetches to complete */
void svdb_prefetcher_wait(svdb_prefetcher_t* prefetcher);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_PREFETCH_H */
