/* metrics.h — Storage metrics C API */
#pragma once
#ifndef SVDB_DS_METRICS_H
#define SVDB_DS_METRICS_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Storage metrics snapshot */
typedef struct {
    int page_count;
    int used_pages;
    int free_pages;
    double compression_ratio;
    int64_t wal_size;
    time_t last_checkpoint;
    int total_rows;
    int total_tables;
} svdb_storage_metrics_t;

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_METRICS_H */
