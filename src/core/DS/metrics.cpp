/* metrics.cpp — Storage metrics implementation */
#include "metrics.h"
#include <string.h>

/* Collect metrics from page manager */
svdb_storage_metrics_t svdb_metrics_collect(int total_pages, int free_pages, double compression_ratio, int64_t wal_size) {
    svdb_storage_metrics_t metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    metrics.page_count = total_pages;
    metrics.free_pages = free_pages;
    metrics.used_pages = total_pages - free_pages;
    if (metrics.used_pages < 0) metrics.used_pages = 0;
    metrics.compression_ratio = compression_ratio > 0 ? compression_ratio : 1.0;
    metrics.wal_size = wal_size;
    metrics.last_checkpoint = time(NULL);
    metrics.total_rows = 0;
    metrics.total_tables = 0;
    
    return metrics;
}
