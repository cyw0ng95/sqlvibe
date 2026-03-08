/* parallel_scan.cpp — Parallel table scan implementation */
#include "parallel_scan.h"
#include "simd.h"
#include "../SF/svdb_assert.h"

#include <cstring>
#include <cstdlib>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>

extern "C" {

/* ============================================================================
 * Parallel Scan Context
 * ============================================================================ */

struct svdb_parallel_scan_s {
    svdb_worker_pool_t* pool;
    std::atomic<int> pending_chunks;
    std::atomic<int> error_count;
    std::mutex mutex;

    svdb_parallel_scan_s(svdb_worker_pool_t* p)
        : pool(p), pending_chunks(0), error_count(0) {}
};

/* Chunk scan task data */
struct ChunkScanTask {
    svdb_parallel_scan_t* scan;
    int64_t start_row;
    int64_t end_row;
    int (*get_row_fn)(void*, int64_t, int64_t*, const void**, size_t*);
    int (*filter_fn)(void*, int64_t, const void*, size_t);
    void (*result_fn)(void*, const svdb_parallel_row_t*, size_t);
    void* user_data;
};

/* Execute a chunk scan */
static void execute_chunk_scan(void* user_data) {
    ChunkScanTask* task = static_cast<ChunkScanTask*>(user_data);
    if (!task) return;

    svdb_scan_result_buffer_t* buffer = svdb_scan_result_buffer_create(1024);
    if (!buffer) {
        task->scan->error_count++;
        delete task;
        task->scan->pending_chunks--;
        return;
    }

    /* Scan rows in this chunk */
    for (int64_t idx = task->start_row; idx < task->end_row; idx++) {
        int64_t rowid;
        const void* data;
        size_t data_len;

        if (task->get_row_fn(task->user_data, idx, &rowid, &data, &data_len) != 0) {
            continue;  /* Skip failed row */
        }

        /* Apply filter */
        if (task->filter_fn && task->filter_fn(task->user_data, rowid, data, data_len) == 0) {
            continue;  /* Filtered out */
        }

        /* Add to result buffer */
        if (svdb_scan_result_buffer_add(buffer, rowid, data, data_len) != 0) {
            break;  /* Out of memory */
        }
    }

    /* Emit results */
    if (task->result_fn && buffer->count > 0) {
        task->result_fn(task->user_data, buffer->rows, buffer->count);
    }

    svdb_scan_result_buffer_destroy(buffer);
    task->scan->pending_chunks--;
    delete task;
}

svdb_parallel_scan_t* svdb_parallel_scan_create(svdb_worker_pool_t* pool) {
    if (!pool) return nullptr;

    try {
        return new svdb_parallel_scan_t(pool);
    } catch (...) {
        return nullptr;
    }
}

void svdb_parallel_scan_destroy(svdb_parallel_scan_t* scan) {
    delete scan;
}

int svdb_parallel_scan_execute(svdb_parallel_scan_t* scan,
                                int64_t row_count,
                                int (*get_row_fn)(void*, int64_t, int64_t*, const void**, size_t*),
                                const svdb_parallel_scan_config_t* config) {
    if (!scan || !get_row_fn || !config) return -1;
    if (row_count <= 0) return 0;

    int num_workers = config->num_workers > 0 ? config->num_workers : 4;
    int chunk_size = config->chunk_size > 0 ? config->chunk_size : 1000;

    /* Calculate number of chunks */
    int64_t num_chunks = (row_count + chunk_size - 1) / chunk_size;

    scan->pending_chunks = static_cast<int>(num_chunks);
    scan->error_count = 0;

    /* Submit chunk scan tasks */
    for (int64_t chunk = 0; chunk < num_chunks; chunk++) {
        ChunkScanTask* task = new (std::nothrow) ChunkScanTask();
        if (!task) {
            scan->error_count++;
            scan->pending_chunks--;
            continue;
        }

        task->scan = scan;
        task->start_row = chunk * chunk_size;
        task->end_row = std::min((chunk + 1) * chunk_size, row_count);
        task->get_row_fn = get_row_fn;
        task->filter_fn = config->filter_fn;
        task->result_fn = config->result_fn;
        task->user_data = config->user_data;

        svdb_worker_pool_submit(scan->pool, execute_chunk_scan, task);
    }

    return 0;
}

void svdb_parallel_scan_wait(svdb_parallel_scan_t* scan) {
    if (!scan) return;

    while (scan->pending_chunks > 0) {
        /* Yield to avoid busy spinning */
        std::this_thread::yield();
    }
}

/* ============================================================================
 * Hash Join Context
 * ============================================================================ */

struct svdb_hash_join_s {
    /* Hash table using open addressing */
    struct Entry {
        int64_t rowid;
        uint64_t hash;
        size_t key_len;
        std::vector<uint8_t> key;

        Entry() : rowid(0), hash(0), key_len(0) {}
    };

    std::vector<Entry> table;
    std::vector<int64_t> rowid_lists;  /* For collisions */
    std::atomic<int64_t> entry_count;
    int capacity_mask;
    std::mutex mutex;

    svdb_hash_join_s(int capacity = 65536) : entry_count(0) {
        /* Round up to power of 2 */
        int cap = 1;
        while (cap < capacity) cap <<= 1;
        table.resize(cap);
        capacity_mask = cap - 1;
    }
};

svdb_hash_join_t* svdb_hash_join_create(int num_workers) {
    (void)num_workers;  /* Currently single hash table */
    try {
        return new svdb_hash_join_t();
    } catch (...) {
        return nullptr;
    }
}

void svdb_hash_join_destroy(svdb_hash_join_t* hj) {
    delete hj;
}

int svdb_hash_join_build(svdb_hash_join_t* hj,
                          int64_t rowid,
                          const void* key,
                          size_t key_len) {
    if (!hj || !key) return -1;

    /* Compute hash */
    uint64_t hash = svdb_crc32_bytes(key, key_len, 0);

    /* Find slot using linear probing */
    size_t idx = hash & hj->capacity_mask;
    size_t start_idx = idx;

    std::lock_guard<std::mutex> lock(hj->mutex);

    while (true) {
        if (hj->table[idx].key_len == 0) {
            /* Empty slot - insert here */
            hj->table[idx].rowid = rowid;
            hj->table[idx].hash = hash;
            hj->table[idx].key_len = key_len;
            hj->table[idx].key.assign(static_cast<const uint8_t*>(key),
                                       static_cast<const uint8_t*>(key) + key_len);
            hj->entry_count++;
            return 0;
        }

        /* Check if same key */
        if (hj->table[idx].hash == hash &&
            hj->table[idx].key_len == key_len &&
            memcmp(hj->table[idx].key.data(), key, key_len) == 0) {
            /* Duplicate key - could store multiple rowids, but for now just overwrite */
            /* In a real implementation, we'd maintain a list of rowids */
            return 0;
        }

        /* Linear probe */
        idx = (idx + 1) & hj->capacity_mask;
        if (idx == start_idx) {
            /* Table full - would need to resize */
            return -1;
        }
    }
}

int svdb_hash_join_probe(svdb_hash_join_t* hj,
                          const void* key,
                          size_t key_len,
                          int64_t* matching_rowids,
                          int max_matches) {
    if (!hj || !key || !matching_rowids || max_matches <= 0) return 0;

    uint64_t hash = svdb_crc32_bytes(key, key_len, 0);
    size_t idx = hash & hj->capacity_mask;
    size_t start_idx = idx;
    int matches = 0;

    while (true) {
        if (hj->table[idx].key_len == 0) {
            /* Empty slot - key not found */
            break;
        }

        if (hj->table[idx].hash == hash &&
            hj->table[idx].key_len == key_len &&
            memcmp(hj->table[idx].key.data(), key, key_len) == 0) {
            /* Found match */
            if (matches < max_matches) {
                matching_rowids[matches] = hj->table[idx].rowid;
                matches++;
            }
            /* For now, only one match per key - in real impl, we'd find all */
            break;
        }

        idx = (idx + 1) & hj->capacity_mask;
        if (idx == start_idx) break;
    }

    return matches;
}

int64_t svdb_hash_join_entry_count(svdb_hash_join_t* hj) {
    return hj ? hj->entry_count.load() : 0;
}

/* ============================================================================
 * Parallel Aggregation
 * ============================================================================ */

struct svdb_agg_s {
    svdb_agg_type_t type;
    int is_int;
    std::atomic<int64_t> count;
    std::atomic<int64_t> int_val;
    std::atomic<double> dbl_val;  /* Note: atomic double may not be lock-free */
    std::mutex mutex;  /* For double operations */

    svdb_agg_s(svdb_agg_type_t t, int i) : type(t), is_int(i), count(0), int_val(0), dbl_val(0.0) {}
};

svdb_agg_t* svdb_agg_create(svdb_agg_type_t type, int is_int) {
    try {
        return new svdb_agg_t(type, is_int);
    } catch (...) {
        return nullptr;
    }
}

void svdb_agg_destroy(svdb_agg_t* agg) {
    delete agg;
}

void svdb_agg_update_int(svdb_agg_t* agg, int64_t value) {
    if (!agg || !agg->is_int) return;

    agg->count++;

    switch (agg->type) {
        case SVDB_AGG_COUNT:
            /* count is already incremented */
            break;
        case SVDB_AGG_SUM:
            agg->int_val += value;
            break;
        case SVDB_AGG_MIN:
            {
                int64_t current = agg->int_val.load();
                while (value < current &&
                       !agg->int_val.compare_exchange_weak(current, value)) {}
            }
            break;
        case SVDB_AGG_MAX:
            {
                int64_t current = agg->int_val.load();
                while (value > current &&
                       !agg->int_val.compare_exchange_weak(current, value)) {}
            }
            break;
        case SVDB_AGG_AVG:
            agg->int_val += value;
            break;
    }
}

void svdb_agg_update_dbl(svdb_agg_t* agg, double value) {
    if (!agg || agg->is_int) return;

    std::lock_guard<std::mutex> lock(agg->mutex);

    agg->count++;

    switch (agg->type) {
        case SVDB_AGG_COUNT:
            break;
        case SVDB_AGG_SUM:
        case SVDB_AGG_AVG:
            agg->dbl_val = agg->dbl_val.load() + value;
            break;
        case SVDB_AGG_MIN:
            {
                double current = agg->dbl_val.load();
                if (value < current || agg->count == 1) {
                    agg->dbl_val = value;
                }
            }
            break;
        case SVDB_AGG_MAX:
            {
                double current = agg->dbl_val.load();
                if (value > current || agg->count == 1) {
                    agg->dbl_val = value;
                }
            }
            break;
    }
}

void svdb_agg_merge(svdb_agg_t* dest, const svdb_agg_t* src) {
    if (!dest || !src || dest->type != src->type || dest->is_int != src->is_int) return;

    dest->count += src->count.load();

    if (dest->is_int) {
        switch (dest->type) {
            case SVDB_AGG_SUM:
            case SVDB_AGG_AVG:
                dest->int_val += src->int_val.load();
                break;
            case SVDB_AGG_MIN:
                {
                    int64_t s = src->int_val.load();
                    int64_t d = dest->int_val.load();
                    if (s < d) dest->int_val = s;
                }
                break;
            case SVDB_AGG_MAX:
                {
                    int64_t s = src->int_val.load();
                    int64_t d = dest->int_val.load();
                    if (s > d) dest->int_val = s;
                }
                break;
            default:
                break;
        }
    } else {
        std::lock_guard<std::mutex> lock(dest->mutex);
        switch (dest->type) {
            case SVDB_AGG_SUM:
            case SVDB_AGG_AVG:
                dest->dbl_val = dest->dbl_val.load() + src->dbl_val.load();
                break;
            case SVDB_AGG_MIN:
                {
                    double s = src->dbl_val.load();
                    double d = dest->dbl_val.load();
                    if (s < d) dest->dbl_val = s;
                }
                break;
            case SVDB_AGG_MAX:
                {
                    double s = src->dbl_val.load();
                    double d = dest->dbl_val.load();
                    if (s > d) dest->dbl_val = s;
                }
                break;
            default:
                break;
        }
    }
}

int64_t svdb_agg_result_int(svdb_agg_t* agg) {
    if (!agg) return 0;

    switch (agg->type) {
        case SVDB_AGG_COUNT:
            return agg->count.load();
        case SVDB_AGG_SUM:
        case SVDB_AGG_MIN:
        case SVDB_AGG_MAX:
            return agg->int_val.load();
        case SVDB_AGG_AVG:
            {
                int64_t c = agg->count.load();
                return c > 0 ? agg->int_val.load() / c : 0;
            }
    }
    return 0;
}

double svdb_agg_result_dbl(svdb_agg_t* agg) {
    if (!agg) return 0.0;

    switch (agg->type) {
        case SVDB_AGG_COUNT:
            return static_cast<double>(agg->count.load());
        case SVDB_AGG_SUM:
        case SVDB_AGG_MIN:
        case SVDB_AGG_MAX:
            return agg->dbl_val.load();
        case SVDB_AGG_AVG:
            {
                int64_t c = agg->count.load();
                return c > 0 ? agg->dbl_val.load() / static_cast<double>(c) : 0.0;
            }
    }
    return 0.0;
}

/* ============================================================================
 * Result Buffer
 * ============================================================================ */

svdb_scan_result_buffer_t* svdb_scan_result_buffer_create(size_t initial_capacity) {
    try {
        svdb_scan_result_buffer_t* buf = new svdb_scan_result_buffer_t();
        buf->rows = new svdb_parallel_row_t[initial_capacity];
        buf->count = 0;
        buf->capacity = initial_capacity;
        return buf;
    } catch (...) {
        return nullptr;
    }
}

void svdb_scan_result_buffer_destroy(svdb_scan_result_buffer_t* buf) {
    if (!buf) return;
    delete[] buf->rows;
    delete buf;
}

int svdb_scan_result_buffer_add(svdb_scan_result_buffer_t* buf,
                                 int64_t rowid,
                                 const void* data,
                                 size_t data_len) {
    if (!buf) return -1;

    /* Grow if needed */
    if (buf->count >= buf->capacity) {
        size_t new_capacity = buf->capacity * 2;
        svdb_parallel_row_t* new_rows = new (std::nothrow) svdb_parallel_row_t[new_capacity];
        if (!new_rows) return -1;

        memcpy(new_rows, buf->rows, buf->count * sizeof(svdb_parallel_row_t));
        delete[] buf->rows;
        buf->rows = new_rows;
        buf->capacity = new_capacity;
    }

    buf->rows[buf->count].rowid = rowid;
    buf->rows[buf->count].data = data;
    buf->rows[buf->count].data_len = data_len;
    buf->count++;

    return 0;
}

void svdb_scan_result_buffer_clear(svdb_scan_result_buffer_t* buf) {
    if (buf) buf->count = 0;
}

int svdb_scan_result_buffer_merge(svdb_scan_result_buffer_t* dest,
                                   const svdb_scan_result_buffer_t* const* srcs,
                                   size_t num_srcs) {
    if (!dest || !srcs) return -1;

    for (size_t i = 0; i < num_srcs; i++) {
        const svdb_scan_result_buffer_t* src = srcs[i];
        if (!src) continue;

        for (size_t j = 0; j < src->count; j++) {
            if (svdb_scan_result_buffer_add(dest,
                                             src->rows[j].rowid,
                                             src->rows[j].data,
                                             src->rows[j].data_len) != 0) {
                return -1;
            }
        }
    }

    return 0;
}

} // extern "C"