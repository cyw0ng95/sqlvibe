#ifndef SVDB_TM_MVCC_H
#define SVDB_TM_MVCC_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque MVCC store handle. */
typedef struct svdb_mvcc_store_t svdb_mvcc_store_t;

/* Opaque snapshot handle. */
typedef struct svdb_mvcc_snapshot_t svdb_mvcc_snapshot_t;

/* Versioned value for C API. */
typedef struct {
    uint64_t commit_id;
    bool deleted;
    const void* data;
    size_t data_len;
} svdb_versioned_value_t;

/* Create a new MVCC store. */
svdb_mvcc_store_t* svdb_mvcc_store_create(void);

/* Destroy an MVCC store. */
void svdb_mvcc_store_destroy(svdb_mvcc_store_t* store);

/* Create a snapshot at the current commit ID. */
svdb_mvcc_snapshot_t* svdb_mvcc_store_snapshot(svdb_mvcc_store_t* store);

/* Free a snapshot. */
void svdb_mvcc_snapshot_free(svdb_mvcc_snapshot_t* snapshot);

/*
 * Get value for key under snapshot visibility.
 * Returns 1 if found (data/data_len set), 0 if not found or deleted.
 * Returned data pointer is valid until next store mutation.
 */
int svdb_mvcc_store_get(
    svdb_mvcc_store_t* store,
    svdb_mvcc_snapshot_t* snapshot,
    const char* key,
    size_t key_len,
    const char** data,
    size_t* data_len
);

/*
 * Put a new version for key.
 * Returns the new commit ID.
 * Caller must ensure concurrent writes to same key are serialized.
 */
uint64_t svdb_mvcc_store_put(
    svdb_mvcc_store_t* store,
    const char* key,
    size_t key_len,
    const void* data,
    size_t data_len
);

/*
 * Mark key as deleted.
 * Returns the new commit ID.
 */
uint64_t svdb_mvcc_store_delete(
    svdb_mvcc_store_t* store,
    const char* key,
    size_t key_len
);

/*
 * Garbage collect old versions.
 * keep_below: minimum commit ID to retain (except last version per key).
 * Returns number of versions pruned.
 */
size_t svdb_mvcc_store_gc(svdb_mvcc_store_t* store, uint64_t keep_below);

/* Get current commit ID. */
uint64_t svdb_mvcc_store_commit_id(svdb_mvcc_store_t* store);

/* Get number of keys in store. */
size_t svdb_mvcc_store_key_count(svdb_mvcc_store_t* store);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_TM_MVCC_H */
