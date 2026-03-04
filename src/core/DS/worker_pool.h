/* worker_pool.h — Worker pool C API */
#pragma once
#ifndef SVDB_DS_WORKER_POOL_H
#define SVDB_DS_WORKER_POOL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque worker pool handle */
typedef struct svdb_worker_pool_s svdb_worker_pool_t;

/* Task function type */
typedef void (*svdb_task_fn)(void* user_data);

/* Create worker pool with given number of workers */
svdb_worker_pool_t* svdb_worker_pool_create(int workers);

/* Destroy worker pool (waits for pending tasks) */
void svdb_worker_pool_destroy(svdb_worker_pool_t* pool);

/* Submit task for execution */
void svdb_worker_pool_submit(svdb_worker_pool_t* pool, svdb_task_fn task, void* user_data);

/* Wait for all submitted tasks to complete */
void svdb_worker_pool_wait(svdb_worker_pool_t* pool);

/* Close pool (no more tasks accepted) */
void svdb_worker_pool_close(svdb_worker_pool_t* pool);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_DS_WORKER_POOL_H */
