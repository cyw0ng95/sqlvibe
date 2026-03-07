/* io_uring.h — io_uring based async I/O for Linux
 *
 * This provides a high-performance async I/O interface using Linux io_uring.
 * Benefits over traditional I/O:
 *   - Batched submissions and completions
 *   - Zero syscalls per I/O operation (with SQPOLL)
 *   - Vectored I/O for scatter-gather operations
 *   - True async (non-blocking) file operations
 */
#ifndef SVDB_PB_IO_URING_H
#define SVDB_PB_IO_URING_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* io_uring configuration */
#define SVDB_IO_URING_DEFAULT_QUEUE_DEPTH  256
#define SVDB_IO_URING_MAX_BATCH            64

/* Completion callback type */
typedef void (*svdb_io_uring_cb)(void* user_data, int result, uint64_t user_tag);

/* Opaque io_uring handle */
typedef struct svdb_io_uring_s svdb_io_uring_t;

/* Opaque async file handle */
typedef struct svdb_io_file_s svdb_io_file_t;

/* ============================================================================
 * io_uring instance management
 * ============================================================================ */

/*
 * Create an io_uring instance.
 * queue_depth: number of submission queue entries (power of 2 recommended)
 * use_sqpoll: enable kernel polling thread for zero-syscall operation
 * Returns NULL on failure.
 */
svdb_io_uring_t* svdb_io_uring_create(uint32_t queue_depth, int use_sqpoll);

/*
 * Destroy the io_uring instance.
 * Waits for all pending operations to complete.
 */
void svdb_io_uring_destroy(svdb_io_uring_t* ring);

/*
 * Submit all queued I/O requests to the kernel.
 * Returns number of submissions, or negative on error.
 */
int svdb_io_uring_submit(svdb_io_uring_t* ring);

/*
 * Submit and wait for at least 'wait_nr' completions.
 * Returns number of completions processed, or negative on error.
 */
int svdb_io_uring_submit_and_wait(svdb_io_uring_t* ring, uint32_t wait_nr);

/*
 * Reap completed operations.
 * Returns number of completions processed.
 * Calls callback for each completed operation.
 */
int svdb_io_uring_reap(svdb_io_uring_t* ring,
                       void (*callback)(uint64_t tag, int result, void* user_data),
                       void* user_data);

/*
 * Check if io_uring is available on this system.
 * Returns 1 if available, 0 otherwise.
 */
int svdb_io_uring_available(void);

/* ============================================================================
 * Async file operations
 * ============================================================================ */

/*
 * Open a file for async I/O.
 * Returns NULL on failure.
 */
svdb_io_file_t* svdb_io_uring_open_file(svdb_io_uring_t* ring,
                                         const char* path,
                                         int read_write,
                                         int create);

/*
 * Close an async file.
 */
void svdb_io_uring_close_file(svdb_io_file_t* file);

/*
 * Get the underlying file descriptor.
 */
int svdb_io_uring_file_fd(svdb_io_file_t* file);

/*
 * Get file size.
 */
int64_t svdb_io_uring_file_size(svdb_io_file_t* file);

/* ============================================================================
 * Async I/O operations (non-blocking)
 * ============================================================================ */

/*
 * Async read at offset.
 * tag: user-provided value returned on completion
 * Returns 0 on success (submission), negative on error.
 */
int svdb_io_uring_read(svdb_io_uring_t* ring,
                       svdb_io_file_t* file,
                       void* buffer,
                       size_t len,
                       int64_t offset,
                       uint64_t tag);

/*
 * Async write at offset.
 * tag: user-provided value returned on completion
 * Returns 0 on success (submission), negative on error.
 */
int svdb_io_uring_write(svdb_io_uring_t* ring,
                        svdb_io_file_t* file,
                        const void* data,
                        size_t len,
                        int64_t offset,
                        uint64_t tag);

/*
 * Async vectored read (scatter-gather).
 * iov_count: number of iovec structures
 * tag: user-provided value returned on completion
 */
int svdb_io_uring_readv(svdb_io_uring_t* ring,
                        svdb_io_file_t* file,
                        const struct iovec* iov,
                        size_t iov_count,
                        int64_t offset,
                        uint64_t tag);

/*
 * Async vectored write (scatter-gather).
 */
int svdb_io_uring_writev(svdb_io_uring_t* ring,
                         svdb_io_file_t* file,
                         const struct iovec* iov,
                         size_t iov_count,
                         int64_t offset,
                         uint64_t tag);

/*
 * Async fsync.
 */
int svdb_io_uring_fsync(svdb_io_uring_t* ring,
                        svdb_io_file_t* file,
                        uint64_t tag);

/* ============================================================================
 * Batch operations for WAL commit optimization
 * ============================================================================ */

/*
 * Batch write context for accumulating multiple writes.
 */
typedef struct svdb_io_batch_s svdb_io_batch_t;

/*
 * Create a batch context for accumulating I/O operations.
 */
svdb_io_batch_t* svdb_io_batch_create(svdb_io_uring_t* ring);

/*
 * Destroy a batch context.
 */
void svdb_io_batch_destroy(svdb_io_batch_t* batch);

/*
 * Add a write to the batch.
 * The data is copied into the batch buffer.
 */
int svdb_io_batch_add_write(svdb_io_batch_t* batch,
                            svdb_io_file_t* file,
                            const void* data,
                            size_t len,
                            int64_t offset,
                            uint64_t tag);

/*
 * Submit all operations in the batch as a chain.
 * If linked is non-zero, operations are linked (all-or-nothing).
 * Returns number of operations submitted.
 */
int svdb_io_batch_submit(svdb_io_batch_t* batch, int linked);

/*
 * Clear the batch for reuse.
 */
void svdb_io_batch_clear(svdb_io_batch_t* batch);

/*
 * Get number of operations in the batch.
 */
size_t svdb_io_batch_count(svdb_io_batch_t* batch);

/* ============================================================================
 * Convenience synchronous wrappers (for migration compatibility)
 * ============================================================================ */

/*
 * Synchronous read (blocks until complete).
 * Returns bytes read, or negative on error.
 */
int64_t svdb_io_uring_read_sync(svdb_io_uring_t* ring,
                                 svdb_io_file_t* file,
                                 void* buffer,
                                 size_t len,
                                 int64_t offset);

/*
 * Synchronous write (blocks until complete).
 * Returns bytes written, or negative on error.
 */
int64_t svdb_io_uring_write_sync(svdb_io_uring_t* ring,
                                  svdb_io_file_t* file,
                                  const void* data,
                                  size_t len,
                                  int64_t offset);

#ifdef __cplusplus
}
#endif

#endif // SVDB_PB_IO_URING_H