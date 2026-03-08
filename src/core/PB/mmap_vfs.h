/* mmap_vfs.h — Memory-mapped I/O VFS for read-optimized access
 *
 * Benefits of mmap I/O:
 *   - Zero-copy reads from kernel page cache
 *   - Automatic page caching by OS
 *   - Reduced syscall overhead for sequential scans
 *   - Huge pages support for large datasets (reduces TLB misses)
 */
#ifndef SVDB_PB_MMAP_VFS_H
#define SVDB_PB_MMAP_VFS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* mmap configuration */
#define SVDB_MMAP_DEFAULT_SIZE      (64 * 1024 * 1024)  // 64 MB default
#define SVDB_MMAP_HUGE_PAGE_SIZE    (2 * 1024 * 1024)   // 2 MB huge pages
#define SVDB_MMAP_MAX_REGIONS       16

/* Access patterns for madvise */
typedef enum {
    SVDB_MMAP_ACCESS_RANDOM,
    SVDB_MMAP_ACCESS_SEQUENTIAL,
    SVDB_MMAP_ACCESS_WILLNEED,
    SVDB_MMAP_ACCESS_DONTNEED
} svdb_mmap_access_t;

/* Opaque mmap file handle */
typedef struct svdb_mmap_file_s svdb_mmap_file_t;

/* ============================================================================
 * MMap file operations
 * ============================================================================ */

/*
 * Create a memory-mapped file for read-only access.
 * use_huge_pages: use huge pages for files > 2MB
 * Returns NULL on failure.
 */
svdb_mmap_file_t* svdb_mmap_file_open(const char* path, int use_huge_pages);

/*
 * Close and unmap the file.
 */
void svdb_mmap_file_close(svdb_mmap_file_t* file);

/*
 * Get a pointer to the mapped memory at the given offset.
 * Returns NULL if offset is out of bounds.
 * The pointer is valid until the file is closed or remapped.
 */
const void* svdb_mmap_file_get_ptr(svdb_mmap_file_t* file, int64_t offset);

/*
 * Get a pointer to the mapped memory with bounds checking.
 * Returns NULL if [offset, offset+len) is not fully mapped.
 */
const void* svdb_mmap_file_get_range(svdb_mmap_file_t* file,
                                      int64_t offset, int64_t len);

/*
 * Read from the mapped memory into a buffer.
 * Returns bytes read, or negative on error.
 */
int64_t svdb_mmap_file_read(svdb_mmap_file_t* file,
                             void* buffer,
                             int64_t len,
                             int64_t offset);

/*
 * Get the total size of the mapped file.
 */
int64_t svdb_mmap_file_size(svdb_mmap_file_t* file);

/*
 * Check if the file is valid and mapped.
 */
int svdb_mmap_file_is_valid(svdb_mmap_file_t* file);

/*
 * Get the file descriptor.
 */
int svdb_mmap_file_fd(svdb_mmap_file_t* file);

/*
 * Set access pattern hint for the kernel.
 * This calls madvise() with the appropriate flags.
 */
int svdb_mmap_file_set_access(svdb_mmap_file_t* file,
                               svdb_mmap_access_t access,
                               int64_t offset,
                               int64_t len);

/*
 * Prefetch a range of pages into memory.
 * Returns 0 on success, negative on error.
 */
int svdb_mmap_file_prefetch(svdb_mmap_file_t* file,
                             int64_t offset,
                             int64_t len);

/*
 * Release a range of pages (they can be evicted from memory).
 * Useful after processing a large sequential scan.
 */
int svdb_mmap_file_release(svdb_mmap_file_t* file,
                            int64_t offset,
                            int64_t len);

/*
 * Remap the file (e.g., after it has grown).
 * Returns 0 on success, negative on error.
 */
int svdb_mmap_file_remap(svdb_mmap_file_t* file);

/*
 * Sync the mapping to disk (for MAP_SHARED).
 * For read-only mappings, this is a no-op.
 */
int svdb_mmap_file_sync(svdb_mmap_file_t* file);

/* ============================================================================
 * MMap pool for managing multiple regions
 * ============================================================================ */

/* Opaque mmap pool handle */
typedef struct svdb_mmap_pool_s svdb_mmap_pool_t;

/*
 * Create a mmap pool for managing multiple memory-mapped regions.
 * max_regions: maximum number of concurrent mappings
 */
svdb_mmap_pool_t* svdb_mmap_pool_create(int max_regions);

/*
 * Destroy the pool and unmap all files.
 */
void svdb_mmap_pool_destroy(svdb_mmap_pool_t* pool);

/*
 * Open and map a file through the pool.
 * Returns the mmap file handle, or NULL on failure.
 */
svdb_mmap_file_t* svdb_mmap_pool_open(svdb_mmap_pool_t* pool,
                                       const char* path,
                                       int use_huge_pages);

/*
 * Close a file through the pool.
 */
void svdb_mmap_pool_close(svdb_mmap_pool_t* pool, svdb_mmap_file_t* file);

/*
 * Get the total memory used by all mappings in the pool.
 */
int64_t svdb_mmap_pool_total_memory(svdb_mmap_pool_t* pool);

/*
 * Get the number of active mappings.
 */
int svdb_mmap_pool_mapping_count(svdb_mmap_pool_t* pool);

/* ============================================================================
 * Utility functions
 * ============================================================================ */

/*
 * Check if huge pages are available on this system.
 */
int svdb_mmap_huge_pages_available(void);

/*
 * Get the system page size.
 */
int64_t svdb_mmap_page_size(void);

/*
 * Align an offset up to the next page boundary.
 */
int64_t svdb_mmap_page_align_up(int64_t offset);

/*
 * Align an offset down to the previous page boundary.
 */
int64_t svdb_mmap_page_align_down(int64_t offset);

#ifdef __cplusplus
}
#endif

#endif // SVDB_PB_MMAP_VFS_H