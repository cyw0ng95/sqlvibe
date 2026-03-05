#ifndef SVDB_DS_MANAGER_H
#define SVDB_DS_MANAGER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Page-manager utility functions.
 *
 * SQLite-compatible database file header (first 100 bytes of page 1):
 *   Bytes  0-15: magic string "SQLite format 3\0"
 *   Bytes 16-17: page size (big-endian uint16; value 1 means 65536)
 *   Bytes 28-31: database size in pages (big-endian uint32)
 */

#define SVDB_MANAGER_MIN_PAGE_SIZE  512
#define SVDB_MANAGER_MAX_PAGE_SIZE  65536
#define SVDB_MANAGER_HEADER_SIZE    100

/* Return the file byte offset for page page_num (1-based).
 * Page 1 starts at offset 0.  Returns -1 if page_num == 0. */
int64_t svdb_manager_page_offset(uint32_t page_num, uint32_t page_size);

/* Return 1 if page_size is a power of two in [512, 65536]. */
int svdb_manager_is_valid_page_size(uint32_t page_size);

/* Return 1 if the first 16 bytes of data match the SQLite magic string. */
int svdb_manager_header_magic_valid(const uint8_t* data, size_t data_size);

/* Read the page size from the database header.
 * Returns the page size (65536 if the stored value is 1), or 0 on error. */
uint32_t svdb_manager_read_header_page_size(const uint8_t* data, size_t data_size);

/* Write the page size to the database header.
 * Returns 1 on success, 0 if data_size < 18 or page_size is invalid. */
int svdb_manager_write_header_page_size(uint8_t* data, size_t data_size, uint32_t page_size);

/* Read the database size (number of pages) from the header.
 * Returns 0 if data_size < 32. */
uint32_t svdb_manager_read_header_num_pages(const uint8_t* data, size_t data_size);

/* Write the database size to the header.
 * Returns 1 on success, 0 if data_size < 32. */
int svdb_manager_write_header_num_pages(uint8_t* data, size_t data_size, uint32_t num_pages);

/* -------------------------------------------------------------------------
 * Self-contained C++ PageManager (for embedding in B-Tree)
 * ------------------------------------------------------------------------- */

typedef struct svdb_page_manager svdb_page_manager;

/* Create a self-contained PageManager that owns file I/O.
 * The PageManager will:
 *   - Open the database file directly
 *   - Maintain its own page cache
 *   - Handle read/write/allocate/free without Go callbacks
 * 
 * Parameters:
 *   - db_path: Path to the SQLite database file
 *   - page_size: Page size (must be valid per svdb_manager_is_valid_page_size)
 *   - cache_pages: Number of pages to cache (0 = use default 2000)
 * 
 * Returns: PageManager handle, or NULL on error */
svdb_page_manager* svdb_page_manager_create(const char* db_path, uint32_t page_size, int cache_pages);

/* Destroy a PageManager.
 * Closes the file and frees all resources. */
void svdb_page_manager_destroy(svdb_page_manager* pm);

/* Read a page from the database.
 * Returns 1 on success, 0 on error.
 * On success, *page_data points to cached page data (valid until next PM operation). */
int svdb_page_manager_read(svdb_page_manager* pm, uint32_t page_num, 
                           const uint8_t** page_data, size_t* page_size);

/* Write a page to the database.
 * Returns 1 on success, 0 on error. */
int svdb_page_manager_write(svdb_page_manager* pm, uint32_t page_num,
                            const uint8_t* page_data, size_t page_size);

/* Allocate a new page.
 * Returns 1 on success, 0 on error.
 * On success, *page_num contains the new page number. */
int svdb_page_manager_allocate(svdb_page_manager* pm, uint32_t* page_num);

/* Free a page.
 * Returns 1 on success, 0 on error. */
int svdb_page_manager_free(svdb_page_manager* pm, uint32_t page_num);

/* Get the page size. */
uint32_t svdb_page_manager_get_page_size(const svdb_page_manager* pm);

/* Get the current database size (number of pages). */
uint32_t svdb_page_manager_get_num_pages(const svdb_page_manager* pm);

/* Sync all pending writes to disk.
 * Returns 1 on success, 0 on error. */
int svdb_page_manager_sync(svdb_page_manager* pm);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_MANAGER_H */
