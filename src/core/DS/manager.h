#ifndef SVDB_DS_MANAGER_H
#define SVDB_DS_MANAGER_H

#include <stdint.h>
#include <stddef.h>

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

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_MANAGER_H */
