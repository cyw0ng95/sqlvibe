#ifndef SVDB_DS_OVERFLOW_H
#define SVDB_DS_OVERFLOW_H

#include <stdint.h>
#include <stddef.h>
#include "btree.h"
#include "manager.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Overflow page format (mirrors internal/DS/overflow.go):
 *   Bytes 0-3:  next overflow page number (big-endian uint32, 0 = last page)
 *   Bytes 4+:   payload data
 */

#define SVDB_OVERFLOW_PAGE_HEADER_SIZE 4

/*
 * Write payload across a chain of overflow pages (callback-based).
 * out_first_page receives the first page number of the chain (0 if payload_len==0).
 * Returns 1 on success, 0 on error.
 */
int svdb_overflow_write_chain(const svdb_page_manager_t* pm,
                               const uint8_t* payload, size_t payload_len,
                               uint32_t* out_first_page);

/*
 * Write payload across a chain of overflow pages (embedded C++ PageManager).
 * This version eliminates Go callback overhead.
 */
int svdb_overflow_write_chain_embedded(svdb_page_manager* pm,
                                        const uint8_t* payload, size_t payload_len,
                                        uint32_t* out_first_page);

/*
 * Read total_size bytes from the overflow chain starting at first_page.
 * Allocates *out_buf (caller must free with free()); sets *out_len.
 * Returns 1 on success, 0 on error.
 */
int svdb_overflow_read_chain(const svdb_page_manager_t* pm,
                              uint32_t first_page, size_t total_size,
                              uint8_t** out_buf, size_t* out_len);

/*
 * Read total_size bytes from the overflow chain (embedded C++ PageManager).
 */
int svdb_overflow_read_chain_embedded(svdb_page_manager* pm,
                                       uint32_t first_page, size_t total_size,
                                       uint8_t** out_buf, size_t* out_len);

/*
 * Free all pages in the overflow chain starting at first_page.
 * Returns 1 on success, 0 on error.
 */
int svdb_overflow_free_chain(const svdb_page_manager_t* pm, uint32_t first_page);

/*
 * Free all pages in the overflow chain (embedded C++ PageManager).
 */
int svdb_overflow_free_chain_embedded(svdb_page_manager* pm, uint32_t first_page);

/*
 * Count the number of pages in the overflow chain starting at first_page.
 * out_len receives the page count.
 * Returns 1 on success, 0 on error.
 */
int svdb_overflow_chain_length(const svdb_page_manager_t* pm,
                                uint32_t first_page, size_t* out_len);

/*
 * Count pages in overflow chain (embedded C++ PageManager).
 */
int svdb_overflow_chain_length_embedded(svdb_page_manager* pm,
                                         uint32_t first_page, size_t* out_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_OVERFLOW_H */
