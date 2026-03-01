#ifndef SVDB_DS_PAGE_H
#define SVDB_DS_PAGE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Page header format (SQLite-compatible):
 *   Byte 0:     page type
 *                 0x0d = table leaf
 *                 0x05 = table interior
 *                 0x0a = index leaf
 *                 0x02 = index interior
 *   Bytes 1-2:  first freeblock offset (big-endian uint16)
 *   Bytes 3-4:  number of cells (big-endian uint16)
 *   Bytes 5-6:  content area start offset (big-endian uint16, 0 means 65536)
 *   Byte 7:     fragmented free bytes
 *   Bytes 8-11: right-child page number (interior pages only, big-endian uint32)
 *
 * Cell pointer array begins at byte 8 (leaf) or byte 12 (interior).
 * Each cell pointer is a 2-byte big-endian uint16 offset into the page.
 */

#define SVDB_PAGE_LEAF_HEADER_SIZE     8
#define SVDB_PAGE_INTERIOR_HEADER_SIZE 12

#define SVDB_PAGE_TYPE_TABLE_LEAF      0x0d
#define SVDB_PAGE_TYPE_TABLE_INTERIOR  0x05
#define SVDB_PAGE_TYPE_INDEX_LEAF      0x0a
#define SVDB_PAGE_TYPE_INDEX_INTERIOR  0x02

/* Initialize a page buffer to an empty page of the given type.
 * Returns 1 on success, 0 if data_size is too small or type is invalid. */
int svdb_page_init(uint8_t* data, size_t data_size, uint8_t page_type);

/* Return the page type byte, or 0 if data_size < 1. */
uint8_t svdb_page_get_type(const uint8_t* data, size_t data_size);

/* Return the cell count from the page header. */
uint16_t svdb_page_get_num_cells(const uint8_t* data, size_t data_size);

/* Return the content area start offset (0 stored as 65536). */
uint32_t svdb_page_get_content_offset(const uint8_t* data, size_t data_size);

/* Set the cell count in the page header. */
void svdb_page_set_num_cells(uint8_t* data, size_t data_size, uint16_t count);

/* Set the content area start offset. */
void svdb_page_set_content_offset(uint8_t* data, size_t data_size, uint32_t offset);

/* Get the cell pointer (page offset) at slot idx.
 * Returns 0 if idx is out of range. */
uint16_t svdb_page_get_cell_pointer(const uint8_t* data, size_t data_size, int idx);

/* Set the cell pointer at slot idx. */
void svdb_page_set_cell_pointer(uint8_t* data, size_t data_size, int idx, uint16_t offset);

/* Insert a cell pointer at slot idx, shifting existing pointers right.
 * num_cells must already reflect the new (incremented) count before calling.
 * Returns 1 on success, 0 if out of bounds. */
int svdb_page_insert_cell_pointer(uint8_t* data, size_t data_size, int idx, uint16_t offset);

/* Remove the cell pointer at slot idx, shifting later pointers left.
 * num_cells is decremented inside this function.
 * Returns 1 on success, 0 if out of bounds. */
int svdb_page_remove_cell_pointer(uint8_t* data, size_t data_size, int idx);

/* Return 1 if the page is more than threshold_pct percent used. */
int svdb_page_is_overfull(const uint8_t* data, size_t page_size, int threshold_pct);

/* Return 1 if the page is less than threshold_pct percent used. */
int svdb_page_is_underfull(const uint8_t* data, size_t page_size, int threshold_pct);

/* Return the number of bytes currently used on the page. */
size_t svdb_page_used_bytes(const uint8_t* data, size_t page_size);

/* Return the number of free bytes available for new cell content. */
size_t svdb_page_free_bytes(const uint8_t* data, size_t page_size);

/* Defragment the page: pack all cell content toward the end of the page
 * and reset the content-area start to reflect the new layout.
 * Returns 1 on success, 0 on error. */
int svdb_page_compact(uint8_t* data, size_t page_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_PAGE_H */
