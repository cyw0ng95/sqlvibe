#ifndef SVDB_DS_FREELIST_H
#define SVDB_DS_FREELIST_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Freelist trunk page format:
 *   Bytes 0-3:  next trunk page number (big-endian uint32, 0 = last trunk)
 *   Bytes 4-7:  number of leaf entries  (big-endian uint32)
 *   Bytes 8+:   leaf page numbers       (big-endian uint32 each)
 */

/* Return the maximum number of leaf entries that fit in a trunk page. */
uint32_t svdb_freelist_max_entries(size_t page_size);

/* Parse the trunk page header.
 * out_next_trunk and out_count may be NULL.
 * Returns 1 on success, 0 if data_size < 8. */
int svdb_freelist_parse_trunk(const uint8_t* data, size_t data_size,
                               uint32_t* out_next_trunk, uint32_t* out_count);

/* Write the trunk page header.
 * Returns 1 on success, 0 if data_size < 8. */
int svdb_freelist_write_trunk(uint8_t* data, size_t data_size,
                               uint32_t next_trunk, uint32_t count);

/* Return the leaf page number at slot idx, or 0 on error. */
uint32_t svdb_freelist_get_entry(const uint8_t* data, size_t data_size, uint32_t idx);

/* Set the leaf page number at slot idx.
 * Returns 1 on success, 0 if slot is out of range. */
int svdb_freelist_set_entry(uint8_t* data, size_t data_size, uint32_t idx, uint32_t page_num);

/* Append page_num to the entry list.
 * Returns 1 on success, 0 if the trunk page is full or data_size is too small. */
int svdb_freelist_add_entry(uint8_t* data, size_t data_size, uint32_t page_num);

/* Remove the entry at slot idx (shifts later entries left, decrements count).
 * Returns 1 on success, 0 on error. */
int svdb_freelist_remove_entry(uint8_t* data, size_t data_size, uint32_t idx);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_FREELIST_H */
