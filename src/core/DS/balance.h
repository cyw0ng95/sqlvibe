#ifndef SVDB_DS_BALANCE_H
#define SVDB_DS_BALANCE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * B-Tree page balancing utilities.
 * These functions operate on raw page buffers using the format documented in
 * page.h.  They require that all pages passed share the same page_size.
 */

/* Return 1 if the page is more than 90% used (overfull). */
int svdb_balance_is_overfull(const uint8_t* data, size_t page_size);

/* Return 1 if the page is less than 33% used (underfull). */
int svdb_balance_is_underfull(const uint8_t* data, size_t page_size);

/* Split a leaf page at split_point.
 *   left_data  : existing leaf page (modified in-place, retains cells [0..split_point))
 *   right_data : empty page buffer of page_size bytes (receives cells [split_point..n))
 *   split_point: first cell index to move to the right page
 *   divider_key_out    : caller-supplied buffer for the divider key bytes
 *   divider_key_len_out: set to the number of bytes written to divider_key_out
 *
 * The divider key is the raw cell bytes of the first cell on the right page
 * (callers use this as the separator key in the parent interior node).
 * divider_key_out must be at least page_size bytes.
 *
 * Returns 1 on success, 0 on error.
 */
int svdb_balance_split_leaf(uint8_t* left_data, size_t page_size,
                             uint8_t* right_data,
                             int split_point,
                             uint8_t* divider_key_out,
                             size_t*  divider_key_len_out);

/* Merge all cells from right_data into left_data.
 * Returns 1 if all cells fit, 0 if there is not enough space. */
int svdb_balance_merge_pages(uint8_t* left_data, size_t page_size,
                              const uint8_t* right_data);

/* Move move_count cells from src_data to dst_data (appended at the end).
 * Returns 1 on success, 0 on error. */
int svdb_balance_redistribute(uint8_t* src_data, size_t page_size,
                               uint8_t* dst_data, int move_count);

/* Calculate the optimal split point for a leaf page.
 * Returns the cell index at which the page should be split so that both
 * halves are as equal in used bytes as possible. */
int svdb_balance_calculate_split_point(const uint8_t* data, size_t page_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_BALANCE_H */
