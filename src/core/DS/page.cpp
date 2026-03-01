#include "page.h"
#include <string.h>
#include <algorithm>
#include <vector>

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

static inline int is_interior(uint8_t type) {
    return type == SVDB_PAGE_TYPE_TABLE_INTERIOR ||
           type == SVDB_PAGE_TYPE_INDEX_INTERIOR;
}

static inline int header_size(uint8_t type) {
    return is_interior(type) ? SVDB_PAGE_INTERIOR_HEADER_SIZE
                              : SVDB_PAGE_LEAF_HEADER_SIZE;
}

/* Offset of the cell-pointer array start. */
static inline int cell_ptr_base(uint8_t type) {
    return header_size(type);
}

/* Read big-endian uint16 */
static inline uint16_t rd16(const uint8_t* p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

/* Write big-endian uint16 */
static inline void wr16(uint8_t* p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

/* Read big-endian uint32 */
static inline uint32_t rd32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

/* Write big-endian uint32 */
static inline void wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t)(v);
}

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

int svdb_page_init(uint8_t* data, size_t data_size, uint8_t page_type) {
    if (!data) return 0;
    if (page_type != SVDB_PAGE_TYPE_TABLE_LEAF     &&
        page_type != SVDB_PAGE_TYPE_TABLE_INTERIOR &&
        page_type != SVDB_PAGE_TYPE_INDEX_LEAF     &&
        page_type != SVDB_PAGE_TYPE_INDEX_INTERIOR) return 0;
    if (data_size < (size_t)header_size(page_type)) return 0;

    memset(data, 0, data_size);
    data[0] = page_type;
    /* content area starts at the end of the page (empty page) */
    uint32_t content_start = (uint32_t)data_size;
    /* stored as 0 when value == 65536 */
    uint16_t stored = (content_start >= 65536) ? 0 : (uint16_t)content_start;
    wr16(data + 5, stored);
    return 1;
}

uint8_t svdb_page_get_type(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 1) return 0;
    return data[0];
}

uint16_t svdb_page_get_num_cells(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 5) return 0;
    return rd16(data + 3);
}

uint32_t svdb_page_get_content_offset(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 7) return 0;
    uint16_t v = rd16(data + 5);
    return (v == 0) ? 65536u : (uint32_t)v;
}

void svdb_page_set_num_cells(uint8_t* data, size_t data_size, uint16_t count) {
    if (!data || data_size < 5) return;
    wr16(data + 3, count);
}

void svdb_page_set_content_offset(uint8_t* data, size_t data_size, uint32_t offset) {
    if (!data || data_size < 7) return;
    uint16_t stored = (offset >= 65536) ? 0 : (uint16_t)offset;
    wr16(data + 5, stored);
}

uint16_t svdb_page_get_cell_pointer(const uint8_t* data, size_t data_size, int idx) {
    if (!data || idx < 0) return 0;
    uint8_t type = svdb_page_get_type(data, data_size);
    int base = cell_ptr_base(type);
    size_t off = (size_t)base + (size_t)idx * 2;
    if (off + 2 > data_size) return 0;
    return rd16(data + off);
}

void svdb_page_set_cell_pointer(uint8_t* data, size_t data_size, int idx, uint16_t offset) {
    if (!data || idx < 0) return;
    uint8_t type = svdb_page_get_type(data, data_size);
    int base = cell_ptr_base(type);
    size_t off = (size_t)base + (size_t)idx * 2;
    if (off + 2 > data_size) return;
    wr16(data + off, offset);
}

int svdb_page_insert_cell_pointer(uint8_t* data, size_t data_size, int idx, uint16_t offset) {
    if (!data || idx < 0) return 0;
    uint8_t type = svdb_page_get_type(data, data_size);
    int base = cell_ptr_base(type);
    /* num_cells was already incremented by caller */
    uint16_t n = svdb_page_get_num_cells(data, data_size);
    if (idx > (int)n - 1) return 0;
    size_t array_end = (size_t)base + (size_t)n * 2;
    if (array_end > data_size) return 0;

    /* shift pointers [idx..n-2] one slot to the right */
    uint8_t* arr = data + base;
    memmove(arr + (idx + 1) * 2, arr + idx * 2, (size_t)(n - 1 - idx) * 2);
    wr16(arr + idx * 2, offset);
    return 1;
}

int svdb_page_remove_cell_pointer(uint8_t* data, size_t data_size, int idx) {
    if (!data || idx < 0) return 0;
    uint8_t type = svdb_page_get_type(data, data_size);
    int base = cell_ptr_base(type);
    uint16_t n = svdb_page_get_num_cells(data, data_size);
    if (idx >= (int)n) return 0;

    uint8_t* arr = data + base;
    /* shift pointers after idx one slot to the left */
    memmove(arr + idx * 2, arr + (idx + 1) * 2, (size_t)(n - 1 - idx) * 2);
    svdb_page_set_num_cells(data, data_size, (uint16_t)(n - 1));
    return 1;
}

size_t svdb_page_used_bytes(const uint8_t* data, size_t page_size) {
    if (!data || page_size < 8) return 0;
    uint8_t type = svdb_page_get_type(data, page_size);
    int base = cell_ptr_base(type);
    uint16_t n = svdb_page_get_num_cells(data, page_size);
    uint32_t content_start = svdb_page_get_content_offset(data, page_size);
    if (content_start > page_size) content_start = (uint32_t)page_size;

    size_t header_and_ptrs = (size_t)base + (size_t)n * 2;
    size_t cell_content    = page_size - (size_t)content_start;
    return header_and_ptrs + cell_content;
}

size_t svdb_page_free_bytes(const uint8_t* data, size_t page_size) {
    size_t used = svdb_page_used_bytes(data, page_size);
    if (used >= page_size) return 0;
    return page_size - used;
}

int svdb_page_is_overfull(const uint8_t* data, size_t page_size, int threshold_pct) {
    if (!data || page_size == 0) return 0;
    size_t used = svdb_page_used_bytes(data, page_size);
    return (int)(used * 100 / page_size) > threshold_pct;
}

int svdb_page_is_underfull(const uint8_t* data, size_t page_size, int threshold_pct) {
    if (!data || page_size == 0) return 0;
    size_t used = svdb_page_used_bytes(data, page_size);
    return (int)(used * 100 / page_size) < threshold_pct;
}

int svdb_page_compact(uint8_t* data, size_t page_size) {
    if (!data || page_size < 8) return 0;
    uint8_t type = svdb_page_get_type(data, page_size);
    int base = cell_ptr_base(type);
    uint16_t n = svdb_page_get_num_cells(data, page_size);

    if (n == 0) {
        /* Nothing to compact; reset content area to end of page. */
        svdb_page_set_content_offset(data, page_size, (uint32_t)page_size);
        return 1;
    }

    /* Gather (pointer_slot, cell_offset) pairs for all cells. */
    /* Use a simple insertion-sort by descending cell offset so we can pack
     * cell content from the end of the page towards the middle. */
    /* We work with a temporary copy to avoid clobbering live data. */
    std::vector<uint8_t> tmp(page_size);
    memcpy(tmp.data(), data, page_size);

    /* Build index array sorted by ascending cell offset (smallest first). */
    std::vector<int> order(n);
    for (int i = 0; i < (int)n; i++) order[i] = i;

    /* Sort by cell offset ascending: smallest offset = furthest from page end. */
    std::sort(order.begin(), order.end(), [&](int a, int b) {
        return rd16(tmp.data() + base + a * 2) < rd16(tmp.data() + base + b * 2);
    });

    /* Determine the end of each cell by looking at the next higher offset.
     * The array is sorted ascending by offset; iterate in reverse (largest
     * offset first = closest to the page end) so we can pack from end toward
     * the middle.  The cell with the largest offset ends at page_size. */
    uint32_t write_pos = (uint32_t)page_size;
    for (int k = (int)n - 1; k >= 0; k--) {
        int slot = order[k];
        uint16_t cell_off = rd16(tmp.data() + base + slot * 2);
        /* Cell end: next entry in ascending order (k+1) starts at cell_off;
         * for the last element (k == n-1) the end is write_pos == page_size. */
        uint32_t cell_end = write_pos;
        uint32_t cell_len = cell_end - (uint32_t)cell_off;
        write_pos -= cell_len;
        /* Copy cell to new position in data buffer. */
        memmove(data + write_pos, tmp.data() + cell_off, cell_len);
        /* Update pointer in data buffer. */
        wr16(data + base + slot * 2, (uint16_t)write_pos);
    }

    svdb_page_set_content_offset(data, page_size, write_pos);
    /* Clear old cell area between header+ptrs and new content start. */
    size_t ptr_end = (size_t)base + (size_t)n * 2;
    if (write_pos > ptr_end) {
        memset(data + ptr_end, 0, write_pos - ptr_end);
    }

    return 1;
}
