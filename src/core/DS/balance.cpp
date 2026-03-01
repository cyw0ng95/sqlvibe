#include "balance.h"
#include "page.h"
#include <string.h>
#include <algorithm>
#include <vector>

/* -------------------------------------------------------------------------
 * Internal helpers (big-endian uint16 I/O)
 * ---------------------------------------------------------------------- */

static inline uint16_t rd16(const uint8_t* p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

static inline void wr16(uint8_t* p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

/* Return the cell-pointer array base offset for a page. */
static int ptr_base(const uint8_t* data) {
    uint8_t t = data[0];
    return (t == SVDB_PAGE_TYPE_TABLE_INTERIOR || t == SVDB_PAGE_TYPE_INDEX_INTERIOR)
           ? SVDB_PAGE_INTERIOR_HEADER_SIZE
           : SVDB_PAGE_LEAF_HEADER_SIZE;
}

/* Return the byte length of cell at slot idx using a pre-sorted offset array.
 * sorted_offsets must be an array of all cell offsets sorted in ascending order.
 * page_size is used as the sentinel end for the highest-offset cell. */
static size_t cell_len_sorted(uint16_t cell_off, const uint16_t* sorted_offs, int n, size_t page_size) {
    /* Find the smallest sorted offset that is strictly greater than cell_off. */
    uint16_t end = (uint16_t)page_size;
    for (int i = 0; i < n; i++) {
        if (sorted_offs[i] > cell_off && sorted_offs[i] < end)
            end = sorted_offs[i];
    }
    return (size_t)(end - cell_off);
}

/* Build a sorted array of all cell offsets for a page. */
static std::vector<uint16_t> build_sorted_offsets(const uint8_t* data, size_t page_size, int n) {
    int base = ptr_base(data);
    std::vector<uint16_t> offs(n);
    for (int i = 0; i < n; i++) offs[i] = rd16(data + base + i * 2);
    std::sort(offs.begin(), offs.end());
    return offs;
}

/* Return the byte length of cell at slot idx using a pre-built sorted offset array. */
static size_t cell_len_at(const uint8_t* data, size_t page_size, int n, int slot,
                           const std::vector<uint16_t>& sorted_offs) {
    int base = ptr_base(data);
    uint16_t off = rd16(data + base + slot * 2);
    return cell_len_sorted(off, sorted_offs.data(), n, page_size);
}

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

int svdb_balance_is_overfull(const uint8_t* data, size_t page_size) {
    return svdb_page_is_overfull(data, page_size, 90);
}

int svdb_balance_is_underfull(const uint8_t* data, size_t page_size) {
    return svdb_page_is_underfull(data, page_size, 33);
}

int svdb_balance_calculate_split_point(const uint8_t* data, size_t page_size) {
    if (!data || page_size < 8) return 0;
    int n = (int)svdb_page_get_num_cells(data, page_size);
    if (n <= 1) return 0;

    auto sorted_offs = build_sorted_offsets(data, page_size, n);

    /* Sum total cell-content bytes. */
    size_t total = 0;
    for (int i = 0; i < n; i++) total += cell_len_at(data, page_size, n, i, sorted_offs);
    size_t half = total / 2;

    size_t acc = 0;
    for (int i = 0; i < n - 1; i++) {
        acc += cell_len_at(data, page_size, n, i, sorted_offs);
        if (acc >= half) return i + 1;
    }
    return n / 2;
}

int svdb_balance_split_leaf(uint8_t* left_data, size_t page_size,
                             uint8_t* right_data,
                             int split_point,
                             uint8_t* divider_key_out,
                             size_t*  divider_key_len_out) {
    if (!left_data || !right_data || !divider_key_out || !divider_key_len_out) return 0;
    if (page_size < 8) return 0;

    int n = (int)svdb_page_get_num_cells(left_data, page_size);
    if (split_point <= 0 || split_point >= n) return 0;

    uint8_t page_type = svdb_page_get_type(left_data, page_size);
    int base = ptr_base(left_data);

    auto sorted_offs = build_sorted_offsets(left_data, page_size, n);

    /* Initialize right page to the same type. */
    if (!svdb_page_init(right_data, page_size, page_type)) return 0;

    /* Copy cells [split_point .. n) to right page. */
    uint32_t right_write = (uint32_t)page_size;
    int right_count = 0;

    for (int i = split_point; i < n; i++) {
        uint16_t src_off = rd16(left_data + base + i * 2);
        size_t   clen   = cell_len_at(left_data, page_size, n, i, sorted_offs);

        right_write -= (uint32_t)clen;
        memcpy(right_data + right_write, left_data + src_off, clen);

        /* Append cell pointer to right page. */
        wr16(right_data + base + right_count * 2, (uint16_t)right_write);
        right_count++;
    }
    svdb_page_set_num_cells(right_data, page_size, (uint16_t)right_count);
    svdb_page_set_content_offset(right_data, page_size, right_write);

    /* Capture divider key = raw bytes of first cell on right page. */
    uint16_t div_off = rd16(right_data + base);
    auto right_sorted = build_sorted_offsets(right_data, page_size, right_count);
    size_t div_len = cell_len_at(right_data, page_size, right_count, 0, right_sorted);
    memcpy(divider_key_out, right_data + div_off, div_len);
    *divider_key_len_out = div_len;

    /* Truncate left page: keep only [0..split_point). */
    svdb_page_set_num_cells(left_data, page_size, (uint16_t)split_point);

    /* Compact left page so the content area is consistent. */
    svdb_page_compact(left_data, page_size);
    return 1;
}

int svdb_balance_merge_pages(uint8_t* left_data, size_t page_size,
                              const uint8_t* right_data) {
    if (!left_data || !right_data || page_size < 8) return 0;

    int ln = (int)svdb_page_get_num_cells(left_data,  page_size);
    int rn = (int)svdb_page_get_num_cells(right_data, page_size);

    auto right_sorted = build_sorted_offsets(right_data, page_size, rn);

    /* Check that all right cells fit into left's free space. */
    size_t right_content = 0;
    for (int i = 0; i < rn; i++) right_content += cell_len_at(right_data, page_size, rn, i, right_sorted);

    int base = ptr_base(left_data);
    size_t ptr_needed = (size_t)(ln + rn) * 2;
    uint32_t l_content_start = svdb_page_get_content_offset(left_data, page_size);
    if (l_content_start > page_size) l_content_start = (uint32_t)page_size;

    size_t available = (size_t)l_content_start - (size_t)base - ptr_needed;
    if (right_content > available) return 0;

    /* Copy right cells into left. */
    uint32_t write_pos = l_content_start;
    for (int i = 0; i < rn; i++) {
        uint16_t src_off = rd16(right_data + base + i * 2);
        size_t clen = cell_len_at(right_data, page_size, rn, i, right_sorted);
        write_pos -= (uint32_t)clen;
        memcpy(left_data + write_pos, right_data + src_off, clen);
        wr16(left_data + base + (ln + i) * 2, (uint16_t)write_pos);
    }

    svdb_page_set_num_cells(left_data, page_size, (uint16_t)(ln + rn));
    svdb_page_set_content_offset(left_data, page_size, write_pos);
    return 1;
}

int svdb_balance_redistribute(uint8_t* src_data, size_t page_size,
                               uint8_t* dst_data, int move_count) {
    if (!src_data || !dst_data || page_size < 8 || move_count <= 0) return 0;

    int sn = (int)svdb_page_get_num_cells(src_data, page_size);
    if (move_count > sn) return 0;

    auto src_sorted = build_sorted_offsets(src_data, page_size, sn);

    int base = ptr_base(src_data);
    int dn   = (int)svdb_page_get_num_cells(dst_data, page_size);
    uint32_t dst_write = svdb_page_get_content_offset(dst_data, page_size);
    if (dst_write > page_size) dst_write = (uint32_t)page_size;

    for (int i = 0; i < move_count; i++) {
        /* Move cells from the tail of src to dst. */
        int src_slot = sn - move_count + i;
        uint16_t src_off = rd16(src_data + base + src_slot * 2);
        size_t clen = cell_len_at(src_data, page_size, sn, src_slot, src_sorted);

        dst_write -= (uint32_t)clen;
        memcpy(dst_data + dst_write, src_data + src_off, clen);
        wr16(dst_data + base + (dn + i) * 2, (uint16_t)dst_write);
    }

    svdb_page_set_num_cells(dst_data, page_size, (uint16_t)(dn + move_count));
    svdb_page_set_content_offset(dst_data, page_size, dst_write);

    /* Shrink src. */
    svdb_page_set_num_cells(src_data, page_size, (uint16_t)(sn - move_count));
    svdb_page_compact(src_data, page_size);
    return 1;
}
