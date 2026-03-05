#include "freelist.h"
#include <string.h>

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

static inline uint32_t rd32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static inline void wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t)(v);
}

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

uint32_t svdb_freelist_max_entries(size_t page_size) {
    if (page_size < 8) return 0;
    return (uint32_t)((page_size - 8) / 4);
}

int svdb_freelist_parse_trunk(const uint8_t* data, size_t data_size,
                               uint32_t* out_next_trunk, uint32_t* out_count) {
    if (!data || data_size < 8) return 0;
    if (out_next_trunk) *out_next_trunk = rd32(data);
    if (out_count)      *out_count      = rd32(data + 4);
    return 1;
}

int svdb_freelist_write_trunk(uint8_t* data, size_t data_size,
                               uint32_t next_trunk, uint32_t count) {
    if (!data || data_size < 8) return 0;
    wr32(data,     next_trunk);
    wr32(data + 4, count);
    return 1;
}

uint32_t svdb_freelist_get_entry(const uint8_t* data, size_t data_size, uint32_t idx) {
    if (!data || data_size < 8) return 0;
    size_t off = 8 + (size_t)idx * 4;
    if (off + 4 > data_size) return 0;
    return rd32(data + off);
}

int svdb_freelist_set_entry(uint8_t* data, size_t data_size, uint32_t idx, uint32_t page_num) {
    if (!data || data_size < 8) return 0;
    size_t off = 8 + (size_t)idx * 4;
    if (off + 4 > data_size) return 0;
    wr32(data + off, page_num);
    return 1;
}

int svdb_freelist_add_entry(uint8_t* data, size_t data_size, uint32_t page_num) {
    if (!data || data_size < 8) return 0;
    uint32_t count = rd32(data + 4);
    uint32_t max   = svdb_freelist_max_entries(data_size);
    if (count >= max) return 0;
    size_t off = 8 + (size_t)count * 4;
    wr32(data + off, page_num);
    wr32(data + 4, count + 1);
    return 1;
}

int svdb_freelist_remove_entry(uint8_t* data, size_t data_size, uint32_t idx) {
    if (!data || data_size < 8) return 0;
    uint32_t count = rd32(data + 4);
    if (idx >= count) return 0;
    uint8_t* arr = data + 8;
    size_t shift = (size_t)(count - 1 - idx) * 4;
    if (shift > 0) {
        memmove(arr + idx * 4, arr + (idx + 1) * 4, shift);
    }
    wr32(data + 4, count - 1);
    return 1;
}
