#include "manager.h"
#include <string.h>

static const char SVDB_MAGIC[17] = "SQLite format 3\0";

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

static inline uint16_t rd16(const uint8_t* p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

static inline void wr16(uint8_t* p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

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

int64_t svdb_manager_page_offset(uint32_t page_num, uint32_t page_size) {
    if (page_num == 0) return -1;
    return (int64_t)(page_num - 1) * (int64_t)page_size;
}

int svdb_manager_is_valid_page_size(uint32_t page_size) {
    if (page_size < SVDB_MANAGER_MIN_PAGE_SIZE) return 0;
    if (page_size > SVDB_MANAGER_MAX_PAGE_SIZE) return 0;
    /* Must be a power of two. */
    return (page_size & (page_size - 1)) == 0;
}

int svdb_manager_header_magic_valid(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 16) return 0;
    return memcmp(data, SVDB_MAGIC, 16) == 0;
}

uint32_t svdb_manager_read_header_page_size(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 18) return 0;
    uint16_t v = rd16(data + 16);
    return (v == 1) ? 65536u : (uint32_t)v;
}

int svdb_manager_write_header_page_size(uint8_t* data, size_t data_size, uint32_t page_size) {
    if (!data || data_size < 18) return 0;
    if (!svdb_manager_is_valid_page_size(page_size)) return 0;
    uint16_t stored = (page_size == 65536) ? 1 : (uint16_t)page_size;
    wr16(data + 16, stored);
    return 1;
}

uint32_t svdb_manager_read_header_num_pages(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 32) return 0;
    return rd32(data + 28);
}

int svdb_manager_write_header_num_pages(uint8_t* data, size_t data_size, uint32_t num_pages) {
    if (!data || data_size < 32) return 0;
    wr32(data + 28, num_pages);
    return 1;
}
