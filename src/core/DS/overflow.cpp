#include "overflow.h"
#include <cstdlib>
#include <cstring>

/* Maximum overflow chain length (corruption guard). */
static const int SVDB_OVERFLOW_MAX_CHAIN_LENGTH = 10000;

/* Read a big-endian uint32 from buf. */
static uint32_t read_be32(const uint8_t* buf) {
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] << 8)  |  (uint32_t)buf[3];
}

/* Write a big-endian uint32 into buf. */
static void write_be32(uint8_t* buf, uint32_t val) {
    buf[0] = (uint8_t)(val >> 24);
    buf[1] = (uint8_t)(val >> 16);
    buf[2] = (uint8_t)(val >>  8);
    buf[3] = (uint8_t)(val);
}

extern "C" {

int svdb_overflow_write_chain(const svdb_page_manager_t* pm,
                               const uint8_t* payload, size_t payload_len,
                               uint32_t* out_first_page) {
    if (!pm || !out_first_page) return 0;

    *out_first_page = 0;
    if (!payload || payload_len == 0) return 1;

    uint32_t first_page = 0;
    uint32_t prev_page  = 0;
    size_t   offset     = 0;

    while (offset < payload_len) {
        uint32_t page_num = 0;
        if (pm->allocate_page(pm->user_data, &page_num) != 1) return 0;

        if (first_page == 0) first_page = page_num;

        /* Link previous page → current. */
        if (prev_page != 0) {
            uint8_t* prev_data = nullptr;
            size_t   prev_size = 0;
            if (pm->read_page(pm->user_data, prev_page, &prev_data, &prev_size) != 1) return 0;
            write_be32(prev_data, page_num);
            if (pm->write_page(pm->user_data, prev_page, prev_data, prev_size) != 1) return 0;
        }

        /* Read newly-allocated page to obtain its size. */
        uint8_t* page_data = nullptr;
        size_t   page_size = 0;
        if (pm->read_page(pm->user_data, page_num, &page_data, &page_size) != 1) return 0;
        if (page_size <= SVDB_OVERFLOW_PAGE_HEADER_SIZE) return 0;

        size_t usable   = page_size - SVDB_OVERFLOW_PAGE_HEADER_SIZE;
        size_t remaining = payload_len - offset;
        size_t write_sz  = remaining < usable ? remaining : usable;

        write_be32(page_data, 0);  /* next = 0 (last) until updated */
        memcpy(page_data + SVDB_OVERFLOW_PAGE_HEADER_SIZE, payload + offset, write_sz);

        if (pm->write_page(pm->user_data, page_num, page_data, page_size) != 1) return 0;

        prev_page = page_num;
        offset   += write_sz;
    }

    *out_first_page = first_page;
    return 1;
}

int svdb_overflow_read_chain(const svdb_page_manager_t* pm,
                              uint32_t first_page, size_t total_size,
                              uint8_t** out_buf, size_t* out_len) {
    if (!pm || !out_buf || !out_len) return 0;

    *out_buf = nullptr;
    *out_len = 0;

    if (first_page == 0 || total_size == 0) return 1;

    uint8_t* result = (uint8_t*)malloc(total_size);
    if (!result) return 0;

    size_t   collected   = 0;
    uint32_t current     = first_page;

    while (current != 0 && collected < total_size) {
        uint8_t* page_data = nullptr;
        size_t   page_size = 0;
        if (pm->read_page(pm->user_data, current, &page_data, &page_size) != 1) {
            free(result);
            return 0;
        }
        if (page_size <= SVDB_OVERFLOW_PAGE_HEADER_SIZE) {
            free(result);
            return 0;
        }

        uint32_t next    = read_be32(page_data);
        size_t   usable  = page_size - SVDB_OVERFLOW_PAGE_HEADER_SIZE;
        size_t   remain  = total_size - collected;
        size_t   read_sz = remain < usable ? remain : usable;

        memcpy(result + collected, page_data + SVDB_OVERFLOW_PAGE_HEADER_SIZE, read_sz);
        collected += read_sz;
        current    = next;
    }

    if (collected != total_size) {
        free(result);
        return 0;
    }

    *out_buf = result;
    *out_len = total_size;
    return 1;
}

int svdb_overflow_free_chain(const svdb_page_manager_t* pm, uint32_t first_page) {
    if (!pm) return 0;
    if (first_page == 0) return 1;

    uint32_t current = first_page;
    int guard = 0;

    while (current != 0) {
        if (++guard > SVDB_OVERFLOW_MAX_CHAIN_LENGTH) return 0;  /* corruption guard */

        uint8_t* page_data = nullptr;
        size_t   page_size = 0;
        if (pm->read_page(pm->user_data, current, &page_data, &page_size) != 1) return 0;

        uint32_t next = read_be32(page_data);
        if (pm->free_page(pm->user_data, current) != 1) return 0;
        current = next;
    }

    return 1;
}

int svdb_overflow_chain_length(const svdb_page_manager_t* pm,
                                uint32_t first_page, size_t* out_len) {
    if (!pm || !out_len) return 0;
    *out_len = 0;

    if (first_page == 0) return 1;

    uint32_t current = first_page;
    size_t   count   = 0;

    while (current != 0) {
        if (++count > (size_t)SVDB_OVERFLOW_MAX_CHAIN_LENGTH) return 0;  /* corruption guard */

        uint8_t* page_data = nullptr;
        size_t   page_size = 0;
        if (pm->read_page(pm->user_data, current, &page_data, &page_size) != 1) return 0;

        current = read_be32(page_data);
    }

    *out_len = count;
    return 1;
}

} /* extern "C" */
