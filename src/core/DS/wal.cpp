#include "wal.h"
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

/* Write a 4-byte little-endian uint32. */
static inline void wr_le32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v);
    p[1] = (uint8_t)(v >>  8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

/* Read a 4-byte little-endian uint32. */
static inline uint32_t rd_le32(const uint8_t* p) {
    return (uint32_t)p[0]        |
           ((uint32_t)p[1] <<  8) |
           ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

size_t svdb_wal_entry_total_size(size_t json_len) {
    return 4 + json_len;
}

int svdb_wal_encode_entry(uint8_t* buf, size_t buf_size,
                           const uint8_t* json_data, size_t json_len,
                           size_t* out_written) {
    if (!buf || !json_data) return 0;
    size_t total = svdb_wal_entry_total_size(json_len);
    if (buf_size < total) return 0;

    wr_le32(buf, (uint32_t)json_len);
    memcpy(buf + 4, json_data, json_len);
    if (out_written) *out_written = total;
    return 1;
}

uint32_t svdb_wal_decode_entry_length(const uint8_t* buf, size_t buf_size) {
    if (!buf || buf_size < 4) return 0;
    return rd_le32(buf);
}

int svdb_wal_decode_entry_body(const uint8_t* buf, size_t buf_size, size_t offset,
                                const uint8_t** out_body, size_t* out_body_len) {
    if (!buf || !out_body || !out_body_len) return 0;
    if (offset + 4 > buf_size) return 0;

    uint32_t body_len = rd_le32(buf + offset);
    if (offset + 4 + (size_t)body_len > buf_size) return 0;

    *out_body     = buf + offset + 4;
    *out_body_len = (size_t)body_len;
    return 1;
}

int svdb_wal_is_valid_entry(const uint8_t* buf, size_t buf_size) {
    if (!buf || buf_size < 4) return 0;
    uint32_t body_len = rd_le32(buf);
    return (size_t)(4 + body_len) <= buf_size;
}

/* -------------------------------------------------------------------------
 * Record constructors
 * ---------------------------------------------------------------------- */

int svdb_wal_create_insert_record(uint8_t* out_buf, size_t buf_size,
                                   const uint8_t* json_vals, size_t json_len) {
    if (!out_buf || !json_vals) return 0;
    /* Body: {"op":1,"vals":<json_vals>} */
    static const char prefix[] = "{\"op\":1,\"vals\":";
    static const char suffix[] = "}";
    size_t prefix_len = sizeof(prefix) - 1;
    size_t suffix_len = sizeof(suffix) - 1;
    size_t body_len   = prefix_len + json_len + suffix_len;
    size_t total      = 4 + body_len;

    if (buf_size < total) return 0;

    wr_le32(out_buf, (uint32_t)body_len);
    uint8_t* p = out_buf + 4;
    memcpy(p, prefix, prefix_len); p += prefix_len;
    memcpy(p, json_vals, json_len); p += json_len;
    memcpy(p, suffix, suffix_len);
    return 1;
}

int svdb_wal_create_delete_record(uint8_t* out_buf, size_t buf_size, int64_t idx) {
    if (!out_buf) return 0;
    /* Body: {"op":2,"idx":<idx>} */
    char body[64];
    int body_len = snprintf(body, sizeof(body), "{\"op\":2,\"idx\":%" PRId64 "}",
                            idx);
    if (body_len < 0 || (size_t)body_len >= sizeof(body)) return 0;
    size_t total = 4 + (size_t)body_len;
    if (buf_size < total) return 0;

    wr_le32(out_buf, (uint32_t)body_len);
    memcpy(out_buf + 4, body, (size_t)body_len);
    return 1;
}

int svdb_wal_create_update_record(uint8_t* out_buf, size_t buf_size,
                                   int64_t idx,
                                   const uint8_t* json_vals, size_t json_len) {
    if (!out_buf || !json_vals) return 0;
    /* Body: {"op":3,"idx":<idx>,"vals":<json_vals>} */
    char header[64];
    int hlen = snprintf(header, sizeof(header), "{\"op\":3,\"idx\":%" PRId64 ",\"vals\":",
                        idx);
    if (hlen < 0 || (size_t)hlen >= sizeof(header)) return 0;

    static const char suffix[] = "}";
    size_t suffix_len = sizeof(suffix) - 1;
    size_t body_len   = (size_t)hlen + json_len + suffix_len;
    size_t total      = 4 + body_len;

    if (buf_size < total) return 0;

    wr_le32(out_buf, (uint32_t)body_len);
    uint8_t* p = out_buf + 4;
    memcpy(p, header, (size_t)hlen);     p += (size_t)hlen;
    memcpy(p, json_vals, json_len);       p += json_len;
    memcpy(p, suffix, suffix_len);
    return 1;
}
