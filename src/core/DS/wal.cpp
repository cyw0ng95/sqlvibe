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

/* Write an 8-byte little-endian int64. */
static inline void wr_le64(uint8_t* p, int64_t v) {
    uint64_t uv = (uint64_t)v;
    p[0] = (uint8_t)(uv);
    p[1] = (uint8_t)(uv >>  8);
    p[2] = (uint8_t)(uv >> 16);
    p[3] = (uint8_t)(uv >> 24);
    p[4] = (uint8_t)(uv >> 32);
    p[5] = (uint8_t)(uv >> 40);
    p[6] = (uint8_t)(uv >> 48);
    p[7] = (uint8_t)(uv >> 56);
}

/* Read an 8-byte little-endian int64. */
static inline int64_t rd_le64(const uint8_t* p) {
    return (int64_t)(
        (uint64_t)p[0]        |
        ((uint64_t)p[1] <<  8) |
        ((uint64_t)p[2] << 16) |
        ((uint64_t)p[3] << 24) |
        ((uint64_t)p[4] << 32) |
        ((uint64_t)p[5] << 40) |
        ((uint64_t)p[6] << 48) |
        ((uint64_t)p[7] << 56));
}

/* Write an 8-byte little-endian double. */
static inline void wr_ledouble(uint8_t* p, double v) {
    uint64_t uv;
    memcpy(&uv, &v, sizeof(uv));
    wr_le64(p, (int64_t)uv);
}

/* Read an 8-byte little-endian double. */
static inline double rd_ledouble(const uint8_t* p) {
    uint64_t uv = (uint64_t)rd_le64(p);
    double v;
    memcpy(&v, &uv, sizeof(v));
    return v;
}

/* -------------------------------------------------------------------------
 * Public API - Core functions
 * ---------------------------------------------------------------------- */

size_t svdb_wal_entry_total_size(size_t body_len) {
    return 4 + body_len;
}

int svdb_wal_encode_entry(uint8_t* buf, size_t buf_size,
                           const uint8_t* body_data, size_t body_len,
                           size_t* out_written) {
    if (!buf || !body_data) return 0;
    size_t total = svdb_wal_entry_total_size(body_len);
    if (buf_size < total) return 0;

    wr_le32(buf, (uint32_t)body_len);
    memcpy(buf + 4, body_data, body_len);
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

uint8_t svdb_wal_get_op_type(const uint8_t* body, size_t body_len) {
    if (!body || body_len < 1) return 0;
    return body[0];
}

/* -------------------------------------------------------------------------
 * Value encoding functions
 * ---------------------------------------------------------------------- */

size_t svdb_wal_encode_null(uint8_t* buf, size_t buf_size) {
    if (!buf || buf_size < 1) return 0;
    buf[0] = SVDB_WAL_VAL_NULL;
    return 1;
}

size_t svdb_wal_encode_int(uint8_t* buf, size_t buf_size, int64_t value) {
    if (!buf || buf_size < 9) return 0;
    buf[0] = SVDB_WAL_VAL_INT;
    wr_le64(buf + 1, value);
    return 9;
}

size_t svdb_wal_encode_real(uint8_t* buf, size_t buf_size, double value) {
    if (!buf || buf_size < 9) return 0;
    buf[0] = SVDB_WAL_VAL_REAL;
    wr_ledouble(buf + 1, value);
    return 9;
}

size_t svdb_wal_encode_text(uint8_t* buf, size_t buf_size,
                            const char* data, size_t len) {
    if (!buf) return 0;
    if (!data && len > 0) return 0;
    size_t needed = 1 + 4 + len;
    if (buf_size < needed) return 0;
    buf[0] = SVDB_WAL_VAL_TEXT;
    wr_le32(buf + 1, (uint32_t)len);
    if (len > 0) {
        memcpy(buf + 5, data, len);
    }
    return needed;
}

size_t svdb_wal_encode_blob(uint8_t* buf, size_t buf_size,
                            const uint8_t* data, size_t len) {
    if (!buf) return 0;
    if (!data && len > 0) return 0;
    size_t needed = 1 + 4 + len;
    if (buf_size < needed) return 0;
    buf[0] = SVDB_WAL_VAL_BLOB;
    wr_le32(buf + 1, (uint32_t)len);
    if (len > 0) {
        memcpy(buf + 5, data, len);
    }
    return needed;
}

/* -------------------------------------------------------------------------
 * Value decoding function
 * ---------------------------------------------------------------------- */

size_t svdb_wal_decode_value(const uint8_t* buf, size_t buf_size,
                              int* out_type,
                              int64_t* out_int, double* out_real,
                              const char** out_str, size_t* out_str_len,
                              const uint8_t** out_bytes, size_t* out_bytes_len) {
    if (!buf || buf_size < 1 || !out_type) return 0;

    uint8_t type = buf[0];
    *out_type = (int)type;

    switch (type) {
        case SVDB_WAL_VAL_NULL:
            return 1;

        case SVDB_WAL_VAL_INT:
            if (buf_size < 9) return 0;
            if (out_int) *out_int = rd_le64(buf + 1);
            return 9;

        case SVDB_WAL_VAL_REAL:
            if (buf_size < 9) return 0;
            if (out_real) *out_real = rd_ledouble(buf + 1);
            return 9;

        case SVDB_WAL_VAL_TEXT:
            if (buf_size < 5) return 0;
            {
                uint32_t len = rd_le32(buf + 1);
                if (buf_size < 5 + len) return 0;
                if (out_str) *out_str = (const char*)(buf + 5);
                if (out_str_len) *out_str_len = len;
                return 5 + len;
            }

        case SVDB_WAL_VAL_BLOB:
            if (buf_size < 5) return 0;
            {
                uint32_t len = rd_le32(buf + 1);
                if (buf_size < 5 + len) return 0;
                if (out_bytes) *out_bytes = buf + 5;
                if (out_bytes_len) *out_bytes_len = len;
                return 5 + len;
            }

        default:
            return 0;  /* Unknown type */
    }
}

/* -------------------------------------------------------------------------
 * Binary record constructors
 * ---------------------------------------------------------------------- */

int svdb_wal_create_insert_record_binary(uint8_t* out_buf, size_t buf_size,
                                          size_t* out_written,
                                          const uint8_t** col_data, const size_t* col_sizes,
                                          size_t col_count) {
    if (!out_buf || !col_data || !col_sizes) return 0;

    /* Calculate total payload size: op(1) + col_count(4) + sum of col sizes */
    size_t payload_size = 1 + 4;
    for (size_t i = 0; i < col_count; i++) {
        payload_size += col_sizes[i];
    }

    size_t total = 4 + payload_size;
    if (buf_size < total) return 0;

    /* Write length prefix */
    wr_le32(out_buf, (uint32_t)payload_size);

    /* Write op type */
    uint8_t* p = out_buf + 4;
    *p++ = SVDB_WAL_OP_INSERT;

    /* Write column count */
    wr_le32(p, (uint32_t)col_count);
    p += 4;

    /* Write column data */
    for (size_t i = 0; i < col_count; i++) {
        if (col_sizes[i] > 0) {
            memcpy(p, col_data[i], col_sizes[i]);
            p += col_sizes[i];
        }
    }

    if (out_written) *out_written = total;
    return 1;
}

int svdb_wal_create_delete_record_binary(uint8_t* out_buf, size_t buf_size,
                                          int64_t rowid) {
    if (!out_buf) return 0;

    /* Payload: op(1) + rowid(8) = 9 bytes */
    size_t payload_size = 9;
    size_t total = 4 + payload_size;
    if (buf_size < total) return 0;

    /* Write length prefix */
    wr_le32(out_buf, (uint32_t)payload_size);

    /* Write op type */
    uint8_t* p = out_buf + 4;
    *p++ = SVDB_WAL_OP_DELETE;

    /* Write rowid */
    wr_le64(p, rowid);

    return 1;
}

int svdb_wal_create_update_record_binary(uint8_t* out_buf, size_t buf_size,
                                          size_t* out_written,
                                          int64_t rowid,
                                          const uint8_t** col_data, const size_t* col_sizes,
                                          size_t col_count) {
    if (!out_buf || !col_data || !col_sizes) return 0;

    /* Calculate total payload size: op(1) + rowid(8) + col_count(4) + sum of col sizes */
    size_t payload_size = 1 + 8 + 4;
    for (size_t i = 0; i < col_count; i++) {
        payload_size += col_sizes[i];
    }

    size_t total = 4 + payload_size;
    if (buf_size < total) return 0;

    /* Write length prefix */
    wr_le32(out_buf, (uint32_t)payload_size);

    /* Write op type */
    uint8_t* p = out_buf + 4;
    *p++ = SVDB_WAL_OP_UPDATE;

    /* Write rowid */
    wr_le64(p, rowid);
    p += 8;

    /* Write column count */
    wr_le32(p, (uint32_t)col_count);
    p += 4;

    /* Write column data */
    for (size_t i = 0; i < col_count; i++) {
        if (col_sizes[i] > 0) {
            memcpy(p, col_data[i], col_sizes[i]);
            p += col_sizes[i];
        }
    }

    if (out_written) *out_written = total;
    return 1;
}

/* -------------------------------------------------------------------------
 * Legacy JSON record constructors (deprecated)
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

/* ==========================================================================
 * WS8: WAL Batch Commit with io_uring support
 * ========================================================================== */

#include <vector>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#ifdef __linux__
#include "../PB/io_uring.h"
#endif

struct svdb_wal_batch_writer_s {
    std::string path;
    int fd;
    int64_t file_offset;
    int use_io_uring;
    int batch_size;
    int pending_count;

    /* Batch buffer */
    std::vector<uint8_t> batch_buffer;

#ifdef __linux__
    svdb_io_uring_t* io_uring;
#endif

    svdb_wal_batch_writer_s() : fd(-1), file_offset(0), use_io_uring(0),
                                  batch_size(100), pending_count(0) {}
};

svdb_wal_batch_writer_t* svdb_wal_batch_writer_create(const char* path,
                                                       int use_io_uring,
                                                       int batch_size) {
    if (!path) return nullptr;

    svdb_wal_batch_writer_t* writer = new (std::nothrow) svdb_wal_batch_writer_t();
    if (!writer) return nullptr;

    writer->path = path;
    writer->batch_size = batch_size > 0 ? batch_size : 100;
    writer->use_io_uring = use_io_uring;

    /* Open file for append */
    int flags = O_WRONLY | O_CREAT | O_CLOEXEC;
    writer->fd = open(path, flags, 0644);
    if (writer->fd < 0) {
        delete writer;
        return nullptr;
    }

    /* Get current file size */
    struct stat st;
    if (fstat(writer->fd, &st) == 0) {
        writer->file_offset = st.st_size;
    }

    /* Seek to end */
    lseek(writer->fd, 0, SEEK_END);

#ifdef __linux__
    if (use_io_uring && svdb_io_uring_available()) {
        writer->io_uring = svdb_io_uring_create(256, 0);
    }
#endif

    writer->batch_buffer.reserve(writer->batch_size * 256);  /* Pre-allocate */

    return writer;
}

void svdb_wal_batch_writer_destroy(svdb_wal_batch_writer_t* writer) {
    if (!writer) return;

    /* Flush any pending entries */
    svdb_wal_batch_flush(writer);

#ifdef __linux__
    if (writer->io_uring) {
        svdb_io_uring_destroy(writer->io_uring);
    }
#endif

    if (writer->fd >= 0) {
        fsync(writer->fd);
        close(writer->fd);
    }

    delete writer;
}

int svdb_wal_batch_add(svdb_wal_batch_writer_t* writer,
                        const uint8_t* entry_data,
                        size_t entry_len) {
    if (!writer || !entry_data || writer->fd < 0) return -1;

    /* Append to batch buffer */
    writer->batch_buffer.insert(writer->batch_buffer.end(),
                                 entry_data, entry_data + entry_len);
    writer->pending_count++;

    /* Auto-flush if batch is full */
    if (writer->pending_count >= writer->batch_size) {
        return svdb_wal_batch_flush(writer);
    }

    return 0;
}

int svdb_wal_batch_add_insert(svdb_wal_batch_writer_t* writer,
                               const uint8_t** col_data,
                               const size_t* col_sizes,
                               size_t col_count) {
    if (!writer) return -1;

    uint8_t buffer[4096];
    size_t written;

    if (!svdb_wal_create_insert_record_binary(buffer, sizeof(buffer),
                                                &written, col_data, col_sizes, col_count)) {
        return -1;
    }

    return svdb_wal_batch_add(writer, buffer, written);
}

int svdb_wal_batch_add_delete(svdb_wal_batch_writer_t* writer,
                               int64_t rowid) {
    if (!writer) return -1;

    uint8_t buffer[32];

    if (!svdb_wal_create_delete_record_binary(buffer, sizeof(buffer), rowid)) {
        return -1;
    }

    return svdb_wal_batch_add(writer, buffer, 4 + 9);  /* length prefix + payload */
}

int svdb_wal_batch_add_update(svdb_wal_batch_writer_t* writer,
                               int64_t rowid,
                               const uint8_t** col_data,
                               const size_t* col_sizes,
                               size_t col_count) {
    if (!writer) return -1;

    uint8_t buffer[4096];
    size_t written;

    if (!svdb_wal_create_update_record_binary(buffer, sizeof(buffer),
                                                &written, rowid, col_data, col_sizes, col_count)) {
        return -1;
    }

    return svdb_wal_batch_add(writer, buffer, written);
}

int svdb_wal_batch_flush(svdb_wal_batch_writer_t* writer) {
    if (!writer || writer->fd < 0) return -1;
    if (writer->batch_buffer.empty()) return 0;

    int flushed = writer->pending_count;

#ifdef __linux__
    if (writer->use_io_uring && writer->io_uring) {
        /* Use io_uring for async write */
        svdb_io_uring_write_sync(writer->io_uring, nullptr,
                                  writer->batch_buffer.data(),
                                  writer->batch_buffer.size(),
                                  writer->file_offset);
        writer->file_offset += writer->batch_buffer.size();
    } else
#endif
    {
        /* Sync write */
        ssize_t written = write(writer->fd, writer->batch_buffer.data(),
                                 writer->batch_buffer.size());
        if (written < 0) {
            return -1;
        }
        writer->file_offset += written;
    }

    /* Clear batch buffer */
    writer->batch_buffer.clear();
    writer->pending_count = 0;

    return flushed;
}

int svdb_wal_batch_sync(svdb_wal_batch_writer_t* writer) {
    if (!writer || writer->fd < 0) return -1;

    /* Flush first */
    svdb_wal_batch_flush(writer);

    /* Sync to disk */
    return fsync(writer->fd);
}

int svdb_wal_batch_pending_count(svdb_wal_batch_writer_t* writer) {
    return writer ? writer->pending_count : 0;
}

int64_t svdb_wal_batch_file_size(svdb_wal_batch_writer_t* writer) {
    return writer ? writer->file_offset : -1;
}