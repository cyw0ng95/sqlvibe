#ifndef SVDB_DS_WAL_H
#define SVDB_DS_WAL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * WAL (Write-Ahead Log) record utilities.
 *
 * BINARY FORMAT (v0.11.5+):
 *   Record: [4 bytes: total_length][1 byte: op_type][payload]
 *
 * Op types:
 *   INSERT (1): [4 bytes: col_count][col values...]
 *   DELETE (2): [8 bytes: rowid]
 *   UPDATE (3): [8 bytes: rowid][4 bytes: col_count][col values...]
 *
 * Column value encoding:
 *   NULL:  [1 byte: type = 0]
 *   INT:   [1 byte: type = 1][8 bytes: int64 value]
 *   REAL:  [1 byte: type = 2][8 bytes: double value]
 *   TEXT:  [1 byte: type = 3][4 bytes: length][data...]
 *   BLOB:  [1 byte: type = 4][4 bytes: length][data...]
 *
 * Type tags match SVDB_VAL_* constants from SF/types.h
 */

/* WAL operation types */
#define SVDB_WAL_OP_INSERT  1
#define SVDB_WAL_OP_DELETE  2
#define SVDB_WAL_OP_UPDATE  3

/* Value type tags (match SVDB_VAL_* in SF/types.h) */
#define SVDB_WAL_VAL_NULL   0
#define SVDB_WAL_VAL_INT    1
#define SVDB_WAL_VAL_REAL   2
#define SVDB_WAL_VAL_TEXT   3
#define SVDB_WAL_VAL_BLOB   4

/* Return the total encoded size for a record body of body_len bytes. */
size_t svdb_wal_entry_total_size(size_t body_len);

/* Encode a length-prefixed record into buf.
 * out_written is set to the number of bytes written.
 * Returns 1 on success, 0 if buf_size is too small. */
int svdb_wal_encode_entry(uint8_t* buf, size_t buf_size,
                           const uint8_t* body_data, size_t body_len,
                           size_t* out_written);

/* Read the 4-byte little-endian length prefix.
 * Returns the body length, or 0 if buf_size < 4. */
uint32_t svdb_wal_decode_entry_length(const uint8_t* buf, size_t buf_size);

/* Set out_body to point at the body that starts at buf[offset+4].
 * out_body_len is set to the length read from the prefix.
 * Returns 1 on success, 0 if the buffer does not contain a complete record. */
int svdb_wal_decode_entry_body(const uint8_t* buf, size_t buf_size, size_t offset,
                                const uint8_t** out_body, size_t* out_body_len);

/* Return 1 if buf contains a complete, valid entry (prefix + body). */
int svdb_wal_is_valid_entry(const uint8_t* buf, size_t buf_size);

/* Return the operation type (1=INSERT, 2=DELETE, 3=UPDATE) from a body buffer.
 * Returns 0 if body_len < 1. */
uint8_t svdb_wal_get_op_type(const uint8_t* body, size_t body_len);

/* ==========================================================================
 * Binary record constructors (preferred, faster than JSON)
 * ========================================================================== */

/* Encode an INSERT record in binary format.
 * Format: [1 byte: op=1][4 bytes: col_count][col values...]
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_insert_record_binary(uint8_t* out_buf, size_t buf_size,
                                          size_t* out_written,
                                          const uint8_t** col_data, const size_t* col_sizes,
                                          size_t col_count);

/* Encode a DELETE record in binary format.
 * Format: [1 byte: op=2][8 bytes: rowid]
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_delete_record_binary(uint8_t* out_buf, size_t buf_size,
                                          int64_t rowid);

/* Encode an UPDATE record in binary format.
 * Format: [1 byte: op=3][8 bytes: rowid][4 bytes: col_count][col values...]
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_update_record_binary(uint8_t* out_buf, size_t buf_size,
                                          size_t* out_written,
                                          int64_t rowid,
                                          const uint8_t** col_data, const size_t* col_sizes,
                                          size_t col_count);

/* ==========================================================================
 * Value encoding/decoding helpers
 * ========================================================================== */

/* Encode a NULL value: [1 byte: type=0]
 * Returns bytes written (always 1). */
size_t svdb_wal_encode_null(uint8_t* buf, size_t buf_size);

/* Encode an INT value: [1 byte: type=1][8 bytes: value] */
size_t svdb_wal_encode_int(uint8_t* buf, size_t buf_size, int64_t value);

/* Encode a REAL value: [1 byte: type=2][8 bytes: value] */
size_t svdb_wal_encode_real(uint8_t* buf, size_t buf_size, double value);

/* Encode a TEXT value: [1 byte: type=3][4 bytes: len][data] */
size_t svdb_wal_encode_text(uint8_t* buf, size_t buf_size,
                            const char* data, size_t len);

/* Encode a BLOB value: [1 byte: type=4][4 bytes: len][data] */
size_t svdb_wal_encode_blob(uint8_t* buf, size_t buf_size,
                            const uint8_t* data, size_t len);

/* Decode a value from buffer. Returns bytes consumed, or 0 on error.
 * Out parameters are set based on type:
 *   - NULL: *out_type = 0, no other outputs
 *   - INT:  *out_type = 1, *out_int = value
 *   - REAL: *out_type = 2, *out_real = value
 *   - TEXT: *out_type = 3, *out_str = pointer into buf, *out_str_len = length
 *   - BLOB: *out_type = 4, *out_bytes = pointer into buf, *out_bytes_len = length
 */
size_t svdb_wal_decode_value(const uint8_t* buf, size_t buf_size,
                              int* out_type,
                              int64_t* out_int, double* out_real,
                              const char** out_str, size_t* out_str_len,
                              const uint8_t** out_bytes, size_t* out_bytes_len);

/* ==========================================================================
 * Legacy JSON record constructors (deprecated, for backward compatibility)
 * ========================================================================== */

/* Encode an insert record: {"op":1,"vals":<json_vals>}
 * Returns 1 on success, 0 on error.
 * DEPRECATED: Use svdb_wal_create_insert_record_binary instead. */
int svdb_wal_create_insert_record(uint8_t* out_buf, size_t buf_size,
                                   const uint8_t* json_vals, size_t json_len);

/* Encode a delete record: {"op":2,"idx":<idx>}
 * Returns 1 on success, 0 on error.
 * DEPRECATED: Use svdb_wal_create_delete_record_binary instead. */
int svdb_wal_create_delete_record(uint8_t* out_buf, size_t buf_size, int64_t idx);

/* Encode an update record: {"op":3,"idx":<idx>,"vals":<json_vals>}
 * Returns 1 on success, 0 on error.
 * DEPRECATED: Use svdb_wal_create_update_record_binary instead. */
int svdb_wal_create_update_record(uint8_t* out_buf, size_t buf_size,
                                   int64_t idx,
                                   const uint8_t* json_vals, size_t json_len);

/* ==========================================================================
 * WS8: WAL Batch Commit with io_uring support
 * ========================================================================== */

/* Opaque WAL batch writer handle */
typedef struct svdb_wal_batch_writer_s svdb_wal_batch_writer_t;

/*
 * Create a WAL batch writer.
 * path: WAL file path
 * use_io_uring: 1 to use io_uring for async writes, 0 for sync writes
 * batch_size: number of entries to accumulate before flush (0 = default 100)
 */
svdb_wal_batch_writer_t* svdb_wal_batch_writer_create(const char* path,
                                                       int use_io_uring,
                                                       int batch_size);

/*
 * Destroy the batch writer and flush any pending entries.
 */
void svdb_wal_batch_writer_destroy(svdb_wal_batch_writer_t* writer);

/*
 * Add an entry to the batch.
 * Entry is copied into internal buffer.
 * Returns 0 on success, -1 on error.
 */
int svdb_wal_batch_add(svdb_wal_batch_writer_t* writer,
                        const uint8_t* entry_data,
                        size_t entry_len);

/*
 * Add an INSERT record to the batch.
 */
int svdb_wal_batch_add_insert(svdb_wal_batch_writer_t* writer,
                               const uint8_t** col_data,
                               const size_t* col_sizes,
                               size_t col_count);

/*
 * Add a DELETE record to the batch.
 */
int svdb_wal_batch_add_delete(svdb_wal_batch_writer_t* writer,
                               int64_t rowid);

/*
 * Add an UPDATE record to the batch.
 */
int svdb_wal_batch_add_update(svdb_wal_batch_writer_t* writer,
                               int64_t rowid,
                               const uint8_t** col_data,
                               const size_t* col_sizes,
                               size_t col_count);

/*
 * Flush pending entries to disk.
 * Returns number of entries flushed, or -1 on error.
 */
int svdb_wal_batch_flush(svdb_wal_batch_writer_t* writer);

/*
 * Sync the WAL file to disk.
 */
int svdb_wal_batch_sync(svdb_wal_batch_writer_t* writer);

/*
 * Get the current batch size (number of pending entries).
 */
int svdb_wal_batch_pending_count(svdb_wal_batch_writer_t* writer);

/*
 * Get the current WAL file size.
 */
int64_t svdb_wal_batch_file_size(svdb_wal_batch_writer_t* writer);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_WAL_H */
