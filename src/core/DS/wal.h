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
 * Each record uses a length-prefixed binary format:
 *   [4-byte little-endian length][JSON body of exactly `length` bytes]
 *
 * JSON body formats:
 *   Insert : {"op":1,"vals":<json_vals>}
 *   Delete : {"op":2,"idx":<idx>}
 *   Update : {"op":3,"idx":<idx>,"vals":<json_vals>}
 */

/* Return the total encoded size (4 + json_len) for a record body of json_len bytes. */
size_t svdb_wal_entry_total_size(size_t json_len);

/* Encode a length-prefixed record into buf.
 * out_written is set to the number of bytes written.
 * Returns 1 on success, 0 if buf_size is too small. */
int svdb_wal_encode_entry(uint8_t* buf, size_t buf_size,
                           const uint8_t* json_data, size_t json_len,
                           size_t* out_written);

/* Read the 4-byte little-endian length prefix.
 * Returns the body length, or 0 if buf_size < 4. */
uint32_t svdb_wal_decode_entry_length(const uint8_t* buf, size_t buf_size);

/* Set out_body to point at the JSON body that starts at buf[offset+4].
 * out_body_len is set to the length read from the prefix.
 * Returns 1 on success, 0 if the buffer does not contain a complete record. */
int svdb_wal_decode_entry_body(const uint8_t* buf, size_t buf_size, size_t offset,
                                const uint8_t** out_body, size_t* out_body_len);

/* Return 1 if buf contains a complete, valid entry (prefix + body). */
int svdb_wal_is_valid_entry(const uint8_t* buf, size_t buf_size);

/* Encode an insert record: {"op":1,"vals":<json_vals>}
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_insert_record(uint8_t* out_buf, size_t buf_size,
                                   const uint8_t* json_vals, size_t json_len);

/* Encode a delete record: {"op":2,"idx":<idx>}
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_delete_record(uint8_t* out_buf, size_t buf_size, int64_t idx);

/* Encode an update record: {"op":3,"idx":<idx>,"vals":<json_vals>}
 * Returns 1 on success, 0 on error. */
int svdb_wal_create_update_record(uint8_t* out_buf, size_t buf_size,
                                   int64_t idx,
                                   const uint8_t* json_vals, size_t json_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_WAL_H */
