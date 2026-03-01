#ifndef SVDB_DS_VARINT_H
#define SVDB_DS_VARINT_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Varint encoding/decoding following SQLite format
// Maximum 9 bytes for 64-bit values

// GetVarint decodes a varint from buf and returns (value, bytes_read)
// Returns 0 if buffer is too small
int svdb_get_varint(const uint8_t* buf, size_t buf_len, int64_t* value, int* bytes_read);

// PutVarint encodes an int64 as a varint
// Returns number of bytes written, or 0 if buffer too small
int svdb_put_varint(uint8_t* buf, size_t buf_len, int64_t value);

// VarintLen returns the number of bytes required to encode v as a varint
int svdb_varint_len(int64_t value);

// GetVarint32 decodes a varint that fits in 32 bits (optimization for small values)
int svdb_get_varint32(const uint8_t* buf, size_t buf_len, uint32_t* value, int* bytes_read);

// PutVarint32 encodes a 32-bit value as a varint (optimization for small values)
int svdb_put_varint32(uint8_t* buf, size_t buf_len, uint32_t value);

// Batch varint operations for improved throughput
// Decode multiple varints from a buffer
// Returns number of varints successfully decoded
int svdb_batch_get_varint(const uint8_t* buf, size_t buf_len, int64_t* values, int max_values, int* total_bytes);

// Encode multiple varints to a buffer
// Returns number of varints successfully encoded
int svdb_batch_put_varint(uint8_t* buf, size_t buf_len, const int64_t* values, int count);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_VARINT_H
