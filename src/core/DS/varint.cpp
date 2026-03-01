#include "varint.h"
#include <cstring>

extern "C" {

// GetVarint decodes a varint from buf following SQLite format
// Returns 1 on success, 0 if buffer too small
int svdb_get_varint(const uint8_t* buf, size_t buf_len, int64_t* value, int* bytes_read) {
    if (!buf || buf_len == 0 || !value || !bytes_read) {
        return 0;
    }
    
    // Fast path for single byte (0-127)
    if (buf[0] < 0x80) {
        *value = buf[0];
        *bytes_read = 1;
        return 1;
    }
    
    // Multi-byte decode
    uint64_t v = 0;
    uint32_t shift = 0;
    size_t n = 0;
    
    while (n < 8 && n < buf_len) {
        uint8_t b = buf[n];
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        n++;
        
        if (b < 0x80) {
            *value = static_cast<int64_t>(v);
            *bytes_read = static_cast<int>(n);
            return 1;
        }
        shift += 7;
    }
    
    // 9th byte uses all 8 bits
    if (n < buf_len) {
        v |= static_cast<uint64_t>(buf[n]) << shift;
        n++;
        *value = static_cast<int64_t>(v);
        *bytes_read = static_cast<int>(n);
        return 1;
    }
    
    return 0; // Buffer too small
}

// PutVarint encodes an int64 as a varint
// Returns number of bytes written, or 0 if buffer too small
int svdb_put_varint(uint8_t* buf, size_t buf_len, int64_t value) {
    if (!buf || buf_len == 0) {
        return 0;
    }
    
    uint64_t uv = static_cast<uint64_t>(value);
    
    // Handle single byte case (0-127)
    if (uv < 0x80) {
        buf[0] = static_cast<uint8_t>(uv);
        return 1;
    }
    
    // Multi-byte encoding
    size_t n = 0;
    while (n < 8 && uv >= 0x80) {
        if (n >= buf_len) {
            return 0; // Buffer too small
        }
        buf[n] = static_cast<uint8_t>(uv) | 0x80;
        uv >>= 7;
        n++;
    }
    
    // Last byte doesn't have continuation bit
    if (n >= buf_len) {
        return 0;
    }
    buf[n] = static_cast<uint8_t>(uv);
    return static_cast<int>(n + 1);
}

// VarintLen returns the number of bytes required to encode v as a varint
int svdb_varint_len(int64_t value) {
    uint64_t uv = static_cast<uint64_t>(value);
    
    if (uv < 0x80) {
        return 1;
    }
    
    // Calculate bytes needed: ceil(bits / 7)
    int bits = 0;
    uint64_t tmp = uv;
    while (tmp > 0) {
        bits++;
        tmp >>= 1;
    }
    
    int n = (bits + 6) / 7;
    if (n > 9) {
        return 9;
    }
    return n;
}

// GetVarint32 - optimized for 32-bit values
int svdb_get_varint32(const uint8_t* buf, size_t buf_len, uint32_t* value, int* bytes_read) {
    if (!buf || buf_len == 0 || !value || !bytes_read) {
        return 0;
    }
    
    // Fast path for single byte
    if (buf[0] < 0x80) {
        *value = buf[0];
        *bytes_read = 1;
        return 1;
    }
    
    uint32_t v = 0;
    uint32_t shift = 0;
    size_t n = 0;
    
    while (n < 5 && n < buf_len) {
        uint8_t b = buf[n];
        v |= static_cast<uint32_t>(b & 0x7F) << shift;
        n++;
        
        if (b < 0x80) {
            *value = v;
            *bytes_read = static_cast<int>(n);
            return 1;
        }
        shift += 7;
    }
    
    return 0;
}

// PutVarint32 - optimized for 32-bit values
int svdb_put_varint32(uint8_t* buf, size_t buf_len, uint32_t value) {
    if (!buf || buf_len == 0) {
        return 0;
    }
    
    // Handle single byte case
    if (value < 0x80) {
        buf[0] = static_cast<uint8_t>(value);
        return 1;
    }
    
    size_t n = 0;
    while (value >= 0x80) {
        if (n >= buf_len) {
            return 0;
        }
        buf[n] = static_cast<uint8_t>(value) | 0x80;
        value >>= 7;
        n++;
    }
    
    if (n >= buf_len) {
        return 0;
    }
    buf[n] = static_cast<uint8_t>(value);
    return static_cast<int>(n + 1);
}

// Batch varint decode
int svdb_batch_get_varint(const uint8_t* buf, size_t buf_len, int64_t* values, int max_values, int* total_bytes) {
    if (!buf || !values || max_values <= 0 || !total_bytes) {
        return 0;
    }
    
    int count = 0;
    size_t offset = 0;
    
    while (count < max_values && offset < buf_len) {
        int bytes_read;
        int64_t value;
        
        if (!svdb_get_varint(buf + offset, buf_len - offset, &value, &bytes_read)) {
            break;
        }
        
        values[count++] = value;
        offset += bytes_read;
    }
    
    *total_bytes = static_cast<int>(offset);
    return count;
}

// Batch varint encode
int svdb_batch_put_varint(uint8_t* buf, size_t buf_len, const int64_t* values, int count) {
    if (!buf || !values || count <= 0) {
        return 0;
    }
    
    int encoded = 0;
    size_t offset = 0;
    
    for (int i = 0; i < count; i++) {
        int bytes = svdb_put_varint(buf + offset, buf_len - offset, values[i]);
        if (bytes == 0) {
            break;
        }
        offset += bytes;
        encoded++;
    }
    
    return encoded;
}

} // extern "C"
