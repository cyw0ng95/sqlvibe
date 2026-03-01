#include "compression.h"
#include <cstring>
#include <algorithm>

// Simple LZ4-style compression (simplified for demonstration)
// In production, use the actual LZ4 library

namespace {

// Fast copy
inline void fast_copy(const uint8_t* src, uint8_t* dst, size_t len) {
    std::memcpy(dst, src, len);
}

// Find match in sliding window
size_t find_match(
    const uint8_t* data,
    size_t pos,
    size_t size,
    size_t* match_pos,
    size_t window_size = 64 * 1024
) {
    size_t window_start = (pos > window_size) ? pos - window_size : 0;
    size_t max_len = 0;
    
    for (size_t i = window_start; i < pos; i++) {
        size_t len = 0;
        while (pos + len < size && data[i + len] == data[pos + len] && len < 4096) {
            len++;
        }
        if (len > max_len) {
            max_len = len;
            *match_pos = i;
        }
    }
    
    return max_len;
}

} // anonymous namespace

extern "C" {

int svdb_lz4_compress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
) {
    if (input_size == 0 || output_size == 0) {
        return 0;
    }
    
    // Simple LZ4-style compression
    size_t out_pos = 0;
    size_t in_pos = 0;
    
    while (in_pos < input_size) {
        // Check if we have enough output space
        if (out_pos + 16 >= output_size) {
            return 0;
        }
        
        size_t match_pos = 0;
        size_t match_len = find_match(input, in_pos, input_size, &match_pos);
        
        if (match_len >= 4) {
            // Encode match: [offset:2 bytes][length:1 byte]
            size_t offset = in_pos - match_pos;
            output[out_pos++] = static_cast<uint8_t>(match_len);
            output[out_pos++] = static_cast<uint8_t>(offset & 0xFF);
            output[out_pos++] = static_cast<uint8_t>((offset >> 8) & 0xFF);
            in_pos += match_len;
        } else {
            // Copy literal
            size_t literal_len = std::min(size_t(64), input_size - in_pos);
            output[out_pos++] = static_cast<uint8_t>(literal_len | 0x80);
            fast_copy(input + in_pos, output + out_pos, literal_len);
            out_pos += literal_len;
            in_pos += literal_len;
        }
    }
    
    return static_cast<int>(out_pos);
}

int svdb_lz4_decompress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
) {
    if (input_size == 0 || output_size == 0) {
        return 0;
    }
    
    size_t in_pos = 0;
    size_t out_pos = 0;
    
    while (in_pos < input_size && out_pos < output_size) {
        uint8_t token = input[in_pos++];
        
        if (token & 0x80) {
            // Literal
            size_t literal_len = token & 0x7F;
            if (in_pos + literal_len > input_size || out_pos + literal_len > output_size) {
                return 0;
            }
            fast_copy(input + in_pos, output + out_pos, literal_len);
            in_pos += literal_len;
            out_pos += literal_len;
        } else {
            // Match
            if (in_pos + 2 >= input_size) {
                return 0;
            }
            size_t match_len = token;
            size_t offset = input[in_pos] | (input[in_pos + 1] << 8);
            in_pos += 2;
            
            if (out_pos < offset || out_pos + match_len > output_size) {
                return 0;
            }
            
            // Copy from match position
            for (size_t i = 0; i < match_len; i++) {
                output[out_pos + i] = output[out_pos + i - offset];
            }
            out_pos += match_len;
        }
    }
    
    return static_cast<int>(out_pos);
}

size_t svdb_lz4_compress_bound(size_t input_size) {
    // LZ4 worst case: input_size + input_size/256 + 16
    return input_size + (input_size >> 8) + 16;
}

// ZSTD compression (simplified - in production use actual ZSTD library)
int svdb_zstd_compress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size,
    int compression_level
) {
    // For now, use LZ4 compression
    // In production, integrate actual ZSTD library
    (void)compression_level;
    return svdb_lz4_compress(input, input_size, output, output_size);
}

int svdb_zstd_decompress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
) {
    // For now, use LZ4 decompression
    return svdb_lz4_decompress(input, input_size, output, output_size);
}

int svdb_zstd_default_compression_level(void) {
    return 3;  // Default compression level
}

} // extern "C"
