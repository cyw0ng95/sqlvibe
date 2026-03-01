#ifndef SVDB_DS_COMPRESSION_H
#define SVDB_DS_COMPRESSION_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// LZ4 Compression
// Returns compressed size, or 0 on error
int svdb_lz4_compress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
);

// LZ4 Decompression
// Returns decompressed size, or 0 on error
int svdb_lz4_decompress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
);

// Get maximum compressed size
size_t svdb_lz4_compress_bound(size_t input_size);

// ZSTD Compression
// Returns compressed size, or 0 on error
int svdb_zstd_compress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size,
    int compression_level
);

// ZSTD Decompression
// Returns decompressed size, or 0 on error
int svdb_zstd_decompress(
    const uint8_t* input,
    size_t input_size,
    uint8_t* output,
    size_t output_size
);

// Get recommended ZSTD compression level
int svdb_zstd_default_compression_level(void);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_COMPRESSION_H
