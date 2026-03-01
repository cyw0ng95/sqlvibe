#ifndef SVDB_VM_HASH_H
#define SVDB_VM_HASH_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// xxHash32 - 32-bit hash
uint32_t svdb_xxhash32(const void* data, size_t len, uint32_t seed);

// xxHash64 - 64-bit hash  
uint64_t svdb_xxhash64(const void* data, size_t len, uint64_t seed);

// Batch hash 64 - hash multiple keys efficiently
void svdb_xxhash64_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint64_t* hashes,
    size_t count,
    uint64_t seed
);

// Batch hash 32
void svdb_xxhash32_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint32_t* hashes,
    size_t count,
    uint32_t seed
);

// Fast hash for integers
uint64_t svdb_hash_int64(int64_t value, uint64_t seed);

// Fast hash for two int64 (for composite keys)
uint64_t svdb_hash_int64_pair(int64_t a, int64_t b, uint64_t seed);

#ifdef __cplusplus
}
#endif

#endif // SVDB_VM_HASH_H
