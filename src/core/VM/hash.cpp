#include "hash.h"
#include <cstring>
#include <cstdint>

// Simplified xxHash64 implementation (public domain / MIT)
// Based on the xxHash specification

namespace {

constexpr uint64_t PRIME64_1 = 0x9E3779B185EBCA87ULL;
constexpr uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
constexpr uint64_t PRIME64_3 = 0x165667B19E3779F9ULL;
constexpr uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63ULL;
constexpr uint64_t PRIME64_5 = 0x27D4EB2F165667C5ULL;

inline uint64_t rotl(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}

inline uint64_t mix1(uint64_t h) {
    h ^= h >> 33;
    h *= PRIME64_2;
    h ^= h >> 29;
    h *= PRIME64_3;
    h ^= h >> 32;
    return h;
}

inline uint64_t mix2(uint64_t h, uint64_t v) {
    h += v * PRIME64_2;
    h = rotl(h, 31);
    h *= PRIME64_1;
    return h;
}

inline uint64_t read64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline uint32_t read32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline uint64_t xxh64_impl(const uint8_t* input, size_t len, uint64_t seed) {
    const uint8_t* const end = input + len;
    uint64_t hash;
    
    if (len >= 32) {
        const uint8_t* const limit = end - 32;
        uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
        uint64_t v2 = seed + PRIME64_2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - PRIME64_1;
        
        do {
            v1 = mix2(v1, read64(input));
            input += 8;
            v2 = mix2(v2, read64(input));
            input += 8;
            v3 = mix2(v3, read64(input));
            input += 8;
            v4 = mix2(v4, read64(input));
            input += 8;
        } while (input <= limit);
        
        hash = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
        hash = mix2(hash, v1);
        hash = mix2(hash, v2);
        hash = mix2(hash, v3);
        hash = mix2(hash, v4);
    } else {
        hash = seed + PRIME64_5;
    }
    
    hash += static_cast<uint64_t>(len);
    
    while (input + 8 <= end) {
        hash = mix2(hash, read64(input));
        input += 8;
    }
    
    while (input + 4 <= end) {
        hash = mix2(hash, static_cast<uint64_t>(read32(input)) * PRIME64_1);
        input += 4;
    }
    
    while (input < end) {
        hash = mix2(hash, static_cast<uint64_t>(*input++) * PRIME64_5);
    }
    
    return mix1(hash);
}

} // anonymous namespace

extern "C" {

uint32_t svdb_xxhash32(const void* data, size_t len, uint32_t seed) {
    // Simplified 32-bit hash (using MurmurHash3 finalizer)
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t h = seed;
    
    for (size_t i = 0; i < len; i++) {
        h ^= bytes[i];
        h *= 0x5bd1e995;
        h ^= h >> 15;
    }
    
    h ^= len;
    h *= 0x5bd1e995;
    h ^= h >> 13;
    h *= 0xc4ceb9fe;
    h ^= h >> 16;
    
    return h;
}

uint64_t svdb_xxhash64(const void* data, size_t len, uint64_t seed) {
    return xxh64_impl(
        static_cast<const uint8_t*>(data), 
        len, 
        seed
    );
}

void svdb_xxhash64_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint64_t* hashes,
    size_t count,
    uint64_t seed
) {
    for (size_t i = 0; i < count; i++) {
        hashes[i] = xxh64_impl(keys[i], key_lens[i], seed);
    }
}

void svdb_xxhash32_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint32_t* hashes,
    size_t count,
    uint32_t seed
) {
    for (size_t i = 0; i < count; i++) {
        hashes[i] = svdb_xxhash32(keys[i], key_lens[i], seed);
    }
}

uint64_t svdb_hash_int64(int64_t value, uint64_t seed) {
    // Fast hash for single int64
    uint64_t h = seed ^ static_cast<uint64_t>(value);
    h *= PRIME64_1;
    h ^= h >> 33;
    h *= PRIME64_2;
    h ^= h >> 29;
    h *= PRIME64_3;
    return h;
}

uint64_t svdb_hash_int64_pair(int64_t a, int64_t b, uint64_t seed) {
    // Hash two int64 values
    uint64_t h = seed;
    h = svdb_hash_int64(a, h);
    h = svdb_hash_int64(b, h);
    return mix1(h);
}

} // extern "C"
