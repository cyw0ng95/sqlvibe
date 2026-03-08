#include "fast_hash_table.h"
#include "hash.h"
#include <cstring>
#include <new>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

namespace svdb::vm {

extern "C" {

uint64_t svdb_crc32_hash(const void* data, size_t len) {
#ifdef __SSE4_2__
    uint64_t hash = 0xFFFFFFFF;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    
    size_t i = 0;
    
    for (; i + 8 <= len; i += 8) {
        uint64_t chunk;
        memcpy(&chunk, bytes + i, 8);
        hash = _mm_crc32_u64(hash, chunk);
    }
    
    for (; i + 4 <= len; i += 4) {
        uint32_t chunk;
        memcpy(&chunk, bytes + i, 4);
        hash = _mm_crc32_u32(hash, chunk);
    }
    
    for (; i < len; i++) {
        hash = _mm_crc32_u8(hash, bytes[i]);
    }
    
    return hash ^ 0xFFFFFFFF;
#else
    return svdb_xxhash64(data, len, 0x123456789ABCDEF0ULL);
#endif
}

void svdb_crc32_hash_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint64_t* hashes,
    size_t count
) {
    for (size_t i = 0; i < count; i++) {
        hashes[i] = svdb_crc32_hash(keys[i], key_lens[i]);
    }
}

FastHashTable* svdb_fast_hash_table_build(
    const svdb_row_t* rows,
    size_t n,
    size_t key_col,
    svdb::ds::svdb_arena_v2_t* arena
) {
    if (!rows || n == 0 || !arena) {
        return nullptr;
    }
    
    void* mem = svdb_arena_v2_alloc(arena, sizeof(FastHashTable));
    if (!mem) return nullptr;
    
    FastHashTable* table = new (mem) FastHashTable();
    table->arena = reinterpret_cast<svdb::ds::ArenaV2*>(arena);
    table->count = 0;
    
    size_t capacity = 1;
    while (capacity < n * 2) {
        capacity <<= 1;
    }
    capacity = (capacity > 64) ? capacity : 64;
    
    table->capacity = capacity;
    
    table->keys = static_cast<uint64_t*>(
        svdb_arena_v2_calloc(arena, capacity, sizeof(uint64_t))
    );
    table->values = static_cast<uint32_t*>(
        svdb_arena_v2_calloc(arena, capacity, sizeof(uint32_t))
    );
    table->probes = static_cast<uint8_t*>(
        svdb_arena_v2_calloc(arena, capacity, sizeof(uint8_t))
    );
    
    if (!table->keys || !table->values || !table->probes) {
        return nullptr;
    }
    
    for (size_t i = 0; i < capacity; i++) {
        table->keys[i] = FastHashTable::EMPTY_KEY;
    }
    
    for (size_t i = 0; i < n; i++) {
        const svdb_row_t& row = rows[i];
        
        if (key_col >= row.num_columns) continue;
        
        const char* keyVal = row.values[key_col];
        size_t keyLen = row.value_lens ? row.value_lens[key_col] : 
                        (keyVal ? strlen(keyVal) : 0);
        
        if (!keyVal || keyLen == 0) continue;
        
        uint64_t hash = svdb_crc32_hash(keyVal, keyLen);
        table->Insert(hash, static_cast<uint32_t>(i));
    }
    
    return table;
}

void svdb_fast_hash_table_destroy(FastHashTable* table) {
}

uint32_t* svdb_fast_hash_table_find(
    FastHashTable* table,
    uint64_t hash,
    size_t* count
) {
    if (!table || !count) {
        *count = 0;
        return nullptr;
    }
    
    return table->Find(hash, count);
}

} 

void FastHashTable::Insert(uint64_t hash, uint32_t row_idx) {
    if (hash == EMPTY_KEY) {
        hash = 0xDEADBEEF;
    }
    
    size_t idx = HashToBucket(hash);
    uint8_t probe_len = 0;
    
    while (!IsEmpty(idx) && probe_len < MAX_PROBE) {
        idx = NextBucket(idx);
        probe_len++;
    }
    
    if (probe_len < MAX_PROBE) {
        keys[idx] = hash;
        values[idx] = row_idx;
        probes[idx] = probe_len;
        count++;
    }
}

uint32_t* FastHashTable::Find(uint64_t hash, size_t* match_count) {
    *match_count = 0;
    if (hash == EMPTY_KEY) {
        hash = 0xDEADBEEF;
    }
    
    size_t idx = HashToBucket(hash);
    uint8_t probe_len = 0;
    
    static uint32_t matches[256];
    size_t max_matches = sizeof(matches) / sizeof(matches[0]);
    
    while (probe_len < MAX_PROBE && *match_count < max_matches) {
        if (IsEmpty(idx)) {
            break;
        }
        
        if (keys[idx] == hash && probes[idx] >= probe_len) {
            matches[*match_count] = values[idx];
            (*match_count)++;
        }
        
        idx = NextBucket(idx);
        probe_len++;
    }
    
    return (*match_count > 0) ? matches : nullptr;
}

FastHashTable* FastHashTable::Build(const svdb_row_t* rows, size_t n, 
                                    size_t key_col, svdb::ds::ArenaV2* arena) {
    return svdb_fast_hash_table_build(
        rows, n, key_col, 
        reinterpret_cast<svdb::ds::svdb_arena_v2_t*>(arena)
    );
}

} 
