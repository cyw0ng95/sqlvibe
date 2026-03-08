#ifndef SVDB_VM_FAST_HASH_TABLE_H
#define SVDB_VM_FAST_HASH_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include "../DS/arena_v2.h"
#include "hash_join.h"

namespace svdb::vm {

class FastHashTable {
public:
    static constexpr size_t BUCKET_COUNT = 65536;
    static constexpr size_t MAX_PROBE = 16;
    static constexpr uint64_t EMPTY_KEY = 0xFFFFFFFFFFFFFFFFULL;
    
    uint64_t* keys;
    uint32_t* values;
    uint8_t* probes;
    size_t count;
    size_t capacity;
    svdb::ds::ArenaV2* arena;
    
    FastHashTable() : keys(nullptr), values(nullptr), probes(nullptr), 
                      count(0), capacity(0), arena(nullptr) {}
    
    ~FastHashTable() = default;
    
    static FastHashTable* Build(const svdb_row_t* rows, size_t n, 
                                size_t key_col, svdb::ds::ArenaV2* arena);
    
    uint32_t* Find(uint64_t hash, size_t* match_count);
    
    void Insert(uint64_t hash, uint32_t row_idx);
    
    bool IsEmpty(size_t idx) const {
        return keys[idx] == EMPTY_KEY;
    }
    
private:
    size_t HashToBucket(uint64_t hash) const {
        return hash & (capacity - 1);
    }
    
    size_t NextBucket(size_t idx) const {
        return (idx + 1) & (capacity - 1);
    }
};

extern "C" {

uint64_t svdb_crc32_hash(const void* data, size_t len);

void svdb_crc32_hash_batch(
    const uint8_t** keys,
    const size_t* key_lens,
    uint64_t* hashes,
    size_t count
);

FastHashTable* svdb_fast_hash_table_build(
    const svdb_row_t* rows,
    size_t n,
    size_t key_col,
    svdb::ds::svdb_arena_v2_t* arena
);

void svdb_fast_hash_table_destroy(FastHashTable* table);

uint32_t* svdb_fast_hash_table_find(
    FastHashTable* table,
    uint64_t hash,
    size_t* count
);

}

} 

#endif 
