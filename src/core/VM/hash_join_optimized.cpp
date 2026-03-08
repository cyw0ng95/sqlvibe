#include "hash_join.h"
#include "fast_hash_table.h"
#include "../DS/arena_v2.h"
#include <cstring>
#include <algorithm>

extern "C" {

typedef enum {
    JOIN_TYPE_INNER = 0,
    JOIN_TYPE_LEFT = 1,
    JOIN_TYPE_RIGHT = 2,
    JOIN_TYPE_FULL = 3
} JoinType;

static inline bool isNull(const char* value, size_t len) {
    return value == nullptr || len == 0;
}

static inline size_t estimate_result_size(
    size_t left_count, 
    size_t right_count,
    JoinType join_type
) {
    switch (join_type) {
        case JOIN_TYPE_INNER:
            return std::min(left_count, right_count);
        case JOIN_TYPE_LEFT:
            return left_count * 2;
        case JOIN_TYPE_RIGHT:
            return right_count * 2;
        case JOIN_TYPE_FULL:
            return (left_count + right_count) * 2;
        default:
            return left_count;
    }
}

static inline void copyRow(
    svdb_row_t& dest,
    const svdb_row_t& left,
    const svdb_row_t* right,
    size_t num_left_cols,
    size_t num_right_cols
) {
    dest.num_columns = num_left_cols + num_right_cols;
    dest.values = new char*[dest.num_columns];
    dest.value_lens = new size_t[dest.num_columns];
    
    for (size_t c = 0; c < num_left_cols && c < left.num_columns; c++) {
        dest.values[c] = const_cast<char*>(left.values[c]);
        dest.value_lens[c] = left.value_lens ? left.value_lens[c] : 
                            (left.values[c] ? strlen(left.values[c]) : 0);
    }
    
    if (right) {
        for (size_t c = 0; c < num_right_cols && c < right->num_columns; c++) {
            size_t outCol = num_left_cols + c;
            dest.values[outCol] = const_cast<char*>(right->values[c]);
            dest.value_lens[outCol] = right->value_lens ? right->value_lens[c] : 
                                      (right->values[c] ? strlen(right->values[c]) : 0);
        }
    } else {
        for (size_t c = 0; c < num_right_cols; c++) {
            size_t outCol = num_left_cols + c;
            dest.values[outCol] = nullptr;
            dest.value_lens[outCol] = 0;
        }
    }
}

svdb_join_result_t svdb_hash_join_batch(
    const svdb_row_t* left_rows,
    size_t left_count,
    const svdb_row_t* right_rows,
    size_t right_count,
    size_t left_join_key_col,
    size_t right_join_key_col,
    size_t num_left_cols,
    size_t num_right_cols,
    int include_left_nulls,
    int include_right_nulls
) {
    svdb_join_result_t result;
    result.rows = nullptr;
    result.num_rows = 0;
    result.capacity = 0;

    if (left_rows == nullptr || right_rows == nullptr || 
        left_count == 0 || right_count == 0) {
        return result;
    }

    if (left_join_key_col >= num_left_cols || 
        right_join_key_col >= num_right_cols) {
        return result;
    }

    JoinType join_type = JOIN_TYPE_INNER;
    if (include_left_nulls && !include_right_nulls) {
        join_type = JOIN_TYPE_LEFT;
    } else if (!include_left_nulls && include_right_nulls) {
        join_type = JOIN_TYPE_RIGHT;
    } else if (include_left_nulls && include_right_nulls) {
        join_type = JOIN_TYPE_FULL;
    }

    svdb::ds::ArenaV2 arena(512 * 1024);
    
    svdb::vm::FastHashTable* hash_table = svdb::vm::FastHashTable::Build(
        right_rows, right_count, right_join_key_col, &arena
    );
    
    if (!hash_table) {
        return result;
    }

    size_t estimated_rows = estimate_result_size(left_count, right_count, join_type);
    result.capacity = estimated_rows;
    result.rows = new svdb_row_t[result.capacity];
    
    uint8_t* right_matched = nullptr;
    if (join_type == JOIN_TYPE_RIGHT || join_type == JOIN_TYPE_FULL) {
        right_matched = new uint8_t[right_count]();
    }

    size_t result_idx = 0;
    
    for (size_t i = 0; i < left_count; i++) {
        const svdb_row_t& leftRow = left_rows[i];
        
        if (left_join_key_col >= leftRow.num_columns) {
            continue;
        }
        
        const char* keyVal = leftRow.values[left_join_key_col];
        size_t keyLen = leftRow.value_lens ? leftRow.value_lens[left_join_key_col] : 
                        (keyVal ? strlen(keyVal) : 0);
        
        bool is_null = isNull(keyVal, keyLen);
        
        if (is_null && !include_left_nulls) {
            continue;
        }
        
        if (result_idx >= result.capacity) {
            size_t new_capacity = result.capacity * 2;
            svdb_row_t* new_rows = new svdb_row_t[new_capacity];
            std::copy(result.rows, result.rows + result.capacity, new_rows);
            delete[] result.rows;
            result.rows = new_rows;
            result.capacity = new_capacity;
        }
        
        if (is_null) {
            if (join_type == JOIN_TYPE_LEFT || join_type == JOIN_TYPE_FULL) {
                copyRow(result.rows[result_idx], leftRow, nullptr, 
                       num_left_cols, num_right_cols);
                result_idx++;
            }
            continue;
        }
        
        uint64_t hash = svdb::vm::svdb_crc32_hash(keyVal, keyLen);
        
        size_t match_count = 0;
        uint32_t* matches = hash_table->Find(hash, &match_count);
        
        if (match_count == 0) {
            if (join_type == JOIN_TYPE_LEFT || join_type == JOIN_TYPE_FULL) {
                copyRow(result.rows[result_idx], leftRow, nullptr, 
                       num_left_cols, num_right_cols);
                result_idx++;
            }
            continue;
        }
        
        for (size_t m = 0; m < match_count; m++) {
            uint32_t right_idx = matches[m];
            const svdb_row_t& rightRow = right_rows[right_idx];
            
            const char* rightKey = rightRow.values[right_join_key_col];
            size_t rightLen = rightRow.value_lens ? 
                             rightRow.value_lens[right_join_key_col] :
                             (rightKey ? strlen(rightKey) : 0);
            
            if (isNull(rightKey, rightLen)) {
                continue;
            }
            
            if (keyLen == rightLen && memcmp(keyVal, rightKey, keyLen) == 0) {
                if (result_idx >= result.capacity) {
                    size_t new_capacity = result.capacity * 2;
                    svdb_row_t* new_rows = new svdb_row_t[new_capacity];
                    std::copy(result.rows, result.rows + result.capacity, new_rows);
                    delete[] result.rows;
                    result.rows = new_rows;
                    result.capacity = new_capacity;
                }
                
                copyRow(result.rows[result_idx], leftRow, &rightRow, 
                       num_left_cols, num_right_cols);
                
                if (right_matched) {
                    right_matched[right_idx] = 1;
                }
                
                result_idx++;
            }
        }
    }
    
    if (join_type == JOIN_TYPE_FULL && right_matched) {
        for (size_t i = 0; i < right_count; i++) {
            if (!right_matched[i]) {
                const svdb_row_t& rightRow = right_rows[i];
                
                if (result_idx >= result.capacity) {
                    size_t new_capacity = result.capacity * 2;
                    svdb_row_t* new_rows = new svdb_row_t[new_capacity];
                    std::copy(result.rows, result.rows + result.capacity, new_rows);
                    delete[] result.rows;
                    result.rows = new_rows;
                    result.capacity = new_capacity;
                }
                
                svdb_row_t& mergedRow = result.rows[result_idx];
                mergedRow.num_columns = num_left_cols + num_right_cols;
                mergedRow.values = new char*[mergedRow.num_columns];
                mergedRow.value_lens = new size_t[mergedRow.num_columns];
                
                for (size_t c = 0; c < num_left_cols; c++) {
                    mergedRow.values[c] = nullptr;
                    mergedRow.value_lens[c] = 0;
                }
                
                for (size_t c = 0; c < num_right_cols && c < rightRow.num_columns; c++) {
                    size_t outCol = num_left_cols + c;
                    mergedRow.values[outCol] = const_cast<char*>(rightRow.values[c]);
                    mergedRow.value_lens[outCol] = rightRow.value_lens ? 
                                                   rightRow.value_lens[c] :
                                                   (rightRow.values[c] ? strlen(rightRow.values[c]) : 0);
                }
                
                result_idx++;
            }
        }
    }
    
    if (right_matched) {
        delete[] right_matched;
    }

    result.num_rows = result_idx;
    return result;
}

void svdb_free_join_result(svdb_join_result_t* result) {
    if (result == nullptr || result->rows == nullptr) {
        return;
    }
    
    for (size_t i = 0; i < result->num_rows; i++) {
        delete[] result->rows[i].values;
        delete[] result->rows[i].value_lens;
    }
    
    delete[] result->rows;
    result->rows = nullptr;
    result->num_rows = 0;
    result->capacity = 0;
}

int svdb_hash_join_simd_level(void) {
#ifdef __SSE4_2__
    return 3;
#elif defined(__AVX2__)
    return 2;
#elif defined(__SSE4_1__)
    return 1;
#else
    return 0;
#endif
}

} 
