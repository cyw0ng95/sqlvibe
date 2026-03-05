#include "hash_join.h"
#include <unordered_map>
#include <vector>
#include <cstring>
#include <string>
#include <algorithm>

// Normalize join key: convert to string if binary, otherwise use as-is
static std::string normalizeKey(const char* value, size_t len) {
    return std::string(value, len);
}

// Check if value is NULL (empty string with length 0, or explicit NULL marker)
static bool isNull(const char* value, size_t len) {
    return value == nullptr || len == 0;
}

extern "C" {

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

    // Validate column indices
    if (left_join_key_col >= num_left_cols || 
        right_join_key_col >= num_right_cols) {
        return result;
    }

    // Build hash table on right side
    // Key: normalized join key value
    // Value: vector of row indices that match this key
    std::unordered_map<std::string, std::vector<size_t>> hashTable;
    hashTable.reserve(right_count);

    for (size_t i = 0; i < right_count; i++) {
        const svdb_row_t& row = right_rows[i];
        
        // Get join key value
        if (right_join_key_col >= row.num_columns) {
            continue;
        }
        
        const char* keyVal = row.values[right_join_key_col];
        size_t keyLen = row.value_lens ? row.value_lens[right_join_key_col] : strlen(keyVal);
        
        // Skip NULL values (NULL ≠ NULL in SQL)
        if (isNull(keyVal, keyLen) && !include_right_nulls) {
            continue;
        }
        
        std::string key = normalizeKey(keyVal, keyLen);
        hashTable[key].push_back(i);
    }

    // Estimate result size for pre-allocation
    size_t estimated_rows = std::min(left_count * 2, left_count * (right_count / 10 + 1));
    result.capacity = estimated_rows;
    result.rows = new svdb_row_t[result.capacity];

    // Probe phase: iterate left rows and find matches
    size_t result_idx = 0;
    
    for (size_t i = 0; i < left_count; i++) {
        const svdb_row_t& leftRow = left_rows[i];
        
        // Get join key value
        if (left_join_key_col >= leftRow.num_columns) {
            continue;
        }
        
        const char* keyVal = leftRow.values[left_join_key_col];
        size_t keyLen = leftRow.value_lens ? leftRow.value_lens[left_join_key_col] : strlen(keyVal);
        
        // Skip NULL values (NULL never matches in equi-join)
        if (isNull(keyVal, keyLen) && !include_left_nulls) {
            continue;
        }
        
        std::string key = normalizeKey(keyVal, keyLen);
        
        // Look up matches in hash table
        auto it = hashTable.find(key);
        if (it == hashTable.end() || it->second.empty()) {
            continue;
        }
        
        // For each matching right row, create result row
        for (size_t rightIdx : it->second) {
            const svdb_row_t& rightRow = right_rows[rightIdx];
            
            // Ensure capacity
            if (result_idx >= result.capacity) {
                size_t new_capacity = result.capacity * 2;
                svdb_row_t* new_rows = new svdb_row_t[new_capacity];
                std::copy(result.rows, result.rows + result.capacity, new_rows);
                delete[] result.rows;
                result.rows = new_rows;
                result.capacity = new_capacity;
            }
            
            // Create merged row: left columns + right columns
            svdb_row_t& mergedRow = result.rows[result_idx];
            mergedRow.num_columns = num_left_cols + num_right_cols;
            mergedRow.values = new char*[mergedRow.num_columns];
            mergedRow.value_lens = new size_t[mergedRow.num_columns];
            
            // Copy left columns
            for (size_t c = 0; c < num_left_cols && c < leftRow.num_columns; c++) {
                mergedRow.values[c] = const_cast<char*>(leftRow.values[c]);
                mergedRow.value_lens[c] = leftRow.value_lens ? leftRow.value_lens[c] : strlen(leftRow.values[c]);
            }
            
            // Copy right columns
            for (size_t c = 0; c < num_right_cols && c < rightRow.num_columns; c++) {
                size_t outCol = num_left_cols + c;
                mergedRow.values[outCol] = const_cast<char*>(rightRow.values[c]);
                mergedRow.value_lens[outCol] = rightRow.value_lens ? rightRow.value_lens[c] : strlen(rightRow.values[c]);
            }
            
            result_idx++;
        }
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
    // Return SIMD capability level (0=none, 1=SSE4.1, 2=AVX, 3=AVX2)
    // Hash join doesn't use SIMD directly, but hash table operations benefit from CPU cache
    return 0;
}

} // extern "C"
