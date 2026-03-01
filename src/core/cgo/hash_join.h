#ifndef SVDB_HASH_JOIN_H
#define SVDB_HASH_JOIN_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Row structure for hash join
typedef struct {
    char** values;          // Column values (array of strings)
    size_t* value_lens;     // Length of each value (for binary data)
    size_t num_columns;     // Number of columns
} svdb_row_t;

// Result structure for hash join output
typedef struct {
    svdb_row_t* rows;         // Array of result rows
    size_t num_rows;          // Number of result rows
    size_t capacity;          // Allocated capacity
} svdb_join_result_t;

// Batch hash join - builds hash table on right side, probes with left side
// Returns result structure with all matching rows
// Caller must call svdb_free_join_result to free memory
svdb_join_result_t svdb_hash_join_batch(
    const svdb_row_t* left_rows,      // Left table rows
    size_t left_count,                // Number of left rows
    const svdb_row_t* right_rows,     // Right table rows
    size_t right_count,               // Number of right rows
    size_t left_join_key_col,         // Column index for join key in left
    size_t right_join_key_col,        // Column index for join key in right
    size_t num_left_cols,             // Number of columns from left table
    size_t num_right_cols,            // Number of columns from right table
    int include_left_nulls,           // Include NULL values from left (for LEFT JOIN)
    int include_right_nulls           // Include NULL values from right (for LEFT JOIN)
);

// Free hash join result
void svdb_free_join_result(svdb_join_result_t* result);

// Get SIMD level for hash join
int svdb_hash_join_simd_level(void);

#ifdef __cplusplus
}
#endif

#endif // SVDB_HASH_JOIN_H
