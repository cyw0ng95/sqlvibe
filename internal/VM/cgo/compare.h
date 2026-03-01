#ifndef SVDB_VM_COMPARE_H
#define SVDB_VM_COMPARE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Compare two byte slices
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
int svdb_compare(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len);

// Batch compare - compare multiple pairs efficiently
// results[i] = -1/0/1 for each comparison
void svdb_compare_batch(
    const uint8_t** a_ptrs,
    const size_t* a_lens,
    const uint8_t** b_ptrs,
    const size_t* b_lens,
    int* results,
    size_t count
);

// Check equality for multiple pairs
// results[i] = 1 if equal, 0 if not
void svdb_equal_batch(
    const uint8_t** a_ptrs,
    const size_t* a_lens,
    const uint8_t** b_ptrs,
    const size_t* b_lens,
    uint8_t* results,
    size_t count
);

// Find first difference position
size_t svdb_find_diff(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len);

// Compare with prefix
// Returns 1 if a starts with prefix, 0 otherwise
int svdb_has_prefix(const uint8_t* a, size_t a_len, const uint8_t* prefix, size_t prefix_len);

#ifdef __cplusplus
}
#endif

#endif // SVDB_VM_COMPARE_H
