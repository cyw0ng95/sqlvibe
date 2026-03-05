#ifndef SVDB_VM_SORT_H
#define SVDB_VM_SORT_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Sort int64 array in place
void svdb_sort_int64(int64_t* data, size_t count);

// Sort int64 with indices (for ORDER BY with row IDs)
void svdb_sort_int64_with_indices(
    int64_t* data,
    int64_t* indices,
    size_t count
);

// Radix sort for uint64 (faster than comparison sort)
void svdb_radix_sort_uint64(uint64_t* data, size_t count);

// Sort strings (null-terminated)
void svdb_sort_strings(const char** data, size_t count);

// Sort strings with indices
void svdb_sort_strings_with_indices(
    const char** data,
    int64_t* indices,
    size_t count
);

// Sort byte slices
void svdb_sort_bytes(
    const uint8_t** data,
    const size_t* lengths,
    size_t count
);

// Sort byte slices with indices
void svdb_sort_bytes_with_indices(
    const uint8_t** data,
    const size_t* lengths,
    int64_t* indices,
    size_t count
);

// Get sorted order (returns permutation array)
// result[i] = original index of i-th smallest element
void svdb_argsort_int64(
    const int64_t* data,
    size_t count,
    size_t* result
);

#ifdef __cplusplus
}
#endif

#endif // SVDB_VM_SORT_H
