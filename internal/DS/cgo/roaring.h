#ifndef SVDB_DS_ROARING_H
#define SVDB_DS_ROARING_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Roaring bitmap handle (opaque)
typedef struct svdb_roaring_bitmap svdb_roaring_bitmap_t;

// Create an empty roaring bitmap
svdb_roaring_bitmap_t* svdb_roaring_create(void);

// Free a roaring bitmap
void svdb_roaring_free(svdb_roaring_bitmap_t* rb);

// Add a value to the bitmap
void svdb_roaring_add(svdb_roaring_bitmap_t* rb, uint32_t value);

// Remove a value from the bitmap
void svdb_roaring_remove(svdb_roaring_bitmap_t* rb, uint32_t value);

// Check if a value is in the bitmap
int svdb_roaring_contains(svdb_roaring_bitmap_t* rb, uint32_t value);

// Get cardinality (number of set bits)
size_t svdb_roaring_cardinality(svdb_roaring_bitmap_t* rb);

// Check if bitmap is empty
int svdb_roaring_is_empty(svdb_roaring_bitmap_t* rb);

// AND operation (intersection): a = a & b
void svdb_roaring_and(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b);

// OR operation (union): a = a | b
void svdb_roaring_or(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b);

// XOR operation (symmetric difference): a = a ^ b
void svdb_roaring_xor(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b);

// AND NOT operation: a = a & ~b
void svdb_roaring_andnot(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b);

// Get all values as a sorted array
// Returns number of values, caller must free the array
uint32_t* svdb_roaring_to_array(svdb_roaring_bitmap_t* rb, size_t* count);

// Create bitmap from sorted array
svdb_roaring_bitmap_t* svdb_roaring_from_array(const uint32_t* values, size_t count);

// Get minimum value (or UINT32_MAX if empty)
uint32_t svdb_roaring_min(svdb_roaring_bitmap_t* rb);

// Get maximum value (or 0 if empty)
uint32_t svdb_roaring_max(svdb_roaring_bitmap_t* rb);

// Rank: count values <= x
size_t svdb_roaring_rank(svdb_roaring_bitmap_t* rb, uint32_t x);

// Select: find n-th smallest value (0-indexed)
// Returns UINT32_MAX if n >= cardinality
uint32_t svdb_roaring_select(svdb_roaring_bitmap_t* rb, size_t n);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_ROARING_H
