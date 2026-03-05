#ifndef SVDB_DS_CELL_H
#define SVDB_DS_CELL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Cell types
typedef enum {
    SVDB_CELL_TABLE_LEAF = 0,
    SVDB_CELL_TABLE_INTERIOR = 1,
    SVDB_CELL_INDEX_LEAF = 2,
    SVDB_CELL_INDEX_INTERIOR = 3
} svdb_cell_type_t;

// Cell data structure
typedef struct {
    svdb_cell_type_t type;
    uint32_t left_child;  // For interior cells only
    int64_t rowid;        // For table cells only
    uint8_t* key;         // For index cells only
    size_t key_len;
    uint8_t* payload;     // Cell payload
    size_t payload_len;
    uint32_t overflow_page;
    int local_size;
} svdb_cell_data_t;

// Encode table leaf cell
// Format: payload_size (varint) + rowid (varint) + payload + [overflow_page (4 bytes)]
// Returns encoded size, or 0 on error
int svdb_encode_table_leaf_cell(uint8_t* buf, size_t buf_len, int64_t rowid, 
                                 const uint8_t* payload, size_t payload_len, uint32_t overflow_page);

// Decode table leaf cell
// Returns 1 on success, 0 on error
int svdb_decode_table_leaf_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell);

// Encode table interior cell
// Format: left_child (4 bytes) + rowid (varint)
int svdb_encode_table_interior_cell(uint8_t* buf, size_t buf_len, uint32_t left_child, int64_t rowid);

// Decode table interior cell
int svdb_decode_table_interior_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell);

// Encode index leaf cell
// Format: payload_size (varint) + rowid (varint) + payload
int svdb_encode_index_leaf_cell(uint8_t* buf, size_t buf_len, 
                                 const uint8_t* key, size_t key_len,
                                 const uint8_t* payload, size_t payload_len);

// Decode index leaf cell
int svdb_decode_index_leaf_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell);

// Encode index interior cell
// Format: left_child (4 bytes) + key
int svdb_encode_index_interior_cell(uint8_t* buf, size_t buf_len, 
                                     uint32_t left_child, const uint8_t* key, size_t key_len);

// Decode index interior cell
int svdb_decode_index_interior_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell);

// Get encoded cell size
int svdb_table_leaf_cell_size(int64_t rowid, size_t payload_len, uint32_t overflow_page);
int svdb_table_interior_cell_size(int64_t rowid);
int svdb_index_leaf_cell_size(size_t key_len, size_t payload_len);
int svdb_index_interior_cell_size(size_t key_len);

// Free cell data (for key and payload which are allocated)
void svdb_free_cell_data(svdb_cell_data_t* cell);

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_CELL_H
