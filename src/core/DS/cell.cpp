#include "cell.h"
#include "varint.h"
#include <cstring>
#include <cstdlib>

extern "C" {

// Get encoded cell size
int svdb_table_leaf_cell_size(int64_t rowid, size_t payload_len, uint32_t overflow_page) {
    int size = svdb_varint_len(static_cast<int64_t>(payload_len));
    size += svdb_varint_len(rowid);
    size += static_cast<int>(payload_len);
    if (overflow_page > 0) {
        size += 4;
    }
    return size;
}

int svdb_table_interior_cell_size(int64_t rowid) {
    return 4 + svdb_varint_len(rowid);
}

int svdb_index_leaf_cell_size(size_t key_len, size_t payload_len) {
    return svdb_varint_len(static_cast<int64_t>(payload_len)) + 
           svdb_varint_len(static_cast<int64_t>(key_len)) +
           static_cast<int>(key_len) + 
           static_cast<int>(payload_len);
}

int svdb_index_interior_cell_size(size_t key_len) {
    return 4 + static_cast<int>(key_len);
}

// Encode table leaf cell
int svdb_encode_table_leaf_cell(uint8_t* buf, size_t buf_len, int64_t rowid, 
                                 const uint8_t* payload, size_t payload_len, uint32_t overflow_page) {
    if (!buf || !payload || payload_len == 0 || rowid <= 0) {
        return 0;
    }
    
    int required_size = svdb_table_leaf_cell_size(rowid, payload_len, overflow_page);
    if (buf_len < static_cast<size_t>(required_size)) {
        return 0;
    }
    
    size_t pos = 0;
    
    // Write payload size
    int n = svdb_put_varint(buf + pos, buf_len - pos, static_cast<int64_t>(payload_len));
    if (n == 0) return 0;
    pos += n;
    
    // Write rowid
    n = svdb_put_varint(buf + pos, buf_len - pos, rowid);
    if (n == 0) return 0;
    pos += n;
    
    // Write payload
    std::memcpy(buf + pos, payload, payload_len);
    pos += payload_len;
    
    // Write overflow page if needed
    if (overflow_page > 0) {
        buf[pos] = static_cast<uint8_t>((overflow_page >> 24) & 0xFF);
        buf[pos + 1] = static_cast<uint8_t>((overflow_page >> 16) & 0xFF);
        buf[pos + 2] = static_cast<uint8_t>((overflow_page >> 8) & 0xFF);
        buf[pos + 3] = static_cast<uint8_t>(overflow_page & 0xFF);
        pos += 4;
    }
    
    return static_cast<int>(pos);
}

// Decode table leaf cell
int svdb_decode_table_leaf_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell) {
    if (!buf || buf_len < 2 || !cell) {
        return 0;
    }
    
    std::memset(cell, 0, sizeof(svdb_cell_data_t));
    cell->type = SVDB_CELL_TABLE_LEAF;
    
    size_t pos = 0;
    
    // Read payload size
    int64_t payload_size;
    int n;
    if (!svdb_get_varint(buf + pos, buf_len - pos, &payload_size, &n)) {
        return 0;
    }
    pos += n;
    cell->payload_len = static_cast<size_t>(payload_size);
    
    // Read rowid
    int64_t rowid;
    if (!svdb_get_varint(buf + pos, buf_len - pos, &rowid, &n)) {
        return 0;
    }
    pos += n;
    cell->rowid = rowid;
    
    // Check if there's enough data for payload + optional overflow
    size_t remaining = buf_len - pos;
    if (remaining < cell->payload_len) {
        return 0;
    }
    
    // Determine if there's an overflow page
    size_t expected_size = cell->payload_len;
    if (remaining >= cell->payload_len + 4) {
        // Might have overflow page - check if payload size matches
        expected_size = cell->payload_len + 4;
    }
    
    // Allocate and copy payload
    cell->payload = static_cast<uint8_t*>(std::malloc(cell->payload_len));
    if (!cell->payload) {
        return 0;
    }
    std::memcpy(cell->payload, buf + pos, cell->payload_len);
    pos += cell->payload_len;
    cell->local_size = static_cast<int>(cell->payload_len);
    
    // Read overflow page if present
    if (remaining >= cell->payload_len + 4) {
        cell->overflow_page = (static_cast<uint32_t>(buf[pos]) << 24) |
                              (static_cast<uint32_t>(buf[pos + 1]) << 16) |
                              (static_cast<uint32_t>(buf[pos + 2]) << 8) |
                              static_cast<uint32_t>(buf[pos + 3]);
    }
    
    return 1;
}

// Encode table interior cell
int svdb_encode_table_interior_cell(uint8_t* buf, size_t buf_len, uint32_t left_child, int64_t rowid) {
    if (!buf || buf_len < 5 || left_child == 0 || rowid <= 0) {
        return 0;
    }
    
    int required_size = svdb_table_interior_cell_size(rowid);
    if (buf_len < static_cast<size_t>(required_size)) {
        return 0;
    }
    
    // Write left child (4 bytes, big-endian)
    buf[0] = static_cast<uint8_t>((left_child >> 24) & 0xFF);
    buf[1] = static_cast<uint8_t>((left_child >> 16) & 0xFF);
    buf[2] = static_cast<uint8_t>((left_child >> 8) & 0xFF);
    buf[3] = static_cast<uint8_t>(left_child & 0xFF);
    
    // Write rowid
    int n = svdb_put_varint(buf + 4, buf_len - 4, rowid);
    if (n == 0) return 0;
    
    return 4 + n;
}

// Decode table interior cell
int svdb_decode_table_interior_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell) {
    if (!buf || buf_len < 5 || !cell) {
        return 0;
    }
    
    std::memset(cell, 0, sizeof(svdb_cell_data_t));
    cell->type = SVDB_CELL_TABLE_INTERIOR;
    
    // Read left child
    cell->left_child = (static_cast<uint32_t>(buf[0]) << 24) |
                       (static_cast<uint32_t>(buf[1]) << 16) |
                       (static_cast<uint32_t>(buf[2]) << 8) |
                       static_cast<uint32_t>(buf[3]);
    
    // Read rowid
    int n;
    int64_t rowid;
    if (!svdb_get_varint(buf + 4, buf_len - 4, &rowid, &n)) {
        return 0;
    }
    cell->rowid = rowid;
    
    return 1;
}

// Encode index leaf cell
int svdb_encode_index_leaf_cell(uint8_t* buf, size_t buf_len, 
                                 const uint8_t* key, size_t key_len,
                                 const uint8_t* payload, size_t payload_len) {
    if (!buf || !key || key_len == 0 || !payload || payload_len == 0) {
        return 0;
    }
    
    int required_size = svdb_index_leaf_cell_size(key_len, payload_len);
    if (buf_len < static_cast<size_t>(required_size)) {
        return 0;
    }
    
    size_t pos = 0;
    
    // Write payload size
    int n = svdb_put_varint(buf + pos, buf_len - pos, static_cast<int64_t>(payload_len));
    if (n == 0) return 0;
    pos += n;
    
    // Write key size
    n = svdb_put_varint(buf + pos, buf_len - pos, static_cast<int64_t>(key_len));
    if (n == 0) return 0;
    pos += n;
    
    // Write key
    std::memcpy(buf + pos, key, key_len);
    pos += key_len;
    
    // Write payload
    std::memcpy(buf + pos, payload, payload_len);
    
    return static_cast<int>(pos + payload_len);
}

// Decode index leaf cell
int svdb_decode_index_leaf_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell) {
    if (!buf || buf_len < 2 || !cell) {
        return 0;
    }
    
    std::memset(cell, 0, sizeof(svdb_cell_data_t));
    cell->type = SVDB_CELL_INDEX_LEAF;
    
    size_t pos = 0;
    
    // Read payload size
    int64_t payload_size;
    int n;
    if (!svdb_get_varint(buf + pos, buf_len - pos, &payload_size, &n)) {
        return 0;
    }
    pos += n;
    cell->payload_len = static_cast<size_t>(payload_size);
    
    // Read key size
    int64_t key_size;
    if (!svdb_get_varint(buf + pos, buf_len - pos, &key_size, &n)) {
        return 0;
    }
    pos += n;
    cell->key_len = static_cast<size_t>(key_size);
    
    // Check buffer has enough data
    size_t remaining = buf_len - pos;
    if (remaining < cell->key_len + cell->payload_len) {
        return 0;
    }
    
    // Allocate and copy key
    cell->key = static_cast<uint8_t*>(std::malloc(cell->key_len));
    if (!cell->key) {
        return 0;
    }
    std::memcpy(cell->key, buf + pos, cell->key_len);
    pos += cell->key_len;
    
    // Allocate and copy payload
    cell->payload = static_cast<uint8_t*>(std::malloc(cell->payload_len));
    if (!cell->payload) {
        std::free(cell->key);
        cell->key = nullptr;
        return 0;
    }
    std::memcpy(cell->payload, buf + pos, cell->payload_len);
    cell->local_size = static_cast<int>(cell->payload_len);
    
    return 1;
}

// Encode index interior cell
int svdb_encode_index_interior_cell(uint8_t* buf, size_t buf_len, 
                                     uint32_t left_child, const uint8_t* key, size_t key_len) {
    if (!buf || !key || key_len == 0 || left_child == 0) {
        return 0;
    }
    
    int required_size = svdb_index_interior_cell_size(key_len);
    if (buf_len < static_cast<size_t>(required_size)) {
        return 0;
    }
    
    // Write left child (4 bytes, big-endian)
    buf[0] = static_cast<uint8_t>((left_child >> 24) & 0xFF);
    buf[1] = static_cast<uint8_t>((left_child >> 16) & 0xFF);
    buf[2] = static_cast<uint8_t>((left_child >> 8) & 0xFF);
    buf[3] = static_cast<uint8_t>(left_child & 0xFF);
    
    // Write key
    std::memcpy(buf + 4, key, key_len);
    
    return 4 + static_cast<int>(key_len);
}

// Decode index interior cell
int svdb_decode_index_interior_cell(const uint8_t* buf, size_t buf_len, svdb_cell_data_t* cell) {
    if (!buf || buf_len < 5 || !cell) {
        return 0;
    }
    
    std::memset(cell, 0, sizeof(svdb_cell_data_t));
    cell->type = SVDB_CELL_INDEX_INTERIOR;
    
    // Read left child
    cell->left_child = (static_cast<uint32_t>(buf[0]) << 24) |
                       (static_cast<uint32_t>(buf[1]) << 16) |
                       (static_cast<uint32_t>(buf[2]) << 8) |
                       static_cast<uint32_t>(buf[3]);
    
    // Copy key
    cell->key_len = buf_len - 4;
    cell->key = static_cast<uint8_t*>(std::malloc(cell->key_len));
    if (!cell->key) {
        return 0;
    }
    std::memcpy(cell->key, buf + 4, cell->key_len);
    
    return 1;
}

// Free cell data
void svdb_free_cell_data(svdb_cell_data_t* cell) {
    if (!cell) return;
    
    if (cell->key) {
        std::free(cell->key);
        cell->key = nullptr;
    }
    if (cell->payload) {
        std::free(cell->payload);
        cell->payload = nullptr;
    }
}

} // extern "C"
