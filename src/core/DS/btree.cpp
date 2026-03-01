#include "btree.h"
#include "varint.h"
#include "cell.h"
#include <cstring>
#include <cstdlib>

// Internal B-Tree structure
struct svdb_btree {
    svdb_btree_config_t config;
    svdb_page_manager_t pm;
    uint32_t depth;
    uint32_t leaf_count;
};

// Page type constants
static const uint8_t PAGE_TYPE_TABLE_LEAF = 0x0d;
static const uint8_t PAGE_TYPE_TABLE_INTERIOR = 0x05;
static const uint8_t PAGE_TYPE_INDEX_LEAF = 0x0a;
static const uint8_t PAGE_TYPE_INDEX_INTERIOR = 0x02;

// Binary search for cell index in a page
extern "C" int svdb_btree_binary_search(const uint8_t* page_data, size_t page_size,
                                         const uint8_t* key, size_t key_len, int is_table) {
    if (!page_data || page_size < 8 || !key || key_len == 0) {
        return -1;
    }
    
    uint8_t page_type = page_data[0];
    int num_cells = (page_data[3] << 8) | page_data[4];
    
    if (num_cells == 0) {
        return -1;
    }
    
    // Binary search
    int lo = 0, hi = num_cells - 1;
    int result = -1;
    
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        
        // Get cell pointer offset
        int cell_ptr_offset = 8 + mid * 2;
        if (cell_ptr_offset + 2 > static_cast<int>(page_size)) {
            break;
        }
        
        uint16_t cell_offset = (page_data[cell_ptr_offset] << 8) | page_data[cell_ptr_offset + 1];
        if (cell_offset >= page_size) {
            break;
        }
        
        // Compare key based on page type
        int cmp = 0;
        if (is_table) {
            // Table: compare rowid from cell
            int64_t cell_rowid;
            int bytes_read;
            
            // Skip payload size varint
            int64_t payload_size;
            if (!svdb_get_varint(page_data + cell_offset, page_size - cell_offset, &payload_size, &bytes_read)) {
                break;
            }
            
            // Read rowid
            if (!svdb_get_varint(page_data + cell_offset + bytes_read, 
                                 page_size - cell_offset - bytes_read, &cell_rowid, &bytes_read)) {
                break;
            }
            
            // Compare with key (which contains rowid as varint)
            int64_t search_rowid;
            int key_bytes;
            svdb_get_varint(key, key_len, &search_rowid, &key_bytes);
            
            if (cell_rowid < search_rowid) {
                cmp = -1;
            } else if (cell_rowid > search_rowid) {
                cmp = 1;
            }
        } else {
            // Index: compare key bytes
            // Skip payload size and key size varints
            int pos = 0;
            int64_t payload_size, key_size;
            int bytes_read;
            
            if (!svdb_get_varint(page_data + cell_offset, page_size - cell_offset, &payload_size, &bytes_read)) {
                break;
            }
            pos += bytes_read;
            
            if (!svdb_get_varint(page_data + cell_offset + pos, page_size - cell_offset - pos, &key_size, &bytes_read)) {
                break;
            }
            pos += bytes_read;
            
            // Compare keys
            size_t cmp_len = (key_len < static_cast<size_t>(key_size)) ? key_len : static_cast<size_t>(key_size);
            cmp = std::memcmp(key, page_data + cell_offset + pos, cmp_len);
            if (cmp == 0 && key_len != static_cast<size_t>(key_size)) {
                cmp = (key_len < static_cast<size_t>(key_size)) ? -1 : 1;
            }
        }
        
        if (cmp == 0) {
            return mid;
        } else if (cmp < 0) {
            result = mid;  // Potential insertion point
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    
    return result;
}

// Search in a leaf page
static int search_leaf_page(const uint8_t* page_data, size_t page_size,
                            const uint8_t* key, size_t key_len, int is_table,
                            uint8_t** value, size_t* value_len) {
    int cell_idx = svdb_btree_binary_search(page_data, page_size, key, key_len, is_table);
    if (cell_idx < 0) {
        return 0;
    }
    
    // Get cell pointer
    int cell_ptr_offset = 8 + cell_idx * 2;
    uint16_t cell_offset = (page_data[cell_ptr_offset] << 8) | page_data[cell_ptr_offset + 1];
    
    if (is_table) {
        // Decode table leaf cell
        svdb_cell_data_t cell;
        if (!svdb_decode_table_leaf_cell(page_data + cell_offset, page_size - cell_offset, &cell)) {
            return 0;
        }
        
        // Check if rowid matches
        int64_t search_rowid;
        int key_bytes;
        svdb_get_varint(key, key_len, &search_rowid, &key_bytes);
        
        if (cell.rowid == search_rowid) {
            // Copy payload
            *value = static_cast<uint8_t*>(std::malloc(cell.payload_len));
            if (*value) {
                std::memcpy(*value, cell.payload, cell.payload_len);
                *value_len = cell.payload_len;
            }
            svdb_free_cell_data(&cell);
            return *value != nullptr ? 1 : 0;
        }
        svdb_free_cell_data(&cell);
    } else {
        // Decode index leaf cell
        svdb_cell_data_t cell;
        if (!svdb_decode_index_leaf_cell(page_data + cell_offset, page_size - cell_offset, &cell)) {
            return 0;
        }
        
        // Compare keys
        if (cell.key_len == key_len && std::memcmp(cell.key, key, key_len) == 0) {
            // Copy payload
            *value = static_cast<uint8_t*>(std::malloc(cell.payload_len));
            if (*value) {
                std::memcpy(*value, cell.payload, cell.payload_len);
                *value_len = cell.payload_len;
            }
            svdb_free_cell_data(&cell);
            return *value != nullptr ? 1 : 0;
        }
        svdb_free_cell_data(&cell);
    }
    
    return 0;
}

// Search in an interior page
static int search_interior_page(const uint8_t* page_data, size_t page_size,
                                const uint8_t* key, size_t key_len, int is_table,
                                const svdb_page_manager_t* pm, uint32_t* child_page) {
    int num_cells = (page_data[3] << 8) | page_data[4];
    
    // Find the right child
    for (int i = 0; i < num_cells; i++) {
        int cell_ptr_offset = 8 + i * 2;
        uint16_t cell_offset = (page_data[cell_ptr_offset] << 8) | page_data[cell_ptr_offset + 1];
        
        uint32_t left_child;
        int64_t rowid;
        int n;
        
        if (is_table) {
            // Table interior: left_child (4 bytes) + rowid (varint)
            left_child = (page_data[cell_offset] << 24) |
                         (page_data[cell_offset + 1] << 16) |
                         (page_data[cell_offset + 2] << 8) |
                         page_data[cell_offset + 3];
            
            svdb_get_varint(page_data + cell_offset + 4, page_size - cell_offset - 4, &rowid, &n);
            
            // Compare rowid
            int64_t search_rowid;
            int key_bytes;
            svdb_get_varint(key, key_len, &search_rowid, &key_bytes);
            
            if (search_rowid <= rowid) {
                *child_page = left_child;
                return 1;
            }
        } else {
            // Index interior: left_child (4 bytes) + key
            left_child = (page_data[cell_offset] << 24) |
                         (page_data[cell_offset + 1] << 16) |
                         (page_data[cell_offset + 2] << 8) |
                         page_data[cell_offset + 3];
            
            size_t key_len_cell = page_size - cell_offset - 4;
            const uint8_t* cell_key = page_data + cell_offset + 4;
            
            // Compare keys
            size_t cmp_len = (key_len < key_len_cell) ? key_len : key_len_cell;
            int cmp = std::memcmp(key, cell_key, cmp_len);
            
            if (cmp <= 0) {
                *child_page = left_child;
                return 1;
            }
        }
    }
    
    // Use rightmost child
    uint32_t right_child = (page_data[8] << 24) | (page_data[9] << 16) | 
                           (page_data[10] << 8) | page_data[11];
    *child_page = right_child;
    return 1;
}

extern "C" {

svdb_btree_t* svdb_btree_create(const svdb_btree_config_t* config, const svdb_page_manager_t* pm) {
    if (!config || !pm) {
        return nullptr;
    }
    
    svdb_btree_t* bt = static_cast<svdb_btree_t*>(std::calloc(1, sizeof(svdb_btree_t)));
    if (!bt) {
        return nullptr;
    }
    
    bt->config = *config;
    bt->pm = *pm;
    bt->depth = 1;
    bt->leaf_count = 0;
    
    return bt;
}

void svdb_btree_destroy(svdb_btree_t* bt) {
    if (bt) {
        std::free(bt);
    }
}

int svdb_btree_search(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                      uint8_t** value, size_t* value_len) {
    if (!bt || !key || key_len == 0 || !value || !value_len) {
        return 0;
    }
    
    *value = nullptr;
    *value_len = 0;
    
    if (bt->config.root_page == 0) {
        return 0;  // Empty tree
    }
    
    // Read root page
    uint8_t* page_data = nullptr;
    size_t page_size = 0;
    
    if (!bt->pm.read_page || bt->pm.read_page(bt->pm.user_data, bt->config.root_page, &page_data, &page_size) != 0) {
        return 0;
    }
    
    if (!page_data || page_size < 12) {
        return 0;
    }
    
    uint8_t page_type = page_data[0];
    int is_table = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_TABLE_INTERIOR);
    int is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);
    
    // Navigate tree
    uint32_t current_page = bt->config.root_page;
    
    while (current_page != 0) {
        // Read current page
        if (current_page != bt->config.root_page) {
            if (!bt->pm.read_page || bt->pm.read_page(bt->pm.user_data, current_page, &page_data, &page_size) != 0) {
                return 0;
            }
            if (!page_data || page_size < 12) {
                return 0;
            }
            page_type = page_data[0];
            is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);
        }
        
        if (is_leaf) {
            // Search in leaf
            int found = search_leaf_page(page_data, page_size, key, key_len, is_table, value, value_len);
            return found;
        } else {
            // Navigate to child
            uint32_t child_page;
            if (!search_interior_page(page_data, page_size, key, key_len, is_table, &bt->pm, &child_page)) {
                return 0;
            }
            current_page = child_page;
        }
    }
    
    return 0;
}

int svdb_btree_insert(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                      const uint8_t* value, size_t value_len) {
    // Placeholder - full implementation requires page split logic
    // For now, return not implemented
    (void)bt;
    (void)key;
    (void)key_len;
    (void)value;
    (void)value_len;
    return 0;
}

int svdb_btree_delete(svdb_btree_t* bt, const uint8_t* key, size_t key_len) {
    // Placeholder - full implementation requires page merge logic
    (void)bt;
    (void)key;
    (void)key_len;
    return 0;
}

uint32_t svdb_btree_get_depth(svdb_btree_t* bt) {
    if (!bt) return 0;
    return bt->depth;
}

uint32_t svdb_btree_get_leaf_count(svdb_btree_t* bt) {
    if (!bt) return 0;
    return bt->leaf_count;
}

} // extern "C"
