#include "btree.h"
#include "manager.h"
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

// Helper: compute size of a table-leaf cell for key (rowid) + payload
static int compute_table_leaf_cell_size(int64_t rowid, size_t payload_len) {
    uint8_t tmp[9];
    int rowid_bytes = svdb_put_varint(tmp, sizeof(tmp), rowid);
    int payload_bytes = svdb_put_varint(tmp, sizeof(tmp), (int64_t)payload_len);
    return payload_bytes + rowid_bytes + (int)payload_len;
}

// Helper: compute size of an index-leaf cell for key + payload
static int compute_index_leaf_cell_size(size_t key_len, size_t payload_len) {
    uint8_t tmp[9];
    int total = (int)(key_len + payload_len);
    int payload_bytes = svdb_put_varint(tmp, sizeof(tmp), (int64_t)total);
    int key_bytes = svdb_put_varint(tmp, sizeof(tmp), (int64_t)key_len);
    return payload_bytes + key_bytes + total;
}

// Helper: write a table-leaf cell into buf at position pos
static int write_table_leaf_cell(uint8_t* buf, size_t buf_size, int pos,
                                  int64_t rowid, const uint8_t* payload, size_t payload_len) {
    if (!buf || (size_t)pos >= buf_size) return 0;
    // payload_size varint
    int n = svdb_put_varint(buf + pos, buf_size - pos, (int64_t)payload_len);
    if (n <= 0) return 0;
    pos += n;
    // rowid varint
    n = svdb_put_varint(buf + pos, buf_size - pos, rowid);
    if (n <= 0) return 0;
    pos += n;
    // payload
    if (pos + (int)payload_len > (int)buf_size) return 0;
    std::memcpy(buf + pos, payload, payload_len);
    return pos + (int)payload_len;
}

// Helper: write an index-leaf cell into buf at position pos
static int write_index_leaf_cell(uint8_t* buf, size_t buf_size, int pos,
                                  const uint8_t* key, size_t key_len,
                                  const uint8_t* payload, size_t payload_len) {
    if (!buf || (size_t)pos >= buf_size) return 0;
    int total = (int)(key_len + payload_len);
    // total payload size varint
    int n = svdb_put_varint(buf + pos, buf_size - pos, (int64_t)total);
    if (n <= 0) return 0;
    pos += n;
    // key size varint
    n = svdb_put_varint(buf + pos, buf_size - pos, (int64_t)key_len);
    if (n <= 0) return 0;
    pos += n;
    // key + payload
    if (pos + total > (int)buf_size) return 0;
    std::memcpy(buf + pos, key, key_len);
    pos += (int)key_len;
    std::memcpy(buf + pos, payload, payload_len);
    return pos + (int)payload_len;
}

// Insert into a leaf page (no split). Returns 1 on success, 0 if page is full.
static int insert_into_leaf(uint8_t* page_data, size_t page_size,
                             const uint8_t* key, size_t key_len,
                             const uint8_t* value, size_t value_len,
                             int is_table) {
    int num_cells = (page_data[3] << 8) | page_data[4];
    int content_start = (page_data[5] << 8) | page_data[6];
    if (content_start == 0) content_start = (int)page_size;

    // Compute cell size
    int cell_size;
    int64_t rowid = 0;
    if (is_table) {
        int key_bytes;
        svdb_get_varint(key, key_len, &rowid, &key_bytes);
        cell_size = compute_table_leaf_cell_size(rowid, value_len);
    } else {
        cell_size = compute_index_leaf_cell_size(key_len, value_len);
    }

    // Check free space: need cell_size bytes in content area + 2 bytes for cell pointer
    int header_end = 8 + (num_cells + 1) * 2; // after inserting one more pointer
    int new_content_start = content_start - cell_size;
    if (new_content_start < header_end) {
        return 0; // Not enough space
    }

    // Find insertion point using binary search (maintain sorted order)
    int ins_pos = num_cells; // default: append at end
    for (int i = 0; i < num_cells; i++) {
        int ptr_off = 8 + i * 2;
        uint16_t cell_off = (page_data[ptr_off] << 8) | page_data[ptr_off + 1];
        int cmp = 0;
        if (is_table) {
            int64_t cell_rowid; int n;
            int64_t ps; int pn;
            svdb_get_varint(page_data + cell_off, page_size - cell_off, &ps, &pn);
            svdb_get_varint(page_data + cell_off + pn, page_size - cell_off - pn, &cell_rowid, &n);
            cmp = (rowid < cell_rowid) ? -1 : (rowid > cell_rowid) ? 1 : 0;
        } else {
            int64_t total_len, key_size; int tn, kn;
            svdb_get_varint(page_data + cell_off, page_size - cell_off, &total_len, &tn);
            svdb_get_varint(page_data + cell_off + tn, page_size - cell_off - tn, &key_size, &kn);
            int cmp_len = (key_len < (size_t)key_size) ? (int)key_len : (int)key_size;
            cmp = std::memcmp(key, page_data + cell_off + tn + kn, cmp_len);
            if (cmp == 0) cmp = (key_len < (size_t)key_size) ? -1 : (key_len > (size_t)key_size) ? 1 : 0;
        }
        if (cmp <= 0) { ins_pos = i; break; }
    }

    // Write cell data at new_content_start
    if (is_table) {
        write_table_leaf_cell(page_data, page_size, new_content_start, rowid, value, value_len);
    } else {
        write_index_leaf_cell(page_data, page_size, new_content_start, key, key_len, value, value_len);
    }

    // Shift cell pointers to make room at ins_pos
    for (int i = num_cells; i > ins_pos; i--) {
        int dst = 8 + i * 2;
        int src = 8 + (i - 1) * 2;
        page_data[dst]     = page_data[src];
        page_data[dst + 1] = page_data[src + 1];
    }
    // Write new cell pointer
    int ptr_off = 8 + ins_pos * 2;
    page_data[ptr_off]     = (uint8_t)(new_content_start >> 8);
    page_data[ptr_off + 1] = (uint8_t)(new_content_start & 0xff);

    // Update header
    num_cells++;
    page_data[3] = (uint8_t)(num_cells >> 8);
    page_data[4] = (uint8_t)(num_cells & 0xff);
    page_data[5] = (uint8_t)(new_content_start >> 8);
    page_data[6] = (uint8_t)(new_content_start & 0xff);

    return 1;
}

// Delete from a leaf page. Returns 1 if deleted, 0 if not found.
static int delete_from_leaf(uint8_t* page_data, size_t page_size,
                              const uint8_t* key, size_t key_len, int is_table) {
    int num_cells = (page_data[3] << 8) | page_data[4];
    int found_idx = -1;
    int64_t search_rowid = 0;

    if (is_table) {
        int n; svdb_get_varint(key, key_len, &search_rowid, &n);
    }

    for (int i = 0; i < num_cells; i++) {
        int ptr_off = 8 + i * 2;
        uint16_t cell_off = (page_data[ptr_off] << 8) | page_data[ptr_off + 1];
        if (cell_off >= page_size) continue;

        int match = 0;
        if (is_table) {
            int64_t ps, cell_rowid; int pn, rn;
            svdb_get_varint(page_data + cell_off, page_size - cell_off, &ps, &pn);
            svdb_get_varint(page_data + cell_off + pn, page_size - cell_off - pn, &cell_rowid, &rn);
            match = (cell_rowid == search_rowid) ? 1 : 0;
        } else {
            int64_t total, ks; int tn, kn;
            svdb_get_varint(page_data + cell_off, page_size - cell_off, &total, &tn);
            svdb_get_varint(page_data + cell_off + tn, page_size - cell_off - tn, &ks, &kn);
            if ((size_t)ks == key_len &&
                std::memcmp(key, page_data + cell_off + tn + kn, key_len) == 0) {
                match = 1;
            }
        }
        if (match) { found_idx = i; break; }
    }

    if (found_idx < 0) return 0;

    // Remove cell pointer by shifting remaining pointers left
    for (int i = found_idx; i < num_cells - 1; i++) {
        int dst = 8 + i * 2;
        int src = 8 + (i + 1) * 2;
        page_data[dst]     = page_data[src];
        page_data[dst + 1] = page_data[src + 1];
    }
    num_cells--;
    page_data[3] = (uint8_t)(num_cells >> 8);
    page_data[4] = (uint8_t)(num_cells & 0xff);
    // Note: content bytes are wasted (fragmented) until a page compaction

    return 1;
}

int svdb_btree_insert(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                      const uint8_t* value, size_t value_len) {
    if (!bt || !key || key_len == 0 || !value) return 0;
    if (bt->config.root_page == 0) return 0;
    if (!bt->pm.read_page || !bt->pm.write_page) return 0;

    // Read root page
    uint8_t* page_data = nullptr;
    size_t page_size = 0;
    if (bt->pm.read_page(bt->pm.user_data, bt->config.root_page, &page_data, &page_size) != 0)
        return 0;
    if (!page_data || page_size < 8) return 0;

    uint8_t page_type = page_data[0];
    int is_table = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_TABLE_INTERIOR);
    int is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);

    if (!is_leaf) {
        // Navigate to the leaf (simplified: only handles single-level tree for now)
        uint32_t child_page;
        if (!search_interior_page(page_data, page_size, key, key_len, is_table, &bt->pm, &child_page))
            return 0;
        if (bt->pm.read_page(bt->pm.user_data, child_page, &page_data, &page_size) != 0)
            return 0;
        if (!page_data || page_size < 8) return 0;
        page_type = page_data[0];
        is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);
        if (!is_leaf) return 0; // deeper than 2 levels: skip for now

        // Try to insert into leaf
        int ok = insert_into_leaf(page_data, page_size, key, key_len, value, value_len, is_table);
        if (ok) {
            bt->pm.write_page(bt->pm.user_data, child_page, page_data, page_size);
            return 1;
        }
        return 0; // Would need split – left as future work
    }

    // Root is a leaf: try direct insert
    int ok = insert_into_leaf(page_data, page_size, key, key_len, value, value_len, is_table);
    if (ok) {
        bt->pm.write_page(bt->pm.user_data, bt->config.root_page, page_data, page_size);
        bt->leaf_count++;
        return 1;
    }

    // Page is full – allocate a new page and split (simple root-only split)
    if (!bt->pm.allocate_page) return 0;
    uint32_t new_page_num = 0;
    if (bt->pm.allocate_page(bt->pm.user_data, &new_page_num) != 0) return 0;

    // Read new page
    uint8_t* new_page_data = nullptr;
    size_t new_page_size = 0;
    if (bt->pm.read_page(bt->pm.user_data, new_page_num, &new_page_data, &new_page_size) != 0)
        return 0;
    if (!new_page_data) return 0;

    // Initialize new page
    std::memset(new_page_data, 0, new_page_size);
    new_page_data[0] = page_type;
    new_page_data[5] = (uint8_t)(new_page_size >> 8);
    new_page_data[6] = (uint8_t)(new_page_size & 0xff);

    // Move half the cells from root to new page (simple split)
    int num_cells = (page_data[3] << 8) | page_data[4];
    int split = num_cells / 2;
    for (int i = split; i < num_cells; i++) {
        int ptr_off = 8 + i * 2;
        uint16_t cell_off = (page_data[ptr_off] << 8) | page_data[ptr_off + 1];
        if (cell_off >= page_size) continue;

        // Get cell size (minimal: payload size varint + rowid/key size varint + data)
        int64_t payload_size; int pn;
        if (!svdb_get_varint(page_data + cell_off, page_size - cell_off, &payload_size, &pn))
            continue;
        // Bounds check before reading second varint
        if ((size_t)(cell_off + pn) >= page_size) continue;
        int cell_total;
        if (is_table) {
            int64_t rowid; int rn;
            if (!svdb_get_varint(page_data + cell_off + pn, page_size - cell_off - pn, &rowid, &rn))
                continue;
            cell_total = pn + rn + (int)payload_size;
        } else {
            int64_t key_size; int kn;
            if (!svdb_get_varint(page_data + cell_off + pn, page_size - cell_off - pn, &key_size, &kn))
                continue;
            cell_total = pn + kn + (int)payload_size;
        }
        if (cell_total <= 0 || (size_t)(cell_off + cell_total) > page_size) continue;

        // Copy cell to new page
        int new_content = (new_page_data[5] << 8) | new_page_data[6];
        if (new_content == 0) new_content = (int)new_page_size;
        new_content -= cell_total;
        int new_cells = (new_page_data[3] << 8) | new_page_data[4];
        int new_ptr_off = 8 + new_cells * 2;
        if (new_content < new_ptr_off + 2) break;

        std::memcpy(new_page_data + new_content, page_data + cell_off, cell_total);
        new_page_data[new_ptr_off]     = (uint8_t)(new_content >> 8);
        new_page_data[new_ptr_off + 1] = (uint8_t)(new_content & 0xff);
        new_cells++;
        new_page_data[3] = (uint8_t)(new_cells >> 8);
        new_page_data[4] = (uint8_t)(new_cells & 0xff);
        new_page_data[5] = (uint8_t)(new_content >> 8);
        new_page_data[6] = (uint8_t)(new_content & 0xff);
    }

    // Trim root to split cells
    page_data[3] = (uint8_t)(split >> 8);
    page_data[4] = (uint8_t)(split & 0xff);

    // Write both pages
    bt->pm.write_page(bt->pm.user_data, bt->config.root_page, page_data, page_size);
    bt->pm.write_page(bt->pm.user_data, new_page_num, new_page_data, new_page_size);
    bt->depth = 2;
    bt->leaf_count++;

    // Determine which page the new key belongs to based on key comparison.
    // The split divider is the first key of new_page. Keys < divider go to root_page.
    // Keys >= divider go to new_page.
    int use_new_page = 0;
    {
        int new_cells_check = (new_page_data[3] << 8) | new_page_data[4];
        if (new_cells_check > 0) {
            uint16_t first_off = (new_page_data[8] << 8) | new_page_data[9];
            if (first_off < new_page_size) {
                if (is_table) {
                    int64_t ps2, div_rowid; int pn2, rn2;
                    svdb_get_varint(new_page_data + first_off, new_page_size - first_off, &ps2, &pn2);
                    if ((size_t)(first_off + pn2) < new_page_size)
                        svdb_get_varint(new_page_data + first_off + pn2, new_page_size - first_off - pn2, &div_rowid, &rn2);
                    int64_t ins_rowid; int irn;
                    svdb_get_varint(key, key_len, &ins_rowid, &irn);
                    use_new_page = (ins_rowid >= div_rowid) ? 1 : 0;
                } else {
                    int64_t total2, ks2; int tn2, kn2;
                    svdb_get_varint(new_page_data + first_off, new_page_size - first_off, &total2, &tn2);
                    svdb_get_varint(new_page_data + first_off + tn2, new_page_size - first_off - tn2, &ks2, &kn2);
                    size_t cmp_len = (key_len < (size_t)ks2) ? key_len : (size_t)ks2;
                    int cmp = std::memcmp(key, new_page_data + first_off + tn2 + kn2, cmp_len);
                    if (cmp == 0) cmp = (key_len < (size_t)ks2) ? -1 : (key_len > (size_t)ks2) ? 1 : 0;
                    use_new_page = (cmp >= 0) ? 1 : 0;
                }
            }
        }
    }

    // Insert the new key into the determined page
    if (!use_new_page) {
        if (bt->pm.read_page(bt->pm.user_data, bt->config.root_page, &page_data, &page_size) == 0 &&
            page_data && page_size >= 8) {
            int r = insert_into_leaf(page_data, page_size, key, key_len, value, value_len, is_table);
            if (r) {
                bt->pm.write_page(bt->pm.user_data, bt->config.root_page, page_data, page_size);
                return 1;
            }
        }
    } else {
        if (bt->pm.read_page(bt->pm.user_data, new_page_num, &new_page_data, &new_page_size) == 0 &&
            new_page_data && new_page_size >= 8) {
            int r = insert_into_leaf(new_page_data, new_page_size, key, key_len, value, value_len, is_table);
            if (r) {
                bt->pm.write_page(bt->pm.user_data, new_page_num, new_page_data, new_page_size);
                return 1;
            }
        }
    }
    return 0;
}

int svdb_btree_delete(svdb_btree_t* bt, const uint8_t* key, size_t key_len) {
    if (!bt || !key || key_len == 0) return 0;
    if (bt->config.root_page == 0) return 0;
    if (!bt->pm.read_page || !bt->pm.write_page) return 0;

    // Read root page
    uint8_t* page_data = nullptr;
    size_t page_size = 0;
    if (bt->pm.read_page(bt->pm.user_data, bt->config.root_page, &page_data, &page_size) != 0)
        return 0;
    if (!page_data || page_size < 8) return 0;

    uint8_t page_type = page_data[0];
    int is_table = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_TABLE_INTERIOR);
    int is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);

    if (!is_leaf) {
        // Navigate to leaf for delete
        uint32_t child_page;
        if (!search_interior_page(page_data, page_size, key, key_len, is_table, &bt->pm, &child_page))
            return 0;
        if (bt->pm.read_page(bt->pm.user_data, child_page, &page_data, &page_size) != 0)
            return 0;
        if (!page_data || page_size < 8) return 0;
        page_type = page_data[0];
        is_leaf = (page_type == PAGE_TYPE_TABLE_LEAF || page_type == PAGE_TYPE_INDEX_LEAF);
        if (!is_leaf) return 0;

        int ok = delete_from_leaf(page_data, page_size, key, key_len, is_table);
        if (ok) bt->pm.write_page(bt->pm.user_data, child_page, page_data, page_size);
        return ok;
    }

    // Root is a leaf
    int ok = delete_from_leaf(page_data, page_size, key, key_len, is_table);
    if (ok) {
        bt->pm.write_page(bt->pm.user_data, bt->config.root_page, page_data, page_size);
        if (bt->leaf_count > 0) bt->leaf_count--;
    }
    return ok;
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

/* -------------------------------------------------------------------------
 * Embedded B-Tree (no Go callbacks)
 * ----------------------------------------------------------------------- */

#include <string>

namespace svdb {
namespace ds {

// Forward declarations for embedded B-Tree operations
static int embedded_btree_search(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                                  uint8_t** value, size_t* value_len);
static int embedded_btree_insert(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                                  const uint8_t* value, size_t value_len);
static int embedded_btree_delete(svdb_btree_t* bt, const uint8_t* key, size_t key_len);

struct EmbeddedBTree {
    svdb_btree_config_t config;
    svdb_page_manager* pm;  // Embedded C++ PageManager
    uint32_t depth;
    uint32_t leaf_count;
    bool own_pm;  // Whether we own the PageManager
};

// Create B-Tree with embedded PageManager
extern "C" {

svdb_btree_t* svdb_btree_create_embedded(const char* db_path, uint32_t root_page,
                                          int is_table, uint32_t page_size, int cache_pages) {
    if (!db_path || !svdb_manager_is_valid_page_size(page_size)) {
        return nullptr;
    }

    EmbeddedBTree* bt = new (std::nothrow) EmbeddedBTree;
    if (!bt) return nullptr;

    bt->config.root_page = root_page;
    bt->config.is_table = is_table;
    bt->config.page_size = page_size;
    bt->depth = 0;
    bt->leaf_count = 0;
    bt->own_pm = true;

    // Create embedded PageManager
    bt->pm = svdb_page_manager_create(db_path, page_size, cache_pages);
    if (!bt->pm) {
        delete bt;
        return nullptr;
    }

    // Initialize root page if needed (new B-Tree)
    if (root_page == 0) {
        uint32_t new_root;
        if (svdb_page_manager_allocate(bt->pm, &new_root)) {
            bt->config.root_page = new_root;
            
            // Initialize empty leaf page
            const uint8_t* page_data;
            size_t page_size_out;
            if (svdb_page_manager_read(bt->pm, new_root, &page_data, &page_size_out)) {
                // Page already zero-initialized from allocation
                bt->depth = 1;
                bt->leaf_count = 0;
            }
        }
    }

    return reinterpret_cast<svdb_btree_t*>(bt);
}

// Wrapper functions for embedded B-Tree
static int embedded_pm_read(void* user_data, uint32_t page_num, uint8_t** page_data, size_t* page_size) {
    EmbeddedBTree* bt = static_cast<EmbeddedBTree*>(user_data);
    const uint8_t* data = nullptr;
    size_t size = 0;
    if (svdb_page_manager_read(bt->pm, page_num, &data, &size)) {
        // Need to copy since cached data may change
        *page_data = static_cast<uint8_t*>(std::malloc(size));
        if (*page_data) {
            std::memcpy(*page_data, data, size);
            *page_size = size;
            return 1;
        }
    }
    return 0;
}

static int embedded_pm_write(void* user_data, uint32_t page_num, const uint8_t* page_data, size_t page_size) {
    EmbeddedBTree* bt = static_cast<EmbeddedBTree*>(user_data);
    return svdb_page_manager_write(bt->pm, page_num, page_data, page_size);
}

static int embedded_pm_allocate(void* user_data, uint32_t* page_num) {
    EmbeddedBTree* bt = static_cast<EmbeddedBTree*>(user_data);
    return svdb_page_manager_allocate(bt->pm, page_num);
}

static int embedded_pm_free(void* user_data, uint32_t page_num) {
    EmbeddedBTree* bt = static_cast<EmbeddedBTree*>(user_data);
    return svdb_page_manager_free(bt->pm, page_num);
}

} // extern "C"

// C++ wrapper implementation
BTreeEmbedded::BTreeEmbedded(const std::string& db_path, uint32_t root_page,
                             bool is_table, uint32_t page_size, int cache_pages)
    : btree_(nullptr) {
    btree_ = svdb_btree_create_embedded(db_path.c_str(), root_page, 
                                        is_table ? 1 : 0, page_size, cache_pages);
}

BTreeEmbedded::~BTreeEmbedded() {
    if (btree_) {
        EmbeddedBTree* bt = reinterpret_cast<EmbeddedBTree*>(btree_);
        if (bt->pm && bt->own_pm) {
            svdb_page_manager_destroy(bt->pm);
        }
        delete bt;
        btree_ = nullptr;
    }
}

bool BTreeEmbedded::Search(const uint8_t* key, size_t key_len, uint8_t** value, size_t* value_len) {
    if (!btree_) return false;
    return svdb_btree_search(btree_, key, key_len, value, value_len) == 1;
}

bool BTreeEmbedded::Insert(const uint8_t* key, size_t key_len, const uint8_t* value, size_t value_len) {
    if (!btree_) return false;
    return svdb_btree_insert(btree_, key, key_len, value, value_len) == 1;
}

bool BTreeEmbedded::Delete(const uint8_t* key, size_t key_len) {
    if (!btree_) return false;
    return svdb_btree_delete(btree_, key, key_len) == 1;
}

uint32_t BTreeEmbedded::GetDepth() const {
    if (!btree_) return 0;
    return svdb_btree_get_depth(btree_);
}

uint32_t BTreeEmbedded::GetLeafCount() const {
    if (!btree_) return 0;
    return svdb_btree_get_leaf_count(btree_);
}

bool BTreeEmbedded::Sync() {
    if (!btree_) return false;
    EmbeddedBTree* bt = reinterpret_cast<EmbeddedBTree*>(btree_);
    if (bt->pm) {
        return svdb_page_manager_sync(bt->pm) == 1;
    }
    return false;
}

} // namespace ds
} // namespace svdb
