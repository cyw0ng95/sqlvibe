#include "columnar.h"
#include "manager.h"
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>

/*
 * Each column is stored as a contiguous vector of svdb_value_t copies.
 * String and blob data is owned by a parallel std::string vector so that
 * pointers returned to callers remain stable until the next mutation.
 */
struct ColumnData {
    int              col_type;
    std::string      col_name;
    std::vector<svdb_value_t>  values;  /* raw value structs     */
    std::vector<std::string>   strings; /* backing storage for str_data / bytes_data */

    void append(const svdb_value_t* v) {
        if (!v) {
            svdb_value_t entry{};
            entry.val_type = SVDB_TYPE_NULL;
            values.push_back(entry);
            strings.emplace_back();
            return;
        }
        svdb_value_t entry = *v;
        if (v->val_type == SVDB_TYPE_TEXT && v->str_data) {
            strings.emplace_back(v->str_data, v->str_len);
            entry.str_data = nullptr; /* data lives in strings[]; pointer reconstructed in get() */
        } else if (v->val_type == SVDB_TYPE_BLOB && v->bytes_data) {
            strings.emplace_back(v->bytes_data, v->bytes_len);
            entry.bytes_data = nullptr; /* data lives in strings[]; pointer reconstructed in get() */
        } else {
            strings.emplace_back();
        }
        values.push_back(entry);
    }

    svdb_value_t get(int idx) {
        if (idx < 0 || idx >= (int)values.size()) {
            svdb_value_t n{}; n.val_type = SVDB_TYPE_NULL; return n;
        }
        svdb_value_t v = values[(size_t)idx];
        /* Re-attach string pointer from backing storage. */
        if (v.val_type == SVDB_TYPE_TEXT) {
            v.str_data = strings[(size_t)idx].data();
            v.str_len  = strings[(size_t)idx].size();
        } else if (v.val_type == SVDB_TYPE_BLOB) {
            v.bytes_data = strings[(size_t)idx].data();
            v.bytes_len  = strings[(size_t)idx].size();
        }
        return v;
    }
};

struct svdb_column_store_t {
    std::vector<ColumnData> cols;
    std::vector<bool>       deleted;
    int                     live;
    int                     num_cols;

    svdb_column_store_t() : live(0), num_cols(0) {}
};

extern "C" {

svdb_column_store_t* svdb_column_store_create(const char* const* col_names,
                                               const int* col_types,
                                               int num_cols) {
    if (!col_names || !col_types || num_cols <= 0) return nullptr;
    auto* store = new svdb_column_store_t();
    store->num_cols = num_cols;
    store->cols.resize((size_t)num_cols);
    for (int i = 0; i < num_cols; ++i) {
        store->cols[(size_t)i].col_name = col_names[i] ? col_names[i] : "";
        store->cols[(size_t)i].col_type = col_types[i];
    }
    return store;
}

void svdb_column_store_destroy(svdb_column_store_t* store) {
    delete store;
}

void svdb_column_store_append_row(svdb_column_store_t* store,
                                   const svdb_value_t* values,
                                   int num_values) {
    if (!store) return;
    store->deleted.push_back(false);
    ++store->live;
    for (int i = 0; i < store->num_cols; ++i) {
        const svdb_value_t* v = (values && i < num_values) ? &values[i] : nullptr;
        store->cols[(size_t)i].append(v);
    }
}

int svdb_column_store_get_row(svdb_column_store_t* store, int idx,
                               svdb_value_t* out_values, int* out_count) {
    if (!store || !out_values || idx < 0 || idx >= (int)store->deleted.size())
        return 0;
    for (int i = 0; i < store->num_cols; ++i) {
        out_values[i] = store->cols[(size_t)i].get(idx);
    }
    if (out_count) *out_count = store->num_cols;
    return 1;
}

void svdb_column_store_delete_row(svdb_column_store_t* store, int idx) {
    if (!store || idx < 0 || idx >= (int)store->deleted.size()) return;
    if (store->deleted[(size_t)idx]) return;
    store->deleted[(size_t)idx] = true;
    --store->live;
}

int svdb_column_store_is_deleted(svdb_column_store_t* store, int idx) {
    if (!store || idx < 0 || idx >= (int)store->deleted.size()) return 1;
    return store->deleted[(size_t)idx] ? 1 : 0;
}

void svdb_column_store_update_row(svdb_column_store_t* store, int idx,
                                   const svdb_value_t* values, int num_values) {
    if (!store || idx < 0 || idx >= (int)store->deleted.size()) return;
    for (int i = 0; i < store->num_cols; ++i) {
        const svdb_value_t* v = (values && i < num_values) ? &values[i] : nullptr;
        ColumnData& col = store->cols[(size_t)i];
        if (idx >= (int)col.values.size()) continue;
        if (!v) {
            svdb_value_t null_val{}; null_val.val_type = SVDB_TYPE_NULL;
            col.values[(size_t)idx] = null_val;
            col.strings[(size_t)idx] = "";
        } else {
            svdb_value_t entry = *v;
            if (v->val_type == SVDB_TYPE_TEXT && v->str_data) {
                col.strings[(size_t)idx] = std::string(v->str_data, v->str_len);
                entry.str_data = nullptr;
            } else if (v->val_type == SVDB_TYPE_BLOB && v->bytes_data) {
                col.strings[(size_t)idx] = std::string(v->bytes_data, v->bytes_len);
                entry.bytes_data = nullptr;
            } else {
                col.strings[(size_t)idx] = "";
            }
            col.values[(size_t)idx] = entry;
        }
    }
}

int svdb_column_store_row_count(svdb_column_store_t* store) {
    if (!store) return 0;
    return (int)store->deleted.size();
}

int svdb_column_store_live_count(svdb_column_store_t* store) {
    if (!store) return 0;
    return store->live;
}

/* -------------------------------------------------------------------------
 * Embedded Column Store (with PageManager for persistence)
 * ----------------------------------------------------------------------- */

/*
 * Column Store Persistence Format:
 * 
 * Page 1 (Header Page):
 *   [0-3]   Magic number: 0xC0L5 (column store)
 *   [4-7]   Number of columns
 *   [8-11]  Total row count
 *   [12-15] Live row count
 *   [16+]   Column metadata (name length + name + type for each column)
 * 
 * Page 2+ (Data Pages):
 *   For each column, store values in row order:
 *   - Type tag (1 byte)
 *   - Value (8 bytes for int/real, or length + data for text/blob)
 *   - Deleted bitmap (1 bit per row, packed at end of data pages)
 */

static const uint32_t COLUMN_STORE_MAGIC = 0xC0150000;

struct svdb_column_store_embedded_t {
    svdb_column_store_t* store;
    svdb_page_manager* pm;
    uint32_t root_page;
    uint32_t data_start_page;
    bool dirty;

    svdb_column_store_embedded_t(svdb_column_store_t* s, svdb_page_manager* p, uint32_t r)
        : store(s), pm(p), root_page(r), data_start_page(0), dirty(false) {}
};

/* Serialize column store to disk */
static int column_store_serialize(svdb_column_store_embedded_t* embedded) {
    if (!embedded || !embedded->store || !embedded->pm) return 0;
    
    svdb_column_store_t* store = embedded->store;
    svdb_page_manager* pm = embedded->pm;
    
    const uint32_t page_size = svdb_page_manager_get_page_size(pm);
    const int num_cols = store->num_cols;
    const int row_count = (int)store->deleted.size();
    
    // Calculate space needed for header
    size_t header_size = 16; // magic + num_cols + row_count + live_count
    for (int i = 0; i < num_cols; ++i) {
        header_size += 1 + store->cols[i].col_name.size() + 4; // name_len + name + type
    }
    
    // Allocate header page
    uint32_t header_page;
    if (embedded->root_page == 0) {
        if (!svdb_page_manager_allocate(pm, &header_page)) return 0;
        embedded->root_page = header_page;
        embedded->data_start_page = header_page + 1;
    } else {
        header_page = embedded->root_page;
        embedded->data_start_page = header_page + 1;
    }
    
    // Build header
    std::vector<uint8_t> header_buf(page_size, 0);
    size_t pos = 0;
    
    // Magic number
    if (pos + 4 > page_size) return 0;
    header_buf[pos++] = (COLUMN_STORE_MAGIC >> 24) & 0xFF;
    header_buf[pos++] = (COLUMN_STORE_MAGIC >> 16) & 0xFF;
    header_buf[pos++] = (COLUMN_STORE_MAGIC >> 8) & 0xFF;
    header_buf[pos++] = COLUMN_STORE_MAGIC & 0xFF;
    
    // Number of columns
    if (pos + 4 > page_size) return 0;
    header_buf[pos++] = (num_cols >> 24) & 0xFF;
    header_buf[pos++] = (num_cols >> 16) & 0xFF;
    header_buf[pos++] = (num_cols >> 8) & 0xFF;
    header_buf[pos++] = num_cols & 0xFF;
    
    // Row count
    if (pos + 4 > page_size) return 0;
    header_buf[pos++] = (row_count >> 24) & 0xFF;
    header_buf[pos++] = (row_count >> 16) & 0xFF;
    header_buf[pos++] = (row_count >> 8) & 0xFF;
    header_buf[pos++] = row_count & 0xFF;
    
    // Live count
    if (pos + 4 > page_size) return 0;
    header_buf[pos++] = (store->live >> 24) & 0xFF;
    header_buf[pos++] = (store->live >> 16) & 0xFF;
    header_buf[pos++] = (store->live >> 8) & 0xFF;
    header_buf[pos++] = store->live & 0xFF;
    
    // Column metadata
    for (int i = 0; i < num_cols; ++i) {
        const std::string& name = store->cols[i].col_name;
        const int type = store->cols[i].col_type;
        
        if (pos + 1 + name.size() + 4 > page_size) return 0;
        header_buf[pos++] = (uint8_t)name.size();
        for (char c : name) {
            header_buf[pos++] = (uint8_t)c;
        }
        header_buf[pos++] = (type >> 24) & 0xFF;
        header_buf[pos++] = (type >> 16) & 0xFF;
        header_buf[pos++] = (type >> 8) & 0xFF;
        header_buf[pos++] = type & 0xFF;
    }
    
    // Write header page
    if (!svdb_page_manager_write(pm, header_page, header_buf.data(), header_buf.size())) {
        return 0;
    }
    
    // Write data pages - simplified: store all column data sequentially
    uint32_t current_page = embedded->data_start_page;
    size_t page_pos = 0;
    std::vector<uint8_t> data_buf(page_size);
    
    for (int col_idx = 0; col_idx < num_cols; ++col_idx) {
        const ColumnData& col = store->cols[col_idx];
        
        for (int row_idx = 0; row_idx < row_count; ++row_idx) {
            svdb_value_t val = col.values[row_idx];
            
            // Write type tag
            if (page_pos + 1 > page_size) {
                if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                current_page++;
                page_pos = 0;
            }
            data_buf[page_pos++] = (uint8_t)val.val_type;
            
            // Write value based on type
            switch (val.val_type) {
                case SVDB_TYPE_NULL:
                    break;
                    
                case SVDB_TYPE_INT: {
                    if (page_pos + 8 > page_size) {
                        if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                        current_page++;
                        page_pos = 0;
                    }
                    int64_t ival = val.int_val;
                    for (int b = 0; b < 8; b++) {
                        data_buf[page_pos++] = (ival >> (b * 8)) & 0xFF;
                    }
                    break;
                }
                    
                case SVDB_TYPE_REAL: {
                    if (page_pos + 8 > page_size) {
                        if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                        current_page++;
                        page_pos = 0;
                    }
                    double rval = val.float_val;
                    const uint8_t* rbytes = reinterpret_cast<const uint8_t*>(&rval);
                    for (int b = 0; b < 8; b++) {
                        data_buf[page_pos++] = rbytes[b];
                    }
                    break;
                }
                    
                case SVDB_TYPE_TEXT:
                case SVDB_TYPE_BLOB: {
                    const uint8_t* bytes = val.val_type == SVDB_TYPE_TEXT ? 
                        reinterpret_cast<const uint8_t*>(val.str_data) :
                        reinterpret_cast<const uint8_t*>(val.bytes_data);
                    uint32_t len = val.val_type == SVDB_TYPE_TEXT ? val.str_len : val.bytes_len;
                    
                    if (page_pos + 4 > page_size) {
                        if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                        current_page++;
                        page_pos = 0;
                    }
                    for (int b = 0; b < 4; b++) {
                        data_buf[page_pos++] = (len >> (b * 8)) & 0xFF;
                    }
                    
                    for (uint32_t b = 0; b < len; b++) {
                        if (page_pos + 1 > page_size) {
                            if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                            current_page++;
                            page_pos = 0;
                        }
                        data_buf[page_pos++] = bytes[b];
                    }
                    break;
                }
            }
        }
    }
    
    // Write final data page if needed
    if (page_pos > 0) {
        if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) {
            return 0;
        }
    }
    
    // Write deleted bitmap
    uint32_t bitmap_page = current_page + 1;
    std::vector<uint8_t> bitmap_buf((row_count + 7) / 8, 0);
    for (int i = 0; i < row_count; i++) {
        if (store->deleted[i]) {
            bitmap_buf[i / 8] |= (1 << (i % 8));
        }
    }
    
    if (!svdb_page_manager_write(pm, bitmap_page, bitmap_buf.data(), bitmap_buf.size())) {
        return 0;
    }
    
    embedded->dirty = false;
    return 1;
}

svdb_column_store_t* svdb_column_store_create_embedded(const char* const* col_names,
                                                       const int* col_types,
                                                       int num_cols,
                                                       svdb_page_manager* pm,
                                                       uint32_t root_page) {
    if (!col_names || !col_types || num_cols <= 0 || !pm) return nullptr;
    
    // Create new store (loading from disk would go here in future)
    svdb_column_store_t* store = svdb_column_store_create(col_names, col_types, num_cols);
    if (!store) return nullptr;
    
    svdb_column_store_embedded_t* embedded = 
        new svdb_column_store_embedded_t(store, pm, root_page);
    embedded->dirty = false;
    
    return reinterpret_cast<svdb_column_store_t*>(embedded);
}

int svdb_column_store_persist(svdb_column_store_t* store, svdb_page_manager* pm, uint32_t* out_root_page) {
    if (!store || !pm || !out_root_page) return 0;
    
    svdb_column_store_embedded_t* embedded = 
        reinterpret_cast<svdb_column_store_embedded_t*>(store);
    
    if (!embedded->store) return 0;
    
    if (!column_store_serialize(embedded)) {
        return 0;
    }
    
    *out_root_page = embedded->root_page;
    return 1;
}

void svdb_column_store_destroy_embedded(svdb_column_store_t* store) {
    if (!store) return;
    delete reinterpret_cast<svdb_column_store_embedded_t*>(store);
}

} /* extern "C" */
