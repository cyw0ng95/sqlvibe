#include "row_store.h"
#include "columnar.h"
#include "manager.h"
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>

/*
 * A single stored value with owned string/blob backing.
 */
struct StoredValue {
    svdb_value_t  val;
    std::string   backing; /* owns str_data or bytes_data content */

    void clear() { val.val_type = 0; val.int_val = 0; val.float_val = 0;
                   val.str_data = nullptr; val.str_len = 0;
                   val.bytes_data = nullptr; val.bytes_len = 0; }

    StoredValue() { clear(); }

    explicit StoredValue(const svdb_value_t* v) {
        if (!v) { clear(); return; }
        val = *v;
        if (v->val_type == 3 /* TEXT */ && v->str_data) {
            backing = std::string(v->str_data, v->str_len);
            val.str_data = nullptr;
        } else if (v->val_type == 4 /* BLOB */ && v->bytes_data) {
            backing = std::string(v->bytes_data, v->bytes_len);
            val.bytes_data = nullptr;
        }
    }

    svdb_value_t get() const {
        svdb_value_t out = val;
        if (val.val_type == 3) { out.str_data = backing.data(); out.str_len = backing.size(); }
        else if (val.val_type == 4) { out.bytes_data = backing.data(); out.bytes_len = backing.size(); }
        return out;
    }
};

struct RowRecord {
    std::vector<StoredValue> cells;
};

struct svdb_row_store_t {
    std::vector<std::string> col_names;
    std::vector<int>         col_types;
    int                      num_cols;
    std::vector<RowRecord>   rows;
    std::vector<bool>        deleted;
    int                      live;

    svdb_row_store_t() : num_cols(0), live(0) {}
};

extern "C" {

svdb_row_store_t* svdb_row_store_create(const char* const* col_names,
                                         const int* col_types,
                                         int num_cols) {
    if (!col_names || !col_types || num_cols <= 0) return nullptr;
    auto* store = new svdb_row_store_t();
    store->num_cols = num_cols;
    for (int i = 0; i < num_cols; ++i) {
        store->col_names.emplace_back(col_names[i] ? col_names[i] : "");
        store->col_types.push_back(col_types[i]);
    }
    return store;
}

void svdb_row_store_destroy(svdb_row_store_t* store) {
    delete store;
}

int svdb_row_store_insert(svdb_row_store_t* store,
                           const svdb_value_t* values,
                           int num_values) {
    if (!store) return -1;
    RowRecord rec;
    rec.cells.reserve((size_t)store->num_cols);
    for (int i = 0; i < store->num_cols; ++i) {
        const svdb_value_t* v = (values && i < num_values) ? &values[i] : nullptr;
        rec.cells.emplace_back(v);
    }
    int idx = (int)store->rows.size();
    store->rows.push_back(std::move(rec));
    store->deleted.push_back(false);
    ++store->live;
    return idx;
}

int svdb_row_store_get(svdb_row_store_t* store, int idx,
                        svdb_value_t* out_values, int* out_count) {
    if (!store || !out_values || idx < 0 || idx >= (int)store->rows.size())
        return 0;
    const RowRecord& rec = store->rows[(size_t)idx];
    int n = (int)rec.cells.size();
    for (int i = 0; i < n; ++i) out_values[i] = rec.cells[(size_t)i].get();
    if (out_count) *out_count = n;
    return 1;
}

void svdb_row_store_update(svdb_row_store_t* store, int idx,
                            const svdb_value_t* values, int num_values) {
    if (!store || idx < 0 || idx >= (int)store->rows.size()) return;
    RowRecord& rec = store->rows[(size_t)idx];
    rec.cells.clear();
    rec.cells.reserve((size_t)store->num_cols);
    for (int i = 0; i < store->num_cols; ++i) {
        const svdb_value_t* v = (values && i < num_values) ? &values[i] : nullptr;
        rec.cells.emplace_back(v);
    }
}

void svdb_row_store_delete(svdb_row_store_t* store, int idx) {
    if (!store || idx < 0 || idx >= (int)store->rows.size()) return;
    if (store->deleted[(size_t)idx]) return;
    store->deleted[(size_t)idx] = true;
    --store->live;
}

int svdb_row_store_is_deleted(svdb_row_store_t* store, int idx) {
    if (!store || idx < 0 || idx >= (int)store->deleted.size()) return 1;
    return store->deleted[(size_t)idx] ? 1 : 0;
}

int svdb_row_store_row_count(svdb_row_store_t* store) {
    if (!store) return 0;
    return (int)store->rows.size();
}

int svdb_row_store_live_count(svdb_row_store_t* store) {
    if (!store) return 0;
    return store->live;
}

/* -------------------------------------------------------------------------
 * Embedded Row Store (with PageManager for persistence)
 * ----------------------------------------------------------------------- */

/*
 * Row Store Persistence Format:
 * 
 * Page 1 (Header Page):
 *   [0-3]   Magic number: 0xR0W5 (row store)
 *   [4-7]   Number of columns
 *   [8-11]  Total row count
 *   [12-15] Live row count
 *   [16+]   Column metadata (name length + name + type for each column)
 * 
 * Page 2+ (Data Pages):
 *   For each row, in order:
 *   - Row length (4 bytes)
 *   - Per column: type tag (1 byte) + value
 *   - Deleted flag (1 bit per row, packed at end)
 */

static const uint32_t ROW_STORE_MAGIC = 0x524F5700;

struct svdb_row_store_embedded_t {
    svdb_row_store_t* store;
    svdb_page_manager* pm;
    uint32_t root_page;
    uint32_t data_start_page;
    bool dirty;

    svdb_row_store_embedded_t(svdb_row_store_t* s, svdb_page_manager* p, uint32_t r)
        : store(s), pm(p), root_page(r), data_start_page(0), dirty(false) {}
};

/* Serialize row store to disk */
static int row_store_serialize(svdb_row_store_embedded_t* embedded) {
    if (!embedded || !embedded->store || !embedded->pm) return 0;
    
    svdb_row_store_t* store = embedded->store;
    svdb_page_manager* pm = embedded->pm;
    
    const uint32_t page_size = svdb_page_manager_get_page_size(pm);
    const int num_cols = store->num_cols;
    const int row_count = (int)store->rows.size();
    
    // Calculate header size
    size_t header_size = 16; // magic + num_cols + row_count + live_count
    for (int i = 0; i < num_cols; ++i) {
        header_size += 1 + store->col_names[i].size() + 4;
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
    header_buf[pos++] = (ROW_STORE_MAGIC >> 24) & 0xFF;
    header_buf[pos++] = (ROW_STORE_MAGIC >> 16) & 0xFF;
    header_buf[pos++] = (ROW_STORE_MAGIC >> 8) & 0xFF;
    header_buf[pos++] = ROW_STORE_MAGIC & 0xFF;
    
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
        const std::string& name = store->col_names[i];
        const int type = store->col_types[i];
        
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
    
    // Write data pages - row-major format
    uint32_t current_page = embedded->data_start_page;
    size_t page_pos = 0;
    std::vector<uint8_t> data_buf(page_size);
    
    for (int row_idx = 0; row_idx < row_count; ++row_idx) {
        const RowRecord& rec = store->rows[row_idx];
        
        // Calculate row size
        size_t row_size = 4; // row length prefix
        for (int col_idx = 0; col_idx < num_cols; ++col_idx) {
            row_size += 1; // type tag
            const StoredValue& sv = rec.cells[col_idx];
            switch (sv.val.val_type) {
                case SVDB_TYPE_NULL: break;
                case SVDB_TYPE_INT: row_size += 8; break;
                case SVDB_TYPE_REAL: row_size += 8; break;
                case SVDB_TYPE_TEXT: row_size += 4 + sv.val.str_len; break;
                case SVDB_TYPE_BLOB: row_size += 4 + sv.val.bytes_len; break;
            }
        }
        
        // Flush page if row doesn't fit
        if (page_pos + row_size > page_size) {
            if (page_pos > 0) {
                if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                current_page++;
                page_pos = 0;
            }
            // If single row is larger than page, would need overflow handling (simplified: skip)
            if (row_size > page_size) continue;
        }
        
        // Write row length
        data_buf[page_pos++] = (row_size >> 24) & 0xFF;
        data_buf[page_pos++] = (row_size >> 16) & 0xFF;
        data_buf[page_pos++] = (row_size >> 8) & 0xFF;
        data_buf[page_pos++] = row_size & 0xFF;
        
        // Write each column
        for (int col_idx = 0; col_idx < num_cols; ++col_idx) {
            const StoredValue& sv = rec.cells[col_idx];
            data_buf[page_pos++] = (uint8_t)sv.val.val_type;
            
            switch (sv.val.val_type) {
                case SVDB_TYPE_NULL:
                    break;
                    
                case SVDB_TYPE_INT: {
                    if (page_pos + 8 > page_size) {
                        if (!svdb_page_manager_write(pm, current_page, data_buf.data(), page_pos)) return 0;
                        current_page++;
                        page_pos = 0;
                    }
                    int64_t ival = sv.val.int_val;
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
                    double rval = sv.val.float_val;
                    const uint8_t* rbytes = reinterpret_cast<const uint8_t*>(&rval);
                    for (int b = 0; b < 8; b++) {
                        data_buf[page_pos++] = rbytes[b];
                    }
                    break;
                }
                    
                case SVDB_TYPE_TEXT:
                case SVDB_TYPE_BLOB: {
                    const uint8_t* bytes = sv.val.val_type == SVDB_TYPE_TEXT ?
                        reinterpret_cast<const uint8_t*>(sv.val.str_data) :
                        reinterpret_cast<const uint8_t*>(sv.val.bytes_data);
                    uint32_t len = sv.val.val_type == SVDB_TYPE_TEXT ? sv.val.str_len : sv.val.bytes_len;
                    
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
    
    // Write final data page
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

svdb_row_store_t* svdb_row_store_create_embedded(const char* const* col_names,
                                                  const int* col_types,
                                                  int num_cols,
                                                  svdb_page_manager* pm,
                                                  uint32_t root_page) {
    if (!col_names || !col_types || num_cols <= 0 || !pm) return nullptr;
    
    svdb_row_store_t* store = svdb_row_store_create(col_names, col_types, num_cols);
    if (!store) return nullptr;
    
    svdb_row_store_embedded_t* embedded = 
        new svdb_row_store_embedded_t(store, pm, root_page);
    embedded->dirty = false;
    
    return reinterpret_cast<svdb_row_store_t*>(embedded);
}

int svdb_row_store_persist(svdb_row_store_t* store, svdb_page_manager* pm, uint32_t* out_root_page) {
    if (!store || !pm || !out_root_page) return 0;
    
    svdb_row_store_embedded_t* embedded = 
        reinterpret_cast<svdb_row_store_embedded_t*>(store);
    
    if (!embedded->store) return 0;
    
    if (!row_store_serialize(embedded)) {
        return 0;
    }
    
    *out_root_page = embedded->root_page;
    return 1;
}

void svdb_row_store_destroy_embedded(svdb_row_store_t* store) {
    if (!store) return;
    delete reinterpret_cast<svdb_row_store_embedded_t*>(store);
}

} /* extern "C" */
