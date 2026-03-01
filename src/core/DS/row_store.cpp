#include "row_store.h"
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

int svdb_row_store_row_count(svdb_row_store_t* store) {
    if (!store) return 0;
    return (int)store->rows.size();
}

int svdb_row_store_live_count(svdb_row_store_t* store) {
    if (!store) return 0;
    return store->live;
}

} /* extern "C" */
