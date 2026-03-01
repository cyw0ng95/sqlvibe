#include "columnar.h"
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

} /* extern "C" */
