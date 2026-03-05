#include "hybrid_store_api.h"
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_map>

/* ── Scan result allocation ─────────────────────────────────────────────── */

svdb_scan_result_t* svdb_scan_result_alloc(int32_t num_rows, int32_t num_cols) {
    if (num_rows < 0 || num_cols < 0) return nullptr;
    
    svdb_scan_result_t* result = (svdb_scan_result_t*)malloc(sizeof(svdb_scan_result_t));
    if (!result) return nullptr;
    
    result->num_rows = num_rows;
    result->num_cols = num_cols;
    result->values = nullptr;
    result->row_indices = nullptr;
    
    if (num_rows > 0 && num_cols > 0) {
        size_t val_count = (size_t)num_rows * (size_t)num_cols;
        result->values = (svdb_value_t*)calloc(val_count, sizeof(svdb_value_t));
        result->row_indices = (int32_t*)malloc((size_t)num_rows * sizeof(int32_t));
        
        if (!result->values || !result->row_indices) {
            free(result->values);
            free(result->row_indices);
            free(result);
            return nullptr;
        }
    }
    
    return result;
}

void svdb_scan_result_free(svdb_scan_result_t* result) {
    if (!result) return;
    
    /* Free string/blob data in values */
    if (result->values && result->num_rows > 0 && result->num_cols > 0) {
        size_t count = (size_t)result->num_rows * (size_t)result->num_cols;
        for (size_t i = 0; i < count; i++) {
            if ((result->values[i].val_type == SVDB_VAL_TEXT || 
                 result->values[i].val_type == SVDB_VAL_BLOB) &&
                result->values[i].str_data) {
                free((void*)result->values[i].str_data);
            }
        }
        free(result->values);
    }
    free(result->row_indices);
    free(result);
}

/* ── Index Engine implementation ────────────────────────────────────────── */

/* Internal C++ index engine with bitmap and skip-list support */
struct svdb_index_engine {
    /* Bitmap index: col_name -> (value_str -> bitmap) */
    std::unordered_map<std::string, 
        std::unordered_map<std::string, std::vector<uint32_t>>> bitmaps;
    
    /* Skip-list index: col_name -> sorted entries */
    std::unordered_map<std::string, 
        std::vector<std::pair<svdb_value_t, uint32_t>>> skiplists;
    
    std::unordered_map<std::string, bool> has_bitmap;
    std::unordered_map<std::string, bool> has_skiplist;
};

svdb_index_engine_t* svdb_index_engine_create(void) {
    return new (std::nothrow) svdb_index_engine_t();
}

void svdb_index_engine_destroy(svdb_index_engine_t* ie) {
    if (ie) delete ie;
}

void svdb_index_engine_add_bitmap(svdb_index_engine_t* ie, const char* col_name) {
    if (!ie || !col_name) return;
    if (!ie->has_bitmap[col_name]) {
        ie->bitmaps[col_name] = std::unordered_map<std::string, std::vector<uint32_t>>();
        ie->has_bitmap[col_name] = true;
    }
}

void svdb_index_engine_add_skiplist(svdb_index_engine_t* ie, const char* col_name) {
    if (!ie || !col_name) return;
    if (!ie->has_skiplist[col_name]) {
        ie->skiplists[col_name] = std::vector<std::pair<svdb_value_t, uint32_t>>();
        ie->has_skiplist[col_name] = true;
    }
}

int svdb_index_engine_has_bitmap(svdb_index_engine_t* ie, const char* col_name) {
    if (!ie || !col_name) return 0;
    return ie->has_bitmap[col_name] ? 1 : 0;
}

int svdb_index_engine_has_skiplist(svdb_index_engine_t* ie, const char* col_name) {
    if (!ie || !col_name) return 0;
    return ie->has_skiplist[col_name] ? 1 : 0;
}

static std::string value_to_key(const svdb_value_t* val) {
    if (!val) return "";
    switch (val->val_type) {
        case SVDB_VAL_NULL:   return "NULL";
        case SVDB_VAL_INT:    return std::to_string(val->int_val);
        case SVDB_VAL_FLOAT:  return std::to_string(val->float_val);
        case SVDB_VAL_TEXT:   
            return val->str_data ? std::string(val->str_data, val->str_len) : "";
        case SVDB_VAL_BLOB:
            return val->str_data ? std::string(val->str_data, val->str_len) : "";
        default: return "";
    }
}

void svdb_index_engine_index_row(svdb_index_engine_t* ie, uint32_t row_idx,
                                  const char* col_name, const svdb_value_t* val) {
    if (!ie || !col_name || !val) return;
    
    /* Bitmap index */
    if (ie->has_bitmap[col_name]) {
        std::string key = value_to_key(val);
        ie->bitmaps[col_name][key].push_back(row_idx);
    }
    
    /* Skip-list index - store sorted by value */
    if (ie->has_skiplist[col_name]) {
        svdb_value_t val_copy = *val;
        if (val->val_type == SVDB_VAL_TEXT || val->val_type == SVDB_VAL_BLOB) {
            if (val->str_data && val->str_len > 0) {
                char* p = (char*)malloc(val->str_len + 1);
                if (p) {
                    memcpy(p, val->str_data, val->str_len);
                    p[val->str_len] = '\0';
                    val_copy.str_data = p;
                }
            }
        }
        ie->skiplists[col_name].push_back({val_copy, row_idx});
    }
}

void svdb_index_engine_unindex_row(svdb_index_engine_t* ie, uint32_t row_idx,
                                    const char* col_name, const svdb_value_t* val) {
    if (!ie || !col_name || !val) return;
    
    /* Bitmap index */
    if (ie->has_bitmap[col_name]) {
        std::string key = value_to_key(val);
        if (ie->bitmaps[col_name].count(key)) {
            auto& bitmap = ie->bitmaps[col_name][key];
            auto it = std::remove(bitmap.begin(), bitmap.end(), row_idx);
            bitmap.erase(it, bitmap.end());
        }
    }
    
    /* Skip-list index */
    if (ie->has_skiplist[col_name]) {
        auto& entries = ie->skiplists[col_name];
        auto it = std::remove_if(entries.begin(), entries.end(),
            [row_idx](const std::pair<svdb_value_t, uint32_t>& e) {
                return e.second == row_idx;
            });
        entries.erase(it, entries.end());
    }
}

/* Helper: compare two values for equality */
static int values_equal(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b) return 0;
    if (a->val_type != b->val_type) return 0;
    
    switch (a->val_type) {
        case SVDB_VAL_NULL:
            return 1;
        case SVDB_VAL_INT:
            return a->int_val == b->int_val;
        case SVDB_VAL_FLOAT:
            return a->float_val == b->float_val;
        case SVDB_VAL_TEXT:
        case SVDB_VAL_BLOB:
            if (a->str_len != b->str_len) return 0;
            return memcmp(a->str_data, b->str_data, a->str_len) == 0;
        default:
            return 0;
    }
}

/* Helper: compare two values for ordering */
static int compare_values(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b) return 0;
    if (a->val_type == SVDB_VAL_NULL && b->val_type == SVDB_VAL_NULL) return 0;
    if (a->val_type == SVDB_VAL_NULL) return -1;
    if (b->val_type == SVDB_VAL_NULL) return 1;
    
    bool a_num = (a->val_type == SVDB_VAL_INT || a->val_type == SVDB_VAL_FLOAT);
    bool b_num = (b->val_type == SVDB_VAL_INT || b->val_type == SVDB_VAL_FLOAT);
    
    if (a_num && b_num) {
        double fa = (a->val_type == SVDB_VAL_FLOAT) ? a->float_val : (double)a->int_val;
        double fb = (b->val_type == SVDB_VAL_FLOAT) ? b->float_val : (double)b->int_val;
        return (fa > fb) - (fa < fb);
    }
    
    if ((a->val_type == SVDB_VAL_TEXT || a->val_type == SVDB_VAL_BLOB) &&
        (b->val_type == SVDB_VAL_TEXT || b->val_type == SVDB_VAL_BLOB)) {
        size_t min_len = a->str_len < b->str_len ? a->str_len : b->str_len;
        int c = memcmp(a->str_data, b->str_data, min_len);
        if (c != 0) return c;
        return (a->str_len > b->str_len) - (a->str_len < b->str_len);
    }
    
    return a->val_type - b->val_type;
}

svdb_scan_result_t* svdb_index_lookup_equal(svdb_index_engine_t* ie, const char* col_name,
                                             const svdb_value_t* val, int32_t num_cols) {
    if (!ie || !col_name || !val || num_cols <= 0) return nullptr;
    
    std::vector<uint32_t> row_indices;
    
    /* Try bitmap index first */
    if (ie->has_bitmap[col_name]) {
        std::string key = value_to_key(val);
        auto it = ie->bitmaps[col_name].find(key);
        if (it != ie->bitmaps[col_name].end()) {
            row_indices = it->second;
        }
    }
    
    /* Try skip-list index */
    if (row_indices.empty() && ie->has_skiplist[col_name]) {
        auto& entries = ie->skiplists[col_name];
        for (const auto& entry : entries) {
            if (values_equal(&entry.first, val)) {
                row_indices.push_back(entry.second);
            }
        }
    }
    
    if (row_indices.empty()) {
        return svdb_scan_result_alloc(0, num_cols);
    }
    
    /* Allocate result - caller will fill values via callback */
    svdb_scan_result_t* result = svdb_scan_result_alloc((int32_t)row_indices.size(), num_cols);
    if (result) {
        for (size_t i = 0; i < row_indices.size(); i++) {
            result->row_indices[i] = (int32_t)row_indices[i];
        }
    }
    return result;
}

svdb_scan_result_t* svdb_index_lookup_range(svdb_index_engine_t* ie, const char* col_name,
                                             const svdb_value_t* lo, const svdb_value_t* hi,
                                             int32_t num_cols, int inclusive) {
    if (!ie || !col_name || !lo || !hi || num_cols <= 0) return nullptr;
    
    std::vector<uint32_t> row_indices;
    
    /* Only skip-list supports range queries */
    if (ie->has_skiplist[col_name]) {
        auto& entries = ie->skiplists[col_name];
        for (const auto& entry : entries) {
            int cmp_lo = compare_values(&entry.first, lo);
            int cmp_hi = compare_values(&entry.first, hi);
            
            bool match_lo = inclusive ? (cmp_lo >= 0) : (cmp_lo > 0);
            bool match_hi = inclusive ? (cmp_hi <= 0) : (cmp_hi < 0);
            
            if (match_lo && match_hi) {
                row_indices.push_back(entry.second);
            }
        }
    }
    
    if (row_indices.empty()) {
        return svdb_scan_result_alloc(0, num_cols);
    }
    
    svdb_scan_result_t* result = svdb_scan_result_alloc((int32_t)row_indices.size(), num_cols);
    if (result) {
        for (size_t i = 0; i < row_indices.size(); i++) {
            result->row_indices[i] = (int32_t)row_indices[i];
        }
    }
    return result;
}

/* Scan with filter using Go callbacks */
svdb_scan_result_t* svdb_scan_with_filter(
    void* row_store,
    svdb_get_row_value_fn get_value,
    int32_t* row_indices,
    int32_t num_indices,
    int32_t num_cols,
    int32_t filter_col_idx,
    const svdb_value_t* filter_val,
    const char* op,
    svdb_compare_values_fn cmp) {
    
    if (!get_value || !row_indices || !filter_val || !op || !cmp || num_cols <= 0) {
        return nullptr;
    }
    
    std::vector<int32_t> matching_indices;
    
    for (int32_t i = 0; i < num_indices; i++) {
        int32_t row_idx = row_indices[i];
        svdb_value_t row_val = get_value(row_idx, filter_col_idx, row_store);
        
        int c = cmp(&row_val, filter_val);
        bool match = false;
        
        if (strcmp(op, "=") == 0) match = (c == 0);
        else if (strcmp(op, "!=") == 0) match = (c != 0);
        else if (strcmp(op, "<") == 0) match = (c < 0);
        else if (strcmp(op, "<=") == 0) match = (c <= 0);
        else if (strcmp(op, ">") == 0) match = (c > 0);
        else if (strcmp(op, ">=") == 0) match = (c >= 0);
        
        if (match) {
            matching_indices.push_back(row_idx);
        }
    }
    
    if (matching_indices.empty()) {
        return svdb_scan_result_alloc(0, num_cols);
    }
    
    svdb_scan_result_t* result = svdb_scan_result_alloc((int32_t)matching_indices.size(), num_cols);
    if (result) {
        for (size_t i = 0; i < matching_indices.size(); i++) {
            result->row_indices[i] = matching_indices[i];
        }
    }
    return result;
}
