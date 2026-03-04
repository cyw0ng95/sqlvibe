/* row.cpp — Row structure implementation */
#include "row.h"
#include <stdlib.h>
#include <string.h>

svdb_row_t* svdb_row_create(const svdb_value_t* values, size_t num_cols) {
    if (num_cols == 0 || num_cols > 64) return nullptr;
    
    svdb_row_t* row = (svdb_row_t*)calloc(1, sizeof(svdb_row_t));
    if (!row) return nullptr;
    
    row->cols = (svdb_value_t*)calloc(num_cols, sizeof(svdb_value_t));
    if (!row->cols) {
        free(row);
        return nullptr;
    }
    
    row->num_cols = num_cols;
    row->bitmap = 0;
    
    /* Copy values and set NULL bitmap */
    for (size_t i = 0; i < num_cols; i++) {
        row->cols[i] = values[i];
        if (values[i].val_type == SVDB_VAL_NULL) {
            row->bitmap |= (1ULL << i);
        }
    }
    
    return row;
}

void svdb_row_destroy(svdb_row_t* row) {
    if (!row) return;
    
    if (row->cols) {
        free(row->cols);
    }
    free(row);
}

int svdb_row_is_null(const svdb_row_t* row, size_t idx) {
    if (!row || idx >= 64) return 0;
    return (row->bitmap >> idx) & 1;
}

void svdb_row_set_null(svdb_row_t* row, size_t idx) {
    if (!row || idx >= 64) return;
    row->bitmap |= (1ULL << idx);
}

void svdb_row_clear_null(svdb_row_t* row, size_t idx) {
    if (!row || idx >= 64) return;
    row->bitmap &= ~(1ULL << idx);
}

const svdb_value_t* svdb_row_get(const svdb_row_t* row, size_t idx) {
    static const svdb_value_t null_val = {SVDB_VAL_NULL, 0, 0.0, nullptr, 0, nullptr, 0};
    
    if (!row || idx >= row->num_cols) {
        return &null_val;
    }
    
    if (svdb_row_is_null(row, idx)) {
        return &null_val;
    }
    
    return &row->cols[idx];
}

void svdb_row_set(svdb_row_t* row, size_t idx, const svdb_value_t* value) {
    if (!row || !value || idx >= row->num_cols) return;
    
    row->cols[idx] = *value;
    
    if (value->val_type == SVDB_VAL_NULL) {
        svdb_row_set_null(row, idx);
    } else {
        svdb_row_clear_null(row, idx);
    }
}

size_t svdb_row_len(const svdb_row_t* row) {
    return row ? row->num_cols : 0;
}
