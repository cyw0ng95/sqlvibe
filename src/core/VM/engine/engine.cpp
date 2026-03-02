#include "engine_api.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>

/* ── helpers ─────────────────────────────────────────────────────────────── */

/* Deep-copy the str_data of a value (TEXT/BLOB only). */
static svdb_value_t copy_val(const svdb_value_t* v) {
    svdb_value_t out = *v;
    if ((v->val_type == SVDB_VAL_TEXT || v->val_type == SVDB_VAL_BLOB) &&
        v->str_data && v->str_len > 0) {
        char* p = (char*)malloc(v->str_len + 1);
        if (p) {
            memcpy(p, v->str_data, v->str_len);
            p[v->str_len] = '\0';
            out.str_data = p;
        } else {
            /* Allocation failed — emit a null value rather than a dangling pointer. */
            out.val_type = SVDB_VAL_NULL;
            out.str_data = nullptr;
            out.str_len  = 0;
        }
    } else {
        out.str_data = nullptr;
        out.str_len  = 0;
    }
    return out;
}

static void free_val_data(svdb_value_t* v) {
    if ((v->val_type == SVDB_VAL_TEXT || v->val_type == SVDB_VAL_BLOB) &&
        v->str_data) {
        free((void*)v->str_data);
        v->str_data = nullptr;
    }
}

/* Fill dst (assumed zero or previously unused) from src — deep copies strings. */
static void copy_row_into(svdb_engine_row_t* dst, const svdb_engine_row_t* src) {
    int32_t n = src->num_cols;
    dst->num_cols  = n;
    dst->col_names = nullptr;
    dst->vals      = nullptr;
    if (n <= 0) return;

    dst->col_names = (char**)malloc((size_t)n * sizeof(char*));
    dst->vals      = (svdb_value_t*)malloc((size_t)n * sizeof(svdb_value_t));
    if (!dst->col_names || !dst->vals) {
        free(dst->col_names);
        free(dst->vals);
        dst->col_names = nullptr;
        dst->vals      = nullptr;
        dst->num_cols  = 0;
        return;
    }
    for (int32_t i = 0; i < n; i++) {
        dst->col_names[i] = src->col_names[i] ? strdup(src->col_names[i]) : nullptr;
        dst->vals[i]      = copy_val(&src->vals[i]);
    }
}

/* Free the arrays inside a row struct without freeing the struct itself. */
static void clear_row(svdb_engine_row_t* row) {
    for (int32_t i = 0; i < row->num_cols; i++) {
        free(row->col_names[i]);
        free_val_data(&row->vals[i]);
    }
    free(row->col_names);
    free(row->vals);
    row->col_names = nullptr;
    row->vals      = nullptr;
    row->num_cols  = 0;
}

/* Merge a + b into dst (b overrides a for same-named columns). */
static void merge_rows_into(svdb_engine_row_t*       dst,
                             const svdb_engine_row_t* a,
                             const svdb_engine_row_t* b,
                             const char*              alias_a,
                             const char*              alias_b) {
    dst->num_cols  = 0;
    dst->col_names = nullptr;
    dst->vals      = nullptr;

    int32_t na = a ? a->num_cols : 0;
    int32_t nb = b ? b->num_cols : 0;

    /* Upper bound: unqualified + qualified for both sides. */
    int32_t alias_extra = (alias_a && alias_a[0] ? na : 0)
                        + (alias_b && alias_b[0] ? nb : 0);
    int32_t total = na + nb + alias_extra;
    if (total <= 0) return;

    dst->col_names = (char**)malloc((size_t)total * sizeof(char*));
    dst->vals      = (svdb_value_t*)malloc((size_t)total * sizeof(svdb_value_t));
    if (!dst->col_names || !dst->vals) {
        free(dst->col_names);
        free(dst->vals);
        dst->col_names = nullptr;
        dst->vals      = nullptr;
        return;
    }

    int32_t k = 0;

    /* Copy all columns from a. */
    for (int32_t i = 0; i < na; i++) {
        dst->col_names[k] = a->col_names[i] ? strdup(a->col_names[i]) : nullptr;
        dst->vals[k]      = copy_val(&a->vals[i]);
        k++;
    }

    /* Add alias-qualified columns from a. */
    if (alias_a && alias_a[0]) {
        for (int32_t i = 0; i < na; i++) {
            if (!a->col_names[i]) continue;
            std::string qname = std::string(alias_a) + "." + a->col_names[i];
            dst->col_names[k] = strdup(qname.c_str());
            dst->vals[k]      = copy_val(&a->vals[i]);
            k++;
        }
    }

    /* Add columns from b (override unqualified; add qualified). */
    for (int32_t i = 0; i < nb; i++) {
        const char* bname = b->col_names[i];
        bool overridden = false;
        if (bname) {
            /* Override existing unqualified column from a. */
            for (int32_t j = 0; j < na; j++) {
                if (dst->col_names[j] && strcmp(dst->col_names[j], bname) == 0) {
                    free_val_data(&dst->vals[j]);
                    dst->vals[j] = copy_val(&b->vals[i]);
                    overridden = true;
                    break;
                }
            }
        }
        if (!overridden) {
            /* Not found in a — append. */
            dst->col_names[k] = bname ? strdup(bname) : nullptr;
            dst->vals[k]      = copy_val(&b->vals[i]);
            k++;
        }
        /* Qualified alias for b. */
        if (alias_b && alias_b[0] && bname) {
            std::string qname = std::string(alias_b) + "." + bname;
            dst->col_names[k] = strdup(qname.c_str());
            dst->vals[k]      = copy_val(&b->vals[i]);
            k++;
        }
    }

    dst->num_cols = k;
}

/* Simple value comparison. Returns <0, 0, >0. */
static int compare_values(const svdb_value_t* a,
                          const svdb_value_t* b) {
    if (a->val_type == SVDB_VAL_NULL && b->val_type == SVDB_VAL_NULL) return 0;
    if (a->val_type == SVDB_VAL_NULL) return -1;
    if (b->val_type == SVDB_VAL_NULL) return  1;

    /* Both numeric. */
    bool a_num = (a->val_type == SVDB_VAL_INT || a->val_type == SVDB_VAL_FLOAT);
    bool b_num = (b->val_type == SVDB_VAL_INT || b->val_type == SVDB_VAL_FLOAT);
    if (a_num && b_num) {
        double fa = (a->val_type == SVDB_VAL_FLOAT) ? a->float_val
                                                     : (double)a->int_val;
        double fb = (b->val_type == SVDB_VAL_FLOAT) ? b->float_val
                                                     : (double)b->int_val;
        return (fa > fb) - (fa < fb);
    }

    /* Both text / blob. */
    if ((a->val_type == SVDB_VAL_TEXT || a->val_type == SVDB_VAL_BLOB) &&
        (b->val_type == SVDB_VAL_TEXT || b->val_type == SVDB_VAL_BLOB)) {
        size_t min_len = a->str_len < b->str_len ? a->str_len : b->str_len;
        int    c       = memcmp(a->str_data, b->str_data, min_len);
        if (c != 0) return c;
        return (a->str_len > b->str_len) - (a->str_len < b->str_len);
    }

    /* Mixed types: order by type ordinal. */
    return a->val_type - b->val_type;
}

/* Find the value of col_name in row (linear search). Returns null-type if missing. */
static const svdb_value_t* find_col(const svdb_engine_row_t* row,
                                           const char*              col_name) {
    static const svdb_value_t null_val = {SVDB_VAL_NULL, 0, 0.0, nullptr, 0};
    if (!row || !col_name) return &null_val;
    for (int32_t i = 0; i < row->num_cols; i++) {
        if (row->col_names[i] && strcmp(row->col_names[i], col_name) == 0)
            return &row->vals[i];
    }
    return &null_val;
}

/* ── Allocation ──────────────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_rows_alloc(int32_t num_rows) {
    svdb_engine_rows_t* rs =
        (svdb_engine_rows_t*)malloc(sizeof(svdb_engine_rows_t));
    if (!rs) return nullptr;
    rs->num_rows = num_rows;
    if (num_rows > 0) {
        rs->rows = (svdb_engine_row_t*)calloc(
            (size_t)num_rows, sizeof(svdb_engine_row_t));
        if (!rs->rows) { free(rs); return nullptr; }
    } else {
        rs->rows = nullptr;
    }
    return rs;
}

void svdb_engine_rows_free(svdb_engine_rows_t* rs) {
    if (!rs) return;
    for (int32_t i = 0; i < rs->num_rows; i++)
        clear_row(&rs->rows[i]);
    free(rs->rows);
    free(rs);
}

svdb_engine_row_t* svdb_engine_row_alloc(int32_t num_cols) {
    svdb_engine_row_t* row =
        (svdb_engine_row_t*)malloc(sizeof(svdb_engine_row_t));
    if (!row) return nullptr;
    row->num_cols = num_cols;
    if (num_cols > 0) {
        row->col_names = (char**)calloc((size_t)num_cols, sizeof(char*));
        row->vals      = (svdb_value_t*)calloc(
            (size_t)num_cols, sizeof(svdb_value_t));
        if (!row->col_names || !row->vals) {
            free(row->col_names);
            free(row->vals);
            free(row);
            return nullptr;
        }
    } else {
        row->col_names = nullptr;
        row->vals      = nullptr;
    }
    return row;
}

void svdb_engine_row_free(svdb_engine_row_t* row) {
    if (!row) return;
    clear_row(row);
    free(row);
}

/* ── SELECT operations ───────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_apply_limit_offset(
    const svdb_engine_rows_t* rows, int32_t limit, int32_t offset) {

    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    if (offset < 0) offset = 0;
    if (offset >= rows->num_rows) return svdb_engine_rows_alloc(0);

    int32_t start = offset;
    int32_t end   = rows->num_rows;
    if (limit > 0 && start + limit < end)
        end = start + limit;

    int32_t count = end - start;
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    for (int32_t i = 0; i < count; i++)
        copy_row_into(&out->rows[i], &rows->rows[start + i]);

    return out;
}

int32_t svdb_engine_col_names(
    const svdb_engine_rows_t* rows, char** out_names, int32_t max_names) {

    if (!rows || rows->num_rows == 0) return 0;

    std::vector<std::string> seen_order;
    std::unordered_set<std::string> seen;

    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_engine_row_t* row = &rows->rows[i];
        for (int32_t j = 0; j < row->num_cols; j++) {
            if (row->col_names[j]) {
                std::string key(row->col_names[j]);
                if (seen.find(key) == seen.end()) {
                    seen.insert(key);
                    seen_order.push_back(key);
                }
            }
        }
    }

    int32_t n = (int32_t)seen_order.size();
    if (out_names && max_names > 0) {
        int32_t fill = n < max_names ? n : max_names;
        /* Point at strings in the first row where possible — callers must not
           free these pointers; they belong to the rows structure. */
        for (int32_t k = 0; k < fill; k++) {
            /* Find this name in the first row that has it. */
            const char* found = nullptr;
            for (int32_t i = 0; i < rows->num_rows && !found; i++) {
                for (int32_t j = 0; j < rows->rows[i].num_cols; j++) {
                    if (rows->rows[i].col_names[j] &&
                        strcmp(rows->rows[i].col_names[j],
                               seen_order[(size_t)k].c_str()) == 0) {
                        found = rows->rows[i].col_names[j];
                        break;
                    }
                }
            }
            out_names[k] = (char*)found;
        }
    }
    return n;
}

/* ── JOIN operations ─────────────────────────────────────────────────────── */

svdb_engine_row_t* svdb_engine_merge_rows(
    const svdb_engine_row_t* a, const svdb_engine_row_t* b) {

    svdb_engine_row_t* out =
        (svdb_engine_row_t*)calloc(1, sizeof(svdb_engine_row_t));
    if (!out) return nullptr;
    merge_rows_into(out, a, b, nullptr, nullptr);
    return out;
}

svdb_engine_row_t* svdb_engine_merge_rows_alias(
    const svdb_engine_row_t* a, const char* alias_a,
    const svdb_engine_row_t* b, const char* alias_b) {

    svdb_engine_row_t* out =
        (svdb_engine_row_t*)calloc(1, sizeof(svdb_engine_row_t));
    if (!out) return nullptr;
    merge_rows_into(out, a, b, alias_a, alias_b);
    return out;
}

svdb_engine_rows_t* svdb_engine_cross_join(
    const svdb_engine_rows_t* left, const svdb_engine_rows_t* right) {

    if (!left || !right || left->num_rows == 0 || right->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    int64_t count64 = (int64_t)left->num_rows * (int64_t)right->num_rows;
    if (count64 > INT32_MAX) return nullptr;
    int32_t count = (int32_t)count64;
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    int32_t k = 0;
    for (int32_t i = 0; i < left->num_rows; i++) {
        for (int32_t j = 0; j < right->num_rows; j++) {
            merge_rows_into(&out->rows[k++],
                            &left->rows[i], &right->rows[j],
                            nullptr, nullptr);
        }
    }
    return out;
}

/* ── Aggregate operations ────────────────────────────────────────────────── */

int64_t svdb_engine_count_rows(
    const svdb_engine_rows_t* rows, const char* col_name) {

    if (!rows) return 0;
    int64_t n = 0;
    for (int32_t i = 0; i < rows->num_rows; i++) {
        if (!col_name || col_name[0] == '\0') {
            n++;
        } else {
            const svdb_value_t* v = find_col(&rows->rows[i], col_name);
            if (v->val_type != SVDB_VAL_NULL) n++;
        }
    }
    return n;
}

/* ── Sort / Reverse ──────────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_sort_rows(
    const svdb_engine_rows_t*     rows,
    const svdb_engine_sort_key_t* sort_keys,
    int32_t                       num_keys) {

    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    /* Build an index array and sort by index. */
    std::vector<int32_t> idx((size_t)rows->num_rows);
    for (int32_t i = 0; i < rows->num_rows; i++) idx[(size_t)i] = i;

    if (sort_keys && num_keys > 0) {
        std::stable_sort(idx.begin(), idx.end(),
            [&](int32_t ia, int32_t ib) {
                const svdb_engine_row_t* ra = &rows->rows[ia];
                const svdb_engine_row_t* rb = &rows->rows[ib];
                for (int32_t k = 0; k < num_keys; k++) {
                    const svdb_engine_sort_key_t* key = &sort_keys[k];
                    const svdb_value_t* va =
                        find_col(ra, key->col_name);
                    const svdb_value_t* vb =
                        find_col(rb, key->col_name);

                    /* Handle NULLs. */
                    bool a_null = (va->val_type == SVDB_VAL_NULL);
                    bool b_null = (vb->val_type == SVDB_VAL_NULL);
                    if (a_null && b_null) continue;
                    if (a_null) return key->null_order == 0; /* NULLS FIRST */
                    if (b_null) return key->null_order != 0; /* NULLS LAST  */

                    int c = compare_values(va, vb);
                    if (c == 0) continue;
                    return (key->order == 1) ? (c > 0) : (c < 0); /* DESC / ASC */
                }
                return false;
            });
    }

    svdb_engine_rows_t* out = svdb_engine_rows_alloc(rows->num_rows);
    if (!out) return nullptr;

    for (int32_t i = 0; i < rows->num_rows; i++)
        copy_row_into(&out->rows[i], &rows->rows[idx[(size_t)i]]);

    return out;
}

svdb_engine_rows_t* svdb_engine_reverse_rows(const svdb_engine_rows_t* rows) {
    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    int32_t n = rows->num_rows;
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(n);
    if (!out) return nullptr;

    for (int32_t i = 0; i < n; i++)
        copy_row_into(&out->rows[i], &rows->rows[n - 1 - i]);

    return out;
}

/* ── Subquery operations ─────────────────────────────────────────────────── */

int32_t svdb_engine_exists_rows(const svdb_engine_rows_t* rows) {
    return (rows && rows->num_rows > 0) ? 1 : 0;
}

int32_t svdb_engine_in_rows(
    const svdb_value_t* value,
    const svdb_engine_rows_t*  rows,
    const char*                col_name) {

    if (!value || value->val_type == SVDB_VAL_NULL || !rows) return 0;

    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (compare_values(value, v) == 0) return 1;
    }
    return 0;
}

int32_t svdb_engine_not_in_rows(
    const svdb_value_t* value,
    const svdb_engine_rows_t*  rows,
    const char*                col_name) {

    if (!value || value->val_type == SVDB_VAL_NULL) return 0;
    if (!rows || rows->num_rows == 0) return 1;

    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (v->val_type == SVDB_VAL_NULL) return 0; /* SQL three-valued logic */
        if (compare_values(value, v) == 0) return 0;
    }
    return 1;
}

/* ── Window operations ───────────────────────────────────────────────────── */

int64_t* svdb_engine_row_numbers(int32_t n) {
    if (n <= 0) return nullptr;
    int64_t* out = (int64_t*)malloc((size_t)n * sizeof(int64_t));
    if (!out) return nullptr;
    for (int32_t i = 0; i < n; i++) out[i] = (int64_t)(i + 1);
    return out;
}

int64_t* svdb_engine_ranks(
    const svdb_engine_rows_t* rows, const char* col_name) {

    if (!rows || rows->num_rows == 0) return nullptr;
    int32_t n  = rows->num_rows;
    int64_t* out = (int64_t*)malloc((size_t)n * sizeof(int64_t));
    if (!out) return nullptr;

    int64_t rank = 1;
    for (int32_t i = 0; i < n; i++) {
        if (i == 0) { out[0] = 1; continue; }
        const svdb_value_t* prev = find_col(&rows->rows[i-1], col_name);
        const svdb_value_t* curr = find_col(&rows->rows[i],   col_name);
        if (compare_values(prev, curr) != 0)
            rank = (int64_t)(i + 1); /* gap — matches SQL RANK() */
        out[i] = rank;
    }
    return out;
}

int64_t* svdb_engine_dense_ranks(
    const svdb_engine_rows_t* rows, const char* col_name) {

    if (!rows || rows->num_rows == 0) return nullptr;
    int32_t n    = rows->num_rows;
    int64_t* out = (int64_t*)malloc((size_t)n * sizeof(int64_t));
    if (!out) return nullptr;

    int64_t rank = 1;
    for (int32_t i = 0; i < n; i++) {
        if (i == 0) { out[0] = 1; continue; }
        const svdb_value_t* prev = find_col(&rows->rows[i-1], col_name);
        const svdb_value_t* curr = find_col(&rows->rows[i],   col_name);
        if (compare_values(prev, curr) != 0) rank++;
        out[i] = rank;
    }
    return out;
}

/* ── Filter operations ───────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_filter_rows(
    const svdb_engine_rows_t* rows,
    svdb_row_predicate_fn pred,
    void* user_data) {

    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    /* First pass: count matching rows */
    std::vector<int32_t> indices;
    indices.reserve((size_t)rows->num_rows);
    for (int32_t i = 0; i < rows->num_rows; i++) {
        if (pred && pred(&rows->rows[i], user_data)) {
            indices.push_back(i);
        }
    }

    int32_t count = (int32_t)indices.size();
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    for (int32_t i = 0; i < count; i++)
        copy_row_into(&out->rows[i], &rows->rows[indices[(size_t)i]]);

    return out;
}

/* ── Distinct operations ──────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_apply_distinct(
    const svdb_engine_rows_t* rows,
    svdb_row_key_fn key_fn,
    void* user_data) {

    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    if (!key_fn) {
        /* No key function — return copy of all rows */
        svdb_engine_rows_t* out = svdb_engine_rows_alloc(rows->num_rows);
        if (!out) return nullptr;
        for (int32_t i = 0; i < rows->num_rows; i++)
            copy_row_into(&out->rows[i], &rows->rows[i]);
        return out;
    }

    std::vector<int32_t> indices;
    indices.reserve((size_t)rows->num_rows);
    std::unordered_set<std::string> seen;

    for (int32_t i = 0; i < rows->num_rows; i++) {
        const char* key = key_fn(&rows->rows[i], user_data);
        if (key) {
            std::string key_str(key);
            if (seen.find(key_str) == seen.end()) {
                seen.insert(key_str);
                indices.push_back(i);
            }
        } else {
            /* NULL key — treat as unique */
            indices.push_back(i);
        }
    }

    int32_t count = (int32_t)indices.size();
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    for (int32_t i = 0; i < count; i++)
        copy_row_into(&out->rows[i], &rows->rows[indices[(size_t)i]]);

    return out;
}

/* ── JOIN operations with predicates ─────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_inner_join(
    const svdb_engine_rows_t* left,
    const svdb_engine_rows_t* right,
    svdb_join_predicate_fn pred,
    void* user_data) {

    if (!left || !right || left->num_rows == 0 || right->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    std::vector<int32_t> result_indices;
    result_indices.reserve((size_t)left->num_rows * (size_t)right->num_rows / 4);

    for (int32_t i = 0; i < left->num_rows; i++) {
        for (int32_t j = 0; j < right->num_rows; j++) {
            /* Build merged row for predicate check */
            svdb_engine_row_t merged;
            merged.num_cols = 0;
            merged.col_names = nullptr;
            merged.vals = nullptr;
            merge_rows_into(&merged, &left->rows[i], &right->rows[j], nullptr, nullptr);

            int32_t keep = 1;
            if (pred) {
                keep = pred(&merged, user_data);
            }
            clear_row(&merged);

            if (keep) {
                result_indices.push_back(i);
                result_indices.push_back(j);
            }
        }
    }

    int32_t count = (int32_t)result_indices.size() / 2;
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    for (int32_t k = 0; k < count; k++) {
        int32_t li = result_indices[k * 2];
        int32_t ri = result_indices[k * 2 + 1];
        merge_rows_into(&out->rows[k], &left->rows[li], &right->rows[ri], nullptr, nullptr);
    }

    return out;
}

svdb_engine_rows_t* svdb_engine_left_outer_join(
    const svdb_engine_rows_t* left,
    const svdb_engine_rows_t* right,
    svdb_join_predicate_fn pred,
    void* user_data,
    const char** right_cols,
    int32_t num_right_cols) {

    if (!left || left->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    if (!right || right->num_rows == 0) {
        /* No right rows — return left rows with NULL right columns */
        svdb_engine_rows_t* out = svdb_engine_rows_alloc(left->num_rows);
        if (!out) return nullptr;

        for (int32_t i = 0; i < left->num_rows; i++) {
            svdb_engine_row_t null_row;
            null_row.num_cols = 0;
            null_row.col_names = nullptr;
            null_row.vals = nullptr;
            merge_rows_into(&out->rows[i], &left->rows[i], &null_row, nullptr, nullptr);
        }
        return out;
    }

    std::vector<int32_t> result_indices;
    std::vector<char> matched;
    result_indices.reserve((size_t)left->num_rows * 2);
    matched.resize((size_t)left->num_rows, 0);

    for (int32_t i = 0; i < left->num_rows; i++) {
        for (int32_t j = 0; j < right->num_rows; j++) {
            svdb_engine_row_t merged;
            merged.num_cols = 0;
            merged.col_names = nullptr;
            merged.vals = nullptr;
            merge_rows_into(&merged, &left->rows[i], &right->rows[j], nullptr, nullptr);

            int32_t keep = 1;
            if (pred) {
                keep = pred(&merged, user_data);
            }
            clear_row(&merged);

            if (keep) {
                result_indices.push_back(i);
                result_indices.push_back(j);
                matched[(size_t)i] = 1;
            }
        }
        /* If no match, emit left row with NULL right columns */
        if (!matched[(size_t)i]) {
            result_indices.push_back(i);
            result_indices.push_back(-1);  /* -1 indicates no right row */
        }
    }

    int32_t count = (int32_t)result_indices.size() / 2;
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    for (int32_t k = 0; k < count; k++) {
        int32_t li = result_indices[k * 2];
        int32_t ri = result_indices[k * 2 + 1];

        if (ri >= 0) {
            merge_rows_into(&out->rows[k], &left->rows[li], &right->rows[ri], nullptr, nullptr);
        } else {
            /* Left row with NULL right columns */
            svdb_engine_row_t null_row;
            null_row.num_cols = 0;
            null_row.col_names = nullptr;
            null_row.vals = nullptr;

            /* Build null row with right column names */
            if (right_cols && num_right_cols > 0) {
                null_row.num_cols = num_right_cols;
                null_row.col_names = (char**)malloc((size_t)num_right_cols * sizeof(char*));
                null_row.vals = (svdb_value_t*)malloc((size_t)num_right_cols * sizeof(svdb_value_t));
                for (int32_t c = 0; c < num_right_cols; c++) {
                    null_row.col_names[c] = strdup(right_cols[c]);
                    null_row.vals[c].val_type = SVDB_VAL_NULL;
                    null_row.vals[c].int_val = 0;
                    null_row.vals[c].float_val = 0.0;
                    null_row.vals[c].str_data = nullptr;
                    null_row.vals[c].str_len = 0;
                }
            }
            merge_rows_into(&out->rows[k], &left->rows[li], &null_row, nullptr, nullptr);
            clear_row(&null_row);
        }
    }

    return out;
}

/* ── Aggregate operations ────────────────────────────────────────────────── */

svdb_engine_rows_t* svdb_engine_group_rows(
    const svdb_engine_rows_t* rows,
    svdb_row_key_fn key_fn,
    void* user_data) {

    if (!rows || rows->num_rows == 0)
        return svdb_engine_rows_alloc(0);

    if (!key_fn) {
        /* No key function — return all rows as single group (not standard, but safe) */
        svdb_engine_rows_t* out = svdb_engine_rows_alloc(1);
        if (!out) return nullptr;
        /* Return empty row as placeholder — caller should use group index */
        out->rows[0].num_cols = 0;
        out->rows[0].col_names = nullptr;
        out->rows[0].vals = nullptr;
        return out;
    }

    /* Group by key, store indices for each group */
    std::unordered_map<std::string, std::vector<int32_t>> groups;
    for (int32_t i = 0; i < rows->num_rows; i++) {
        const char* key = key_fn(&rows->rows[i], user_data);
        if (key) {
            groups[std::string(key)].push_back(i);
        } else {
            groups["__null__"].push_back(i);
        }
    }

    /* Return one row per group (just a placeholder with group key) */
    int32_t count = (int32_t)groups.size();
    svdb_engine_rows_t* out = svdb_engine_rows_alloc(count);
    if (!out) return nullptr;

    int32_t k = 0;
    for (const auto& pair : groups) {
        out->rows[k].num_cols = 1;
        out->rows[k].col_names = (char**)malloc(sizeof(char*));
        out->rows[k].vals = (svdb_value_t*)malloc(sizeof(svdb_value_t));
        out->rows[k].col_names[0] = strdup("group_key");
        out->rows[k].vals[0].val_type = SVDB_VAL_TEXT;
        out->rows[k].vals[0].int_val = 0;
        out->rows[k].vals[0].float_val = 0.0;
        out->rows[k].vals[0].str_data = strdup(pair.first.c_str());
        out->rows[k].vals[0].str_len = pair.first.length();
        k++;
    }

    return out;
}

svdb_value_t svdb_engine_sum_rows(
    const svdb_engine_rows_t* rows, const char* col_name) {

    svdb_value_t null_val;
    null_val.val_type = SVDB_VAL_NULL;
    null_val.int_val = 0;
    null_val.float_val = 0.0;
    null_val.str_data = nullptr;
    null_val.str_len = 0;

    if (!rows) return null_val;
    
    double sum = 0.0;
    int32_t count = 0;
    
    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (v->val_type == SVDB_VAL_INT) {
            sum += (double)v->int_val;
            count++;
        } else if (v->val_type == SVDB_VAL_FLOAT) {
            sum += v->float_val;
            count++;
        }
        /* NULL values are skipped */
    }
    
    if (count == 0) return null_val;
    
    /* SQLite SUM always returns float64 */
    svdb_value_t result;
    result.val_type = SVDB_VAL_FLOAT;
    result.float_val = sum;
    result.int_val = 0;
    result.str_data = nullptr;
    result.str_len = 0;
    return result;
}

double svdb_engine_avg_rows(
    const svdb_engine_rows_t* rows, const char* col_name) {

    if (!rows || rows->num_rows == 0) return 0.0;
    double sum = 0.0;
    int64_t count = 0;
    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (v->val_type == SVDB_VAL_INT) {
            sum += (double)v->int_val;
            count++;
        } else if (v->val_type == SVDB_VAL_FLOAT) {
            sum += v->float_val;
            count++;
        }
        /* NULL values are skipped */
    }
    return count > 0 ? sum / (double)count : 0.0;
}

svdb_value_t svdb_engine_min_rows(
    const svdb_engine_rows_t* rows, const char* col_name) {

    svdb_value_t null_val;
    null_val.val_type = SVDB_VAL_NULL;
    null_val.int_val = 0;
    null_val.float_val = 0.0;
    null_val.str_data = nullptr;
    null_val.str_len = 0;

    if (!rows || rows->num_rows == 0) return null_val;

    const svdb_value_t* min_val = nullptr;
    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (v->val_type == SVDB_VAL_NULL) continue;
        if (!min_val || compare_values(v, min_val) < 0) {
            min_val = v;
        }
    }

    return min_val ? *min_val : null_val;
}

svdb_value_t svdb_engine_max_rows(
    const svdb_engine_rows_t* rows, const char* col_name) {

    svdb_value_t null_val;
    null_val.val_type = SVDB_VAL_NULL;
    null_val.int_val = 0;
    null_val.float_val = 0.0;
    null_val.str_data = nullptr;
    null_val.str_len = 0;

    if (!rows || rows->num_rows == 0) return null_val;

    const svdb_value_t* max_val = nullptr;
    for (int32_t i = 0; i < rows->num_rows; i++) {
        const svdb_value_t* v = find_col(&rows->rows[i], col_name);
        if (v->val_type == SVDB_VAL_NULL) continue;
        if (!max_val || compare_values(v, max_val) > 0) {
            max_val = v;
        }
    }

    return max_val ? *max_val : null_val;
}
