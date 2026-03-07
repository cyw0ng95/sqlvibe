#include "svdb.h"
#include "svdb_types.h"
#include <cstring>
#include <cstdlib>

extern "C" {

int svdb_rows_column_count(svdb_rows_t *rows) {
    if (!rows) return 0;
    return static_cast<int>(rows->col_names.size());
}

const char *svdb_rows_column_name(svdb_rows_t *rows, int col) {
    if (!rows || col < 0 || col >= static_cast<int>(rows->col_names.size()))
        return "";
    return rows->col_names[col].c_str();
}

int svdb_rows_next(svdb_rows_t *rows) {
    if (!rows) return 0;
    rows->cursor++;
    return rows->cursor < static_cast<int>(rows->rows.size()) ? 1 : 0;
}

svdb_val_t svdb_rows_get(svdb_rows_t *rows, int col) {
    svdb_val_t v{};
    v.type = SVDB_TYPE_NULL;
    if (!rows || rows->cursor < 0 ||
        rows->cursor >= static_cast<int>(rows->rows.size()) ||
        col < 0 || col >= static_cast<int>(rows->col_names.size()))
        return v;

    const SvdbVal &sv = rows->rows[rows->cursor][col];
    v.type = sv.type;
    v.ival = sv.ival;
    v.rval = sv.rval;
    if (sv.type == SVDB_TYPE_TEXT || sv.type == SVDB_TYPE_BLOB) {
        v.sval = sv.sval.c_str();
        v.slen = sv.sval.size();
    }
    return v;
}

void svdb_rows_close(svdb_rows_t *rows) {
    delete rows;
}

/* ── Batch result fetching implementation ─────────────────────── */

int svdb_rows_fetch_batch(svdb_rows_t *rows, svdb_row_batch_t *batch, int max_rows) {
    if (!rows || !batch || max_rows <= 0) {
        if (batch) {
            batch->row_count = 0;
            batch->col_count = 0;
            batch->cols = nullptr;
        }
        return 0;
    }

    int col_count = static_cast<int>(rows->col_names.size());
    int remaining = static_cast<int>(rows->rows.size()) - (rows->cursor + 1);
    int fetch_count = (remaining < max_rows) ? remaining : max_rows;

    batch->row_count = fetch_count;
    batch->col_count = col_count;
    batch->cols = nullptr;

    if (fetch_count == 0) {
        return 0;
    }

    /* Allocate column array */
    batch->cols = (svdb_batch_col_t *)std::calloc(col_count, sizeof(svdb_batch_col_t));
    if (!batch->cols) {
        batch->row_count = 0;
        return 0;
    }

    /* Initialize each column */
    for (int c = 0; c < col_count; c++) {
        svdb_batch_col_t *col = &batch->cols[c];

        /* Allocate arrays for this column */
        col->type_arr  = (svdb_type_t *)std::calloc(fetch_count, sizeof(svdb_type_t));
        col->ival_arr  = (int64_t *)std::calloc(fetch_count, sizeof(int64_t));
        col->rval_arr  = (double *)std::calloc(fetch_count, sizeof(double));
        col->sval_arr  = (char **)std::calloc(fetch_count, sizeof(char *));
        col->slen_arr  = (size_t *)std::calloc(fetch_count, sizeof(size_t));
        col->null_mask = (uint8_t *)std::calloc(fetch_count, sizeof(uint8_t));

        if (!col->type_arr || !col->ival_arr || !col->rval_arr ||
            !col->sval_arr || !col->slen_arr || !col->null_mask) {
            /* Allocation failed - clean up and return 0 */
            svdb_row_batch_free(batch);
            batch->row_count = 0;
            return 0;
        }

        /* Fill column data from rows */
        col->type = SVDB_TYPE_NULL;
        for (int r = 0; r < fetch_count; r++) {
            int row_idx = rows->cursor + 1 + r;
            const SvdbVal &sv = rows->rows[row_idx][c];

            col->type_arr[r] = sv.type;
            /* dominant type = first non-NULL type seen */
            if (col->type == SVDB_TYPE_NULL && sv.type != SVDB_TYPE_NULL)
                col->type = sv.type;
            switch (sv.type) {
                case SVDB_TYPE_NULL:
                    col->null_mask[r] = 1;
                    break;
                case SVDB_TYPE_INT:
                    col->ival_arr[r] = sv.ival;
                    break;
                case SVDB_TYPE_REAL:
                    col->rval_arr[r] = sv.rval;
                    break;
                case SVDB_TYPE_TEXT:
                case SVDB_TYPE_BLOB:
                    col->sval_arr[r] = const_cast<char*>(sv.sval.c_str());
                    col->slen_arr[r] = sv.sval.size();
                    break;
            }
        }
    }

    /* Advance cursor past fetched rows */
    rows->cursor += fetch_count;

    return fetch_count;
}

void svdb_row_batch_free(svdb_row_batch_t *batch) {
    if (!batch) return;

    if (batch->cols) {
        for (int c = 0; c < batch->col_count; c++) {
            svdb_batch_col_t *col = &batch->cols[c];
            if (col->type_arr) std::free(col->type_arr);
            if (col->ival_arr) std::free(col->ival_arr);
            if (col->rval_arr) std::free(col->rval_arr);
            if (col->sval_arr) std::free(col->sval_arr);  /* pointers are into rows memory */
            if (col->slen_arr) std::free(col->slen_arr);
            if (col->null_mask) std::free(col->null_mask);
        }
        std::free(batch->cols);
        batch->cols = nullptr;
    }
    batch->row_count = 0;
    batch->col_count = 0;
}

/* ── Batch data accessor functions (for CGO compatibility) ───── */

int svdb_batch_row_count(const svdb_row_batch_t *batch) {
    return batch ? batch->row_count : 0;
}

int svdb_batch_col_count(const svdb_row_batch_t *batch) {
    return batch ? batch->col_count : 0;
}

svdb_type_t svdb_batch_col_type(const svdb_row_batch_t *batch, int col) {
    if (!batch || col < 0 || col >= batch->col_count || !batch->cols)
        return SVDB_TYPE_NULL;
    return batch->cols[col].type;
}

svdb_type_t svdb_batch_get_row_type(const svdb_row_batch_t *batch, int col, int row) {
    if (!batch || col < 0 || col >= batch->col_count ||
        row < 0 || row >= batch->row_count || !batch->cols)
        return SVDB_TYPE_NULL;
    const svdb_batch_col_t *c = &batch->cols[col];
    if (c->type_arr) return c->type_arr[row];
    return c->type;
}

int svdb_batch_is_null(const svdb_row_batch_t *batch, int col, int row) {
    if (!batch || col < 0 || col >= batch->col_count ||
        row < 0 || row >= batch->row_count || !batch->cols)
        return 1;
    const svdb_batch_col_t *c = &batch->cols[col];
    return (c->null_mask && c->null_mask[row]) ? 1 : 0;
}

int64_t svdb_batch_get_int(const svdb_row_batch_t *batch, int col, int row) {
    if (!batch || col < 0 || col >= batch->col_count ||
        row < 0 || row >= batch->row_count || !batch->cols)
        return 0;
    const svdb_batch_col_t *c = &batch->cols[col];
    return c->ival_arr ? c->ival_arr[row] : 0;
}

double svdb_batch_get_real(const svdb_row_batch_t *batch, int col, int row) {
    if (!batch || col < 0 || col >= batch->col_count ||
        row < 0 || row >= batch->row_count || !batch->cols)
        return 0.0;
    const svdb_batch_col_t *c = &batch->cols[col];
    return c->rval_arr ? c->rval_arr[row] : 0.0;
}

const char* svdb_batch_get_text(const svdb_row_batch_t *batch, int col, int row, size_t *out_len) {
    if (out_len) *out_len = 0;
    if (!batch || col < 0 || col >= batch->col_count ||
        row < 0 || row >= batch->row_count || !batch->cols)
        return nullptr;
    const svdb_batch_col_t *c = &batch->cols[col];
    if (!c->sval_arr || !c->sval_arr[row]) return nullptr;
    if (out_len && c->slen_arr) *out_len = c->slen_arr[row];
    return c->sval_arr[row];
}

const uint8_t* svdb_batch_get_blob(const svdb_row_batch_t *batch, int col, int row, size_t *out_len) {
    return (const uint8_t*)svdb_batch_get_text(batch, col, row, out_len);
}

} /* extern "C" */
