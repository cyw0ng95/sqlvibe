#include "svdb.h"
#include "svdb_types.h"
#include <cstring>

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

} /* extern "C" */
