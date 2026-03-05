/* svdb.h — SVDB Public C API */
#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handles ──────────────────────────────────────────── */
typedef struct svdb_db_s     svdb_db_t;
typedef struct svdb_stmt_s   svdb_stmt_t;
typedef struct svdb_rows_s   svdb_rows_t;
typedef struct svdb_tx_s     svdb_tx_t;

/* ── Error codes ─────────────────────────────────────────────── */
typedef enum {
    SVDB_OK        = 0,
    SVDB_ERR       = 1,
    SVDB_NOTFOUND  = 2,
    SVDB_BUSY      = 3,
    SVDB_READONLY  = 4,
    SVDB_CORRUPT   = 5,
    SVDB_NOMEM     = 6,
    SVDB_DONE      = 7,
} svdb_code_t;

/* ── Result ──────────────────────────────────────────────────── */
typedef struct {
    svdb_code_t code;
    const char *errmsg;    /* valid until next API call on same db */
    int64_t     rows_affected;
    int64_t     last_insert_rowid;
} svdb_result_t;

/* ── Value ───────────────────────────────────────────────────── */
typedef enum {
    SVDB_TYPE_NULL = 0,
    SVDB_TYPE_INT  = 1,
    SVDB_TYPE_REAL = 2,
    SVDB_TYPE_TEXT = 3,
    SVDB_TYPE_BLOB = 4,
} svdb_type_t;

typedef struct {
    svdb_type_t type;
    int64_t     ival;
    double      rval;
    const char *sval;   /* points into engine-owned memory */
    size_t      slen;
} svdb_val_t;

/* ── Database lifecycle ──────────────────────────────────────── */
svdb_code_t   svdb_open(const char *path, svdb_db_t **db);
svdb_code_t   svdb_close(svdb_db_t *db);
const char   *svdb_errmsg(svdb_db_t *db);

/* ── Direct execute (no result set) ─────────────────────────── */
svdb_code_t   svdb_exec(svdb_db_t *db, const char *sql, svdb_result_t *res);

/* ── Query (returns result set) ─────────────────────────────── */
svdb_code_t   svdb_query(svdb_db_t *db, const char *sql, svdb_rows_t **rows);
int           svdb_rows_column_count(svdb_rows_t *rows);
const char   *svdb_rows_column_name(svdb_rows_t *rows, int col);
int           svdb_rows_next(svdb_rows_t *rows);   /* 1=row, 0=done */
svdb_val_t    svdb_rows_get(svdb_rows_t *rows, int col);
void          svdb_rows_close(svdb_rows_t *rows);

/* ── Prepared statements ─────────────────────────────────────── */
svdb_code_t   svdb_prepare(svdb_db_t *db, const char *sql, svdb_stmt_t **stmt);
svdb_code_t   svdb_stmt_bind_int(svdb_stmt_t *stmt, int idx, int64_t val);
svdb_code_t   svdb_stmt_bind_real(svdb_stmt_t *stmt, int idx, double val);
svdb_code_t   svdb_stmt_bind_text(svdb_stmt_t *stmt, int idx,
                                   const char *val, size_t len);
svdb_code_t   svdb_stmt_bind_null(svdb_stmt_t *stmt, int idx);
svdb_code_t   svdb_stmt_exec(svdb_stmt_t *stmt, svdb_result_t *res);
svdb_code_t   svdb_stmt_query(svdb_stmt_t *stmt, svdb_rows_t **rows);
svdb_code_t   svdb_stmt_reset(svdb_stmt_t *stmt);
svdb_code_t   svdb_stmt_close(svdb_stmt_t *stmt);

/* ── Transactions ────────────────────────────────────────────── */
svdb_code_t   svdb_begin(svdb_db_t *db, svdb_tx_t **tx);
svdb_code_t   svdb_commit(svdb_tx_t *tx);
svdb_code_t   svdb_rollback(svdb_tx_t *tx);
svdb_code_t   svdb_savepoint(svdb_tx_t *tx, const char *name);
svdb_code_t   svdb_release(svdb_tx_t *tx, const char *name);
svdb_code_t   svdb_rollback_to(svdb_tx_t *tx, const char *name);

/* ── Schema introspection ────────────────────────────────────── */
svdb_code_t   svdb_tables(svdb_db_t *db, svdb_rows_t **rows);
svdb_code_t   svdb_columns(svdb_db_t *db, const char *table, svdb_rows_t **rows);
svdb_code_t   svdb_indexes(svdb_db_t *db, const char *table, svdb_rows_t **rows);

/* ── Backup ──────────────────────────────────────────────────── */
svdb_code_t   svdb_backup(svdb_db_t *src, const char *dest_path);

/* ── Version ─────────────────────────────────────────────────── */
const char   *svdb_version(void);
int           svdb_version_number(void);

/* ── Virtual Tables ──────────────────────────────────────────── */
/* Forward declare vtab types from IS/vtab_api.h */
typedef struct svdb_vtab_module_s   svdb_vtab_module_t;
typedef struct svdb_vtab_s          svdb_vtab_t;
typedef struct svdb_vtab_cursor_s   svdb_vtab_cursor_t;

/* Virtual table module registration */
svdb_code_t   svdb_register_vtab_module(const char *name, svdb_vtab_module_t *module);
int           svdb_has_vtab_module(const char *name);
int           svdb_get_vtab_module_count(void);
svdb_code_t   svdb_get_vtab_module_name(int index, char *buffer, size_t buffer_size);

/* Virtual table operations */
svdb_vtab_t*  svdb_vtab_create(svdb_vtab_module_t *module,
                               const char **args, int arg_count);
svdb_vtab_t*  svdb_vtab_connect(svdb_vtab_module_t *module,
                                const char **args, int arg_count);
int           svdb_vtab_column_count(svdb_vtab_t *vtab);
const char*   svdb_vtab_column_name(svdb_vtab_t *vtab, int col);
svdb_vtab_cursor_t* svdb_vtab_cursor_open(svdb_vtab_t *vtab);
svdb_code_t   svdb_vtab_close(svdb_vtab_t *vtab, int destroy);

/* Cursor operations */
svdb_code_t   svdb_vtab_cursor_filter(svdb_vtab_cursor_t *cursor, int idx_num,
                                      const char *idx_str,
                                      const char **args, int arg_count);
svdb_code_t   svdb_vtab_cursor_next(svdb_vtab_cursor_t *cursor);
int           svdb_vtab_cursor_eof(svdb_vtab_cursor_t *cursor);
svdb_code_t   svdb_vtab_cursor_column(svdb_vtab_cursor_t *cursor, int col,
                                      int *out_type, int64_t *out_ival,
                                      double *out_rval, const char **out_sval,
                                      size_t *out_slen);
svdb_code_t   svdb_vtab_cursor_rowid(svdb_vtab_cursor_t *cursor, int64_t *out_rowid);
svdb_code_t   svdb_vtab_cursor_close(svdb_vtab_cursor_t *cursor);

#ifdef __cplusplus
}
#endif
