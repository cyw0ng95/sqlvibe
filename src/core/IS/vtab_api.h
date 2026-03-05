/* vtab_api.h — C API for Virtual Table Operations */
#pragma once
#ifndef SVDB_VTAB_API_H
#define SVDB_VTAB_API_H

#include <stdint.h>
#include <stddef.h>
#include "svdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handles ──────────────────────────────────────────── */
typedef struct svdb_vtab_module_s svdb_vtab_module_t;
typedef struct svdb_vtab_s       svdb_vtab_t;
typedef struct svdb_vtab_cursor_s svdb_vtab_cursor_t;

/* ── Value types (matches svdb_type_t) ───────────────────────── */
typedef enum {
    SVDB_VTAB_TYPE_NULL = 0,
    SVDB_VTAB_TYPE_INT  = 1,
    SVDB_VTAB_TYPE_REAL = 2,
    SVDB_VTAB_TYPE_TEXT = 3,
    SVDB_VTAB_TYPE_BLOB = 4,
} svdb_vtab_type_t;

/* ── Module registration ─────────────────────────────────────── */

/**
 * Register a virtual table module
 * @param name Module name (case-insensitive)
 * @param module Module instance
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_register_module(const char* name, svdb_vtab_module_t* module);

/**
 * Get a registered module by name
 * @param name Module name
 * @return Module instance, or NULL if not found
 */
svdb_vtab_module_t* svdb_vtab_get_module(const char* name);

/**
 * Check if a module is registered
 * @param name Module name
 * @return 1 if registered, 0 otherwise
 */
int svdb_vtab_has_module(const char* name);

/**
 * Get count of registered modules
 * @return Number of registered modules
 */
int svdb_vtab_get_module_count(void);

/**
 * Get module name by index
 * @param index Module index (0-based)
 * @param buffer Output buffer
 * @param buffer_size Buffer size
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_get_module_name(int index, char* buffer, size_t buffer_size);

/* ── Virtual table operations ────────────────────────────────── */

/**
 * Create a permanent virtual table
 * @param module Module instance
 * @param args Module arguments
 * @param arg_count Number of arguments
 * @return New virtual table, or NULL on error
 */
svdb_vtab_t* svdb_vtab_create(svdb_vtab_module_t* module, 
                              const char** args, int arg_count);

/**
 * Connect to a transient virtual table (table function)
 * @param module Module instance
 * @param args Module arguments
 * @param arg_count Number of arguments
 * @return New virtual table, or NULL on error
 */
svdb_vtab_t* svdb_vtab_connect(svdb_vtab_module_t* module,
                               const char** args, int arg_count);

/**
 * Get column count for a virtual table
 * @param vtab Virtual table
 * @return Number of columns
 */
int svdb_vtab_column_count(svdb_vtab_t* vtab);

/**
 * Get column name by index
 * @param vtab Virtual table
 * @param col Column index (0-based)
 * @return Column name
 */
const char* svdb_vtab_column_name(svdb_vtab_t* vtab, int col);

/**
 * Open a cursor on a virtual table
 * @param vtab Virtual table
 * @return New cursor, or NULL on error
 */
svdb_vtab_cursor_t* svdb_vtab_cursor_open(svdb_vtab_t* vtab);

/**
 * Close and destroy a virtual table
 * @param vtab Virtual table
 * @param destroy If 1, call Destroy(); if 0, call Disconnect()
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_close(svdb_vtab_t* vtab, int destroy);

/* ── Cursor operations ───────────────────────────────────────── */

/**
 * Filter cursor based on constraints
 * @param cursor Cursor instance
 * @param idx_num Index number (0 = no index)
 * @param idx_str Index constraint string
 * @param args Filter arguments
 * @param arg_count Number of arguments
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_cursor_filter(svdb_vtab_cursor_t* cursor, int idx_num,
                            const char* idx_str,
                            const char** args, int arg_count);

/**
 * Advance cursor to next row
 * @param cursor Cursor instance
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_cursor_next(svdb_vtab_cursor_t* cursor);

/**
 * Check if cursor is at end
 * @param cursor Cursor instance
 * @return 1 if at end, 0 otherwise
 */
int svdb_vtab_cursor_eof(svdb_vtab_cursor_t* cursor);

/**
 * Get column value from current row
 * @param cursor Cursor instance
 * @param col Column index (0-based)
 * @param out_type Output: value type
 * @param out_ival Output: integer value
 * @param out_rval Output: real value
 * @param out_sval Output: string/blob pointer
 * @param out_slen Output: string/blob length
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_cursor_column(svdb_vtab_cursor_t* cursor, int col,
                            int* out_type, int64_t* out_ival,
                            double* out_rval, const char** out_sval,
                            size_t* out_slen);

/**
 * Get current row ID
 * @param cursor Cursor instance
 * @param out_rowid Output: row ID
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_cursor_rowid(svdb_vtab_cursor_t* cursor, int64_t* out_rowid);

/**
 * Close cursor and free resources
 * @param cursor Cursor instance
 * @return 0 on success, non-zero on error
 */
svdb_code_t svdb_vtab_cursor_close(svdb_vtab_cursor_t* cursor);

/* ── Module interface (for C implementations) ────────────────── */

/**
 * Module callbacks
 */
typedef struct {
    /* Create/connect callbacks */
    svdb_vtab_t* (*create)(const char** args, int arg_count);
    svdb_vtab_t* (*connect)(const char** args, int arg_count);
    
    /* VTab callbacks */
    int  (*column_count)(svdb_vtab_t* vtab);
    const char* (*column_name)(svdb_vtab_t* vtab, int col);
    svdb_vtab_cursor_t* (*cursor_open)(svdb_vtab_t* vtab);
    int  (*cursor_close)(svdb_vtab_cursor_t* cursor);
    int  (*cursor_filter)(svdb_vtab_cursor_t* cursor, int idx_num,
                          const char* idx_str,
                          const char** args, int arg_count);
    int  (*cursor_next)(svdb_vtab_cursor_t* cursor);
    int  (*cursor_eof)(svdb_vtab_cursor_t* cursor);
    int  (*cursor_column)(svdb_vtab_cursor_t* cursor, int col,
                          int* out_type, int64_t* out_ival,
                          double* out_rval, const char** out_sval,
                          size_t* out_slen);
    int  (*cursor_rowid)(svdb_vtab_cursor_t* cursor, int64_t* out_rowid);
} svdb_vtab_callbacks_t;

/**
 * Create a C-based virtual table module
 * @param callbacks Callback functions
 * @param user_data User data pointer
 * @return New module instance
 */
svdb_vtab_module_t* svdb_vtab_module_create_c(
    const svdb_vtab_callbacks_t* callbacks, void* user_data);

/**
 * Set user data on a module
 * @param module Module instance
 * @param user_data User data pointer
 */
void svdb_vtab_module_set_user_data(svdb_vtab_module_t* module, 
                                    void* user_data);

/**
 * Get user data from a module
 * @param module Module instance
 * @return User data pointer
 */
void* svdb_vtab_module_get_user_data(svdb_vtab_module_t* module);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VTAB_API_H */
