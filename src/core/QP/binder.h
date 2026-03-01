#ifndef SVDB_QP_BINDER_H
#define SVDB_QP_BINDER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Count all ? and :name/@name placeholders */
int svdb_binder_count_placeholders(const char* sql, size_t sql_len);

/* Count positional ? placeholders only */
int svdb_binder_count_positional(const char* sql, size_t sql_len);

/* Count :name and @name named placeholders */
int svdb_binder_count_named(const char* sql, size_t sql_len);

/*
 * Return the idx-th named parameter name (without : or @) into out_buf.
 * Returns name length, or -1 if idx is out of range or buffer too small.
 */
int svdb_binder_get_named_param(const char* sql,
                                 size_t      sql_len,
                                 int         idx,
                                 char*       out_buf,
                                 int         out_buf_size);

/*
 * Replace each ? in sql with the corresponding value from a JSON array
 * ["val1","val2",...].  The replacement values are inserted as SQL literals
 * (string values are single-quoted; numbers are unquoted).
 * Returns output length, or -1 if buffer too small.
 */
int svdb_binder_substitute_positional(const char* sql,
                                       size_t      sql_len,
                                       const char* values_json,
                                       char*       out_buf,
                                       int         out_buf_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_BINDER_H */
