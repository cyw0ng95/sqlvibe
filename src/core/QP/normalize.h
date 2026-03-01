#ifndef SVDB_QP_NORMALIZE_H
#define SVDB_QP_NORMALIZE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Normalize a SQL query for cache-key generation:
 *   - lowercase all characters
 *   - trim leading/trailing whitespace
 *   - replace string literals 'x' with ?
 *   - replace numeric literals 123 / 3.14 with ?
 *
 * Writes at most out_buf_size-1 characters plus a NUL terminator.
 * Returns the number of characters written (excluding NUL), or -1 if
 * out_buf is too small.
 */
int svdb_normalize_query(
    const char* sql,
    size_t      sql_len,
    char*       out_buf,
    size_t      out_buf_size
);

/*
 * Return the minimum out_buf size (including NUL) required by
 * svdb_normalize_query for the given input.
 */
size_t svdb_normalize_get_required_size(const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_NORMALIZE_H */
