#ifndef SVDB_CG_DIRECT_COMPILER_H
#define SVDB_CG_DIRECT_COMPILER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Returns 1 if the query is a simple SELECT without subqueries, CTEs, or
 * window/join/set-operation keywords.
 */
int svdb_direct_is_simple_select(const char* sql, size_t sql_len);

/*
 * Extract the first table name from the FROM clause.
 * Returns the length of the name written to out_buf (NUL-terminated), or -1.
 */
int svdb_direct_extract_table_name(const char* sql,
                                    size_t      sql_len,
                                    char*       out_buf,
                                    int         out_buf_size);

/*
 * Extract the LIMIT value.  Returns -1 if no LIMIT clause is present.
 */
int64_t svdb_direct_extract_limit(const char* sql, size_t sql_len);

/*
 * Extract the OFFSET value.  Returns 0 if no OFFSET is present.
 */
int64_t svdb_direct_extract_offset(const char* sql, size_t sql_len);

/* Returns 1 if sql contains a WHERE clause */
int svdb_direct_has_where(const char* sql, size_t sql_len);

/* Returns 1 if sql contains ORDER BY */
int svdb_direct_has_order_by(const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_DIRECT_COMPILER_H */
