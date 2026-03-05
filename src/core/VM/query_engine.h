#ifndef SVDB_VM_QUERY_ENGINE_H
#define SVDB_VM_QUERY_ENGINE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Classify a SQL statement.
 * Returns: 0=unknown, 1=SELECT, 2=INSERT, 3=UPDATE, 4=DELETE,
 *          5=CREATE, 6=DROP, 7=ALTER, 8=BEGIN, 9=COMMIT,
 *         10=ROLLBACK, 11=PRAGMA
 */
int svdb_qe_classify_query(const char* sql, size_t sql_len);

/*
 * Extract the main table name from any DML/DDL statement.
 * Looks for FROM/INTO/TABLE/UPDATE keywords.
 * Returns name length, or -1 if not found/buffer too small.
 */
int svdb_qe_extract_table_name(const char* sql, size_t sql_len,
                                char* out_buf, int out_buf_size);

/* Returns 1 if query is SELECT or PRAGMA (read-only) */
int svdb_qe_is_read_only(const char* sql, size_t sql_len);

/* Returns 1 if query is BEGIN/COMMIT/ROLLBACK/SAVEPOINT */
int svdb_qe_is_transaction(const char* sql, size_t sql_len);

/* Returns 1 if query needs schema lookup */
int svdb_qe_needs_schema(const char* sql, size_t sql_len);

/*
 * Remove -- line comments and slash-star block comments from sql.
 * Returns cleaned SQL length, or -1 if buffer too small.
 */
int svdb_qe_strip_comments(const char* sql, size_t sql_len,
                             char* out_buf, int out_buf_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_QUERY_ENGINE_H */
