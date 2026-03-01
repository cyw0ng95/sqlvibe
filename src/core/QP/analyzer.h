#ifndef SVDB_QP_ANALYZER_H
#define SVDB_QP_ANALYZER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Collect all column names referenced in WHERE/ORDER BY/GROUP BY clauses
 * plus the column_names_csv list.  Returns the number of unique names written
 * as a comma-separated string into out_buf (NUL-terminated).
 * Returns -1 if out_buf is too small.
 */
int svdb_analyzer_required_columns(
    const char* column_names_csv,
    const char* where_sql,
    const char* orderby_sql,
    const char* groupby_sql,
    char*       out_buf,
    int         out_buf_size
);

/* Returns 1 if sql contains SUM(, COUNT(, AVG(, MIN(, or MAX( */
int svdb_analyzer_has_aggregates(const char* sql);

/* Returns 1 if sql contains a nested SELECT inside parentheses */
int svdb_analyzer_has_subquery(const char* sql);

/* Returns 1 if column_list_sql is exactly "*" or ends with ".*" */
int svdb_analyzer_is_star_select(const char* column_list_sql);

/* Returns the number of tables in the FROM clause (commas + 1, capped) */
int svdb_analyzer_count_join_tables(const char* from_sql);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_ANALYZER_H */
