#ifndef SVDB_CG_BYTECODE_COMPILER_H
#define SVDB_CG_BYTECODE_COMPILER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Returns 1 if the SQL is a simple single-table SELECT with no advanced
 * clauses (WITH, WINDOW, JOIN, UNION, INTERSECT, EXCEPT, OVER).
 */
int svdb_bc_can_use_fast_path(const char* sql, size_t sql_len);

/*
 * Returns 1 if sql contains aggregate function keywords:
 * SUM(, COUNT(, AVG(, MIN(, MAX(, GROUP_CONCAT(
 */
int svdb_bc_has_aggregates(const char* sql, size_t sql_len);

/* Returns 1 if sql contains ORDER BY */
int svdb_bc_needs_sort(const char* sql, size_t sql_len);

/* Returns 1 if sql contains the LIMIT keyword */
int svdb_bc_has_limit(const char* sql, size_t sql_len);

/* Returns 1 if sql contains GROUP BY */
int svdb_bc_has_group_by(const char* sql, size_t sql_len);

/* Returns 1 if sql contains OVER ( (window function call) */
int svdb_bc_has_window_func(const char* sql, size_t sql_len);

/*
 * Estimate the number of VM registers needed:
 *   num_columns + (has_where ? 2 : 0) + (has_agg ? num_columns : 0) + 4
 */
int svdb_bc_estimate_reg_count(int num_columns, int has_where, int has_agg);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_BYTECODE_COMPILER_H */
