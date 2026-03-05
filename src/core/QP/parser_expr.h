#ifndef SVDB_QP_PARSER_EXPR_H
#define SVDB_QP_PARSER_EXPR_H

#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse a SQL expression from sql[0..sql_len).
 * Returns an AST node of type SVDB_AST_EXPR, or NULL on error.
 *
 * Supported expression syntax:
 *   - Literals: integer, float, string (quoted), NULL, TRUE, FALSE
 *   - Column references: col, table.col
 *   - Arithmetic: +, -, *, /, %
 *   - Comparison: =, !=, <>, <, <=, >, >=
 *   - Logical: AND, OR, NOT
 *   - IS NULL, IS NOT NULL
 *   - LIKE, NOT LIKE
 *   - IN (...), NOT IN (...)
 *   - BETWEEN ... AND ...
 *   - Function calls: func(...)
 *   - CASE WHEN ... THEN ... END
 *   - Parenthesized sub-expressions
 */
svdb_ast_node_t* svdb_parser_parse_expr(svdb_parser_t* parser,
                                          const char* sql, size_t sql_len);

/*
 * Extract the first top-level expression from sql at *pos.
 * On return *pos points past the expression.
 * Returns a new AST node of type SVDB_AST_EXPR, or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_expr_at(svdb_parser_t* parser,
                                             const char* sql, size_t sql_len,
                                             size_t* pos);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_EXPR_H */
