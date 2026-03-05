#ifndef SVDB_QP_PARSER_SELECT_H
#define SVDB_QP_PARSER_SELECT_H

#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse a SELECT statement.
 * Returns an AST node of type SVDB_AST_SELECT, or NULL on error.
 * (Stub implementation — full parsing is incremental.)
 */
svdb_ast_node_t* svdb_parser_parse_select(svdb_parser_t* parser,
                                            const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_SELECT_H */
