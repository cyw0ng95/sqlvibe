#ifndef SVDB_QP_PARSER_DDL_H
#define SVDB_QP_PARSER_DDL_H

#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse a CREATE statement (TABLE, INDEX, VIEW, etc.).
 * Returns SVDB_AST_CREATE node or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_create(svdb_parser_t* parser,
                                            const char* sql, size_t sql_len);

/*
 * Parse a DROP statement.
 * Returns SVDB_AST_DROP node or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_drop(svdb_parser_t* parser,
                                          const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_DDL_H */
