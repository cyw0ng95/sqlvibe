#ifndef SVDB_QP_PARSER_DML_H
#define SVDB_QP_PARSER_DML_H

#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse an INSERT statement.
 * Returns SVDB_AST_INSERT node or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_insert(svdb_parser_t* parser,
                                            const char* sql, size_t sql_len);

/*
 * Parse an UPDATE statement.
 * Returns SVDB_AST_UPDATE node or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_update(svdb_parser_t* parser,
                                            const char* sql, size_t sql_len);

/*
 * Parse a DELETE statement.
 * Returns SVDB_AST_DELETE node or NULL on error.
 */
svdb_ast_node_t* svdb_parser_parse_delete(svdb_parser_t* parser,
                                            const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_DML_H */
