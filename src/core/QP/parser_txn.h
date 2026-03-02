#ifndef SVDB_QP_PARSER_TXN_H
#define SVDB_QP_PARSER_TXN_H

#include <stdint.h>
#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Parse transaction statement
 * Supported: BEGIN, BEGIN TRANSACTION
 *            COMMIT
 *            ROLLBACK
 * Returns AST node or NULL on error */
svdb_ast_node_t* svdb_parser_parse_txn(svdb_parser_t* parser,
                                       const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_TXN_H */
