#ifndef SVDB_QP_PARSER_ALTER_H
#define SVDB_QP_PARSER_ALTER_H

#include <stdint.h>
#include <stddef.h>
#include "parser.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Parse ALTER TABLE statement
 * Supported: ALTER TABLE table_name ADD column_name type
 *            ALTER TABLE table_name RENAME TO new_name
 *            ALTER TABLE table_name DROP COLUMN column_name
 * Returns AST node or NULL on error */
svdb_ast_node_t* svdb_parser_parse_alter(svdb_parser_t* parser,
                                          const char* sql, size_t sql_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_ALTER_H */
