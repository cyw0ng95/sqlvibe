#ifndef SVDB_QP_PARSER_H
#define SVDB_QP_PARSER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque parser handle. */
typedef struct svdb_parser_t svdb_parser_t;

/* Opaque AST node handle. */
typedef struct svdb_ast_node_t svdb_ast_node_t;

/* AST node type constants. */
#define SVDB_AST_UNKNOWN    0
#define SVDB_AST_SELECT     1
#define SVDB_AST_INSERT     2
#define SVDB_AST_UPDATE     3
#define SVDB_AST_DELETE     4
#define SVDB_AST_CREATE     5
#define SVDB_AST_DROP       6
#define SVDB_AST_EXPR       7

/* Create a parser for the given SQL string (sql_len bytes). */
svdb_parser_t* svdb_parser_create(const char* sql, size_t sql_len);

/* Destroy a parser instance and free resources. */
void svdb_parser_destroy(svdb_parser_t* parser);

/* Free an AST node tree returned by any parse function. */
void svdb_ast_node_free(svdb_ast_node_t* node);

/* Return the AST node type for node, or SVDB_AST_UNKNOWN if node is NULL. */
int svdb_ast_node_type(const svdb_ast_node_t* node);

/*
 * Parse the SQL held by the parser and return the root AST node.
 * Dispatches to the appropriate sub-parser based on the first keyword.
 * Returns NULL on parse error.
 */
svdb_ast_node_t* svdb_parser_parse(svdb_parser_t* parser);

/* Return the last error message (empty string if none). */
const char* svdb_parser_error(const svdb_parser_t* parser);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_H */
