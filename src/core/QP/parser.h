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

/*
 * AST node attribute accessors.
 * All returned strings are valid until svdb_ast_node_free() is called.
 */

/* Return the table name extracted from the statement (or "" if none). */
const char* svdb_ast_get_table(const svdb_ast_node_t* node);

/* Return the number of columns extracted (SELECT column list / INSERT columns). */
int svdb_ast_get_column_count(const svdb_ast_node_t* node);

/* Return the column name at index i (0-based), or "" if out of range. */
const char* svdb_ast_get_column(const svdb_ast_node_t* node, int i);

/* Return the number of value rows in INSERT … VALUES (…), (…) */
int svdb_ast_get_value_row_count(const svdb_ast_node_t* node);

/* Return the number of values in value row row_idx. */
int svdb_ast_get_value_count(const svdb_ast_node_t* node, int row_idx);

/* Return the value string at (row_idx, col_idx), or "" if out of range. */
const char* svdb_ast_get_value(const svdb_ast_node_t* node, int row_idx, int col_idx);

/* Return the WHERE clause text (or "" if no WHERE clause). */
const char* svdb_ast_get_where(const svdb_ast_node_t* node);

/* Return the raw SQL text stored in the AST node. */
const char* svdb_ast_get_sql(const svdb_ast_node_t* node);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_PARSER_H */
