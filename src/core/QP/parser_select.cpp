#include "parser_select.h"
#include "parser_expr.h"

/* Forward declaration of internal node constructor (defined in parser.cpp). */
struct svdb_ast_node_t;

/* Stub: allocate a SELECT node; full parsing is implemented incrementally. */
extern "C" svdb_ast_node_t* svdb_parser_parse_select(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement full SELECT parsing */
    return nullptr;
}
