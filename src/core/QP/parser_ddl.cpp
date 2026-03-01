#include "parser_ddl.h"

/* Stubs: DDL parsing is implemented incrementally. */

extern "C" svdb_ast_node_t* svdb_parser_parse_create(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement CREATE parsing */
    return nullptr;
}

extern "C" svdb_ast_node_t* svdb_parser_parse_drop(svdb_parser_t* parser,
                                                     const char* sql,
                                                     size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement DROP parsing */
    return nullptr;
}
