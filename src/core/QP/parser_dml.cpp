#include "parser_dml.h"

/* Stubs: DML parsing is implemented incrementally. */

extern "C" svdb_ast_node_t* svdb_parser_parse_insert(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement INSERT parsing */
    return nullptr;
}

extern "C" svdb_ast_node_t* svdb_parser_parse_update(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement UPDATE parsing */
    return nullptr;
}

extern "C" svdb_ast_node_t* svdb_parser_parse_delete(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    (void)parser; (void)sql; (void)sql_len;
    /* TODO: implement DELETE parsing */
    return nullptr;
}
