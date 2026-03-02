#ifndef SVDB_QP_PARSER_INTERNAL_H
#define SVDB_QP_PARSER_INTERNAL_H

/* Internal parser helpers — not part of the public C API. */
#ifdef __cplusplus

#include "parser.h"
#include <string>
#include <vector>

/* Forward declarations of AST node manipulation helpers (defined in parser.cpp). */
svdb_ast_node_t* svdb_ast_node_create(int node_type);
void svdb_ast_node_set_table(svdb_ast_node_t* node, const std::string& table);
void svdb_ast_node_set_sql(svdb_ast_node_t* node, const char* sql, size_t len);
void svdb_ast_node_add_column(svdb_ast_node_t* node, const std::string& col);
void svdb_ast_node_add_value_row(svdb_ast_node_t* node, const std::vector<std::string>& row);
void svdb_ast_node_set_where(svdb_ast_node_t* node, const std::string& where);

/* Lexer helpers (defined in parser.cpp). */
std::string parser_read_ident(const std::string& sql, size_t& pos);
std::string parser_read_keyword(const std::string& sql, size_t& pos);
bool        parser_expect_keyword(const std::string& sql, size_t& pos, const char* expected);
bool        parser_skip_to_keyword(const std::string& sql, size_t& pos, const char* kw);
std::vector<std::string> parser_read_ident_list(const std::string& sql, size_t& pos);
std::string parser_read_value(const std::string& sql, size_t& pos);
std::string parser_read_where(const std::string& sql, size_t pos);
size_t      parser_skip_ws(const std::string& sql, size_t pos);
char        parser_peek_char(const std::string& sql, size_t pos);

#endif /* __cplusplus */
#endif /* SVDB_QP_PARSER_INTERNAL_H */
