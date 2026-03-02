#include "parser_dml.h"
#include "parser_internal.h"
#include <string>
#include <vector>
#include <cctype>

/*
 * INSERT INTO <table> [(<col-list>)] VALUES (<val-list>)[, (<val-list>)...]
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_insert(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    if (!parser_expect_keyword(s, pos, "INSERT")) return nullptr;
    parser_expect_keyword(s, pos, "INTO"); /* optional in some dialects */

    std::string table = parser_read_ident(s, pos);
    if (table.empty()) return nullptr;

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_INSERT);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_table(node, table);

    /* Optional column list: (<col>, ...) */
    pos = parser_skip_ws(s, pos);
    if (pos < s.size() && s[pos] == '(') {
        ++pos; /* consume '(' */
        while (pos < s.size() && s[pos] != ')') {
            std::string col = parser_read_ident(s, pos);
            if (!col.empty()) svdb_ast_node_add_column(node, col);
            pos = parser_skip_ws(s, pos);
            if (pos < s.size() && s[pos] == ',') ++pos;
        }
        if (pos < s.size()) ++pos; /* consume ')' */
    }

    /* VALUES keyword */
    if (!parser_expect_keyword(s, pos, "VALUES")) return node;

    /* Read one or more value rows: (v1, v2, ...), (...) */
    while (pos < s.size()) {
        pos = parser_skip_ws(s, pos);
        if (pos >= s.size() || s[pos] != '(') break;
        ++pos; /* consume '(' */

        std::vector<std::string> row;
        while (pos < s.size() && s[pos] != ')') {
            std::string val = parser_read_value(s, pos);
            row.push_back(val);
            pos = parser_skip_ws(s, pos);
            if (pos < s.size() && s[pos] == ',') ++pos;
        }
        if (pos < s.size()) ++pos; /* consume ')' */
        svdb_ast_node_add_value_row(node, row);

        pos = parser_skip_ws(s, pos);
        if (pos < s.size() && s[pos] == ',') { ++pos; continue; }
        break;
    }

    return node;
}

/*
 * UPDATE <table> SET <col>=<val>[, ...] [WHERE <expr>]
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_update(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    if (!parser_expect_keyword(s, pos, "UPDATE")) return nullptr;

    std::string table = parser_read_ident(s, pos);
    if (table.empty()) return nullptr;

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_UPDATE);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_table(node, table);

    if (!parser_expect_keyword(s, pos, "SET")) return node;

    /* Read SET assignments: col=val, col=val, ...
     * All column names are collected in node->columns and all corresponding
     * values are stored as a single parallel row in node->values[0],
     * so columns[i] = values[0][i] for each assignment. */
    std::vector<std::string> val_row;
    while (pos < s.size()) {
        pos = parser_skip_ws(s, pos);
        /* Peek for WHERE keyword to stop (don't consume it) */
        size_t tmp = pos;
        std::string kw = parser_read_keyword(s, tmp);
        if (kw == "WHERE" || kw.empty()) break;

        /* Read column identifier */
        std::string col = parser_read_ident(s, pos);
        if (col.empty()) break;

        pos = parser_skip_ws(s, pos);
        if (pos < s.size() && s[pos] == '=') ++pos;

        std::string val = parser_read_value(s, pos);
        svdb_ast_node_add_column(node, col);
        val_row.push_back(val);

        pos = parser_skip_ws(s, pos);
        if (pos < s.size() && s[pos] == ',') { ++pos; continue; }
        break;
    }
    /* Store all values as a single row so columns[i] ↔ values[0][i]. */
    if (!val_row.empty()) svdb_ast_node_add_value_row(node, val_row);

    /* Optional WHERE clause */
    std::string where_text = parser_read_where(s, pos);
    svdb_ast_node_set_where(node, where_text);

    return node;
}

/*
 * DELETE FROM <table> [WHERE <expr>]
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_delete(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    if (!parser_expect_keyword(s, pos, "DELETE")) return nullptr;
    parser_expect_keyword(s, pos, "FROM");

    std::string table = parser_read_ident(s, pos);
    if (table.empty()) return nullptr;

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_DELETE);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_table(node, table);

    std::string where_text = parser_read_where(s, pos);
    svdb_ast_node_set_where(node, where_text);

    return node;
}
