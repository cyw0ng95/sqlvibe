#include "parser_ddl.h"
#include "parser_internal.h"
#include <string>
#include <vector>
#include <cctype>

/*
 * CREATE TABLE <table> (<col-def>, ...)
 * CREATE INDEX <name> ON <table> (<cols>)
 * CREATE [UNIQUE] INDEX ...
 * CREATE VIEW <name> AS <select>
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_create(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    if (!parser_expect_keyword(s, pos, "CREATE")) return nullptr;

    /* Optional UNIQUE */
    {
        size_t tmp = pos;
        std::string kw = parser_read_keyword(s, tmp);
        if (kw == "UNIQUE") pos = tmp;
    }

    /* TABLE, INDEX, VIEW, VIRTUAL TABLE, etc. */
    size_t tmp = pos;
    std::string obj_type = parser_read_keyword(s, tmp);
    pos = tmp;

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_CREATE);
    svdb_ast_node_set_sql(node, sql, sql_len);

    if (obj_type == "TABLE") {
        /* Optional IF NOT EXISTS */
        size_t t2 = pos;
        std::string kw2 = parser_read_keyword(s, t2);
        if (kw2 == "IF") {
            parser_expect_keyword(s, t2, "NOT");
            parser_expect_keyword(s, t2, "EXISTS");
            pos = t2;
        }
        std::string table = parser_read_ident(s, pos);
        svdb_ast_node_set_table(node, table);

        /* Column definitions: (col type, col type, ...) */
        pos = parser_skip_ws(s, pos);
        if (pos < s.size() && s[pos] == '(') {
            ++pos;
            while (pos < s.size() && s[pos] != ')') {
                std::string col = parser_read_ident(s, pos);
                if (!col.empty()) svdb_ast_node_add_column(node, col);
                /* Skip the type and any constraints until next ',' or ')' */
                int depth = 0;
                while (pos < s.size()) {
                    if (s[pos] == '(') { ++depth; ++pos; }
                    else if (s[pos] == ')') {
                        if (depth == 0) break;
                        --depth; ++pos;
                    } else if (s[pos] == ',' && depth == 0) { ++pos; break; }
                    else ++pos;
                }
            }
        }
    } else if (obj_type == "INDEX") {
        /* CREATE INDEX <name> ON <table> (<cols>) */
        std::string idx_name = parser_read_ident(s, pos);
        svdb_ast_node_add_column(node, idx_name); /* store index name as first col */
        parser_expect_keyword(s, pos, "ON");
        std::string table = parser_read_ident(s, pos);
        svdb_ast_node_set_table(node, table);
        /* Index columns */
        pos = parser_skip_ws(s, pos);
        if (pos < s.size() && s[pos] == '(') {
            ++pos;
            while (pos < s.size() && s[pos] != ')') {
                std::string col = parser_read_ident(s, pos);
                if (!col.empty()) svdb_ast_node_add_column(node, col);
                pos = parser_skip_ws(s, pos);
                if (pos < s.size() && s[pos] == ',') ++pos;
            }
        }
    } else if (obj_type == "VIEW") {
        std::string view_name = parser_read_ident(s, pos);
        svdb_ast_node_set_table(node, view_name);
    } else {
        /* Unknown CREATE type: store object type as "column" for introspection */
        svdb_ast_node_add_column(node, obj_type);
    }

    return node;
}

/*
 * DROP TABLE [IF EXISTS] <table>
 * DROP INDEX [IF EXISTS] <index>
 * DROP VIEW  [IF EXISTS] <view>
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_drop(svdb_parser_t* parser,
                                                     const char* sql,
                                                     size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    if (!parser_expect_keyword(s, pos, "DROP")) return nullptr;

    /* TABLE / INDEX / VIEW */
    std::string obj_type = parser_read_keyword(s, pos);

    /* Optional IF EXISTS */
    {
        size_t tmp = pos;
        std::string kw = parser_read_keyword(s, tmp);
        if (kw == "IF") {
            parser_expect_keyword(s, tmp, "EXISTS");
            pos = tmp;
        }
    }

    std::string name = parser_read_ident(s, pos);

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_DROP);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_table(node, name);
    svdb_ast_node_add_column(node, obj_type); /* store what was dropped */

    return node;
}
