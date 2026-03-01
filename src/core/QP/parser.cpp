#include "parser.h"
#include "parser_select.h"
#include "parser_dml.h"
#include "parser_ddl.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <cctype>

struct svdb_ast_node_t {
    int         node_type;
    std::string text;

    explicit svdb_ast_node_t(int t = SVDB_AST_UNKNOWN) : node_type(t) {}
};

struct svdb_parser_t {
    std::string sql;
    std::string error_msg;

    svdb_parser_t(const char* s, size_t len) : sql(s ? s : "", len) {}
};

/* Return the first non-whitespace word (uppercase) from sql. */
static std::string first_keyword(const std::string& sql) {
    size_t i = 0;
    while (i < sql.size() && isspace((unsigned char)sql[i])) ++i;
    size_t start = i;
    while (i < sql.size() && isalpha((unsigned char)sql[i])) ++i;
    std::string kw = sql.substr(start, i - start);
    for (auto& c : kw) c = (char)toupper((unsigned char)c);
    return kw;
}

extern "C" {

svdb_parser_t* svdb_parser_create(const char* sql, size_t sql_len) {
    return new svdb_parser_t(sql, sql_len);
}

void svdb_parser_destroy(svdb_parser_t* parser) {
    delete parser;
}

void svdb_ast_node_free(svdb_ast_node_t* node) {
    delete node;
}

int svdb_ast_node_type(const svdb_ast_node_t* node) {
    return node ? node->node_type : SVDB_AST_UNKNOWN;
}

svdb_ast_node_t* svdb_parser_parse(svdb_parser_t* parser) {
    if (!parser) return nullptr;
    parser->error_msg.clear();

    std::string kw = first_keyword(parser->sql);

    if (kw == "SELECT") {
        return svdb_parser_parse_select(parser, parser->sql.data(), parser->sql.size());
    }
    if (kw == "INSERT") {
        return svdb_parser_parse_insert(parser, parser->sql.data(), parser->sql.size());
    }
    if (kw == "UPDATE") {
        return svdb_parser_parse_update(parser, parser->sql.data(), parser->sql.size());
    }
    if (kw == "DELETE") {
        return svdb_parser_parse_delete(parser, parser->sql.data(), parser->sql.size());
    }
    if (kw == "CREATE") {
        return svdb_parser_parse_create(parser, parser->sql.data(), parser->sql.size());
    }
    if (kw == "DROP") {
        return svdb_parser_parse_drop(parser, parser->sql.data(), parser->sql.size());
    }

    parser->error_msg = "unsupported statement: " + kw;
    return nullptr;
}

const char* svdb_parser_error(const svdb_parser_t* parser) {
    return parser ? parser->error_msg.c_str() : "";
}

} /* extern "C" */
