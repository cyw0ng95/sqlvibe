#include "parser_select.h"
#include "parser_internal.h"
#include "../SF/svdb_assert.h"
#include <string>
#include <vector>
#include <cctype>

/*
 * SELECT [DISTINCT] <col-list> FROM <table>
 *        [WHERE <expr>]
 *        [GROUP BY <cols>]
 *        [HAVING <expr>]
 *        [ORDER BY <cols>]
 *        [LIMIT <n> [OFFSET <m>]]
 *
 * This parser extracts the table name, column list (or '*'), and WHERE text.
 */
extern "C" svdb_ast_node_t* svdb_parser_parse_select(svdb_parser_t* parser,
                                                       const char* sql,
                                                       size_t sql_len) {
    BUG_ON(parser == nullptr);
    BUG_ON(sql == nullptr);
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;

    /* Consume SELECT */
    if (!parser_expect_keyword(s, pos, "SELECT")) return nullptr;

    /* Optional DISTINCT / ALL */
    {
        size_t tmp = pos;
        std::string kw = parser_read_keyword(s, tmp);
        if (kw == "DISTINCT" || kw == "ALL") pos = tmp;
    }

    /* Read column list until FROM */
    std::vector<std::string> cols;
    pos = parser_skip_ws(s, pos);
    if (pos < s.size() && s[pos] == '*') {
        cols.push_back("*");
        ++pos;
    } else {
        /* Collect comma-separated column expressions until FROM */
        while (pos < s.size()) {
            pos = parser_skip_ws(s, pos);
            if (pos >= s.size()) break;
            /* Check for FROM keyword */
            {
                size_t tmp = pos;
                std::string kw = parser_read_keyword(s, tmp);
                if (kw == "FROM") break;
            }
            /* Read column expression: may be "table.col", "expr AS alias", function call.
             * Do NOT advance pos here — process the current character as column content. */
            size_t start = pos;
            int depth = 0;
            while (pos < s.size()) {
                char c = s[pos];
                if (c == '(') { ++depth; ++pos; }
                else if (c == ')') {
                    if (depth == 0) break;
                    --depth; ++pos;
                } else if (c == ',' && depth == 0) break;
                else if (depth == 0 && isalpha((unsigned char)c)) {
                    size_t t2 = pos;
                    std::string k2 = parser_read_keyword(s, t2);
                    if (k2 == "FROM") break; /* stop before FROM, leave pos at 'F' */
                    pos = t2; /* advance past the keyword (it's part of the expression) */
                } else {
                    ++pos;
                }
            }
            std::string expr = s.substr(start, pos - start);
            /* Trim */
            size_t e = expr.size();
            while (e > 0 && isspace((unsigned char)expr[e-1])) --e;
            size_t b = 0;
            while (b < e && isspace((unsigned char)expr[b])) ++b;
            if (b < e) cols.push_back(expr.substr(b, e - b));
            if (pos < s.size() && s[pos] == ',') ++pos;
        }
    }

    /* FROM is optional — SELECT without FROM produces a single-row result */
    std::string table;
    std::string where_text;
    if (parser_expect_keyword(s, pos, "FROM")) {
        /* Read table name */
        table = parser_read_ident(s, pos);
        if (table.empty()) return nullptr;
        /* Read optional WHERE clause */
        where_text = parser_read_where(s, pos);
    }

    /* Build AST node */
    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_SELECT);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_table(node, table);
    for (const auto& c : cols) svdb_ast_node_add_column(node, c);
    svdb_ast_node_set_where(node, where_text);
    return node;
}
