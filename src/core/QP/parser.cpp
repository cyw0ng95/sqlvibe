#include "parser.h"
#include "parser_select.h"
#include "parser_dml.h"
#include "parser_ddl.h"
#include "parser_expr.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>

/* ─── Internal helpers ──────────────────────────────────────────────── */

static inline bool is_ident_char(unsigned char c) {
    return isalnum(c) || c == '_' || c == '$';
}

/* Skip leading whitespace and SQL comments. */
static size_t skip_ws(const std::string& sql, size_t pos) {
    while (pos < sql.size()) {
        if (isspace((unsigned char)sql[pos])) {
            ++pos;
        } else if (pos + 1 < sql.size() && sql[pos] == '-' && sql[pos+1] == '-') {
            /* Line comment */
            while (pos < sql.size() && sql[pos] != '\n') ++pos;
        } else if (pos + 1 < sql.size() && sql[pos] == '/' && sql[pos+1] == '*') {
            /* Block comment */
            pos += 2;
            while (pos + 1 < sql.size() && !(sql[pos] == '*' && sql[pos+1] == '/')) ++pos;
            if (pos + 1 < sql.size()) pos += 2;
        } else {
            break;
        }
    }
    return pos;
}

/* Read an identifier (unquoted or double-quoted). Returns empty string on fail. */
static std::string read_ident(const std::string& sql, size_t& pos) {
    pos = skip_ws(sql, pos);
    if (pos >= sql.size()) return "";
    if (sql[pos] == '"' || sql[pos] == '`') {
        char q = sql[pos++];
        size_t start = pos;
        while (pos < sql.size() && sql[pos] != q) ++pos;
        std::string s = sql.substr(start, pos - start);
        if (pos < sql.size()) ++pos; /* consume closing quote */
        return s;
    }
    if (!is_ident_char((unsigned char)sql[pos])) return "";
    size_t start = pos;
    while (pos < sql.size() && is_ident_char((unsigned char)sql[pos])) ++pos;
    return sql.substr(start, pos - start);
}

/* Read the next keyword (uppercase). */
static std::string read_keyword(const std::string& sql, size_t& pos) {
    pos = skip_ws(sql, pos);
    size_t start = pos;
    while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
    std::string kw = sql.substr(start, pos - start);
    for (auto& c : kw) c = (char)toupper((unsigned char)c);
    return kw;
}

/* Check if next keyword matches expected (case-insensitive), consuming it. */
static bool expect_keyword(const std::string& sql, size_t& pos, const char* expected) {
    size_t tmp = pos;
    std::string kw = read_keyword(sql, tmp);
    if (kw == expected) { pos = tmp; return true; }
    return false;
}

/* Skip to the named keyword (consumes up to and including keyword). */
static bool skip_to_keyword(const std::string& sql, size_t& pos, const char* kw) {
    std::string ukw(kw);
    while (pos < sql.size()) {
        size_t tmp = pos;
        std::string found = read_keyword(sql, tmp);
        if (found == ukw) { pos = tmp; return true; }
        /* Skip one character and retry */
        if (pos < sql.size()) ++pos;
    }
    return false;
}

/* Peek at next non-whitespace character without consuming. */
static char peek_char(const std::string& sql, size_t pos) {
    pos = skip_ws(sql, pos);
    return pos < sql.size() ? sql[pos] : '\0';
}

/* Read a comma-separated list of identifiers until ')' or FROM/SET/WHERE/VALUES. */
static std::vector<std::string> read_ident_list(const std::string& sql, size_t& pos) {
    std::vector<std::string> result;
    while (pos < sql.size()) {
        pos = skip_ws(sql, pos);
        if (pos >= sql.size()) break;
        char c = sql[pos];
        if (c == ')' || c == ';') break;
        /* Peek at keyword to detect end-of-list sentinel without consuming it. */
        size_t tmp = pos;
        std::string kw = read_keyword(sql, tmp);
        if (kw == "FROM" || kw == "SET" || kw == "WHERE" || kw == "VALUES") break;
        /* The token at pos is an identifier (possibly keyword-like, e.g. "name").
         * Use read_ident so quoted identifiers are handled correctly. */
        std::string id = read_ident(sql, pos);
        if (id.empty()) { ++pos; continue; } /* skip unrecognised char */
        result.push_back(id);
        pos = skip_ws(sql, pos);
        if (pos < sql.size() && sql[pos] == ',') ++pos;
    }
    return result;
}

/* Read a value token: quoted string, number, or keyword NULL/TRUE/FALSE. */
static std::string read_value(const std::string& sql, size_t& pos) {
    pos = skip_ws(sql, pos);
    if (pos >= sql.size()) return "";
    char c = sql[pos];
    /* Quoted string — preserve quotes so parse_literal can recognize TEXT */
    if (c == '\'' || c == '"') {
        char q = c;
        size_t start = pos;
        ++pos;
        while (pos < sql.size()) {
            if (sql[pos] == q) {
                if (pos + 1 < sql.size() && sql[pos+1] == q) {
                    pos += 2; /* escaped '' or "" */
                } else {
                    ++pos; /* skip closing quote */
                    break;
                }
            } else if (sql[pos] == '\\' && pos + 1 < sql.size()) {
                pos += 2; /* skip escape sequence */
            } else {
                ++pos;
            }
        }
        /* Return the full quoted literal, e.g. "'hello'" */
        return sql.substr(start, pos - start);
    }
    /* Number (possibly negative), including scientific notation like 1.23e-10 */
    if (isdigit(c) || (c == '-' && pos + 1 < sql.size() && isdigit((unsigned char)sql[pos+1]))) {
        size_t start = pos;
        if (c == '-') ++pos;
        while (pos < sql.size() && (isdigit((unsigned char)sql[pos]) || sql[pos] == '.')) ++pos;
        /* Handle scientific notation: optional e/E followed by optional +/- and digits */
        if (pos < sql.size() && (sql[pos] == 'e' || sql[pos] == 'E')) {
            size_t e_pos = pos;
            ++pos; /* consume 'e'/'E' */
            if (pos < sql.size() && (sql[pos] == '+' || sql[pos] == '-')) ++pos;
            if (pos < sql.size() && isdigit((unsigned char)sql[pos])) {
                while (pos < sql.size() && isdigit((unsigned char)sql[pos])) ++pos;
            } else {
                pos = e_pos; /* not valid scientific notation, back up to before 'e' */
            }
        }
        return sql.substr(start, pos - start);
    }
    /* NULL/TRUE/FALSE */
    if (isalpha(c)) {
        size_t tmp = pos;
        std::string kw = read_keyword(sql, tmp);
        if (kw == "NULL" || kw == "TRUE" || kw == "FALSE") {
            pos = tmp;
            return kw;
        }
    }
    /* Expression: read until ',' or ')' or end, stopping at top-level SQL keywords */
    size_t start = pos;
    int depth = 0;
    while (pos < sql.size()) {
        if (sql[pos] == '\'') {
            /* Skip quoted string */
            char q = sql[pos++];
            while (pos < sql.size()) {
                if (sql[pos] == q) { ++pos; if (pos < sql.size() && sql[pos] == q) ++pos; else break; }
                else ++pos;
            }
        } else if (sql[pos] == '(') { ++depth; ++pos; }
        else if (sql[pos] == ')') {
            if (depth == 0) break;
            --depth; ++pos;
        } else if (sql[pos] == ',' && depth == 0) break;
        else if (depth == 0 && isalpha((unsigned char)sql[pos])) {
            /* Peek for SQL clause keywords that end the value expression */
            size_t tmp = pos;
            std::string kw = read_keyword(sql, tmp);
            if (kw == "WHERE" || kw == "ORDER" || kw == "GROUP" || kw == "LIMIT" ||
                kw == "HAVING" || kw == "RETURNING" || kw == "ON" || kw == "JOIN" ||
                kw == "UNION" || kw == "INTERSECT" || kw == "EXCEPT") {
                break; /* end of value expression */
            }
            pos = tmp; /* consumed keyword as part of expression (e.g. CASE, WHEN, END, IS, NOT, etc.) */
        }
        else ++pos;
    }
    std::string v = sql.substr(start, pos - start);
    /* Trim trailing whitespace */
    size_t end = v.size();
    while (end > 0 && isspace((unsigned char)v[end-1])) --end;
    return v.substr(0, end);
}

/* Read WHERE clause text from current position to end / semicolon. */
static std::string read_where_clause(const std::string& sql, size_t pos) {
    pos = skip_ws(sql, pos);
    /* Check if keyword is WHERE */
    size_t tmp = pos;
    std::string kw = read_keyword(sql, tmp);
    if (kw != "WHERE") return "";
    pos = tmp;
    /* Read until end, ORDER BY, GROUP BY, LIMIT, HAVING, semicolon */
    size_t start = skip_ws(sql, pos);
    size_t end = start;
    int depth = 0;
    while (end < sql.size()) {
        if (sql[end] == '(') { ++depth; ++end; continue; }
        if (sql[end] == ')') { if (depth > 0) --depth; else break; ++end; continue; }
        if (sql[end] == ';') break;
        if (depth == 0 && isalpha((unsigned char)sql[end])) {
            size_t t2 = end;
            std::string k2 = read_keyword(sql, t2);
            if (k2 == "ORDER" || k2 == "GROUP" || k2 == "LIMIT" || k2 == "HAVING" ||
                k2 == "UNION" || k2 == "INTERSECT" || k2 == "EXCEPT") {
                /* 'end' intentionally stops at the keyword start so the
                 * keyword is excluded from the WHERE clause text. */
                break;
            }
        }
        ++end;
    }
    std::string clause = sql.substr(start, end - start);
    /* Trim trailing whitespace */
    size_t e2 = clause.size();
    while (e2 > 0 && isspace((unsigned char)clause[e2-1])) --e2;
    return clause.substr(0, e2);
}

/* ─── AST node ──────────────────────────────────────────────────────── */

struct svdb_ast_node_t {
    int         node_type;
    std::string sql_text;   /* raw SQL */
    std::string table_name;
    std::string where_text;
    std::vector<std::string>              columns;
    std::vector<std::vector<std::string>> values; /* INSERT VALUES rows */

    explicit svdb_ast_node_t(int t = SVDB_AST_UNKNOWN) : node_type(t) {}
};

/* ─── Parser internal ───────────────────────────────────────────────── */

struct svdb_parser_t {
    std::string sql;
    std::string error_msg;

    svdb_parser_t(const char* s, size_t len) : sql(s ? std::string(s, len) : std::string()) {}
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

/* ─── C API ─────────────────────────────────────────────────────────── */

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

    /* Fall through to expression parsing when the SQL does NOT start with a
     * known SQL statement keyword (e.g. "1 + 2", "a IS NULL", "(x > 0)").
     * If it starts with an unrecognised keyword such as EXPLAIN or PRAGMA,
     * treat it as an unsupported statement rather than an expression. */
    if (kw.empty()) {
        svdb_ast_node_t* expr_node = svdb_parser_parse_expr(
            parser, parser->sql.data(), parser->sql.size());
        if (expr_node) return expr_node;
    }

    parser->error_msg = "unsupported statement: " + (kw.empty() ? "(unknown)" : kw);
    return nullptr;
}

const char* svdb_parser_error(const svdb_parser_t* parser) {
    return parser ? parser->error_msg.c_str() : "";
}

/* ─── AST attribute accessors ───────────────────────────────────────── */

const char* svdb_ast_get_table(const svdb_ast_node_t* node) {
    return node ? node->table_name.c_str() : "";
}

int svdb_ast_get_column_count(const svdb_ast_node_t* node) {
    return node ? (int)node->columns.size() : 0;
}

const char* svdb_ast_get_column(const svdb_ast_node_t* node, int i) {
    if (!node || i < 0 || i >= (int)node->columns.size()) return "";
    return node->columns[(size_t)i].c_str();
}

int svdb_ast_get_value_row_count(const svdb_ast_node_t* node) {
    return node ? (int)node->values.size() : 0;
}

int svdb_ast_get_value_count(const svdb_ast_node_t* node, int row_idx) {
    if (!node || row_idx < 0 || row_idx >= (int)node->values.size()) return 0;
    return (int)node->values[(size_t)row_idx].size();
}

const char* svdb_ast_get_value(const svdb_ast_node_t* node, int row_idx, int col_idx) {
    if (!node || row_idx < 0 || row_idx >= (int)node->values.size()) return "";
    const auto& row = node->values[(size_t)row_idx];
    if (col_idx < 0 || col_idx >= (int)row.size()) return "";
    return row[(size_t)col_idx].c_str();
}

const char* svdb_ast_get_where(const svdb_ast_node_t* node) {
    return node ? node->where_text.c_str() : "";
}

const char* svdb_ast_get_sql(const svdb_ast_node_t* node) {
    return node ? node->sql_text.c_str() : "";
}

} /* extern "C" */

/* ─── Helper accessible from sub-parsers ────────────────────────────── */

svdb_ast_node_t* svdb_ast_node_create(int node_type) {
    return new svdb_ast_node_t(node_type);
}

void svdb_ast_node_set_table(svdb_ast_node_t* node, const std::string& table) {
    if (node) node->table_name = table;
}

void svdb_ast_node_set_sql(svdb_ast_node_t* node, const char* sql, size_t len) {
    if (node) node->sql_text.assign(sql, len);
}

void svdb_ast_node_add_column(svdb_ast_node_t* node, const std::string& col) {
    if (node) node->columns.push_back(col);
}

void svdb_ast_node_add_value_row(svdb_ast_node_t* node, const std::vector<std::string>& row) {
    if (node) node->values.push_back(row);
}

void svdb_ast_node_set_where(svdb_ast_node_t* node, const std::string& where) {
    if (node) node->where_text = where;
}

/* Expose helpers to sub-parsers */
std::string parser_read_ident(const std::string& sql, size_t& pos) {
    return read_ident(sql, pos);
}
std::string parser_read_keyword(const std::string& sql, size_t& pos) {
    return read_keyword(sql, pos);
}
bool parser_expect_keyword(const std::string& sql, size_t& pos, const char* expected) {
    return expect_keyword(sql, pos, expected);
}
bool parser_skip_to_keyword(const std::string& sql, size_t& pos, const char* kw) {
    return skip_to_keyword(sql, pos, kw);
}
std::vector<std::string> parser_read_ident_list(const std::string& sql, size_t& pos) {
    return read_ident_list(sql, pos);
}
std::string parser_read_value(const std::string& sql, size_t& pos) {
    return read_value(sql, pos);
}
std::string parser_read_where(const std::string& sql, size_t pos) {
    return read_where_clause(sql, pos);
}
size_t parser_skip_ws(const std::string& sql, size_t pos) {
    return skip_ws(sql, pos);
}
char parser_peek_char(const std::string& sql, size_t pos) {
    return peek_char(sql, pos);
}
