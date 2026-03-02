#include "parser_expr.h"
#include "parser_internal.h"
#include <string>
#include <cctype>
#include <cstring>

/*
 * SQL expression parser.
 *
 * The AST node returned is of type SVDB_AST_EXPR.
 * The expression text is stored in the node's sql_text.
 * The where_text field stores the canonical (trimmed) expression.
 *
 * Sub-expressions are NOT built into a tree (full AST is beyond scope);
 * instead we extract the full expression text for downstream use.
 */

/* ─── Forward declarations ──────────────────────────────────────────── */
static std::string parse_expr_str(const std::string& sql, size_t& pos, int min_prec);
static std::string parse_primary(const std::string& sql, size_t& pos);

/* ─── Operator precedence ───────────────────────────────────────────── */
static int infix_precedence(const std::string& sql, size_t pos) {
    pos = parser_skip_ws(sql, pos);
    if (pos >= sql.size()) return -1;
    char c = sql[pos];
    /* Two-char operators */
    if (pos + 1 < sql.size()) {
        char c2 = sql[pos + 1];
        if ((c == '<' && c2 == '=') || (c == '>' && c2 == '=') ||
            (c == '!' && c2 == '=') || (c == '<' && c2 == '>')) return 4;
        if (c == '|' && c2 == '|') return 5; /* string concat */
    }
    if (c == '=' || c == '<' || c == '>') return 4;
    if (c == '+' || c == '-') return 6;
    if (c == '*' || c == '/' || c == '%') return 7;
    /* Keyword operators */
    size_t tmp = pos;
    std::string kw = parser_read_keyword(sql, tmp);
    if (kw == "AND") return 2;
    if (kw == "OR")  return 1;
    if (kw == "NOT") return 3;
    if (kw == "LIKE" || kw == "IN" || kw == "BETWEEN" ||
        kw == "IS")  return 4;
    return -1; /* not an infix operator */
}

/* Consume an infix operator token and return it as a string. */
static std::string consume_infix_op(const std::string& sql, size_t& pos) {
    pos = parser_skip_ws(sql, pos);
    if (pos >= sql.size()) return "";
    char c = sql[pos];
    /* Two-char operators */
    if (pos + 1 < sql.size()) {
        char c2 = sql[pos + 1];
        if ((c == '<' && c2 == '=') || (c == '>' && c2 == '=') ||
            (c == '!' && c2 == '=') || (c == '<' && c2 == '>') ||
            (c == '|' && c2 == '|')) {
            pos += 2;
            return std::string(1, c) + c2;
        }
    }
    if (c == '=' || c == '<' || c == '>' ||
        c == '+' || c == '-' || c == '*' || c == '/' || c == '%') {
        return std::string(1, sql[pos++]);
    }
    /* Keyword operators */
    size_t tmp = pos;
    std::string kw = parser_read_keyword(sql, tmp);
    if (kw == "AND" || kw == "OR" || kw == "LIKE" || kw == "IS" || kw == "IN") {
        pos = tmp;
        /* IS NOT / NOT LIKE / NOT IN / NOT BETWEEN */
        size_t t2 = pos;
        std::string kw2 = parser_read_keyword(sql, t2);
        if ((kw == "IS" && kw2 == "NOT") || (kw == "NOT")) {
            pos = t2;
            size_t t3 = pos;
            std::string kw3 = parser_read_keyword(sql, t3);
            if (kw3 == "LIKE" || kw3 == "IN" || kw3 == "BETWEEN" || kw3 == "NULL") {
                pos = t3;
                return kw + " NOT " + kw3;
            }
            return kw + " " + kw2;
        }
        return kw;
    }
    if (kw == "BETWEEN") {
        pos = tmp;
        return kw;
    }
    return "";
}

/* Parse a comma-separated expression list until ')'. Returns all as one string. */
static std::string parse_expr_list(const std::string& sql, size_t& pos) {
    std::string result;
    bool first = true;
    while (pos < sql.size()) {
        pos = parser_skip_ws(sql, pos);
        if (pos >= sql.size() || sql[pos] == ')') break;
        if (!first) result += ", ";
        first = false;
        std::string e = parse_expr_str(sql, pos, 0);
        if (e.empty()) break;
        result += e;
        pos = parser_skip_ws(sql, pos);
        if (pos < sql.size() && sql[pos] == ',') ++pos;
    }
    return result;
}

/* Parse a primary expression (atom). */
static std::string parse_primary(const std::string& sql, size_t& pos) {
    pos = parser_skip_ws(sql, pos);
    if (pos >= sql.size()) return "";
    char c = sql[pos];

    /* Parenthesized expression or IN list */
    if (c == '(') {
        ++pos;
        std::string inner = parse_expr_list(sql, pos);
        if (pos < sql.size() && sql[pos] == ')') ++pos;
        return "(" + inner + ")";
    }

    /* Unary minus */
    if (c == '-') {
        ++pos;
        std::string rhs = parse_primary(sql, pos);
        return "-" + rhs;
    }

    /* Unary NOT */
    if (c == '!') {
        ++pos;
        std::string rhs = parse_primary(sql, pos);
        return "!" + rhs;
    }

    /* Quoted string literal */
    if (c == '\'' || c == '"') {
        char q = c;
        size_t start = pos++;
        while (pos < sql.size()) {
            if (sql[pos] == q) {
                if (pos + 1 < sql.size() && sql[pos + 1] == q) { pos += 2; continue; }
                ++pos; break;
            } else if (sql[pos] == '\\' && pos + 1 < sql.size()) {
                pos += 2;
            } else {
                ++pos;
            }
        }
        return sql.substr(start, pos - start);
    }

    /* Numeric literal */
    if (isdigit(c) || c == '.') {
        size_t start = pos;
        while (pos < sql.size() && (isdigit((unsigned char)sql[pos]) ||
               sql[pos] == '.' || sql[pos] == 'e' || sql[pos] == 'E' ||
               ((sql[pos] == '+' || sql[pos] == '-') && pos > start &&
                (sql[pos-1] == 'e' || sql[pos-1] == 'E')))) ++pos;
        return sql.substr(start, pos - start);
    }

    /* Keyword-based literals and operators */
    if (isalpha(c) || c == '_') {
        size_t tmp = pos;
        std::string kw = parser_read_keyword(sql, tmp);

        /* NOT unary */
        if (kw == "NOT") {
            pos = tmp;
            std::string rhs = parse_primary(sql, pos);
            return "NOT " + rhs;
        }
        /* NULL / TRUE / FALSE literals */
        if (kw == "NULL" || kw == "TRUE" || kw == "FALSE") {
            pos = tmp;
            return kw;
        }
        /* CASE WHEN ... THEN ... [ELSE ...] END */
        if (kw == "CASE") {
            pos = tmp;
            std::string result = "CASE";
            while (pos < sql.size()) {
                pos = parser_skip_ws(sql, pos);
                size_t t2 = pos;
                std::string k2 = parser_read_keyword(sql, t2);
                if (k2 == "END") { pos = t2; result += " END"; break; }
                if (k2 == "WHEN" || k2 == "THEN" || k2 == "ELSE") {
                    pos = t2;
                    result += " " + k2 + " " + parse_expr_str(sql, pos, 0);
                } else {
                    /* Unknown token - skip to avoid infinite loop */
                    if (pos < sql.size()) ++pos;
                }
            }
            return result;
        }

        /* Identifier or function call */
        pos = tmp; /* Already consumed kw via read_keyword */
        std::string name = kw; /* function/column name */

        /* Check for qualified name: name.col */
        if (pos < sql.size() && sql[pos] == '.') {
            ++pos;
            std::string col = parser_read_ident(sql, pos);
            name = name + "." + col;
        }

        /* Function call */
        pos = parser_skip_ws(sql, pos);
        if (pos < sql.size() && sql[pos] == '(') {
            ++pos;
            std::string args = parse_expr_list(sql, pos);
            if (pos < sql.size() && sql[pos] == ')') ++pos;
            return name + "(" + args + ")";
        }

        return name;
    }

    /* Backtick/bracket quoted identifier */
    if (c == '`' || c == '[') {
        char close = (c == '[') ? ']' : '`';
        size_t start = pos++;
        while (pos < sql.size() && sql[pos] != close) ++pos;
        if (pos < sql.size()) ++pos;
        return sql.substr(start, pos - start);
    }

    /* Unknown character - consume and return */
    return std::string(1, sql[pos++]);
}

/* Pratt parser: parse an expression with precedence climbing. */
static std::string parse_expr_str(const std::string& sql, size_t& pos, int min_prec) {
    std::string left = parse_primary(sql, pos);
    if (left.empty()) return left;

    while (true) {
        size_t save = pos;
        pos = parser_skip_ws(sql, pos);
        int prec = infix_precedence(sql, pos);
        if (prec < min_prec) { pos = save; break; }
        std::string op = consume_infix_op(sql, pos);
        if (op.empty()) { pos = save; break; }

        /* BETWEEN needs special handling: BETWEEN a AND b */
        if (op == "BETWEEN") {
            std::string a = parse_expr_str(sql, pos, 5);
            size_t tmp = pos;
            std::string kw = parser_read_keyword(sql, tmp);
            if (kw == "AND") pos = tmp;
            std::string b = parse_expr_str(sql, pos, 5);
            left = left + " BETWEEN " + a + " AND " + b;
            continue;
        }
        /* IS [NOT] NULL — right side is NULL keyword */
        if (op == "IS" || op == "IS NOT") {
            pos = parser_skip_ws(sql, pos);
            size_t tmp = pos;
            std::string kw = parser_read_keyword(sql, tmp);
            if (kw == "NULL") {
                pos = tmp;
                left = left + " " + op + " NULL";
            } else {
                left = left + " " + op;
            }
            continue;
        }

        std::string right = parse_expr_str(sql, pos, prec + 1);
        left = left + " " + op + " " + right;
    }
    return left;
}

/* ─── C API ─────────────────────────────────────────────────────────── */

extern "C" {

svdb_ast_node_t* svdb_parser_parse_expr(svdb_parser_t* parser,
                                          const char* sql,
                                          size_t sql_len) {
    if (!parser || !sql || sql_len == 0) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = 0;
    std::string expr_text = parse_expr_str(s, pos, 0);

    if (expr_text.empty()) {
        (void)parser;
        return nullptr;
    }

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_EXPR);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_where(node, expr_text); /* store result in where_text */
    return node;
}

svdb_ast_node_t* svdb_parser_parse_expr_at(svdb_parser_t* parser,
                                             const char* sql,
                                             size_t sql_len,
                                             size_t* pos_ptr) {
    if (!parser || !sql || sql_len == 0 || !pos_ptr) return nullptr;
    if (*pos_ptr >= sql_len) return nullptr;

    std::string s(sql, sql_len);
    size_t pos = *pos_ptr;
    std::string expr_text = parse_expr_str(s, pos, 0);
    *pos_ptr = pos;

    if (expr_text.empty()) return nullptr;

    svdb_ast_node_t* node = svdb_ast_node_create(SVDB_AST_EXPR);
    svdb_ast_node_set_sql(node, sql, sql_len);
    svdb_ast_node_set_where(node, expr_text);
    return node;
}

} /* extern "C" */
