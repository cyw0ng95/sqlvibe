/*
 * query.cpp — svdb_query: SELECT execution via in-memory scan
 *
 * For Phase 1/2 this implements a simple column-projection + WHERE-filter
 * over the db->data in-memory row store.  Complex queries (JOINs, GROUP BY,
 * window functions) fall back to the raw SQL parser result column names and
 * return empty rows so callers don't crash.
 *
 * NOTE: The arithmetic expression evaluator in eval_expr performs left-to-right
 * evaluation and does not implement full operator precedence (* / before + -).
 * This is a known limitation of the Phase 2 implementation.
 */
#include "svdb.h"
#include "svdb_types.h"
#include "svdb_util.h"
#include "QP/parser.h"

#include <cctype>
#include <algorithm>
#include <string>
#include <vector>
#include <cstring>
#include <cmath>

static std::string qry_upper(std::string s) {
    for (auto &c : s) c = (char)toupper((unsigned char)c);
    return s;
}

static std::string qry_trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char)s[a])) ++a;
    while (b > a && isspace((unsigned char)s[b-1])) --b;
    return s.substr(a, b-a);
}

/* Evaluate a simple expression (col reference or literal) against a row */
static SvdbVal eval_expr(const std::string &expr, const Row &row,
                          const std::vector<std::string> &col_order) {
    std::string e = qry_trim(expr);
    if (e.empty()) { SvdbVal v; return v; }

    /* NULL literal */
    if (qry_upper(e) == "NULL") { SvdbVal v; return v; }

    /* Numeric literal */
    if (isdigit((unsigned char)e[0]) || e[0] == '-' || e[0] == '+') {
        bool is_real = false;
        for (char c : e) if (c == '.' || c == 'e' || c == 'E') is_real = true;
        SvdbVal v;
        try {
            if (is_real) { v.type = SVDB_TYPE_REAL; v.rval = std::stod(e); }
            else          { v.type = SVDB_TYPE_INT;  v.ival = std::stoll(e); }
        } catch (...) {
            v.type = SVDB_TYPE_TEXT; v.sval = e;
        }
        return v;
    }

    /* String literal */
    if (e[0] == '\'') {
        SvdbVal v; v.type = SVDB_TYPE_TEXT;
        v.sval = e.size() >= 2 ? e.substr(1, e.size()-2) : "";
        /* unescape '' → ' */
        std::string &s2 = v.sval;
        std::string out; out.reserve(s2.size());
        for (size_t i = 0; i < s2.size(); ++i) {
            if (s2[i] == '\'' && i+1 < s2.size() && s2[i+1] == '\'') { out += '\''; ++i; }
            else out += s2[i];
        }
        v.sval = out;
        return v;
    }

    /* Simple arithmetic: 1+1, 2*3 etc. */
    for (size_t i = 0; i < e.size(); ++i) {
        char c = e[i];
        if ((c == '+' || c == '-' || c == '*' || c == '/') && i > 0) {
            std::string lhs_s = qry_trim(e.substr(0, i));
            std::string rhs_s = qry_trim(e.substr(i+1));
            SvdbVal lhs = eval_expr(lhs_s, row, col_order);
            SvdbVal rhs = eval_expr(rhs_s, row, col_order);
            SvdbVal v;
            if (lhs.type == SVDB_TYPE_REAL || rhs.type == SVDB_TYPE_REAL) {
                double l = (lhs.type == SVDB_TYPE_INT) ? (double)lhs.ival : lhs.rval;
                double r = (rhs.type == SVDB_TYPE_INT) ? (double)rhs.ival : rhs.rval;
                if (c == '/') {
                    /* SQLite: integer/0 = NULL; real/0 = NULL */
                    if (r == 0.0) return SvdbVal{};
                    v.type = SVDB_TYPE_REAL; v.rval = l / r;
                } else {
                    v.type = SVDB_TYPE_REAL;
                    if (c == '+') v.rval = l + r;
                    else if (c == '-') v.rval = l - r;
                    else if (c == '*') v.rval = l * r;
                }
            } else if (lhs.type == SVDB_TYPE_INT && rhs.type == SVDB_TYPE_INT) {
                if (c == '/') {
                    /* SQLite: integer/0 = NULL */
                    if (rhs.ival == 0) return SvdbVal{};
                    v.type = SVDB_TYPE_INT; v.ival = lhs.ival / rhs.ival;
                } else {
                    v.type = SVDB_TYPE_INT;
                    if (c == '+') v.ival = lhs.ival + rhs.ival;
                    else if (c == '-') v.ival = lhs.ival - rhs.ival;
                    else if (c == '*') v.ival = lhs.ival * rhs.ival;
                }
            }
            return v;
        }
    }

    /* Column reference (possibly table.col) */
    std::string col = e;
    /* strip table prefix */
    auto dot = col.find('.');
    if (dot != std::string::npos) col = col.substr(dot + 1);
    /* strip quotes */
    if (col.size() >= 2 && (col.front() == '"' || col.front() == '`'))
        col = col.substr(1, col.size() - 2);

    auto it = row.find(col);
    if (it != row.end()) return it->second;

    /* Not found — return NULL */
    return SvdbVal{};
}

/* ── WHERE evaluation ───────────────────────────────────────────── */

static bool qry_eval_where(const Row &row,
                             const std::vector<std::string> &col_order,
                             const std::string &where_text);

/* Forward declared; implementation mirrors exec.cpp eval_where but calls
 * eval_expr for values so arithmetic works */
static bool qry_eval_where(const Row &row,
                             const std::vector<std::string> &col_order,
                             const std::string &where_text) {
    if (where_text.empty()) return true;
    std::string wt = qry_trim(where_text);

    /* AND */
    std::string wu = qry_upper(wt);
    size_t and_pos = std::string::npos;
    {
        int depth = 0;
        for (size_t i = 0; i < wu.size(); ++i) {
            if (wu[i] == '(') { ++depth; continue; }
            if (wu[i] == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && i + 4 <= wu.size() && wu.substr(i, 4) == " AND") {
                and_pos = i; break;
            }
        }
    }
    if (and_pos != std::string::npos)
        return qry_eval_where(row, col_order, wt.substr(0, and_pos)) &&
               qry_eval_where(row, col_order, wt.substr(and_pos + 4));

    /* OR */
    size_t or_pos = std::string::npos;
    {
        int depth = 0;
        for (size_t i = 0; i < wu.size(); ++i) {
            if (wu[i] == '(') { ++depth; continue; }
            if (wu[i] == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && i + 3 <= wu.size() && wu.substr(i, 3) == " OR") {
                or_pos = i; break;
            }
        }
    }
    if (or_pos != std::string::npos)
        return qry_eval_where(row, col_order, wt.substr(0, or_pos)) ||
               qry_eval_where(row, col_order, wt.substr(or_pos + 3));

    /* Find operator */
    const char *ops[] = {"!=", "<>", "<=", ">=", "=", "<", ">", nullptr};
    size_t op_start = std::string::npos, op_len = 0;
    for (int i = 0; ops[i]; ++i) {
        size_t pos = wt.find(ops[i]);
        if (pos != std::string::npos && (op_start == std::string::npos || pos < op_start)) {
            op_start = pos; op_len = strlen(ops[i]);
        }
    }
    if (op_start == std::string::npos) return true;

    std::string lhs_s = qry_trim(wt.substr(0, op_start));
    std::string op    = wt.substr(op_start, op_len);
    std::string rhs_s = qry_trim(wt.substr(op_start + op_len));

    SvdbVal lhs = eval_expr(lhs_s, row, col_order);
    SvdbVal rhs = eval_expr(rhs_s, row, col_order);

    /* Both numeric */
    if ((lhs.type == SVDB_TYPE_INT || lhs.type == SVDB_TYPE_REAL) &&
        (rhs.type == SVDB_TYPE_INT || rhs.type == SVDB_TYPE_REAL)) {
        double l = (lhs.type == SVDB_TYPE_INT) ? (double)lhs.ival : lhs.rval;
        double r = (rhs.type == SVDB_TYPE_INT) ? (double)rhs.ival : rhs.rval;
        if (op == "=" || op == "==") return l == r;
        if (op == "!=" || op == "<>") return l != r;
        if (op == "<")  return l < r;
        if (op == ">")  return l > r;
        if (op == "<=") return l <= r;
        if (op == ">=") return l >= r;
    }

    /* Text comparison */
    auto to_str = [](const SvdbVal &v) -> std::string {
        if (v.type == SVDB_TYPE_TEXT || v.type == SVDB_TYPE_BLOB) return v.sval;
        if (v.type == SVDB_TYPE_INT) return std::to_string(v.ival);
        if (v.type == SVDB_TYPE_REAL) return std::to_string(v.rval);
        return "";
    };
    std::string l = to_str(lhs), r = to_str(rhs);
    if (op == "=" || op == "==") return l == r;
    if (op == "!=" || op == "<>") return l != r;
    if (op == "<")  return l < r;
    if (op == ">")  return l > r;
    if (op == "<=") return l <= r;
    if (op == ">=") return l >= r;
    return true;
}

/* ── SELECT result building ──────────────────────────────────────── */

svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                  svdb_rows_t **rows_out) {
    if (!rows_out) return SVDB_ERR;

    /* Use parser to extract table name and column list */
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);

    std::string tname;
    std::string where_txt;
    std::vector<std::string> sel_cols;
    bool star = false;

    if (ast) {
        tname     = svdb_ast_get_table(ast);
        where_txt = svdb_ast_get_where(ast);
        int nc    = svdb_ast_get_column_count(ast);
        for (int i = 0; i < nc; ++i) {
            std::string cn = svdb_ast_get_column(ast, i);
            if (cn == "*") star = true;
            else           sel_cols.push_back(cn);
        }
        if (nc == 0) star = true;
        svdb_ast_node_free(ast);
    } else {
        star = true;
    }
    svdb_parser_destroy(p);

    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;

    /* Handle SELECT without a table (e.g. SELECT 1+1, 'hello') */
    if (tname.empty() || !db->schema.count(tname)) {
        if (!sel_cols.empty()) {
            /* Evaluate expressions over an empty row */
            Row empty_row;
            std::vector<std::string> empty_order;
            std::vector<SvdbVal> result_row;
            for (const auto &expr : sel_cols) {
                r->col_names.push_back(expr);
                result_row.push_back(eval_expr(expr, empty_row, empty_order));
            }
            r->rows.push_back(result_row);
        } else {
            /* Unknown table or pure expression — return empty */
        }
        *rows_out = r;
        return SVDB_OK;
    }

    /* Determine output columns */
    const auto &col_order = db->col_order.at(tname);
    std::vector<std::string> out_cols = star ? col_order : sel_cols;

    r->col_names = out_cols;

    /* Scan rows */
    for (const auto &row : db->data.at(tname)) {
        if (!qry_eval_where(row, col_order, where_txt)) continue;
        std::vector<SvdbVal> result_row;
        for (const auto &cn : out_cols) {
            auto it = row.find(cn);
            if (it != row.end()) result_row.push_back(it->second);
            else                 result_row.push_back(eval_expr(cn, row, col_order));
        }
        r->rows.push_back(result_row);
    }

    *rows_out = r;
    return SVDB_OK;
}

extern "C" {

svdb_code_t svdb_query(svdb_db_t *db, const char *sql, svdb_rows_t **rows) {
    if (!db || !sql || !rows) return SVDB_ERR;
    std::lock_guard<std::mutex> lk(db->mu);
    db->last_error.clear();
    return svdb_query_internal(db, std::string(sql), rows);
}

} /* extern "C" */
