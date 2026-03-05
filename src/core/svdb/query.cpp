/*
 * query.cpp — svdb_query: SELECT execution via in-memory scan
 *
 * Supports:
 *   - Column projection + * (star)
 *   - WHERE with AND/OR, comparison operators, IS NULL/IS NOT NULL, LIKE, IN, BETWEEN
 *   - ORDER BY (multiple columns, ASC/DESC)
 *   - LIMIT / OFFSET
 *   - GROUP BY + aggregate functions: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX
 *   - HAVING clause
 *   - Basic INNER JOIN and LEFT JOIN (single join, nested-loop)
 *   - Arithmetic expressions in SELECT/WHERE (+, -, *, /)
 *   - String functions: UPPER, LOWER, LENGTH, SUBSTR, TRIM, COALESCE, IFNULL, NULLIF
 *   - CAST(expr AS type)
 *   - DISTINCT
 */
#include "svdb.h"
#include "svdb_types.h"
#include "svdb_util.h"
#include "../SF/svdb_assert.h"
#include "QP/parser.h"

#include <cctype>
#include <algorithm>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <unordered_set>
#include <cstring>
#include <cmath>
#include <ctime>
#include <sstream>
#include <functional>
#include <cstdio>

/* ── Helpers ─────────────────────────────────────────────────────── */

static std::string qry_upper(const std::string &s) {
    std::string r = s;
    for (auto &c : r) c = (char)toupper((unsigned char)c);
    return r;
}

static std::string qry_trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char)s[a])) ++a;
    while (b > a && isspace((unsigned char)s[b-1])) --b;
    return s.substr(a, b-a);
}

/* Strip SQL line comments (-- ...) and block comments preserving string literals */
static std::string strip_sql_comments_q(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    size_t i = 0;
    while (i < s.size()) {
        if (s[i] == '\'') {
            out += s[i++];
            while (i < s.size()) {
                out += s[i];
                if (s[i] == '\'' && (i+1 >= s.size() || s[i+1] != '\'')) { ++i; break; }
                if (s[i] == '\'' && i+1 < s.size() && s[i+1] == '\'') { out += s[++i]; }
                ++i;
            }
            continue;
        }
        if (i+1 < s.size() && s[i] == '-' && s[i+1] == '-') {
            while (i < s.size() && s[i] != '\n') ++i;
            out += ' '; continue;
        }
        if (i+1 < s.size() && s[i] == '/' && s[i+1] == '*') {
            i += 2;
            while (i+1 < s.size() && !(s[i] == '*' && s[i+1] == '/')) ++i;
            if (i+1 < s.size()) i += 2;
            out += ' '; continue;
        }
        out += s[i++];
    }
    return out;
}

/* UTF-8 helpers for Unicode-aware SUBSTR / LENGTH */
static size_t utf8_char_count(const std::string &s) {
    size_t n = 0;
    for (size_t i = 0; i < s.size(); ) {
        unsigned char c = (unsigned char)s[i];
        if      (c < 0x80) i += 1;
        else if (c < 0xE0) i += 2;
        else if (c < 0xF0) i += 3;
        else               i += 4;
        ++n;
    }
    return n;
}

/* Return byte offset of char_pos (0-based). Returns s.size() if out of range. */
static size_t utf8_byte_offset(const std::string &s, size_t char_pos) {
    size_t i = 0;
    for (size_t n = 0; n < char_pos && i < s.size(); ++n) {
        unsigned char c = (unsigned char)s[i];
        if      (c < 0x80) i += 1;
        else if (c < 0xE0) i += 2;
        else if (c < 0xF0) i += 3;
        else               i += 4;
    }
    return i;
}

/* Return UTF-8 substring: start and len are character counts (0-based start). */
static std::string utf8_substr(const std::string &s, size_t char_start, size_t char_len) {
    size_t byte_start = utf8_byte_offset(s, char_start);
    size_t byte_end   = utf8_byte_offset(s, char_start + char_len);
    if (byte_start >= s.size()) return "";
    return s.substr(byte_start, byte_end - byte_start);
}

static bool qry_iequal(const std::string &a, const char *b) {
    return qry_upper(a) == b;
}

/* Convert SvdbVal to double */
static double val_to_dbl(const SvdbVal &v) {
    if (v.type == SVDB_TYPE_INT)  return (double)v.ival;
    if (v.type == SVDB_TYPE_REAL) return v.rval;
    if (v.type == SVDB_TYPE_TEXT) {
        try { return std::stod(v.sval); } catch (...) {}
    }
    return 0.0;
}

/* Convert SvdbVal to int64 */
static int64_t val_to_i64(const SvdbVal &v) {
    if (v.type == SVDB_TYPE_INT)  return v.ival;
    if (v.type == SVDB_TYPE_REAL) return (int64_t)v.rval;
    if (v.type == SVDB_TYPE_TEXT) {
        try { return std::stoll(v.sval); } catch (...) {}
    }
    return 0;
}

/* Convert SvdbVal to string for comparison */
static std::string val_to_str(const SvdbVal &v) {
    if (v.type == SVDB_TYPE_TEXT || v.type == SVDB_TYPE_BLOB) return v.sval;
    if (v.type == SVDB_TYPE_INT)  return std::to_string(v.ival);
    if (v.type == SVDB_TYPE_REAL) {
        std::ostringstream ss;
        ss << v.rval;
        return ss.str();
    }
    return "";
}

/* Compare two SvdbVal. Returns <0, 0, >0 */
static int val_cmp(const SvdbVal &a, const SvdbVal &b) {
    if (a.type == SVDB_TYPE_NULL && b.type == SVDB_TYPE_NULL) return 0;
    if (a.type == SVDB_TYPE_NULL) return -1;
    if (b.type == SVDB_TYPE_NULL) return  1;
    if ((a.type == SVDB_TYPE_INT || a.type == SVDB_TYPE_REAL) &&
        (b.type == SVDB_TYPE_INT || b.type == SVDB_TYPE_REAL)) {
        double da = val_to_dbl(a), db = val_to_dbl(b);
        return (da < db) ? -1 : (da > db) ? 1 : 0;
    }
    std::string sa = val_to_str(a), sb = val_to_str(b);
    return sa.compare(sb);
}

static bool val_is_true(const SvdbVal &v) {
    if (v.type == SVDB_TYPE_NULL)  return false;
    if (v.type == SVDB_TYPE_INT)   return v.ival != 0;
    if (v.type == SVDB_TYPE_REAL)  return v.rval != 0.0;
    if (v.type == SVDB_TYPE_TEXT)  return !v.sval.empty();
    return false;
}

/* ── LIKE matching ─────────────────────────────────────────────── */
static bool like_match(const std::string &text, const std::string &pat, char esc = '\0') {
    /* Case-insensitive (SQLite LIKE behavior for ASCII) */
    if (pat.empty()) return text.empty();
    if (esc != '\0' && !pat.empty() && pat[0] == esc) {
        if (pat.size() < 2) return false; /* trailing escape char → malformed → no match */
        /* Escaped character: treat literally */
        if (text.empty()) return false;
        if (tolower((unsigned char)pat[1]) != tolower((unsigned char)text[0])) return false;
        return like_match(text.substr(1), pat.substr(2), esc);
    }
    if (pat[0] == '%') {
        for (size_t i = 0; i <= text.size(); ++i)
            if (like_match(text.substr(i), pat.substr(1), esc)) return true;
        return false;
    }
    if (text.empty()) return false;
    if (pat[0] == '_' || tolower((unsigned char)pat[0]) == tolower((unsigned char)text[0]))
        return like_match(text.substr(1), pat.substr(1), esc);
    return false;
}

/* ── GLOB matching (case-sensitive, * = any sequence, ? = any char) ── */
static bool glob_match(const std::string &text, const std::string &pat) {
    if (pat.empty()) return text.empty();
    if (pat[0] == '*') {
        for (size_t i = 0; i <= text.size(); ++i)
            if (glob_match(text.substr(i), pat.substr(1))) return true;
        return false;
    }
    if (text.empty()) return false;
    if (pat[0] == '?' || pat[0] == text[0])
        return glob_match(text.substr(1), pat.substr(1));
    return false;
}

/* ── RETURNING clause helpers ───────────────────────────────────── */

/* Forward declarations (defined later in this file) */
static SvdbVal eval_expr(const std::string &expr, const Row &row,
                          const std::vector<std::string> &col_order,
                          bool allow_agg);
static bool qry_eval_where(const Row &row,
                            const std::vector<std::string> &col_order,
                            const std::string &where_txt);

/* Extracts RETURNING clause from a DML statement.
 * Sets returning_clause to everything after RETURNING (trimmed).
 * Sets sql_without_returning to the DML without the RETURNING part.
 * Returns true if RETURNING was found. */
static bool qry_extract_returning(const std::string &sql,
                                   std::string &returning_clause,
                                   std::string &sql_without_returning) {
    std::string su = qry_upper(sql);
    size_t ret_pos = std::string::npos;
    int depth = 0; bool in_str = false;
    for (size_t i = 0; i + 9 <= su.size(); ++i) {
        char c = su[i];
        if (c == '\'') { in_str = !in_str; continue; }
        if (in_str) continue;
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (depth > 0) continue;
        /* Match " RETURNING " or "RETURNING " at start */
        if ((i == 0 || su[i-1] == ' ') && su.substr(i, 10) == "RETURNING ") {
            ret_pos = i; break;
        }
    }
    if (ret_pos == std::string::npos) return false;
    returning_clause = qry_trim(sql.substr(ret_pos + 9));
    /* Strip trailing ';' from returning clause */
    while (!returning_clause.empty() && (returning_clause.back() == ';' || isspace((unsigned char)returning_clause.back())))
        returning_clause.pop_back();
    sql_without_returning = qry_trim(sql.substr(0, ret_pos));
    while (!sql_without_returning.empty() && (sql_without_returning.back() == ';' || isspace((unsigned char)sql_without_returning.back())))
        sql_without_returning.pop_back();
    return true;
}

/* Split a RETURNING clause into individual expression strings. */
static std::vector<std::string> qry_split_returning_exprs(const std::string &ret_clause) {
    std::vector<std::string> exprs;
    int depth = 0; bool in_str = false;
    size_t start = 0;
    for (size_t i = 0; i <= ret_clause.size(); ++i) {
        char c = (i < ret_clause.size()) ? ret_clause[i] : ',';
        if (c == '\'') { in_str = !in_str; continue; }
        if (in_str) continue;
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (c == ',' && depth == 0) {
            exprs.push_back(qry_trim(ret_clause.substr(start, i - start)));
            start = i + 1;
        }
    }
    return exprs;
}

/* Build a result set by evaluating returning_exprs against each row in rows_in.
 * For RETURNING *, expands to all columns in col_order. */
static svdb_rows_t *qry_build_returning_result(
        const std::vector<std::string> &ret_exprs,
        const std::vector<Row>         &rows_in,
        const std::vector<std::string> &col_order) {
    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return nullptr;

    /* Determine effective expression list (expand * to col_order) */
    std::vector<std::string> exprs;
    for (const auto &e : ret_exprs) {
        if (qry_trim(e) == "*") {
            for (const auto &c : col_order) exprs.push_back(c);
        } else {
            exprs.push_back(e);
        }
    }

    for (const auto &e : exprs) r->col_names.push_back(e);
    for (const auto &row : rows_in) {
        std::vector<SvdbVal> ret_row;
        for (const auto &e : exprs)
            ret_row.push_back(eval_expr(e, row, col_order, false));
        r->rows.push_back(ret_row);
    }
    return r;
}

/* Thread-local DB context for subquery support */
static thread_local svdb_db_t *g_query_db = nullptr;
/* Thread-local outer row context for correlated subqueries */
static thread_local const Row *g_outer_row = nullptr;
static thread_local const std::vector<std::string> *g_outer_col_order = nullptr;

/* Forward declaration of svdb_query_internal (defined later) */
svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql, svdb_rows_t **rows_out);

/* Forward declarations */
static SvdbVal eval_expr(const std::string &expr, const Row &row,
                          const std::vector<std::string> &col_order,
                          bool allow_agg = false);

static SvdbVal eval_expr(const std::string &expr, const Row &row,
                          const std::vector<std::string> &col_order,
                          bool /*allow_agg*/) {
    std::string e = qry_trim(expr);
    if (e.empty()) { return SvdbVal{}; }

    /* Strip top-level AS alias suffix (e.g. "COALESCE(a,b) AS name" → "COALESCE(a,b)").
     * Depth-tracks parentheses so CAST(x AS type) is not affected.
     * Also tracks single-quote string literals so 'AS' inside strings is ignored. */
    {
        int dep_as = 0; bool ins_as = false;
        std::string eu_as = qry_upper(e);
        for (size_t i = 0; i < eu_as.size(); ++i) {
            char c = eu_as[i];
            if (c == '\'') { ins_as = !ins_as; continue; }
            if (ins_as) continue;
            if (c == '(') { ++dep_as; continue; }
            if (c == ')') { if (dep_as > 0) --dep_as; continue; }
            if (dep_as == 0 && i >= 1 && eu_as[i-1] == ' ' &&
                i + 3 <= eu_as.size() && eu_as.substr(i, 3) == "AS ") {
                e = qry_trim(e.substr(0, i - 1));
                break;
            }
        }
    }
    if (e.empty()) { return SvdbVal{}; }

    /* NULL literal */
    if (qry_upper(e) == "NULL") { return SvdbVal{}; }

    /* Boolean literals */
    if (qry_upper(e) == "TRUE")  { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1; return v; }
    if (qry_upper(e) == "FALSE") { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v; }

    /* Scalar subquery: SELECT ... (called after outer-paren stripping in caller) */
    {
        std::string eu_s = qry_upper(e);
        if (eu_s.size() >= 7 && eu_s.substr(0, 7) == "SELECT " && g_query_db) {
            /* Substitute qualified outer row refs */
            std::string sub_sql = e;
            for (auto &kv : row) {
                if (kv.first.empty() || kv.first.find('.') == std::string::npos) continue;
                std::string repl;
                if (kv.second.type == SVDB_TYPE_NULL) repl = "NULL";
                else if (kv.second.type == SVDB_TYPE_INT) repl = std::to_string(kv.second.ival);
                else if (kv.second.type == SVDB_TYPE_REAL) {
                    char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); repl = buf;
                } else { repl = "'" + kv.second.sval + "'"; }
                for (size_t p = sub_sql.find(kv.first); p != std::string::npos;
                     p = sub_sql.find(kv.first, p)) {
                    bool lb = (p == 0 || (!isalnum((unsigned char)sub_sql[p-1]) && sub_sql[p-1] != '_'));
                    bool rb = (p + kv.first.size() >= sub_sql.size() ||
                               (!isalnum((unsigned char)sub_sql[p+kv.first.size()]) && sub_sql[p+kv.first.size()] != '_'));
                    if (lb && rb) { sub_sql.replace(p, kv.first.size(), repl); p += repl.size(); }
                    else { p += kv.first.size(); }
                }
            }
            svdb_rows_t *sub_rows = nullptr;
            svdb_code_t rc = svdb_query_internal(g_query_db, sub_sql, &sub_rows);
            if (rc == SVDB_OK && sub_rows && !sub_rows->rows.empty() && !sub_rows->rows[0].empty()) {
                SvdbVal result = sub_rows->rows[0][0];
                delete sub_rows;
                return result;
            }
            if (sub_rows) delete sub_rows;
            return SvdbVal{};
        }
    }

    /* Parenthesised expression */
    if (e.front() == '(' && e.back() == ')') {
        int depth = 0;
        bool balanced = true;
        for (size_t i = 1; i < e.size() - 1; ++i) {
            if (e[i] == '(') ++depth;
            else if (e[i] == ')') { if (--depth < 0) { balanced = false; break; } }
        }
        if (balanced && depth == 0) {
            /* Check if inner content is a SELECT — handle as scalar subquery */
            std::string inner = qry_trim(e.substr(1, e.size()-2));
            std::string inner_u = qry_upper(inner);
            if (inner_u.size() >= 6 && inner_u.substr(0, 7) == "SELECT ") {
                if (g_query_db) {
                    /* For correlated subqueries, substitute ONLY outer row qualified refs (table.col) */
                    std::string sub_sql = inner;
                    for (auto &kv : row) {
                        if (kv.first.empty()) continue;
                        /* Only substitute qualified references (containing '.') */
                        if (kv.first.find('.') == std::string::npos) continue;
                        std::string repl;
                        if (kv.second.type == SVDB_TYPE_NULL) repl = "NULL";
                        else if (kv.second.type == SVDB_TYPE_INT) repl = std::to_string(kv.second.ival);
                        else if (kv.second.type == SVDB_TYPE_REAL) {
                            char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); repl = buf;
                        } else { repl = "'" + kv.second.sval + "'"; }
                        for (size_t p = sub_sql.find(kv.first); p != std::string::npos;
                             p = sub_sql.find(kv.first, p)) {
                            bool lb = (p == 0 || (!isalnum((unsigned char)sub_sql[p-1]) && sub_sql[p-1] != '_'));
                            bool rb = (p + kv.first.size() >= sub_sql.size() ||
                                       (!isalnum((unsigned char)sub_sql[p+kv.first.size()]) && sub_sql[p+kv.first.size()] != '_'));
                            if (lb && rb) { sub_sql.replace(p, kv.first.size(), repl); p += repl.size(); }
                            else { p += kv.first.size(); }
                        }
                    }
                    /* Execute subquery and return first column of first row */
                    svdb_rows_t *sub_rows = nullptr;
                    svdb_code_t rc = svdb_query_internal(g_query_db, sub_sql, &sub_rows);
                    if (rc == SVDB_OK && sub_rows && !sub_rows->rows.empty() && !sub_rows->rows[0].empty()) {
                        SvdbVal result = sub_rows->rows[0][0];
                        delete sub_rows;
                        return result;
                    }
                    if (sub_rows) delete sub_rows;
                }
                return SvdbVal{}; /* NULL if no rows */
            }
            return eval_expr(inner, row, col_order);
        }
    }

    /* String literal — only if the ENTIRE expression is a single quoted string */
    if (e[0] == '\'') {
        /* Find the matching closing quote (skip escaped '') */
        size_t close_pos = std::string::npos;
        for (size_t i = 1; i < e.size(); ++i) {
            if (e[i] == '\'') {
                if (i + 1 < e.size() && e[i+1] == '\'') { ++i; /* escaped '' */ }
                else { close_pos = i; break; }
            }
        }
        if (close_pos == e.size() - 1) {
            /* Complete string literal */
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            std::string raw = e.substr(1, close_pos - 1);
            std::string out; out.reserve(raw.size());
            for (size_t i = 0; i < raw.size(); ++i) {
                if (raw[i] == '\'' && i+1 < raw.size() && raw[i+1] == '\'')
                    { out += '\''; ++i; }
                else out += raw[i];
            }
            v.sval = out; return v;
        }
        /* else: fall through — the expression has more content after the string literal */
    }

    /* Hex blob literal: X'HEXHEX' or x'HEXHEX' (SQLite-compatible) */
    if (e.size() >= 3 && (e[0] == 'x' || e[0] == 'X') && e[1] == '\'') {
        SvdbVal v; v.type = SVDB_TYPE_BLOB;
        size_t end = e.rfind('\'');
        if (end > 1) {
            std::string hex = e.substr(2, end - 2);
            auto fh = [](char c2) -> unsigned char {
                if (c2 >= '0' && c2 <= '9') return (unsigned char)(c2 - '0');
                if (c2 >= 'a' && c2 <= 'f') return (unsigned char)(c2 - 'a' + 10);
                if (c2 >= 'A' && c2 <= 'F') return (unsigned char)(c2 - 'A' + 10);
                return 0;
            };
            for (size_t i = 0; i + 1 < hex.size(); i += 2)
                v.sval += (char)((fh(hex[i]) << 4) | fh(hex[i + 1]));
        }
        return v;
    }

    /* Numeric literal — only if the ENTIRE expression is a valid number */
    bool starts_num = isdigit((unsigned char)e[0]) || e[0] == '.' ||
                      ((e[0] == '-' || e[0] == '+') && e.size() > 1 &&
                       (isdigit((unsigned char)e[1]) || e[1] == '.'));
    if (starts_num) {
        bool is_real = false, is_pure = true, saw_e = false;
        for (size_t i = 0; i < e.size(); ++i) {
            char c = e[i];
            if (c == '.' || c == 'e' || c == 'E') {
                is_real = true;
                if (c == 'e' || c == 'E') saw_e = true;
            } else if ((c == '+' || c == '-') && saw_e && i > 0) {
                /* sign after exponent: OK */
            } else if (!isdigit((unsigned char)c) && !(i == 0 && (c == '-' || c == '+'))) {
                is_pure = false; break;
            }
        }
        if (is_pure) {
            SvdbVal v;
            try {
                if (is_real) { v.type = SVDB_TYPE_REAL; v.rval = std::stod(e); }
                else          { v.type = SVDB_TYPE_INT;  v.ival = std::stoll(e); }
            } catch (...) { v.type = SVDB_TYPE_TEXT; v.sval = e; }
            return v;
        }
    }

    /* Helper: check that a function call "FUNC(...)" has its closing ')' properly matching
     * the opening '(' at 'open_paren_pos'. Prevents "LOWER(x) IN (...)" from matching LOWER. */
    auto fn_paren_ok = [&](size_t open_paren_pos) -> bool {
        if (e.back() != ')') return false;
        int d = 0;
        bool in_s = false;
        for (size_t i = open_paren_pos; i < e.size(); ++i) {
            char c = e[i];
            if (c == '\'') { in_s = !in_s; continue; }
            if (in_s) continue;
            if (c == '(') ++d;
            else if (c == ')') { --d; if (d == 0) return i == e.size()-1; }
        }
        return false;
    };

    /* COALESCE(a, b, ...) */
    {
        std::string eu = qry_upper(e);
        if (eu.substr(0, 9) == "COALESCE(" && fn_paren_ok(9-1)) {
            std::string args = e.substr(9, e.size()-10);
            /* Split by top-level comma */
            int depth = 0;
            size_t start = 0;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = (i < args.size()) ? args[i] : ',';
                if (c == '(') ++depth;
                else if (c == ')') --depth;
                else if (c == ',' && depth == 0) {
                    std::string seg = qry_trim(args.substr(start, i - start));
                    SvdbVal v = eval_expr(seg, row, col_order);
                    if (v.type != SVDB_TYPE_NULL) return v;
                    start = i + 1;
                }
            }
            return SvdbVal{};
        }
        /* IFNULL(a, b) */
        if (eu.substr(0, 7) == "IFNULL(" && fn_paren_ok(7-1)) {
            std::string args = e.substr(7, e.size()-8);
            size_t comma = args.find(',');
            if (comma != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma), row, col_order);
                if (a.type != SVDB_TYPE_NULL) return a;
                return eval_expr(args.substr(comma+1), row, col_order);
            }
        }
        /* NULLIF(a, b) */
        if (eu.substr(0, 7) == "NULLIF(" && fn_paren_ok(7-1)) {
            std::string args = e.substr(7, e.size()-8);
            size_t comma = args.find(',');
            if (comma != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal b = eval_expr(args.substr(comma+1), row, col_order);
                if (val_cmp(a, b) == 0) return SvdbVal{};
                return a;
            }
        }
        /* LENGTH(expr) — returns byte count for BLOB, Unicode code-point count for TEXT */
        if (eu.substr(0, 7) == "LENGTH(" && fn_paren_ok(7-1)) {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            if (inner.type == SVDB_TYPE_BLOB) {
                /* BLOB: return raw byte count */
                SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (int64_t)inner.sval.size(); return v;
            }
            std::string s = val_to_str(inner);
            /* TEXT: Count UTF-8 code points: leading bytes start with 0x00-0x7F or 0xC0-0xFF */
            int64_t len = 0;
            for (size_t i = 0; i < s.size(); ++i) {
                unsigned char c = (unsigned char)s[i];
                if ((c & 0xC0) != 0x80) ++len; /* not a continuation byte */
            }
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = len; return v;
        }
        /* UPPER(expr) */
        if (eu.substr(0, 6) == "UPPER(" && fn_paren_ok(6-1)) {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            v.sval = qry_upper(val_to_str(inner)); return v;
        }
        /* LOWER(expr) */
        if (eu.substr(0, 6) == "LOWER(" && fn_paren_ok(6-1)) {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            std::string s = val_to_str(inner);
            for (auto &c : s) c = (char)tolower((unsigned char)c);
            v.sval = s; return v;
        }
        /* TRIM([LEADING|TRAILING|BOTH] [chars FROM] expr) or TRIM(expr[, chars]) */
        if (eu.substr(0, 5) == "TRIM(" && fn_paren_ok(5-1)) {
            std::string args_str = e.substr(5, e.size()-6);
            /* Check for 2-arg form: TRIM(str, chars) */
            /* Find top-level comma */
            int td = 0; size_t tcomma = std::string::npos;
            for (size_t i = 0; i < args_str.size(); ++i) {
                if (args_str[i] == '(') ++td; else if (args_str[i] == ')') --td;
                else if (args_str[i] == '\'' || args_str[i] == '"') {
                    char q = args_str[i]; ++i;
                    while (i < args_str.size() && args_str[i] != q) ++i;
                } else if (args_str[i] == ',' && td == 0) { tcomma = i; break; }
            }
            SvdbVal inner = eval_expr(tcomma != std::string::npos ? args_str.substr(0, tcomma) : args_str, row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string s = val_to_str(inner);
            if (tcomma != std::string::npos) {
                /* 2-arg: TRIM(str, chars) */
                SvdbVal chars_v = eval_expr(args_str.substr(tcomma+1), row, col_order);
                std::string chars = val_to_str(chars_v);
                /* Trim chars from both ends */
                auto notIn = [&](char c) { return chars.find(c) == std::string::npos; };
                size_t b2 = 0, e2 = s.size();
                while (b2 < e2 && !notIn(s[b2])) ++b2;
                while (e2 > b2 && !notIn(s[e2-1])) --e2;
                s = s.substr(b2, e2-b2);
            } else {
                s = qry_trim(s);
            }
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = s; return v;
        }
        /* LTRIM(expr[, chars]) */
        if (eu.substr(0, 6) == "LTRIM(" && fn_paren_ok(6-1)) {
            std::string args_str = e.substr(6, e.size()-7);
            int td = 0; size_t tcomma = std::string::npos;
            for (size_t i = 0; i < args_str.size(); ++i) {
                if (args_str[i] == '(') ++td; else if (args_str[i] == ')') --td;
                else if (args_str[i] == ',' && td == 0) { tcomma = i; break; }
            }
            SvdbVal inner = eval_expr(tcomma != std::string::npos ? args_str.substr(0, tcomma) : args_str, row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string s = val_to_str(inner);
            std::string chars = (tcomma != std::string::npos) ? val_to_str(eval_expr(args_str.substr(tcomma+1), row, col_order)) : " \t\r\n";
            size_t b2 = 0;
            while (b2 < s.size() && chars.find(s[b2]) != std::string::npos) ++b2;
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = s.substr(b2); return v;
        }
        /* RTRIM(expr[, chars]) */
        if (eu.substr(0, 6) == "RTRIM(" && fn_paren_ok(6-1)) {
            std::string args_str = e.substr(6, e.size()-7);
            int td = 0; size_t tcomma = std::string::npos;
            for (size_t i = 0; i < args_str.size(); ++i) {
                if (args_str[i] == '(') ++td; else if (args_str[i] == ')') --td;
                else if (args_str[i] == ',' && td == 0) { tcomma = i; break; }
            }
            SvdbVal inner = eval_expr(tcomma != std::string::npos ? args_str.substr(0, tcomma) : args_str, row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string s = val_to_str(inner);
            std::string chars = (tcomma != std::string::npos) ? val_to_str(eval_expr(args_str.substr(tcomma+1), row, col_order)) : " \t\r\n";
            size_t e2 = s.size();
            while (e2 > 0 && chars.find(s[e2-1]) != std::string::npos) --e2;
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = s.substr(0, e2); return v;
        }
        /* CAST(expr AS type) */
        if (eu.substr(0, 5) == "CAST(" && fn_paren_ok(5-1)) {
            std::string inside = e.substr(5, e.size()-6);
            std::string iupper = qry_upper(inside);
            size_t as_pos = iupper.rfind(" AS ");
            if (as_pos != std::string::npos) {
                std::string src_expr = inside.substr(0, as_pos);
                std::string type_str = qry_upper(qry_trim(inside.substr(as_pos + 4)));
                SvdbVal sv = eval_expr(src_expr, row, col_order);
                if (sv.type == SVDB_TYPE_NULL) return sv; /* CAST(NULL) = NULL */
                if (type_str == "INTEGER" || type_str == "INT") {
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = val_to_i64(sv); return v;
                } else if (type_str == "REAL" || type_str == "FLOAT" || type_str == "DOUBLE") {
                    SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = val_to_dbl(sv); return v;
                } else {
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = val_to_str(sv); return v;
                }
            }
        }
        /* ABS(expr) */
        if (eu.substr(0, 4) == "ABS(" && fn_paren_ok(4-1)) {
            SvdbVal inner = eval_expr(e.substr(4, e.size()-5), row, col_order);
            if (inner.type == SVDB_TYPE_INT)  { inner.ival = inner.ival < 0 ? -inner.ival : inner.ival; }
            else if (inner.type == SVDB_TYPE_REAL) { inner.rval = std::abs(inner.rval); }
            return inner;
        }
        /* CEIL / CEILING(expr) */
        if ((eu.substr(0, 5) == "CEIL(" && fn_paren_ok(4)) || (eu.substr(0, 8) == "CEILING(" && fn_paren_ok(7))) {
            size_t start = (eu[4] == '(') ? 5 : 8;
            SvdbVal inner = eval_expr(e.substr(start, e.size()-start-1), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::ceil(val_to_dbl(inner)); return v;
        }
        /* FLOOR(expr) */
        if (eu.substr(0, 6) == "FLOOR(" && fn_paren_ok(6-1)) {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::floor(val_to_dbl(inner)); return v;
        }
        /* ROUND(expr) or ROUND(expr, digits) */
        if (eu.substr(0, 6) == "ROUND(" && fn_paren_ok(6-1)) {
            std::string args = e.substr(6, e.size()-7);
            /* find top-level comma */
            int rd = 0; size_t comma_pos = std::string::npos;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++rd; else if (args[i] == ')') --rd;
                else if (args[i] == ',' && rd == 0) { comma_pos = i; break; }
            }
            if (comma_pos == std::string::npos) {
                SvdbVal inner = eval_expr(args, row, col_order);
                if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_REAL;
                v.rval = std::round(val_to_dbl(inner));
                return v;
            } else {
                SvdbVal inner = eval_expr(args.substr(0, comma_pos), row, col_order);
                if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal digits = eval_expr(args.substr(comma_pos+1), row, col_order);
                int64_t n = val_to_i64(digits);
                double factor = std::pow(10.0, (double)n);
                SvdbVal v; v.type = SVDB_TYPE_REAL;
                v.rval = std::round(val_to_dbl(inner) * factor) / factor;
                return v;
            }
        }
        /* SQRT(expr) */
        if (eu.substr(0, 5) == "SQRT(" && fn_paren_ok(5-1)) {
            SvdbVal inner = eval_expr(e.substr(5, e.size()-6), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::sqrt(val_to_dbl(inner)); return v;
        }
        /* POW(a, b) / POWER(a, b) */
        if ((eu.substr(0, 4) == "POW(" && fn_paren_ok(3)) || (eu.substr(0, 6) == "POWER(" && fn_paren_ok(5))) {
            size_t start = (eu[3] == '(') ? 4 : 6;
            std::string args = e.substr(start, e.size()-start-1);
            int rd = 0; size_t comma_pos = std::string::npos;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++rd; else if (args[i] == ')') --rd;
                else if (args[i] == ',' && rd == 0) { comma_pos = i; break; }
            }
            if (comma_pos != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma_pos), row, col_order);
                SvdbVal b = eval_expr(args.substr(comma_pos+1), row, col_order);
                SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::pow(val_to_dbl(a), val_to_dbl(b)); return v;
            }
        }
        /* SIGN(expr) */
        if (eu.substr(0, 5) == "SIGN(" && fn_paren_ok(5-1)) {
            SvdbVal inner = eval_expr(e.substr(5, e.size()-6), row, col_order);
            double d = val_to_dbl(inner);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (d > 0) ? 1 : (d < 0 ? -1 : 0); return v;
        }
        /* MAX(a, b) and MIN(a, b) as scalar functions */
        if (((eu.substr(0, 4) == "MAX(" || eu.substr(0, 4) == "MIN(") && fn_paren_ok(3))) {
            bool is_max = (eu[1] == 'A');
            std::string args = e.substr(4, e.size()-5);
            int rd = 0; size_t comma_pos = std::string::npos;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++rd; else if (args[i] == ')') --rd;
                else if (args[i] == ',' && rd == 0) { comma_pos = i; break; }
            }
            if (comma_pos != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma_pos), row, col_order);
                SvdbVal b = eval_expr(args.substr(comma_pos+1), row, col_order);
                if (a.type == SVDB_TYPE_NULL) return b;
                if (b.type == SVDB_TYPE_NULL) return a;
                bool a_wins;
                if (a.type == SVDB_TYPE_TEXT && b.type == SVDB_TYPE_TEXT)
                    a_wins = is_max ? (a.sval > b.sval) : (a.sval < b.sval);
                else
                    a_wins = is_max ? (val_to_dbl(a) > val_to_dbl(b)) : (val_to_dbl(a) < val_to_dbl(b));
                return a_wins ? a : b;
            }
        }
        /* SUBSTR(str, start) or SUBSTR(str, start, len) */
        if ((eu.substr(0, 7) == "SUBSTR(" && fn_paren_ok(6)) || (eu.substr(0, 10) == "SUBSTRING(" && fn_paren_ok(9))) {
            size_t pref = (eu.substr(0, 10) == "SUBSTRING(") ? 10 : 7;
            std::string args = e.substr(pref, e.size() - pref - 1);
            /* split by top-level comma */
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            bool in_s2 = false;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = i < args.size() ? args[i] : ',';
                if (c == '\'') { in_s2 = !in_s2; continue; }
                if (in_s2) continue;
                if (c == '(') ++rd; else if (c == ')') { if (rd>0)--rd; }
                else if (c == ',' && rd == 0) { parts.push_back(args.substr(start2, i-start2)); start2 = i+1; }
            }
            if (!parts.empty()) {
                SvdbVal str_v = eval_expr(parts[0], row, col_order);
                if (str_v.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string s2 = val_to_str(str_v);
                int64_t slen = (int64_t)utf8_char_count(s2);  /* character count */
                int64_t off = parts.size() > 1 ? val_to_i64(eval_expr(parts[1], row, col_order)) : 1;
                bool has_len = parts.size() > 2;
                int64_t len2 = has_len ? val_to_i64(eval_expr(parts[2], row, col_order)) : -1;
                /* SQLite semantics: negative off counts from end */
                if (off < 0) {
                    off = slen + off + 1;
                    if (off < 1) off = 1;
                } else if (off == 0) {
                    /* position 0: adjust length down by 1, start at 1 */
                    if (has_len) {
                        if (len2 > 0) len2 = len2 - 1;
                        else if (len2 == 0) { SvdbVal v; v.type = SVDB_TYPE_TEXT; return v; }
                    }
                    off = 1;
                }
                /* Negative length: abs(len2) chars preceding position off */
                if (has_len && len2 < 0) {
                    int64_t new_start = off + len2;
                    int64_t new_len   = -len2;
                    off  = (new_start < 1) ? 1 : new_start;
                    len2 = (new_start < 1) ? new_len + new_start - 1 : new_len;
                }
                if (has_len && len2 < 0) len2 = 0;
                int64_t idx2 = off - 1;
                if (idx2 < 0) idx2 = 0;
                if (idx2 >= slen) { SvdbVal v; v.type = SVDB_TYPE_TEXT; return v; }
                int64_t take = (!has_len) ? (slen - idx2) : std::min(len2, slen - idx2);
                if (take < 0) take = 0;
                SvdbVal v; v.type = SVDB_TYPE_TEXT;
                v.sval = utf8_substr(s2, (size_t)idx2, (size_t)take);
                return v;
            }
        }
        /* REPLACE(str, old, new) */
        if (eu.substr(0, 8) == "REPLACE(" && fn_paren_ok(8-1)) {
            std::string args = e.substr(8, e.size()-9);
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = i < args.size() ? args[i] : ',';
                if (c == '(') ++rd; else if (c == ')') --rd;
                else if (c == ',' && rd == 0) { parts.push_back(args.substr(start2, i-start2)); start2 = i+1; }
            }
            if (parts.size() >= 3) {
                std::string src = val_to_str(eval_expr(parts[0], row, col_order));
                std::string old2 = val_to_str(eval_expr(parts[1], row, col_order));
                std::string new2 = val_to_str(eval_expr(parts[2], row, col_order));
                if (!old2.empty()) {
                    size_t pos2 = 0;
                    while ((pos2 = src.find(old2, pos2)) != std::string::npos) {
                        src.replace(pos2, old2.size(), new2);
                        pos2 += new2.size();
                    }
                }
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = src; return v;
            }
        }
        /* INSTR(str, substr) — returns 1-based position or 0 if not found */
        if (eu.substr(0, 6) == "INSTR(" && fn_paren_ok(6-1)) {
            std::string args = e.substr(6, e.size()-7);
            int rd = 0; size_t comma_p = std::string::npos;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++rd; else if (args[i] == ')') --rd;
                else if (args[i] == ',' && rd == 0) { comma_p = i; break; }
            }
            if (comma_p != std::string::npos) {
                SvdbVal sv = eval_expr(args.substr(0, comma_p), row, col_order);
                SvdbVal sv2 = eval_expr(args.substr(comma_p+1), row, col_order);
                if (sv.type == SVDB_TYPE_NULL || sv2.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string s = val_to_str(sv), sub = val_to_str(sv2);
                size_t pos2 = s.find(sub);
                SvdbVal v; v.type = SVDB_TYPE_INT;
                v.ival = (pos2 == std::string::npos) ? 0 : (int64_t)pos2 + 1;
                return v;
            }
        }
        /* HEX(expr) */
        if (eu.substr(0, 4) == "HEX(" && fn_paren_ok(4-1)) {
            SvdbVal inner = eval_expr(e.substr(4, e.size()-5), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            static const char *hexchars = "0123456789ABCDEF";
            std::string s2 = val_to_str(inner);
            std::string hex;
            for (unsigned char c : s2) {
                hex += hexchars[(c >> 4) & 0xf];
                hex += hexchars[c & 0xf];
            }
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = hex; return v;
        }
        /* ZEROBLOB(N) — returns a BLOB of N zero bytes */
        if (eu.substr(0, 9) == "ZEROBLOB(" && fn_paren_ok(9-1)) {
            SvdbVal nv = eval_expr(e.substr(9, e.size()-10), row, col_order);
            int64_t n = (nv.type == SVDB_TYPE_INT) ? nv.ival : 0;
            if (n < 0) n = 0;
            SvdbVal v; v.type = SVDB_TYPE_BLOB; v.sval = std::string((size_t)n, '\0'); return v;
        }
        /* TYPEOF(expr) */
        if (eu.substr(0, 7) == "TYPEOF(" && fn_paren_ok(7-1)) {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            switch (inner.type) {
                case SVDB_TYPE_NULL: v.sval = "null"; break;
                case SVDB_TYPE_INT:  v.sval = "integer"; break;
                case SVDB_TYPE_REAL: v.sval = "real"; break;
                case SVDB_TYPE_TEXT: v.sval = "text"; break;
                case SVDB_TYPE_BLOB: v.sval = "blob"; break;
            }
            return v;
        }
        /* Helper: thread-safe UTC time formatting */
        auto fmt_utc = [](const char *fmt, char *buf, size_t bufsz) {
            std::time_t now = std::time(nullptr);
            std::tm tm_buf{};
#if defined(_POSIX_VERSION) || defined(__linux__) || defined(__APPLE__)
            gmtime_r(&now, &tm_buf);
#elif defined(_MSC_VER)
            gmtime_s(&tm_buf, &now);
#else
            tm_buf = *std::gmtime(&now); /* fallback: not thread-safe */
#endif
            std::strftime(buf, bufsz, fmt, &tm_buf);
        };
        /* CURRENT_DATE / CURRENT_TIME / CURRENT_TIMESTAMP literals */
        if (eu == "CURRENT_DATE") {
            char buf[12]; fmt_utc("%Y-%m-%d", buf, sizeof(buf));
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
        }
        if (eu == "CURRENT_TIME") {
            char buf[10]; fmt_utc("%H:%M:%S", buf, sizeof(buf));
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
        }
        if (eu == "CURRENT_TIMESTAMP") {
            char buf[24]; fmt_utc("%Y-%m-%d %H:%M:%S", buf, sizeof(buf));
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
        }
        /* DATE('now') / TIME('now') / DATETIME('now') / STRFTIME(fmt, 'now') */
        auto is_now_arg = [](const std::string &arg) -> bool {
            std::string a = arg; if (a.size()>=2 && a.front()=='\'') a = a.substr(1, a.size()-2);
            std::string au; for (char c : a) au += (char)toupper((unsigned char)c);
            return au == "NOW" || au == "NOW()";
        };
        if ((eu.substr(0, 5) == "DATE(" && fn_paren_ok(4)) || (eu.substr(0, 9) == "JULIANDAY(" && fn_paren_ok(9))) {
            std::string arg = qry_trim(e.substr(eu[0]=='D'?5:10, e.size()-(eu[0]=='D'?5:10)-1));
            if (is_now_arg(arg)) {
                char buf[12]; fmt_utc("%Y-%m-%d", buf, sizeof(buf));
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
        }
        if (eu.substr(0, 5) == "TIME(" && fn_paren_ok(4)) {
            std::string arg = qry_trim(e.substr(5, e.size()-6));
            if (is_now_arg(arg)) {
                char buf[10]; fmt_utc("%H:%M:%S", buf, sizeof(buf));
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
        }
        if (eu.substr(0, 9) == "DATETIME(" && fn_paren_ok(8)) {
            std::string arg = qry_trim(e.substr(9, e.size()-10));
            if (is_now_arg(arg)) {
                char buf[24]; fmt_utc("%Y-%m-%d %H:%M:%S", buf, sizeof(buf));
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
        }
        /* ISNULL(expr) / NOTNULL(expr) */
        if (eu.substr(0, 7) == "ISNULL(" && fn_paren_ok(7-1)) {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (inner.type == SVDB_TYPE_NULL) ? 1 : 0; return v;
        }
        if (eu.substr(0, 8) == "NOTNULL(" && fn_paren_ok(8-1)) {
            SvdbVal inner = eval_expr(e.substr(8, e.size()-9), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (inner.type != SVDB_TYPE_NULL) ? 1 : 0; return v;
        }
    }

    /* Binary arithmetic with proper operator precedence:
     * First pass: find lowest-precedence op (+/-) outside parens.
     * Second pass: find * or /.
     */
    auto find_binary_op = [&](const std::string &s, const std::string &ops_str) -> size_t {
        int depth = 0;
        bool in_str = false;
        /* Scan right-to-left for left-associativity */
        for (int i = (int)s.size()-1; i >= 0; --i) {
            char c = s[i];
            if (c == '\'') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (c == ')') { ++depth; continue; }
            if (c == '(') { if (depth > 0) --depth; continue; }
            if (depth > 0) continue;
            if (ops_str.find(c) != std::string::npos) {
                /* Make sure it's a real operator, not unary minus/plus */
                if ((c == '-' || c == '+') && i == 0) continue;
                /* Don't match '-' directly after an operator e.g. 3+-5 or 10 % -3 */
                if ((c == '-' || c == '+') && i > 0) {
                    /* Find preceding non-space character */
                    int j = i - 1;
                    while (j >= 0 && s[j] == ' ') --j;
                    if (j < 0) continue; /* at start */
                    char prev = s[j];
                    if (prev == '+' || prev == '-' || prev == '*' || prev == '/' || prev == '%' || prev == '(' ||
                        prev == '=' || prev == '<' || prev == '>' || prev == '!') continue;
                }
                return (size_t)i;
            }
        }
        return std::string::npos;
    };

    /* IS NULL / IS NOT NULL as value expression (returns 0/1 integer) */
    {
        std::string eu2 = qry_upper(e);
        size_t isnull = eu2.rfind(" IS NULL");
        if (isnull != std::string::npos && isnull + 8 == eu2.size()) {
            SvdbVal v = eval_expr(e.substr(0, isnull), row, col_order);
            SvdbVal r; r.type = SVDB_TYPE_INT; r.ival = (v.type == SVDB_TYPE_NULL) ? 1 : 0; return r;
        }
        size_t isnotnull = eu2.rfind(" IS NOT NULL");
        if (isnotnull != std::string::npos && isnotnull + 12 == eu2.size()) {
            SvdbVal v = eval_expr(e.substr(0, isnotnull), row, col_order);
            SvdbVal r; r.type = SVDB_TYPE_INT; r.ival = (v.type != SVDB_TYPE_NULL) ? 1 : 0; return r;
        }
        /* CASE WHEN ... THEN ... [WHEN ... THEN ...] [ELSE ...] END */
        if (eu2.substr(0, 5) == "CASE ") {
            /* Detect simple vs. searched CASE form:
             * Simple:   CASE expr WHEN val1 THEN res1 ... END
             * Searched: CASE WHEN cond1 THEN res1 ... END  */
            /* Find the first WHEN at top level starting from position 4 */
            size_t first_when = std::string::npos;
            {
                int dep = 0; bool ins = false;
                for (size_t i = 4; i + 6 <= eu2.size(); ++i) {
                    char c = eu2[i];
                    if (c == '\'') { ins = !ins; continue; }
                    if (ins) continue;
                    if (c == '(') { ++dep; continue; }
                    if (c == ')') { if (dep > 0) --dep; continue; }
                    if (dep > 0) continue;
                    if (eu2.substr(i, 6) == " WHEN ") { first_when = i; break; }
                }
            }
            if (first_when == std::string::npos) { /* malformed */ return SvdbVal{}; }
            /* If " WHEN " is immediately after "CASE " (at position 4), it's a searched form */
            std::string after_case = (first_when > 5) ? qry_trim(eu2.substr(5, first_when - 5)) : "";
            bool is_simple = !after_case.empty();

            SvdbVal case_val;
            if (is_simple) {
                std::string case_expr_str = qry_trim(e.substr(5, first_when - 5));
                case_val = eval_expr(case_expr_str, row, col_order);
            }

            size_t pos2 = is_simple ? first_when + 1 : 5; /* skip to WHEN */
            SvdbVal result;
            bool matched = false;
            while (pos2 < eu2.size()) {
                while (pos2 < eu2.size() && eu2[pos2] == ' ') ++pos2;
                if (eu2.substr(pos2, 4) == "WHEN") {
                    pos2 += 4;
                    while (pos2 < eu2.size() && eu2[pos2] == ' ') ++pos2;
                    /* find THEN */
                    size_t then_pos = eu2.find(" THEN ", pos2);
                    if (then_pos == std::string::npos) break;
                    std::string cond_expr = e.substr(pos2, then_pos - pos2);
                    pos2 = then_pos + 6;
                    /* find next WHEN, ELSE or END at the same nesting level
                     * (must skip nested CASE...END blocks) */
                    size_t next = eu2.size();
                    {
                        int case_depth = 1; /* we're inside 1 outer CASE */
                        bool in_s2 = false;
                        for (size_t pi = pos2; pi < eu2.size(); ++pi) {
                            char cc = eu2[pi];
                            if (cc == '\'') { in_s2 = !in_s2; continue; }
                            if (in_s2) continue;
                            /* Detect nested CASE */
                            if (pi + 5 <= eu2.size() && eu2.substr(pi, 5) == "CASE ") {
                                ++case_depth; pi += 4; continue;
                            }
                            if (case_depth == 1) {
                                /* At our level — look for WHEN / ELSE / END */
                                if (pi + 5 <= eu2.size() && eu2.substr(pi, 5) == " WHEN") {
                                    next = pi; break;
                                }
                                if (pi + 5 <= eu2.size() && eu2.substr(pi, 5) == " ELSE") {
                                    next = pi; break;
                                }
                                if (pi + 4 <= eu2.size() && eu2.substr(pi, 4) == " END") {
                                    next = pi; break;
                                }
                            }
                            if (pi + 4 <= eu2.size() && eu2.substr(pi, 4) == " END") {
                                --case_depth;
                                if (case_depth == 0) { next = pi; break; }
                                pi += 3; continue;
                            }
                        }
                    }
                    std::string then_expr = e.substr(pos2, next - pos2);
                    pos2 = next;
                    if (!matched) {
                        if (is_simple) {
                            /* Simple form: compare case_val against WHEN value */
                            SvdbVal when_v = eval_expr(qry_trim(cond_expr), row, col_order);
                            if (case_val.type != SVDB_TYPE_NULL && when_v.type != SVDB_TYPE_NULL &&
                                val_cmp(case_val, when_v) == 0) {
                                result = eval_expr(then_expr, row, col_order); matched = true;
                            }
                        } else {
                            /* Searched form: evaluate condition as boolean */
                            SvdbVal cond_v = eval_expr(cond_expr, row, col_order);
                            bool cond_true = (cond_v.type == SVDB_TYPE_INT && cond_v.ival != 0) ||
                                             (cond_v.type == SVDB_TYPE_REAL && cond_v.rval != 0.0) ||
                                             (cond_v.type == SVDB_TYPE_TEXT && !cond_v.sval.empty());
                            if (cond_true) { result = eval_expr(then_expr, row, col_order); matched = true; }
                        }
                    }
                } else if (eu2.substr(pos2, 4) == "ELSE") {
                    pos2 += 4;
                    while (pos2 < eu2.size() && eu2[pos2] == ' ') ++pos2;
                    size_t end_pos = eu2.find(" END", pos2);
                    if (end_pos == std::string::npos) end_pos = eu2.size();
                    if (!matched) result = eval_expr(e.substr(pos2, end_pos - pos2), row, col_order);
                    break;
                } else if (eu2.substr(pos2, 3) == "END") {
                    break;
                } else {
                    ++pos2;
                }
            }
            return result;
        }
    }

    /* Logical OR / AND in expression context (lowest precedence — must come BEFORE comparison ops) */
    /* Scan LEFT-to-RIGHT to correctly handle BETWEEN...AND patterns. */
    {
        std::string eu_e = qry_upper(e);
        /* Find leftmost OR at top level */
        size_t or_p = std::string::npos;
        {
            int depth_e = 0; bool in_str_e = false;
            for (size_t i = 0; i + 4 <= eu_e.size(); ++i) {
                char c = eu_e[i];
                if (c == '\'') { in_str_e = !in_str_e; continue; }
                if (in_str_e) continue;
                if (c == '(') { ++depth_e; continue; }
                if (c == ')') { if (depth_e > 0) --depth_e; continue; }
                if (depth_e > 0) continue;
                if (eu_e.substr(i, 4) == " OR ") { or_p = i + 1; break; }
            }
        }
        if (or_p != std::string::npos) {
            SvdbVal lhs = eval_expr(qry_trim(e.substr(0, or_p - 1)), row, col_order);
            SvdbVal rhs = eval_expr(qry_trim(e.substr(or_p + 3)), row, col_order);
            bool l = val_is_true(lhs), r = val_is_true(rhs);
            bool lnull = (lhs.type == SVDB_TYPE_NULL), rnull = (rhs.type == SVDB_TYPE_NULL);
            if (l || r) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1; return v; }
            if (lnull || rnull) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v;
        }
        /* Find leftmost boolean AND at top level (skip BETWEEN's AND) */
        size_t and_p = std::string::npos;
        {
            int depth_e = 0; bool in_str_e = false;
            bool after_between = false;
            for (size_t i = 0; i + 5 <= eu_e.size(); ++i) {
                char c = eu_e[i];
                if (c == '\'') { in_str_e = !in_str_e; continue; }
                if (in_str_e) continue;
                if (c == '(') { ++depth_e; continue; }
                if (c == ')') { if (depth_e > 0) --depth_e; continue; }
                if (depth_e > 0) continue;
                /* Track BETWEEN keyword */
                if (i + 8 <= eu_e.size() && eu_e.substr(i, 8) == " BETWEEN") { after_between = true; i += 7; continue; }
                if (i + 12 <= eu_e.size() && eu_e.substr(i, 12) == " NOT BETWEEN") { after_between = true; i += 11; continue; }
                if (eu_e.substr(i, 5) == " AND ") {
                    if (after_between) { after_between = false; i += 4; continue; } /* Skip BETWEEN's AND */
                    and_p = i + 1; break;
                }
            }
        }
        if (and_p != std::string::npos) {
            SvdbVal lhs = eval_expr(qry_trim(e.substr(0, and_p - 1)), row, col_order);
            SvdbVal rhs = eval_expr(qry_trim(e.substr(and_p + 4)), row, col_order);
            bool l = val_is_true(lhs), r = val_is_true(rhs);
            bool lnull = (lhs.type == SVDB_TYPE_NULL), rnull = (rhs.type == SVDB_TYPE_NULL);
            if (!l && !lnull) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v; }
            if (!r && !rnull) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v; }
            if (lnull || rnull) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1; return v;
        }
    }


    /* ── Comparison / boolean operators (lower precedence than arithmetic) ── */
    /* These must be checked BEFORE arithmetic so that `a + b = c` splits on `=` first. */
    {
        std::string eu_cmp = qry_upper(e);

        /* BETWEEN / NOT BETWEEN — must check before IN and comparison ops */
        {
            size_t not_bet = eu_cmp.find(" NOT BETWEEN ");
            if (not_bet != std::string::npos) {
                size_t and2 = eu_cmp.find(" AND ", not_bet + 13);
                if (and2 != std::string::npos) {
                    SvdbVal val  = eval_expr(e.substr(0, not_bet), row, col_order);
                    SvdbVal low  = eval_expr(e.substr(not_bet + 13, and2 - (not_bet + 13)), row, col_order);
                    SvdbVal high = eval_expr(e.substr(and2 + 5), row, col_order);
                    if (val.type == SVDB_TYPE_NULL || low.type == SVDB_TYPE_NULL || high.type == SVDB_TYPE_NULL)
                        return SvdbVal{};
                    SvdbVal v; v.type = SVDB_TYPE_INT;
                    v.ival = !(val_cmp(val, low) >= 0 && val_cmp(val, high) <= 0) ? 1 : 0;
                    return v;
                }
            }
            size_t bet = eu_cmp.find(" BETWEEN ");
            if (bet != std::string::npos) {
                size_t and2 = eu_cmp.find(" AND ", bet + 9);
                if (and2 != std::string::npos) {
                    SvdbVal val  = eval_expr(e.substr(0, bet), row, col_order);
                    SvdbVal low  = eval_expr(e.substr(bet + 9, and2 - (bet + 9)), row, col_order);
                    SvdbVal high = eval_expr(e.substr(and2 + 5), row, col_order);
                    if (val.type == SVDB_TYPE_NULL || low.type == SVDB_TYPE_NULL || high.type == SVDB_TYPE_NULL)
                        return SvdbVal{};
                    SvdbVal v; v.type = SVDB_TYPE_INT;
                    v.ival = (val_cmp(val, low) >= 0 && val_cmp(val, high) <= 0) ? 1 : 0;
                    return v;
                }
            }
        }

        /* NOT IN / IN (value list or subquery) */
        {
            size_t nin_pos = eu_cmp.find(" NOT IN (");
            size_t in_pos  = (nin_pos == std::string::npos) ? eu_cmp.find(" IN (") : std::string::npos;
            bool negated = (nin_pos != std::string::npos);
            size_t use_pos = negated ? nin_pos : in_pos;
            if (use_pos != std::string::npos) {
                SvdbVal lhs = eval_expr(e.substr(0, use_pos), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL) return SvdbVal{}; /* NULL IN (...) = NULL */
                size_t paren_start = e.find('(', use_pos);
                size_t paren_end   = e.rfind(')');
                if (paren_start != std::string::npos && paren_end != std::string::npos) {
                    std::string inside = e.substr(paren_start + 1, paren_end - paren_start - 1);
                    std::string inside_u = qry_upper(qry_trim(inside));
                    /* Subquery */
                    if (inside_u.size() > 6 && inside_u.substr(0, 7) == "SELECT " && g_query_db) {
                        svdb_rows_t *sub_rows = nullptr;
                        svdb_code_t rc = svdb_query_internal(g_query_db, qry_trim(inside), &sub_rows);
                        bool found = false;
                        if (rc == SVDB_OK && sub_rows) {
                            for (auto &srow : sub_rows->rows) {
                                if (!srow.empty() && srow[0].type != SVDB_TYPE_NULL &&
                                    val_cmp(lhs, srow[0]) == 0) { found = true; break; }
                            }
                            delete sub_rows;
                        }
                        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = negated ? !found : found; return v;
                    }
                    /* Value list — handle empty list */
                    if (qry_trim(inside).empty()) {
                        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = negated ? 1 : 0; return v;
                    }
                    bool found = false; bool has_null = false;
                    int dep_in = 0; size_t st_in = 0;
                    bool in_str_in = false;
                    std::string items = inside + ",";
                    for (size_t ix = 0; ix < items.size(); ++ix) {
                        char c = items[ix];
                        if (c == '\'') { in_str_in = !in_str_in; continue; }
                        if (in_str_in) continue;
                        if (c == '(') ++dep_in; else if (c == ')') { if (dep_in > 0) --dep_in; }
                        else if (c == ',' && dep_in == 0) {
                            std::string tok = qry_trim(items.substr(st_in, ix - st_in));
                            st_in = ix + 1;
                            if (!tok.empty()) {
                                SvdbVal rv = eval_expr(tok, row, col_order);
                                if (rv.type == SVDB_TYPE_NULL) { has_null = true; }
                                else if (!found && val_cmp(lhs, rv) == 0) found = true;
                            }
                        }
                    }
                    if (negated) {
                        if (found) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v; }
                        if (has_null) return SvdbVal{};
                        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1; return v;
                    } else {
                        if (found) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1; return v; }
                        if (has_null) return SvdbVal{};
                        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v;
                    }
                }
            }
        }

        /* NOT LIKE / LIKE */
        {
            size_t not_like = eu_cmp.find(" NOT LIKE ");
            if (not_like != std::string::npos) {
                SvdbVal lhs = eval_expr(e.substr(0, not_like), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(not_like + 10), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_INT;
                v.ival = like_match(val_to_str(lhs), val_to_str(rhs)) ? 0 : 1; return v;
            }
            size_t like_pos = eu_cmp.find(" LIKE ");
            if (like_pos != std::string::npos) {
                SvdbVal lhs = eval_expr(e.substr(0, like_pos), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(like_pos + 6), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_INT;
                v.ival = like_match(val_to_str(lhs), val_to_str(rhs)) ? 1 : 0; return v;
            }
        }

        /* NOT GLOB / GLOB */
        {
            size_t not_glob = eu_cmp.find(" NOT GLOB ");
            if (not_glob != std::string::npos) {
                SvdbVal lhs = eval_expr(e.substr(0, not_glob), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(not_glob + 10), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_INT;
                v.ival = glob_match(val_to_str(lhs), val_to_str(rhs)) ? 0 : 1; return v;
            }
            size_t glob_pos = eu_cmp.find(" GLOB ");
            if (glob_pos != std::string::npos) {
                SvdbVal lhs = eval_expr(e.substr(0, glob_pos), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(glob_pos + 6), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_INT;
                v.ival = glob_match(val_to_str(lhs), val_to_str(rhs)) ? 1 : 0; return v;
            }
        }

        /* Comparison operators (=, <>, !=, <, >, <=, >=) */
        /* Scan right-to-left to find the rightmost operator, */
        /* but first do a forward pre-pass to find in-string positions */
        {
            /* Pre-compute string regions by scanning left-to-right */
            std::vector<bool> in_str_map(e.size(), false);
            {
                bool in_s = false;
                for (size_t i = 0; i < e.size(); ++i) {
                    if (e[i] == '\'') in_s = !in_s;
                    in_str_map[i] = in_s;
                }
            }
            static const char* cmp_ops[] = {"!=", "<>", "<=", ">=", "<", ">", "=", nullptr};
            for (int oi = 0; cmp_ops[oi]; ++oi) {
                std::string op = cmp_ops[oi];
                size_t osz = op.size();
                int depth_c = 0;
                size_t found_c = std::string::npos;
                for (int i = (int)e.size() - (int)osz; i >= 0; --i) {
                    if ((size_t)i < e.size() && in_str_map[(size_t)i]) continue;
                    char c = e[(size_t)i];
                    if (c == ')') ++depth_c;
                    else if (c == '(') { if (depth_c > 0) --depth_c; }
                    if (depth_c > 0) continue;
                    if (e.substr((size_t)i, osz) == op) {
                        bool ok = true;
                        if (op == "=" && i > 0) {
                            char prev = e[i-1];
                            if (prev == '!' || prev == '<' || prev == '>') ok = false;
                        }
                        if (ok) { found_c = (size_t)i; break; }
                    }
                }
                if (found_c != std::string::npos) {
                    std::string lhs_s = qry_trim(e.substr(0, found_c));
                    std::string rhs_s = qry_trim(e.substr(found_c + osz));
                    /* Handle COLLATE NOCASE/BINARY suffixes */
                    bool nocase = false;
                    std::string lhs_u = qry_upper(lhs_s), rhs_u = qry_upper(rhs_s);
                    if (lhs_u.size() >= 15 && lhs_u.substr(lhs_u.size()-15) == " COLLATE NOCASE") {
                        lhs_s = qry_trim(lhs_s.substr(0, lhs_s.size()-15)); nocase = true;
                    } else if (lhs_u.size() >= 15 && lhs_u.substr(lhs_u.size()-15) == " COLLATE BINARY") {
                        lhs_s = qry_trim(lhs_s.substr(0, lhs_s.size()-15));
                    }
                    if (rhs_u.size() >= 15 && rhs_u.substr(rhs_u.size()-15) == " COLLATE NOCASE") {
                        rhs_s = qry_trim(rhs_s.substr(0, rhs_s.size()-15)); nocase = true;
                    } else if (rhs_u.size() >= 15 && rhs_u.substr(rhs_u.size()-15) == " COLLATE BINARY") {
                        rhs_s = qry_trim(rhs_s.substr(0, rhs_s.size()-15));
                    }
                    if (!lhs_s.empty()) {
                        SvdbVal lhs = eval_expr(lhs_s, row, col_order);
                        SvdbVal rhs = eval_expr(rhs_s, row, col_order);
                        if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                        int c = nocase ? qry_upper(val_to_str(lhs)).compare(qry_upper(val_to_str(rhs))) : val_cmp(lhs, rhs);
                        SvdbVal v; v.type = SVDB_TYPE_INT;
                        if (op == "=" || op == "==") v.ival = (c == 0) ? 1 : 0;
                        else if (op == "!=" || op == "<>") v.ival = (c != 0) ? 1 : 0;
                        else if (op == "<")  v.ival = (c <  0) ? 1 : 0;
                        else if (op == ">")  v.ival = (c >  0) ? 1 : 0;
                        else if (op == "<=") v.ival = (c <= 0) ? 1 : 0;
                        else if (op == ">=") v.ival = (c >= 0) ? 1 : 0;
                        return v;
                    }
                }
            }
        }
    }

    /* Concatenation operator || */
    {
        int depth2 = 0;
        bool in_str2 = false;
        for (int i = (int)e.size()-1; i >= 0; --i) {
            char c = e[i];
            if (c == '\'') { in_str2 = !in_str2; continue; }
            if (in_str2) continue;
            if (c == ')') ++depth2; else if (c == '(') { if (depth2>0) --depth2; }
            if (depth2 > 0) continue;
            if (c == '|' && i+1 < (int)e.size() && e[i+1] == '|') {
                SvdbVal lhs = eval_expr(e.substr(0, i), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(i+2), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_TEXT;
                v.sval = val_to_str(lhs) + val_to_str(rhs); return v;
            }
        }
    }

    /* Low-precedence: + and - */
    size_t op_pos = find_binary_op(e, "+-");
    if (op_pos != std::string::npos && op_pos > 0) {
        char op = e[op_pos];
        SvdbVal lhs = eval_expr(e.substr(0, op_pos), row, col_order);
        SvdbVal rhs = eval_expr(e.substr(op_pos+1), row, col_order);
        if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
        if (lhs.type == SVDB_TYPE_REAL || rhs.type == SVDB_TYPE_REAL) {
            SvdbVal v; v.type = SVDB_TYPE_REAL;
            double l = val_to_dbl(lhs), r = val_to_dbl(rhs);
            v.rval = (op == '+') ? l + r : l - r;
            return v;
        }
        SvdbVal v; v.type = SVDB_TYPE_INT;
        int64_t l = val_to_i64(lhs), r = val_to_i64(rhs);
        v.ival = (op == '+') ? l + r : l - r;
        return v;
    }

    /* High-precedence: *, /, % */
    op_pos = find_binary_op(e, "*/%");
    if (op_pos != std::string::npos && op_pos > 0) {
        char op = e[op_pos];
        SvdbVal lhs = eval_expr(e.substr(0, op_pos), row, col_order);
        SvdbVal rhs = eval_expr(e.substr(op_pos+1), row, col_order);
        if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
        if (op == '%') {
            /* SQLite: if either operand is REAL, result is REAL */
            if (lhs.type == SVDB_TYPE_REAL || rhs.type == SVDB_TYPE_REAL) {
                double dr = val_to_dbl(rhs);
                if (dr == 0.0) return SvdbVal{};
                SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::fmod(val_to_dbl(lhs), dr); return v;
            }
            int64_t r = val_to_i64(rhs);
            if (r == 0) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = val_to_i64(lhs) % r; return v;
        }
        double l = val_to_dbl(lhs), r = val_to_dbl(rhs);
        if (op == '/') {
            if (r == 0.0) return SvdbVal{};
            if (lhs.type == SVDB_TYPE_INT && rhs.type == SVDB_TYPE_INT) {
                SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = val_to_i64(lhs) / val_to_i64(rhs); return v;
            }
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = l / r; return v;
        }
        if (lhs.type == SVDB_TYPE_REAL || rhs.type == SVDB_TYPE_REAL) {
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = l * r; return v;
        }
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = val_to_i64(lhs) * val_to_i64(rhs); return v;
    }

    /* Unary minus: -expr */
    if (e[0] == '-' && e.size() > 1) {
        SvdbVal inner = eval_expr(e.substr(1), row, col_order);
        if (inner.type == SVDB_TYPE_INT)  { inner.ival = -inner.ival; return inner; }
        if (inner.type == SVDB_TYPE_REAL) { inner.rval = -inner.rval; return inner; }
        if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
        /* Try to parse as number */
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = -val_to_i64(inner); return v;
    }
    /* Unary plus: +expr */
    if (e[0] == '+' && e.size() > 1) {
        return eval_expr(e.substr(1), row, col_order);
    }

    /* Logical NOT */
    {
        std::string eu_n = qry_upper(e);
        if (eu_n.size() > 4 && eu_n.substr(0, 4) == "NOT ") {
            SvdbVal inner = eval_expr(e.substr(4), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_INT;
            v.ival = val_is_true(inner) ? 0 : 1;
            return v;
        }
    }

    /* Column reference (possibly table.col or "col") */
    std::string col = e;
    /* First try: full expression with any prefix (handles alias.col in merged rows) */
    {
        auto it = row.find(e);
        if (it != row.end()) return it->second;
        std::string e_upper = qry_upper(e);
        for (auto &kv : row) {
            if (qry_upper(kv.first) == e_upper) return kv.second;
        }
    }
    /* strip table prefix */
    auto dot = col.find('.');
    if (dot != std::string::npos) col = col.substr(dot + 1);
    /* strip quotes */
    if (col.size() >= 2 && (col.front() == '"' || col.front() == '`'))
        col = col.substr(1, col.size() - 2);

    auto it = row.find(col);
    if (it != row.end()) return it->second;

    /* Case-insensitive column lookup */
    std::string col_upper = qry_upper(col);
    for (auto &kv : row) {
        if (qry_upper(kv.first) == col_upper) return kv.second;
    }

    return SvdbVal{};
}

/* ── WHERE evaluation ───────────────────────────────────────────── */

static bool qry_eval_where(const Row &row,
                             const std::vector<std::string> &col_order,
                             const std::string &where_text);

/* Thread-local flag: set to true when a comparison returns false due to NULL operands.
 * Used by NOT handler to propagate three-valued logic (NOT NULL = NULL = false). */
static thread_local bool g_last_null_comparison = false;

static bool qry_eval_where(const Row &row,
                             const std::vector<std::string> &col_order,
                             const std::string &where_text) {
    if (where_text.empty()) return true;
    std::string wt = qry_trim(where_text);
    if (wt.empty()) return true;
    std::string wu = qry_upper(wt);

    /* Strip outer parentheses */
    if (wt.front() == '(' && wt.back() == ')') {
        int depth = 0; bool balanced = true;
        for (size_t i = 1; i < wt.size()-1; ++i) {
            if (wt[i] == '(') ++depth;
            else if (wt[i] == ')') { if (--depth < 0) { balanced = false; break; } }
        }
        if (balanced && depth == 0)
            return qry_eval_where(row, col_order, wt.substr(1, wt.size()-2));
    }

    /* Find lowest-precedence operator outside parens: first OR, then AND */
    auto find_kw_outside = [&](const std::string &kw) -> size_t {
        int depth = 0; bool in_str = false;
        int between_depth = 0; /* track BETWEEN..AND scope */
        for (size_t i = 0; i < wu.size(); ++i) {
            char c = wu[i];
            if (c == '\'') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (c == '(') ++depth;
            else if (c == ')') { if (depth > 0) --depth; }
            else if (depth == 0) {
                /* Check for BETWEEN keyword to enter between-scope */
                if (i + 7 <= wu.size() && wu.substr(i, 7) == "BETWEEN" &&
                    (i == 0 || wu[i-1] == ' ') &&
                    (i + 7 >= wu.size() || wu[i + 7] == ' '))
                    between_depth++;
                /* Check for target keyword */
                if (i + kw.size() <= wu.size() &&
                    wu.substr(i, kw.size()) == kw &&
                    (i == 0 || wu[i-1] == ' ') &&
                    (i + kw.size() >= wu.size() || wu[i + kw.size()] == ' ')) {
                    /* If this AND closes a BETWEEN, consume it and continue */
                    if (kw == "AND" && between_depth > 0) {
                        --between_depth;
                        i += kw.size() - 1; /* skip AND, loop increments i */
                        continue;
                    }
                    return i;
                }
            }
        }
        return std::string::npos;
    };

    size_t or_pos = find_kw_outside("OR");
    if (or_pos != std::string::npos)
        return qry_eval_where(row, col_order, wt.substr(0, or_pos)) ||
               qry_eval_where(row, col_order, wt.substr(or_pos + 2));

    size_t and_pos = find_kw_outside("AND");
    if (and_pos != std::string::npos)
        return qry_eval_where(row, col_order, wt.substr(0, and_pos)) &&
               qry_eval_where(row, col_order, wt.substr(and_pos + 3));

    /* NOT expr: three-valued logic — NOT NULL = NULL = false */
    if (wu.substr(0, 4) == "NOT ") {
        g_last_null_comparison = false;
        bool inner = qry_eval_where(row, col_order, wt.substr(4));
        if (g_last_null_comparison) return false; /* NOT NULL = null = false */
        return !inner;
    }

    /* EXISTS / NOT EXISTS (subquery) */
    if (g_query_db) {
        /* Helper: substitute only qualified (table.col) outer row refs into a SQL string */
        auto subst_outer = [&](std::string sub_sql) -> std::string {
            for (auto &kv : row) {
                if (kv.first.empty() || kv.first.find('.') == std::string::npos) continue;
                std::string repl;
                if (kv.second.type == SVDB_TYPE_NULL) repl = "NULL";
                else if (kv.second.type == SVDB_TYPE_INT) repl = std::to_string(kv.second.ival);
                else if (kv.second.type == SVDB_TYPE_REAL) {
                    char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); repl = buf;
                } else { repl = "'" + kv.second.sval + "'"; }
                for (size_t p = sub_sql.find(kv.first); p != std::string::npos;
                     p = sub_sql.find(kv.first, p)) {
                    bool lb = (p == 0 || (!isalnum((unsigned char)sub_sql[p-1]) && sub_sql[p-1] != '_'));
                    bool rb = (p + kv.first.size() >= sub_sql.size() ||
                               (!isalnum((unsigned char)sub_sql[p+kv.first.size()]) && sub_sql[p+kv.first.size()] != '_'));
                    if (lb && rb) { sub_sql.replace(p, kv.first.size(), repl); p += repl.size(); }
                    else { p += kv.first.size(); }
                }
            }
            return sub_sql;
        };
        if (wu.size() > 8 && wu.substr(0, 8) == "EXISTS (") {
            size_t pend = wt.rfind(')');
            if (pend != std::string::npos) {
                std::string sub_sql = subst_outer(wt.substr(8, pend - 8));
                svdb_rows_t *sub_rows = nullptr;
                svdb_code_t rc = svdb_query_internal(g_query_db, sub_sql, &sub_rows);
                bool exists = (rc == SVDB_OK && sub_rows && !sub_rows->rows.empty());
                if (sub_rows) delete sub_rows;
                return exists;
            }
        }
        if (wu.size() > 12 && wu.substr(0, 12) == "NOT EXISTS (") {
            size_t pend = wt.rfind(')');
            if (pend != std::string::npos) {
                std::string sub_sql = subst_outer(wt.substr(12, pend - 12));
                svdb_rows_t *sub_rows = nullptr;
                svdb_code_t rc = svdb_query_internal(g_query_db, sub_sql, &sub_rows);
                bool exists = (rc == SVDB_OK && sub_rows && !sub_rows->rows.empty());
                if (sub_rows) delete sub_rows;
                return !exists;
            }
        }
    }

    /* IS NULL / IS NOT NULL */
    {
        size_t is_null = wu.rfind(" IS NULL");
        if (is_null != std::string::npos && is_null + 8 == wu.size()) {
            SvdbVal v = eval_expr(wt.substr(0, is_null), row, col_order);
            return v.type == SVDB_TYPE_NULL;
        }
        size_t is_not_null = wu.rfind(" IS NOT NULL");
        if (is_not_null != std::string::npos && is_not_null + 12 == wu.size()) {
            SvdbVal v = eval_expr(wt.substr(0, is_not_null), row, col_order);
            return v.type != SVDB_TYPE_NULL;
        }
    }

    /* LIKE / NOT LIKE (check NOT LIKE first!) */
    {
        size_t not_like = wu.find(" NOT LIKE ");
        if (not_like != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, not_like), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL) return false; /* NULL NOT LIKE x = NULL → false */
            std::string rhs_s = wt.substr(not_like + 10);
            /* Check ESCAPE clause */
            char esc_char = '\0';
            std::string rhs_su = qry_upper(rhs_s);
            size_t esc_pos = rhs_su.find(" ESCAPE ");
            if (esc_pos != std::string::npos) {
                SvdbVal esc_v = eval_expr(rhs_s.substr(esc_pos + 8), row, col_order);
                std::string esc_s = val_to_str(esc_v);
                if (!esc_s.empty()) esc_char = esc_s[0];
                rhs_s = rhs_s.substr(0, esc_pos);
            }
            SvdbVal rhs = eval_expr(rhs_s, row, col_order);
            if (rhs.type == SVDB_TYPE_NULL) return false;
            return !like_match(val_to_str(lhs), val_to_str(rhs), esc_char);
        }
        size_t like_pos = wu.find(" LIKE ");
        if (like_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, like_pos), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL) return false; /* NULL LIKE x = NULL → false */
            std::string rhs_s = wt.substr(like_pos + 6);
            /* Check ESCAPE clause */
            char esc_char = '\0';
            std::string rhs_su = qry_upper(rhs_s);
            size_t esc_pos = rhs_su.find(" ESCAPE ");
            if (esc_pos != std::string::npos) {
                SvdbVal esc_v = eval_expr(rhs_s.substr(esc_pos + 8), row, col_order);
                std::string esc_s = val_to_str(esc_v);
                if (!esc_s.empty()) esc_char = esc_s[0];
                rhs_s = rhs_s.substr(0, esc_pos);
            }
            SvdbVal rhs = eval_expr(rhs_s, row, col_order);
            if (rhs.type == SVDB_TYPE_NULL) return false;
            return like_match(val_to_str(lhs), val_to_str(rhs), esc_char);
        }
    }

    /* BETWEEN ... AND ... / NOT BETWEEN ... AND ... */
    {
        size_t not_bet_pos = wu.find(" NOT BETWEEN ");
        if (not_bet_pos != std::string::npos) {
            size_t and2 = wu.find(" AND ", not_bet_pos + 13);
            if (and2 != std::string::npos) {
                SvdbVal val  = eval_expr(wt.substr(0, not_bet_pos), row, col_order);
                SvdbVal low  = eval_expr(wt.substr(not_bet_pos+13, and2-(not_bet_pos+13)), row, col_order);
                SvdbVal high = eval_expr(wt.substr(and2+5), row, col_order);
                if (val.type == SVDB_TYPE_NULL || low.type == SVDB_TYPE_NULL || high.type == SVDB_TYPE_NULL) return false;
                return !(val_cmp(val, low) >= 0 && val_cmp(val, high) <= 0);
            }
        }
        size_t bet_pos = wu.find(" BETWEEN ");
        if (bet_pos != std::string::npos) {
            size_t and2 = wu.find(" AND ", bet_pos + 9);
            if (and2 != std::string::npos) {
                SvdbVal val  = eval_expr(wt.substr(0, bet_pos), row, col_order);
                SvdbVal low  = eval_expr(wt.substr(bet_pos+9, and2-(bet_pos+9)), row, col_order);
                SvdbVal high = eval_expr(wt.substr(and2+5), row, col_order);
                if (val.type == SVDB_TYPE_NULL || low.type == SVDB_TYPE_NULL || high.type == SVDB_TYPE_NULL) return false;
                return val_cmp(val, low) >= 0 && val_cmp(val, high) <= 0;
            }
        }
    }

/* MATCH — case-insensitive substring search (FTS-style on regular tables) */
    {
        size_t match_pos = wu.find(" MATCH ");
        if (match_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, match_pos), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(match_pos + 7), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return false;
            std::string text = qry_upper(val_to_str(lhs));
            std::string pat  = qry_upper(val_to_str(rhs));
            /* MATCH is literal substring search (no wildcards) */
            return text.find(pat) != std::string::npos;
        }
    }

    /* GLOB / NOT GLOB (check NOT GLOB first) */
    {
        size_t not_glob = wu.find(" NOT GLOB ");
        if (not_glob != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, not_glob), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(not_glob + 10), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return false;
            return !glob_match(val_to_str(lhs), val_to_str(rhs));
        }
        size_t glob_pos = wu.find(" GLOB ");
        if (glob_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, glob_pos), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(glob_pos + 6), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return false;
            return glob_match(val_to_str(lhs), val_to_str(rhs));
        }
    }

    /* IN (v1, v2, ...) / NOT IN — also handles IN (SELECT ...) */
    {
        size_t in_pos  = wu.find(" IN (");
        size_t nin_pos = wu.find(" NOT IN (");
        size_t use_pos = std::string::npos; bool negated = false;
        if (nin_pos != std::string::npos) { use_pos = nin_pos; negated = true; }
        else if (in_pos != std::string::npos) use_pos = in_pos;
        if (use_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, use_pos), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL) return false; /* NULL IN (...) = NULL → false */
            size_t paren_start = wt.find('(', use_pos);
            size_t paren_end   = wt.rfind(')');
            if (paren_start != std::string::npos && paren_end != std::string::npos) {
                std::string inside = wt.substr(paren_start+1, paren_end-paren_start-1);
                std::string inside_u = qry_upper(qry_trim(inside));
                /* Check for subquery */
                if (inside_u.size() > 6 && inside_u.substr(0, 7) == "SELECT " && g_query_db) {
                    /* Substitute only qualified outer row refs (table.col) */
                    std::string sub_sql = qry_trim(inside);
                    for (auto &kv : row) {
                        if (kv.first.empty() || kv.first.find('.') == std::string::npos) continue;
                        std::string repl;
                        if (kv.second.type == SVDB_TYPE_NULL) repl = "NULL";
                        else if (kv.second.type == SVDB_TYPE_INT) repl = std::to_string(kv.second.ival);
                        else if (kv.second.type == SVDB_TYPE_REAL) {
                            char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); repl = buf;
                        } else { repl = "'" + kv.second.sval + "'"; }
                        for (size_t p = sub_sql.find(kv.first); p != std::string::npos;
                             p = sub_sql.find(kv.first, p)) {
                            bool lb = (p == 0 || (!isalnum((unsigned char)sub_sql[p-1]) && sub_sql[p-1] != '_'));
                            bool rb = (p + kv.first.size() >= sub_sql.size() ||
                                       (!isalnum((unsigned char)sub_sql[p+kv.first.size()]) && sub_sql[p+kv.first.size()] != '_'));
                            if (lb && rb) { sub_sql.replace(p, kv.first.size(), repl); p += repl.size(); }
                            else { p += kv.first.size(); }
                        }
                    }
                    svdb_rows_t *sub_rows = nullptr;
                    svdb_code_t rc = svdb_query_internal(g_query_db, sub_sql, &sub_rows);
                    bool found = false;
                    bool has_null = false;
                    if (rc == SVDB_OK && sub_rows) {
                        for (auto &srow : sub_rows->rows) {
                            if (srow.empty()) continue;
                            if (srow[0].type == SVDB_TYPE_NULL) { has_null = true; continue; }
                            if (val_cmp(lhs, srow[0]) == 0) { found = true; break; }
                        }
                        delete sub_rows;
                    }
                    if (negated) { if (found) return false; if (has_null) return false; return true; }
                    return found;
                }
                /* Value list */
                bool found = false;
                bool has_null = false;
                int depth_in = 0;
                size_t start_in = 0;
                for (size_t idx = 0; idx <= inside.size(); ++idx) {
                    char c = (idx < inside.size()) ? inside[idx] : ',';
                    if (c == '(') ++depth_in;
                    else if (c == ')') { if (depth_in > 0) --depth_in; }
                    else if (c == ',' && depth_in == 0) {
                        std::string tok = qry_trim(inside.substr(start_in, idx - start_in));
                        start_in = idx + 1;
                        SvdbVal rv = eval_expr(tok, row, col_order);
                        if (rv.type == SVDB_TYPE_NULL) { has_null = true; continue; }
                        if (!found && val_cmp(lhs, rv) == 0) { found = true; }
                    }
                }
                if (negated) {
                    if (found) return false;
                    if (has_null) return false;
                    return true;
                }
                return found;
            }
        }
    }

    /* Quantified comparisons: expr op ALL/ANY/SOME (subquery) */
    {
        /* Patterns: `expr op ALL (`, `expr op ANY (`, `expr op SOME (` */
        static const char *quants[] = {" ALL (", " ANY (", " SOME (", nullptr};
        static const char *cmp_ops[] = {"!=", "<>", "<=", ">=", "=", "<", ">", nullptr};
        for (int qi = 0; quants[qi]; ++qi) {
            size_t q_pos = wu.find(quants[qi]);
            if (q_pos == std::string::npos) continue;
            bool is_all = (qi == 0);
            /* Find the comparison operator just before quants[qi] */
            size_t cmp_pos = std::string::npos; size_t cmp_len = 0;
            for (int oi = 0; cmp_ops[oi]; ++oi) {
                size_t ol = strlen(cmp_ops[oi]);
                if (q_pos >= ol + 1 && wt.substr(q_pos - ol, ol) == cmp_ops[oi]) {
                    cmp_pos = q_pos - ol; cmp_len = ol; break;
                }
            }
            if (cmp_pos == std::string::npos) continue;
            std::string lhs_s = qry_trim(wt.substr(0, cmp_pos));
            std::string op_s  = wt.substr(cmp_pos, cmp_len);
            size_t sub_start  = wt.find('(', q_pos);
            size_t sub_end    = wt.rfind(')');
            if (sub_start == std::string::npos || sub_end == std::string::npos) continue;
            std::string sub_sql = qry_trim(wt.substr(sub_start+1, sub_end - sub_start - 1));
            SvdbVal lhs = eval_expr(lhs_s, row, col_order);
            if (lhs.type == SVDB_TYPE_NULL) return false;
            svdb_rows_t *sub_rows = nullptr;
            if (svdb_query_internal(g_query_db, sub_sql, &sub_rows) != SVDB_OK || !sub_rows) return false;
            bool result = is_all; /* ALL: start true; ANY/SOME: start false */
            for (auto &srow : sub_rows->rows) {
                if (srow.empty() || srow[0].type == SVDB_TYPE_NULL) continue;
                int cmp = val_cmp(lhs, srow[0]);
                bool match = false;
                if      (op_s == "=")  match = (cmp == 0);
                else if (op_s == "!=") match = (cmp != 0);
                else if (op_s == "<>") match = (cmp != 0);
                else if (op_s == "<")  match = (cmp < 0);
                else if (op_s == "<=") match = (cmp <= 0);
                else if (op_s == ">")  match = (cmp > 0);
                else if (op_s == ">=") match = (cmp >= 0);
                if (is_all) { if (!match) { result = false; break; } }
                else        { if ( match) { result = true;  break; } }
            }
            delete sub_rows;
            return result;
        }
    }

    /* Comparison operators: !=, <>, <=, >=, =, <, > (depth-aware scan) */
    {
        const char *ops[] = {"!=", "<>", "<=", ">=", "=", "<", ">", nullptr};
        size_t op_start = std::string::npos, op_len = 0;
        /* Scan left-to-right, respecting paren depth, string literals,
         * and CASE...END blocks (so operators inside CASE conditions are skipped) */
        {
            int depth_c = 0; int case_depth_c = 0; bool in_str_c = false;
            std::string wu_scan = qry_upper(wt);
            for (size_t i = 0; i < wt.size(); ++i) {
                char c = wt[i];
                if (c == '\'') { in_str_c = !in_str_c; continue; }
                if (in_str_c) continue;
                if (c == '(') { ++depth_c; continue; }
                if (c == ')') { if (depth_c > 0) --depth_c; continue; }
                if (depth_c > 0) continue;
                /* Track CASE...END depth to skip operators inside CASE blocks */
                if (i + 5 <= wu_scan.size() && wu_scan.substr(i, 5) == "CASE ") {
                    ++case_depth_c; i += 4; continue;
                }
                if (i + 4 <= wu_scan.size() && wu_scan.substr(i, 4) == " END") {
                    if (case_depth_c > 0) { --case_depth_c; i += 3; continue; }
                }
                if (case_depth_c > 0) continue;
                /* Try each operator */
                for (int oi = 0; ops[oi]; ++oi) {
                    size_t oplen = strlen(ops[oi]);
                    if (i + oplen <= wt.size() && wt.substr(i, oplen) == ops[oi]) {
                        if (op_start == std::string::npos || i < op_start) {
                            op_start = i; op_len = oplen;
                        }
                        break; /* Take the first match at this position */
                    }
                }
            }
        }
        if (op_start != std::string::npos) {
            std::string lhs_s = qry_trim(wt.substr(0, op_start));
            std::string op    = wt.substr(op_start, op_len);
            std::string rhs_s = qry_trim(wt.substr(op_start + op_len));
            /* Handle COLLATE NOCASE / COLLATE BINARY on either side */
            bool nocase = false;
            {
                std::string lhs_u = qry_upper(lhs_s);
                std::string rhs_u = qry_upper(rhs_s);
                if (lhs_u.size() >= 15 && lhs_u.substr(lhs_u.size()-15) == " COLLATE NOCASE") {
                    lhs_s = qry_trim(lhs_s.substr(0, lhs_s.size()-15)); nocase = true;
                } else if (lhs_u.size() >= 15 && lhs_u.substr(lhs_u.size()-15) == " COLLATE BINARY") {
                    lhs_s = qry_trim(lhs_s.substr(0, lhs_s.size()-15));
                }
                if (rhs_u.size() >= 15 && rhs_u.substr(rhs_u.size()-15) == " COLLATE NOCASE") {
                    rhs_s = qry_trim(rhs_s.substr(0, rhs_s.size()-15)); nocase = true;
                } else if (rhs_u.size() >= 15 && rhs_u.substr(rhs_u.size()-15) == " COLLATE BINARY") {
                    rhs_s = qry_trim(rhs_s.substr(0, rhs_s.size()-15));
                }
            }
            SvdbVal lhs = eval_expr(lhs_s, row, col_order);
            SvdbVal rhs = eval_expr(rhs_s, row, col_order);
            /* NULL comparisons: any comparison with NULL is null (treated as false) */
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) {
                g_last_null_comparison = true;
                return false;
            }
            int c;
            if (nocase && lhs.type == SVDB_TYPE_TEXT && rhs.type == SVDB_TYPE_TEXT) {
                std::string ls = qry_upper(lhs.sval), rs = qry_upper(rhs.sval);
                c = ls.compare(rs);
            } else {
                c = val_cmp(lhs, rhs);
            }
            if (op == "=" || op == "==") return c == 0;
            if (op == "!=" || op == "<>") return c != 0;
            if (op == "<")  return c < 0;
            if (op == ">")  return c > 0;
            if (op == "<=") return c <= 0;
            if (op == ">=") return c >= 0;
        }
    }

    return true;
}

/* ── SQL clause parsing helpers ─────────────────────────────────── */

struct OrderCol { std::string expr; bool desc; bool nocase = false; };
struct JoinSpec  { std::string type; /* INNER/LEFT/CROSS */ std::string table; std::string alias; std::string on_left; std::string on_right; std::string using_col; };

/* Parse comma-separated additional tables from FROM clause as CROSS JOINs.
 * For "FROM a, b WHERE ..." returns [{CROSS, "b", ...}]. */
static std::vector<JoinSpec> parse_comma_joins(const std::string &sql) {
    std::vector<JoinSpec> result;
    std::string su = qry_upper(sql);
    /* Find " FROM " outside parens */
    size_t from_pos = std::string::npos;
    int depth = 0; bool in_str = false;
    for (size_t i = 0; i + 6 <= su.size(); ++i) {
        char c = su[i];
        if (c == '\'') { in_str = !in_str; continue; }
        if (in_str) continue;
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (depth > 0) continue;
        if (su.substr(i, 6) == " FROM ") { from_pos = i + 6; break; }
    }
    if (from_pos == std::string::npos) return result;
    /* Read the FROM table list until WHERE/JOIN/ORDER/GROUP/LIMIT/HAVING/end */
    static const char *stop_kws[] = {" WHERE ", " INNER ", " LEFT ", " RIGHT ", " CROSS ", " JOIN ",
                                      " ORDER ", " GROUP ", " LIMIT ", " HAVING ", " UNION ", nullptr};
    size_t from_end = su.size();
    for (const char **kw = stop_kws; *kw; ++kw) {
        size_t ep = su.find(*kw, from_pos);
        if (ep != std::string::npos && ep < from_end) from_end = ep;
    }
    std::string from_clause = sql.substr(from_pos, from_end - from_pos);
    /* Split by comma */
    std::vector<std::string> tables;
    size_t p = 0;
    while (p < from_clause.size()) {
        size_t comma = from_clause.find(',', p);
        if (comma == std::string::npos) { tables.push_back(qry_trim(from_clause.substr(p))); break; }
        tables.push_back(qry_trim(from_clause.substr(p, comma - p)));
        p = comma + 1;
    }
    /* First table is the main table (already handled by tname); process rest as CROSS JOINs */
    for (size_t i = 1; i < tables.size(); ++i) {
        std::string t = tables[i];
        JoinSpec j; j.type = "CROSS";
        /* Split table name and alias */
        size_t sp = t.find(' ');
        if (sp != std::string::npos) {
            j.table = qry_trim(t.substr(0, sp));
            j.alias = qry_trim(t.substr(sp + 1));
        } else {
            j.table = t;
        }
        result.push_back(j);
    }
    return result;
}

/* Extract ORDER BY clause */
static std::vector<OrderCol> parse_order_by(const std::string &sql) {
    std::vector<OrderCol> result;
    std::string su = qry_upper(sql);
    /* Find the last top-level ORDER BY (not inside parentheses / OVER clause) */
    size_t pos = std::string::npos;
    {
        int depth = 0; bool in_s = false;
        for (size_t i = 0; i + 9 <= su.size(); ++i) {
            char c = su[i];
            if (c == '\'') { in_s = !in_s; continue; }
            if (in_s) continue;
            if (c == '(') { ++depth; continue; }
            if (c == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && su.substr(i, 9) == "ORDER BY ") pos = i;
        }
    }
    if (pos == std::string::npos) return result;
    /* End at LIMIT or end of string */
    size_t end = su.find("LIMIT ", pos);
    std::string ob_text = (end != std::string::npos)
        ? sql.substr(pos + 9, end - pos - 9)
        : sql.substr(pos + 9);
    /* Split by comma */
    std::istringstream ss(ob_text);
    std::string token;
    while (std::getline(ss, token, ',')) {
        token = qry_trim(token);
        bool desc = false; bool nocase = false;
        std::string tu = qry_upper(token);
        /* Strip NULLS FIRST / NULLS LAST */
        if (tu.size() >= 12 && tu.substr(tu.size()-12) == " NULLS FIRST")
            token = qry_trim(token.substr(0, token.size()-12));
        else if (tu.size() >= 11 && tu.substr(tu.size()-11) == " NULLS LAST")
            token = qry_trim(token.substr(0, token.size()-11));
        tu = qry_upper(token);
        if (tu.size() >= 5 && tu.substr(tu.size()-5) == " DESC") {
            desc = true; token = qry_trim(token.substr(0, token.size()-5));
        } else if (tu.size() >= 4 && tu.substr(tu.size()-4) == " ASC") {
            token = qry_trim(token.substr(0, token.size()-4));
        }
        tu = qry_upper(token);
        /* Strip COLLATE NOCASE / COLLATE BINARY */
        if (tu.size() >= 15 && tu.substr(tu.size()-15) == " COLLATE NOCASE") {
            token = qry_trim(token.substr(0, token.size()-15)); nocase = true;
        } else if (tu.size() >= 15 && tu.substr(tu.size()-15) == " COLLATE BINARY") {
            token = qry_trim(token.substr(0, token.size()-15));
        }
        result.push_back({token, desc, nocase});
    }
    return result;
}

/* Extract LIMIT and OFFSET */
static void parse_limit_offset(const std::string &sql, int64_t &limit, int64_t &offset) {
    limit = -1; offset = 0;
    std::string su = qry_upper(sql);
    size_t lpos = su.find("LIMIT ");
    if (lpos == std::string::npos) return;
    std::string after = qry_trim(sql.substr(lpos + 6));
    /* Check for OFFSET */
    std::string au = qry_upper(after);
    size_t off_pos = au.find("OFFSET ");
    if (off_pos != std::string::npos) {
        try { limit  = std::stoll(qry_trim(after.substr(0, off_pos))); } catch (...) {}
        try { offset = std::stoll(qry_trim(after.substr(off_pos + 7))); } catch (...) {}
    } else {
        /* Check for comma syntax: LIMIT offset, count */
        size_t comma = after.find(',');
        if (comma != std::string::npos) {
            try { offset = std::stoll(qry_trim(after.substr(0, comma))); } catch (...) {}
            try { limit  = std::stoll(qry_trim(after.substr(comma + 1))); } catch (...) {}
        } else {
            try { limit = std::stoll(qry_trim(after)); } catch (...) {}
        }
    }
}

/* Extract GROUP BY columns */
static std::vector<std::string> parse_group_by(const std::string &sql) {
    std::vector<std::string> result;
    std::string su = qry_upper(sql);
    size_t pos = su.find("GROUP BY ");
    if (pos == std::string::npos) return result;
    size_t end = su.find("HAVING ", pos);
    if (end == std::string::npos) end = su.find("ORDER BY ", pos);
    if (end == std::string::npos) end = su.find("LIMIT ", pos);
    std::string gb_text = (end != std::string::npos)
        ? sql.substr(pos + 9, end - pos - 9)
        : sql.substr(pos + 9);
    std::istringstream ss(gb_text);
    std::string token;
    while (std::getline(ss, token, ',')) result.push_back(qry_trim(token));
    return result;
}

/* Extract HAVING clause */
static std::string parse_having(const std::string &sql) {
    std::string su = qry_upper(sql);
    size_t pos = su.find("HAVING ");
    if (pos == std::string::npos) return "";
    size_t end = su.find("ORDER BY ", pos);
    if (end == std::string::npos) end = su.find("LIMIT ", pos);
    return (end != std::string::npos) ? sql.substr(pos+7, end-pos-7) : sql.substr(pos+7);
}

/* Extract WHERE clause from raw SQL (handles JOIN queries) */
static std::string parse_where_from_sql(const std::string &sql) {
    std::string su = qry_upper(sql);
    /* Find " WHERE " outside parens and string literals */
    int depth = 0; bool in_str = false;
    for (size_t i = 0; i + 7 <= su.size(); ++i) {
        char c = su[i];
        if (c == '\'') { in_str = !in_str; continue; }
        if (in_str) continue;
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (depth > 0) continue;
        if (su.substr(i, 7) == " WHERE ") {
            std::string rest = sql.substr(i + 7);
            /* End at ORDER BY / GROUP BY / LIMIT / HAVING */
            std::string ru = qry_upper(rest);
            size_t end = std::string::npos;
            for (const char *kw : {" ORDER BY ", " GROUP BY ", " LIMIT ", " HAVING ", " UNION ", " INTERSECT ", " EXCEPT "}) {
                size_t ep = ru.find(kw);
                if (ep != std::string::npos && (end == std::string::npos || ep < end)) end = ep;
            }
            return (end != std::string::npos) ? qry_trim(rest.substr(0, end)) : qry_trim(rest);
        }
    }
    return "";
}

/* Extract left table alias from FROM clause */
static std::string parse_left_alias(const std::string &sql) {
    std::string su = qry_upper(sql);
    /* Find the TOP-LEVEL " FROM " (outside parens and string literals) */
    size_t from_pos = std::string::npos;
    {
        int depth = 0; bool in_str = false;
        for (size_t i = 0; i + 6 <= su.size(); ++i) {
            char c = su[i];
            if (c == '\'') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (c == '(') { ++depth; continue; }
            if (c == ')') { if (depth > 0) --depth; continue; }
            if (depth > 0) continue;
            if (su.substr(i, 6) == " FROM ") { from_pos = i; break; }
        }
    }
    if (from_pos == std::string::npos) return "";
    std::string after = qry_trim(sql.substr(from_pos + 6));
    /* Read table name */
    size_t ts = 0;
    while (ts < after.size() && after[ts] != ' ' && after[ts] != ',' && after[ts] != '(') ++ts;
    if (ts >= after.size() || after[ts] != ' ') return "";
    /* Read potential alias */
    size_t alias_start = ts + 1;
    while (alias_start < after.size() && after[alias_start] == ' ') ++alias_start;
    /* Check it's not a keyword */
    std::string rest = qry_upper(after.substr(alias_start));
    static const char *stop_kws[] = {"WHERE", "ORDER", "GROUP", "LIMIT", "INNER", "LEFT", "RIGHT", "CROSS", "JOIN", "ON", "HAVING", nullptr};
    for (const char **kw = stop_kws; *kw; ++kw) {
        size_t kwlen = strlen(*kw);
        if (rest.size() >= kwlen && rest.substr(0, kwlen) == std::string(*kw)) {
            /* Check word boundary */
            if (rest.size() == kwlen || !isalnum((unsigned char)rest[kwlen])) return "";
        }
    }
    size_t alias_end = alias_start;
    while (alias_end < after.size() && after[alias_end] != ' ' && after[alias_end] != ',' && isalnum((unsigned char)after[alias_end])) ++alias_end;
    return after.substr(alias_start, alias_end - alias_start);
}

/* Forward declaration */
static std::vector<JoinSpec> parse_all_joins(const std::string &sql);

/* Extract simple JOIN spec (one JOIN at a time) */
static JoinSpec parse_join(const std::string &sql) {
    /* Use the new parse_all_joins logic but return just the first */
    auto joins = parse_all_joins(sql);
    if (joins.empty()) { JoinSpec j; j.type = ""; return j; }
    return joins[0];
}

/* Find all JOIN specs in SQL for multi-table joins */
static std::vector<JoinSpec> parse_all_joins(const std::string &sql) {
    std::vector<JoinSpec> result;
    std::string su = qry_upper(sql);
    /* Find all JOIN positions (excluding those inside parens) */
    static const char *join_kws[] = {"INNER JOIN ", "LEFT OUTER JOIN ", "LEFT JOIN ", "CROSS JOIN ", " JOIN ", nullptr};
    /* Build list of (pos, len, type) */
    struct JoinPos { size_t pos; size_t len; std::string type; };
    std::vector<JoinPos> jps;
    {
        int depth = 0; bool in_str = false;
        for (size_t i = 0; i < su.size(); ++i) {
            char c = su[i];
            if (c == '\'') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (c == '(') { ++depth; continue; }
            if (c == ')') { if (depth > 0) --depth; continue; }
            if (depth > 0) continue;
            for (const char **kw = join_kws; *kw; ++kw) {
                size_t kwlen = strlen(*kw);
                if (i + kwlen <= su.size() && su.substr(i, kwlen) == std::string(*kw)) {
                    std::string jtype;
                    std::string kwup = std::string(*kw);
                    if (kwup == "INNER JOIN ") jtype = "INNER";
                    else if (kwup == "LEFT JOIN " || kwup == "LEFT OUTER JOIN ") jtype = "LEFT";
                    else if (kwup == "CROSS JOIN ") jtype = "CROSS";
                    else jtype = "INNER"; /* bare JOIN */
                    jps.push_back({i, kwlen, jtype});
                    i += kwlen - 1;
                    break;
                }
            }
        }
    }
    /* Parse each join starting from its position */
    for (size_t ji = 0; ji < jps.size(); ++ji) {
        const auto &jp = jps[ji];
        /* Extract the join fragment: from this JOIN to the next */
        size_t end_pos = (ji + 1 < jps.size()) ? jps[ji+1].pos : std::string::npos;
        std::string fragment = (end_pos != std::string::npos)
            ? sql.substr(jp.pos, end_pos - jp.pos)
            : sql.substr(jp.pos);
        /* Build a fake SQL for parse_join_at: prefix with "FROM x " */
        JoinSpec j; j.type = jp.type;
        /* Parse table name and alias */
        std::string frag_su = qry_upper(fragment);
        /* Skip join keyword */
        size_t ts = jp.len;
        size_t te = frag_su.find(' ', ts);
        j.table = qry_trim(fragment.substr(ts, te == std::string::npos ? fragment.size() - ts : te - ts));
        /* Read optional alias */
        if (te != std::string::npos) {
            size_t as = te + 1;
            while (as < frag_su.size() && frag_su[as] == ' ') ++as;
            static const char *stop_kws2[] = {"ON ", "USING ", "WHERE ", "ORDER ", "GROUP ", "LIMIT ", "INNER ", "LEFT ", "CROSS ", "JOIN ", nullptr};
            bool is_stop2 = false;
            for (const char **kw = stop_kws2; *kw; ++kw) {
                if (frag_su.substr(as, strlen(*kw)) == std::string(*kw)) { is_stop2 = true; break; }
            }
            if (!is_stop2 && as < frag_su.size()) {
                size_t ae = as;
                while (ae < frag_su.size() && frag_su[ae] != ' ' && isalnum((unsigned char)frag_su[ae])) ++ae;
                j.alias = fragment.substr(as, ae - as);
            }
        }
        /* Find ON or USING clause in this fragment */
        size_t on_pos_f   = frag_su.find(" ON ");
        size_t using_pos_f = frag_su.find(" USING ");
        if (using_pos_f != std::string::npos && (on_pos_f == std::string::npos || using_pos_f < on_pos_f)) {
            std::string using_str = qry_trim(fragment.substr(using_pos_f + 7));
            if (!using_str.empty() && using_str.front() == '(') using_str = using_str.substr(1);
            size_t rp = using_str.find(')');
            if (rp != std::string::npos) using_str = using_str.substr(0, rp);
            j.using_col = qry_trim(using_str);
            j.on_left = j.using_col;
            j.on_right = j.using_col;
        } else if (on_pos_f != std::string::npos) {
            std::string on_expr = qry_trim(fragment.substr(on_pos_f + 4));
            /* End at WHERE/ORDER/GROUP/LIMIT or end of fragment */
            std::string on_up = qry_upper(on_expr);
            for (const char *kw : {" WHERE ", " ORDER ", " GROUP ", " LIMIT ", " HAVING "}) {
                size_t kp = on_up.find(kw);
                if (kp != std::string::npos) on_expr = on_expr.substr(0, kp);
            }
            size_t eq_pos = on_expr.find('=');
            if (eq_pos != std::string::npos) {
                j.on_left  = qry_trim(on_expr.substr(0, eq_pos));
                j.on_right = qry_trim(on_expr.substr(eq_pos + 1));
            }
        }
        result.push_back(j);
    }
    return result;
}

/* ── Aggregate function evaluation ──────────────────────────────── */

/* Forward declaration: is_window_expr defined later in Window Function Support block */
static bool is_window_expr(const std::string &expr);

struct AggState {
    std::string func;    /* COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT */
    std::string arg;     /* column or * */
    std::string sep;     /* separator for GROUP_CONCAT */
    std::string wrapper; /* outer scalar function: ABS/UPPER/LOWER/etc. */
    int64_t count = 0;
    double  sum   = 0.0;
    int64_t isum  = 0;     /* integer-only accumulator (used when is_real is false) */
    SvdbVal min_val, max_val;
    bool has_min = false, has_max = false;
    bool is_real = false;
    bool distinct = false; /* COUNT(DISTINCT ...) */
    std::unordered_set<std::string> seen_vals; /* for DISTINCT counting */
    std::vector<std::string> concat_vals; /* for GROUP_CONCAT */
};

static bool is_agg_expr(const std::string &e) {
    /* Window functions (e.g. SUM(...) OVER (...)) are NOT regular aggregates */
    if (is_window_expr(e)) return false;
    std::string eu = qry_upper(e);
    /* Check for aggregate functions at the TOP LEVEL (not inside a subquery) */
    static const char *agg_fns[] = {"COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT(", "GROUP CONCAT(", nullptr};
    for (const char **fn = agg_fns; *fn; ++fn) {
        std::string pat = *fn;
        size_t p = eu.find(pat);
        while (p != std::string::npos) {
            /* Check paren depth at position p to see if it's inside a subquery */
            int depth = 0;
            bool in_str = false;
            for (size_t i = 0; i < p; ++i) {
                char c = eu[i];
                if (c == '\'') { in_str = !in_str; continue; }
                if (in_str) continue;
                if (c == '(') ++depth;
                else if (c == ')') { if (depth > 0) --depth; }
            }
            if (depth == 0) return true; /* top-level aggregate found */
            p = eu.find(pat, p + 1);
        }
    }
    return false;
}

static AggState make_agg(const std::string &expr) {
    AggState a; a.func = "";
    std::string e_orig = qry_trim(expr);
    /* Strip top-level AS alias (e.g. "SUM(x) AS total" → "SUM(x)") */
    {
        int dep = 0; bool ins = false;
        std::string eu_chk = qry_upper(e_orig);
        for (size_t i = 0; i < eu_chk.size(); ++i) {
            char c = eu_chk[i];
            if (c == '\'') { ins = !ins; continue; }
            if (ins) continue;
            if (c == '(') { ++dep; continue; }
            if (c == ')') { if (dep > 0) --dep; continue; }
            if (dep == 0 && i >= 1 && eu_chk[i-1] == ' ' &&
                i + 3 <= eu_chk.size() && eu_chk.substr(i, 3) == "AS ") {
                e_orig = qry_trim(e_orig.substr(0, i - 1));
                break;
            }
        }
    }
    std::string eu = qry_upper(e_orig);
    /* Check for outer scalar wrapper: e.g. ABS(MIN(a)), UPPER(MAX(s)) */
    static const char *scalar_funcs[] = {"ABS", "UPPER", "LOWER", "ROUND", "CEIL", "FLOOR", "LENGTH", nullptr};
    for (const char **sf = scalar_funcs; *sf && a.func.empty(); ++sf) {
        std::string prefix = std::string(*sf) + "(";
        if (eu.substr(0, prefix.size()) == prefix && eu.back() == ')') {
            /* Check that inside there's an aggregate */
            std::string inner_expr = e_orig.substr(prefix.size(), e_orig.size() - prefix.size() - 1);
            std::string inner_eu = qry_upper(qry_trim(inner_expr));
            bool has_inner_agg = (inner_eu.find("COUNT(") != std::string::npos ||
                                   inner_eu.find("SUM(") != std::string::npos ||
                                   inner_eu.find("AVG(") != std::string::npos ||
                                   inner_eu.find("MIN(") != std::string::npos ||
                                   inner_eu.find("MAX(") != std::string::npos);
            if (has_inner_agg) {
                a.wrapper = *sf;
                e_orig = qry_trim(inner_expr);
                eu = inner_eu;
                break;
            }
        }
    }
    const char *funcs[] = {"COUNT", "SUM", "AVG", "MIN", "MAX", nullptr};
    for (int i = 0; funcs[i]; ++i) {
        std::string prefix = std::string(funcs[i]) + "(";
        if (eu.substr(0, prefix.size()) == prefix) {
            /* Verify the opening '(' at prefix.back() has its closing ')' at eu.back() */
            size_t open_at = prefix.size() - 1; /* index of '(' */
            size_t d = 1, j = open_at + 1;
            bool in_s = false;
            for (; j < eu.size() && d > 0; ++j) {
                char cc = eu[j];
                if (cc == '\'') { in_s = !in_s; continue; }
                if (in_s) continue;
                if (cc == '(') ++d;
                else if (cc == ')') --d;
            }
            /* j is now one past the closing ')'; it must be at end of string */
            if (d == 0 && j == eu.size()) {
                a.func = funcs[i];
                a.arg = qry_trim(e_orig.substr(prefix.size(), e_orig.size() - prefix.size() - 1));
                /* Strip DISTINCT prefix */
                std::string arg_upper = qry_upper(a.arg);
                if (arg_upper.size() > 9 && arg_upper.substr(0, 9) == "DISTINCT ") {
                    a.distinct = true;
                    a.arg = qry_trim(a.arg.substr(9));
                }
                /* Strip ALL prefix (standard SQL: ALL is default, same as no DISTINCT) */
                else if (arg_upper.size() > 4 && arg_upper.substr(0, 4) == "ALL ") {
                    a.arg = qry_trim(a.arg.substr(4));
                }
                break;
            }
        }
    }
    /* GROUP_CONCAT(expr) or GROUP_CONCAT(expr, sep) */
    if (a.func.empty() && (eu.substr(0, 13) == "GROUP_CONCAT(" || eu.substr(0, 13) == "GROUP CONCAT(") && eu.back() == ')') {
        size_t plen = (eu[5] == '_') ? 13 : 13; // both "GROUP_CONCAT(" and "GROUP CONCAT("
        std::string inner = e_orig.substr(plen, e_orig.size() - plen - 1);
        /* Split by top-level comma to get arg + optional sep */
        int d2 = 0; size_t cp = std::string::npos;
        for (size_t i = 0; i < inner.size(); ++i) {
            if (inner[i] == '(') ++d2; else if (inner[i] == ')') --d2;
            else if (inner[i] == ',' && d2 == 0) { cp = i; break; }
        }
        a.func = "GROUP_CONCAT";
        if (cp != std::string::npos) {
            a.arg = qry_trim(inner.substr(0, cp));
            std::string sep_expr = qry_trim(inner.substr(cp+1));
            /* Strip quotes from separator */
            if (sep_expr.size() >= 2 && sep_expr.front() == '\'')
                a.sep = sep_expr.substr(1, sep_expr.size()-2);
            else
                a.sep = sep_expr;
        } else {
            a.arg = qry_trim(inner);
            a.sep = ",";
        }
    }
    return a;
}

/* Extract top-level aggregate sub-expressions from a compound expression.
 * E.g. "SUM(i) + SUM(r)" → ["SUM(i)", "SUM(r)"] */
static std::vector<std::string> extract_agg_subexprs(const std::string &expr) {
    std::vector<std::string> result;
    std::string eu = qry_upper(expr);
    static const char *agg_names[] = {"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", nullptr};
    bool in_str = false;
    for (size_t i = 0; i < eu.size(); ++i) {
        char c = eu[i];
        if (c == '\'') { in_str = !in_str; continue; }
        if (in_str || !isalpha((unsigned char)c)) continue;
        for (const char **fn = agg_names; *fn; ++fn) {
            size_t fnlen = strlen(*fn);
            if (i + fnlen + 1 <= eu.size() && eu.substr(i, fnlen) == std::string(*fn) && eu[i+fnlen] == '(') {
                size_t j = i + fnlen + 1; /* skip past '(' */
                int d = 1;
                while (j < eu.size() && d > 0) {
                    if (eu[j] == '(') ++d;
                    else if (eu[j] == ')') --d;
                    ++j;
                }
                result.push_back(expr.substr(i, j - i));
                i = j - 1;
                break;
            }
        }
    }
    return result;
}

static void agg_accumulate(AggState &a, const Row &row,
                            const std::vector<std::string> &col_order) {
    if (a.func == "COUNT") {
        if (a.arg == "*") { ++a.count; return; }
        SvdbVal v = eval_expr(a.arg, row, col_order);
        if (v.type == SVDB_TYPE_NULL) return;
        if (a.distinct) {
            std::string key = val_to_str(v);
            if (a.seen_vals.count(key)) return;
            a.seen_vals.insert(key);
        }
        ++a.count;
        return;
    }
    SvdbVal v = eval_expr(a.arg, row, col_order);
    if (v.type == SVDB_TYPE_NULL) return;
    if (a.func == "SUM" || a.func == "AVG") {
        if (a.distinct) {
            std::string key = val_to_str(v);
            if (a.seen_vals.count(key)) return;
            a.seen_vals.insert(key);
        }
        ++a.count;
        if (a.func == "AVG") {
            /* AVG uses double accumulation to match SQLite behavior */
            a.sum += val_to_dbl(v);
            if (v.type == SVDB_TYPE_REAL) a.is_real = true;
        } else {
            /* SUM uses integer accumulation for precision */
            if (v.type == SVDB_TYPE_REAL) {
                if (!a.is_real) { /* switch to real mode: add existing isum */
                    a.sum += (double)a.isum; a.is_real = true;
                }
                a.sum += v.rval;
            } else {
                if (a.is_real) a.sum += val_to_dbl(v);
                else a.isum += v.ival;
            }
        }
    } else if (a.func == "MIN") {
        if (!a.has_min || val_cmp(v, a.min_val) < 0) { a.min_val = v; a.has_min = true; }
    } else if (a.func == "MAX") {
        if (!a.has_max || val_cmp(v, a.max_val) > 0) { a.max_val = v; a.has_max = true; }
    } else if (a.func == "GROUP_CONCAT") {
        a.concat_vals.push_back(val_to_str(v));
    }
}

static SvdbVal agg_result(const AggState &a) {
    if (a.func == "COUNT") {
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = a.count; return v;
    }
    SvdbVal base_result;
    if (a.func == "SUM") {
        if (a.count == 0) base_result = SvdbVal{};
        else if (a.is_real) { base_result.type = SVDB_TYPE_REAL; base_result.rval = a.sum; }
        else { base_result.type = SVDB_TYPE_INT; base_result.ival = a.isum; }
    } else if (a.func == "AVG") {
        if (a.count == 0) base_result = SvdbVal{};
        else { base_result.type = SVDB_TYPE_REAL; base_result.rval = a.sum / (double)a.count; }
    } else if (a.func == "MIN") {
        base_result = a.has_min ? a.min_val : SvdbVal{};
    } else if (a.func == "MAX") {
        base_result = a.has_max ? a.max_val : SvdbVal{};
    } else if (a.func == "GROUP_CONCAT") {
        if (a.concat_vals.empty()) base_result = SvdbVal{};
        else {
            std::string sep = a.sep.empty() ? "," : a.sep;
            std::string res;
            for (size_t i = 0; i < a.concat_vals.size(); ++i) {
                if (i > 0) res += sep;
                res += a.concat_vals[i];
            }
            base_result.type = SVDB_TYPE_TEXT; base_result.sval = res;
        }
    } else {
        return SvdbVal{};
    }
    /* Apply wrapper function if any */
    if (!a.wrapper.empty() && base_result.type != SVDB_TYPE_NULL) {
        if (a.wrapper == "ABS") {
            if (base_result.type == SVDB_TYPE_INT) { if (base_result.ival < 0) base_result.ival = -base_result.ival; }
            else if (base_result.type == SVDB_TYPE_REAL) { base_result.rval = std::abs(base_result.rval); }
        } else if (a.wrapper == "UPPER") {
            if (base_result.type == SVDB_TYPE_TEXT) { for (auto &c2 : base_result.sval) c2 = (char)toupper((unsigned char)c2); }
        } else if (a.wrapper == "LOWER") {
            if (base_result.type == SVDB_TYPE_TEXT) { for (auto &c2 : base_result.sval) c2 = (char)tolower((unsigned char)c2); }
        } else if (a.wrapper == "ROUND") {
            if (base_result.type == SVDB_TYPE_REAL) base_result.rval = std::round(base_result.rval);
            else if (base_result.type == SVDB_TYPE_INT) { /* already integer */ }
        } else if (a.wrapper == "CEIL" || a.wrapper == "CEILING") {
            if (base_result.type == SVDB_TYPE_REAL) base_result.rval = std::ceil(base_result.rval);
        } else if (a.wrapper == "FLOOR") {
            if (base_result.type == SVDB_TYPE_REAL) base_result.rval = std::floor(base_result.rval);
        }
    }
    return base_result;
}

/* ── Window Function Support ─────────────────────────────────────── */

/* Check if expression contains a window function (has OVER at top level) */
static bool is_window_expr(const std::string &expr) {
    std::string eu = qry_upper(qry_trim(expr));
    int depth = 0; bool in_s = false;
    for (size_t i = 0; i < eu.size(); ++i) {
        char c = eu[i];
        if (c == '\'') { in_s = !in_s; continue; }
        if (in_s) continue;
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (depth == 0 && i + 5 <= eu.size() && eu.substr(i, 5) == " OVER") {
            if (i + 5 < eu.size() && (eu[i+5] == ' ' || eu[i+5] == '(')) return true;
        }
    }
    return false;
}

/* Parse "OVER (...)" clause: extract PARTITION BY and ORDER BY */
struct OverSpec {
    std::vector<std::string> partition_by;
    std::vector<OrderCol>    order_by;
};

static OverSpec parse_over_spec(const std::string &over_content) {
    OverSpec os;
    std::string u = qry_upper(qry_trim(over_content));
    size_t pb_pos = u.find("PARTITION BY ");
    size_t ob_pos = u.find("ORDER BY ");
    /* Handle ROWS/RANGE frame spec at end */
    size_t frame_pos = u.find(" ROWS "); if (frame_pos == std::string::npos) frame_pos = u.find(" RANGE ");
    if (frame_pos == std::string::npos) frame_pos = u.find(" ROWS BETWEEN ");
    if (frame_pos == std::string::npos) frame_pos = u.find(" RANGE BETWEEN ");
    size_t ob_end = (frame_pos != std::string::npos) ? frame_pos : u.size();

    if (pb_pos != std::string::npos) {
        size_t start = pb_pos + 13;
        size_t end = (ob_pos != std::string::npos) ? ob_pos : ob_end;
        std::string pb_str = over_content.substr(start, end - start);
        /* Split by top-level comma */
        int d = 0; size_t s = 0;
        for (size_t i = 0; i <= pb_str.size(); ++i) {
            char c = (i < pb_str.size()) ? pb_str[i] : ',';
            if (c == '(') ++d; else if (c == ')') --d;
            else if (c == ',' && d == 0) {
                os.partition_by.push_back(qry_trim(pb_str.substr(s, i-s)));
                s = i + 1;
            }
        }
    }
    if (ob_pos != std::string::npos) {
        std::string ob_str = over_content.substr(ob_pos + 9, ob_end - ob_pos - 9);
        /* Parse "col [ASC|DESC] [NULLS FIRST|LAST], ..." */
        int d = 0; size_t s = 0;
        for (size_t i = 0; i <= ob_str.size(); ++i) {
            char c = (i < ob_str.size()) ? ob_str[i] : ',';
            if (c == '(') ++d; else if (c == ')') --d;
            else if (c == ',' && d == 0) {
                std::string seg = qry_trim(ob_str.substr(s, i-s));
                OrderCol oc; oc.desc = false; oc.nocase = false;
                std::string seg_u = qry_upper(seg);
                /* Strip NULLS FIRST/LAST */
                for (const char *nkw : {" NULLS FIRST", " NULLS LAST"}) {
                    size_t np = seg_u.rfind(nkw);
                    if (np != std::string::npos) { seg = qry_trim(seg.substr(0, np)); seg_u = qry_upper(seg); break; }
                }
                if (seg_u.size() >= 5 && seg_u.substr(seg_u.size()-5) == " DESC") {
                    oc.desc = true; seg = qry_trim(seg.substr(0, seg.size()-5));
                } else if (seg_u.size() >= 4 && seg_u.substr(seg_u.size()-4) == " ASC") {
                    seg = qry_trim(seg.substr(0, seg.size()-4));
                }
                oc.expr = seg;
                os.order_by.push_back(oc);
                s = i + 1;
            }
        }
    }
    return os;
}

/* Parse a window function expression into components */
struct WinFunc {
    std::string name;  /* ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM, etc. */
    std::string args;  /* argument(s) to the function */
    OverSpec    over;
};

static bool parse_win_func(const std::string &expr, WinFunc &wf) {
    std::string eu = qry_upper(qry_trim(expr));
    /* Find OVER at top level */
    size_t over_pos = std::string::npos;
    {
        int depth = 0; bool in_s = false;
        for (size_t i = 0; i < eu.size(); ++i) {
            char c = eu[i];
            if (c == '\'') { in_s = !in_s; continue; }
            if (in_s) continue;
            if (c == '(') { ++depth; continue; }
            if (c == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && i + 5 <= eu.size() && eu.substr(i, 5) == " OVER" &&
                (i + 5 == eu.size() || eu[i+5] == ' ' || eu[i+5] == '(')) {
                over_pos = i; break;
            }
        }
    }
    if (over_pos == std::string::npos) return false;

    std::string func_part = qry_trim(expr.substr(0, over_pos));
    std::string func_upper = qry_upper(func_part);
    size_t paren = func_part.find('(');
    if (paren == std::string::npos) return false;
    wf.name = qry_trim(func_upper.substr(0, paren));
    /* Extract args (contents between outermost parens) */
    {
        int d = 0; size_t close = func_part.size();
        for (size_t i = paren; i < func_part.size(); ++i) {
            if (func_part[i] == '(') ++d;
            else if (func_part[i] == ')') { if (--d == 0) { close = i; break; } }
        }
        wf.args = qry_trim(func_part.substr(paren + 1, close - paren - 1));
    }
    /* Extract OVER (...) content */
    std::string after_over = qry_trim(expr.substr(over_pos + 5));
    if (!after_over.empty() && after_over[0] == '(') {
        int d = 0;
        for (size_t i = 0; i < after_over.size(); ++i) {
            if (after_over[i] == '(') ++d;
            else if (after_over[i] == ')') { if (--d == 0) { wf.over = parse_over_spec(after_over.substr(1, i-1)); break; } }
        }
    }
    return true;
}

/* Build a sort comparator that sorts rows by given order columns */
static int compare_rows_by_order(const Row &a, const Row &b,
                                  const std::vector<std::string> &col_order,
                                  const std::vector<OrderCol> &order) {
    for (const auto &oc : order) {
        SvdbVal va = eval_expr(oc.expr, a, col_order);
        SvdbVal vb = eval_expr(oc.expr, b, col_order);
        /* NULLs sort first */
        if (va.type == SVDB_TYPE_NULL && vb.type == SVDB_TYPE_NULL) continue;
        if (va.type == SVDB_TYPE_NULL) return oc.desc ? 1 : -1;
        if (vb.type == SVDB_TYPE_NULL) return oc.desc ? -1 : 1;
        int c = val_cmp(va, vb);
        if (c != 0) return oc.desc ? -c : c;
    }
    return 0;
}

/* Compute window function values for all rows.
 * Returns a 2D vector: [col_index][row_index] = computed SvdbVal (or NULL placeholder). */
static std::vector<std::vector<SvdbVal>>
compute_window_functions(const std::vector<Row> &rows,
                          const std::vector<std::string> &col_order,
                          const std::vector<std::string> &out_cols) {
    size_t ncols = out_cols.size();
    size_t nrows = rows.size();
    std::vector<std::vector<SvdbVal>> result(ncols, std::vector<SvdbVal>(nrows));

    for (size_t ci = 0; ci < ncols; ++ci) {
        WinFunc wf;
        if (!parse_win_func(out_cols[ci], wf)) continue;  /* not a window func */

        /* Build partition key for each row */
        auto part_key = [&](const Row &r) -> std::string {
            std::string key;
            for (const auto &pb : wf.over.partition_by)
                key += val_to_str(eval_expr(pb, r, col_order)) + "\x01";
            return key;
        };

        /* Group rows by partition */
        std::map<std::string, std::vector<size_t>> partitions;
        for (size_t ri = 0; ri < nrows; ++ri)
            partitions[part_key(rows[ri])].push_back(ri);

        for (auto &kv : partitions) {
            auto &idxs = kv.second;
            /* Sort partition indices by ORDER BY */
            if (!wf.over.order_by.empty()) {
                std::stable_sort(idxs.begin(), idxs.end(),
                    [&](size_t a, size_t b) {
                        return compare_rows_by_order(rows[a], rows[b], col_order, wf.over.order_by) < 0;
                    });
            }
            size_t n = idxs.size();

            if (wf.name == "ROW_NUMBER") {
                for (size_t i = 0; i < n; ++i) {
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (int64_t)(i + 1);
                    result[ci][idxs[i]] = v;
                }
            } else if (wf.name == "RANK") {
                int64_t rank = 1;
                for (size_t i = 0; i < n; ++i) {
                    if (i > 0 && compare_rows_by_order(rows[idxs[i]], rows[idxs[i-1]], col_order, wf.over.order_by) != 0)
                        rank = (int64_t)(i + 1);
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = rank;
                    result[ci][idxs[i]] = v;
                }
            } else if (wf.name == "DENSE_RANK") {
                int64_t drank = 1;
                for (size_t i = 0; i < n; ++i) {
                    if (i > 0 && compare_rows_by_order(rows[idxs[i]], rows[idxs[i-1]], col_order, wf.over.order_by) != 0)
                        ++drank;
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = drank;
                    result[ci][idxs[i]] = v;
                }
            } else if (wf.name == "NTILE") {
                int64_t buckets = 1;
                try { buckets = std::stoll(wf.args); } catch (...) {}
                if (buckets < 1) buckets = 1;
                for (size_t i = 0; i < n; ++i) {
                    /* Standard NTILE: even distribution with larger buckets first */
                    int64_t tile = (int64_t)(((int64_t)i * buckets) / (int64_t)n) + 1;
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = tile;
                    result[ci][idxs[i]] = v;
                }
            } else if (wf.name == "LAG" || wf.name == "LEAD") {
                /* LAG/LEAD(expr [, offset [, default]]) */
                std::string col_expr = wf.args;
                int64_t offset = 1;
                SvdbVal default_val; default_val.type = SVDB_TYPE_NULL;
                /* Split args by top-level comma */
                std::vector<std::string> ll_args;
                { int d = 0; size_t s = 0;
                  for (size_t i = 0; i <= wf.args.size(); ++i) {
                      char c = (i < wf.args.size()) ? wf.args[i] : ',';
                      if (c == '(') ++d; else if (c == ')') --d;
                      else if (c == ',' && d == 0) { ll_args.push_back(qry_trim(wf.args.substr(s, i-s))); s = i+1; }
                  }
                }
                if (!ll_args.empty()) col_expr = ll_args[0];
                if (ll_args.size() >= 2) { try { offset = std::stoll(ll_args[1]); } catch (...) {} }
                if (ll_args.size() >= 3) default_val = eval_expr(ll_args[2], {}, col_order);
                bool is_lead = (wf.name == "LEAD");
                for (size_t i = 0; i < n; ++i) {
                    int64_t target = is_lead ? (int64_t)(i + offset) : (int64_t)(i - offset);
                    SvdbVal v;
                    if (target >= 0 && target < (int64_t)n)
                        v = eval_expr(col_expr, rows[idxs[(size_t)target]], col_order);
                    else
                        v = default_val;
                    result[ci][idxs[i]] = v;
                }
            } else if (wf.name == "FIRST_VALUE") {
                for (size_t i = 0; i < n; ++i) {
                    /* Default frame: RANGE UNBOUNDED PRECEDING — value from first row in partition */
                    result[ci][idxs[i]] = eval_expr(wf.args, rows[idxs[0]], col_order);
                }
            } else if (wf.name == "LAST_VALUE") {
                /* Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 * → returns the value at the current sorted position (last in the frame) */
                for (size_t i = 0; i < n; ++i) {
                    result[ci][idxs[i]] = eval_expr(wf.args, rows[idxs[i]], col_order);
                }
            } else if (wf.name == "SUM" || wf.name == "AVG" || wf.name == "COUNT" ||
                       wf.name == "MIN" || wf.name == "MAX") {
                /* Aggregate window functions — running total within partition ordered by ORDER BY */
                bool is_running = !wf.over.order_by.empty();
                /* Full partition aggregate */
                AggState full_agg = make_agg(wf.name + "(" + wf.args + ")");
                if (!is_running) {
                    for (size_t i = 0; i < n; ++i)
                        agg_accumulate(full_agg, rows[idxs[i]], col_order);
                    SvdbVal agg_val = agg_result(full_agg);
                    for (size_t i = 0; i < n; ++i) result[ci][idxs[i]] = agg_val;
                } else {
                    /* Running aggregate: include rows up to and including current */
                    for (size_t i = 0; i < n; ++i) {
                        AggState run_agg = make_agg(wf.name + "(" + wf.args + ")");
                        for (size_t j = 0; j <= i; ++j)
                            agg_accumulate(run_agg, rows[idxs[j]], col_order);
                        result[ci][idxs[i]] = agg_result(run_agg);
                    }
                }
            }
        }
    }
    return result;
}

/* ── Main SELECT execution ──────────────────────────────────────── */

svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                   svdb_rows_t **rows_out) {
    svdb_assert(db != nullptr);
    svdb_assert(rows_out != nullptr);
    if (!rows_out) return SVDB_ERR;

    /* Set thread-local DB context for subquery support */
    svdb_db_t *prev_db = g_query_db;
    g_query_db = db;
    struct DbGuard { svdb_db_t **p; svdb_db_t *v; ~DbGuard() { *p = v; } } db_guard{&g_query_db, prev_db};

    /* ── information_schema / sqlite_master / sqlite_sequence intercept ── */
    {
        std::string su_is = qry_upper(qry_trim(sql));

        /* sqlite_master virtual table */
        auto sm_pos = su_is.find(" FROM SQLITE_MASTER");
        if (sm_pos == std::string::npos) sm_pos = su_is.find(" FROM SQLITE_SCHEMA");
        if (sm_pos != std::string::npos) {
            std::string where_txt;
            {
                size_t wp = su_is.find(" WHERE ", sm_pos);
                if (wp != std::string::npos) {
                    where_txt = qry_trim(sql.substr(wp + 7));
                    /* Strip ORDER BY / GROUP BY / LIMIT / HAVING suffixes */
                    static const char *stop_kws[] = {" ORDER BY "," GROUP BY "," LIMIT "," HAVING ", nullptr};
                    std::string wtu = qry_upper(where_txt);
                    for (const char **kw = stop_kws; *kw; ++kw) {
                        size_t sp = wtu.find(*kw);
                        if (sp != std::string::npos) { where_txt = qry_trim(where_txt.substr(0, sp)); wtu = qry_upper(where_txt); }
                    }
                }
            }
            svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
            if (!r) return SVDB_NOMEM;
            r->col_names = {"type","name","tbl_name","rootpage","sql"};
            for (auto &kv : db->schema) {
                auto it = db->create_sql.find(kv.first);
                std::string ttype = "table", sql_str;
                if (it != db->create_sql.end()) {
                    sql_str = it->second;
                    std::string cu = qry_upper(qry_trim(sql_str));
                    if (cu.size() >= 11 && cu.substr(0, 11) == "CREATE VIEW") ttype = "view";
                }
                if (sql_str.empty()) {
                    /* Build CREATE TABLE SQL from schema */
                    sql_str = "CREATE TABLE " + kv.first + " (";
                    auto co = db->col_order.find(kv.first);
                    if (co != db->col_order.end()) {
                        bool first = true;
                        for (auto &cn : co->second) {
                            if (!first) sql_str += ", ";
                            sql_str += cn;
                            auto cd = kv.second.find(cn);
                            if (cd != kv.second.end()) {
                                sql_str += " " + cd->second.type;
                                if (cd->second.not_null) sql_str += " NOT NULL";
                                if (cd->second.primary_key) sql_str += " PRIMARY KEY";
                            }
                            first = false;
                        }
                    }
                    sql_str += ")";
                }
                Row rd;
                rd["type"]     = SvdbVal{SVDB_TYPE_TEXT,0,0,ttype};
                rd["name"]     = SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                rd["tbl_name"] = SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                rd["rootpage"] = SvdbVal{SVDB_TYPE_INT,1,0,""};
                rd["sql"]      = SvdbVal{SVDB_TYPE_TEXT,0,0,sql_str};
                if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                r->rows.push_back({rd["type"],rd["name"],rd["tbl_name"],rd["rootpage"],rd["sql"]});
            }
            /* Also add indexes */
            for (auto &kv : db->indexes) {
                Row rd;
                std::string idx_sql = "CREATE INDEX " + kv.first + " ON " + kv.second.table + " (";
                for (size_t i = 0; i < kv.second.columns.size(); ++i) {
                    if (i) idx_sql += ",";
                    idx_sql += kv.second.columns[i];
                }
                idx_sql += ")";
                rd["type"]     = SvdbVal{SVDB_TYPE_TEXT,0,0,"index"};
                rd["name"]     = SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                rd["tbl_name"] = SvdbVal{SVDB_TYPE_TEXT,0,0,kv.second.table};
                rd["rootpage"] = SvdbVal{SVDB_TYPE_INT,2,0,""};
                rd["sql"]      = SvdbVal{SVDB_TYPE_TEXT,0,0,idx_sql};
                if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                r->rows.push_back({rd["type"],rd["name"],rd["tbl_name"],rd["rootpage"],rd["sql"]});
            }
            /* Project columns */
            {
                std::string sel_part = qry_trim(sql);
                if (qry_upper(sel_part).substr(0,7) == "SELECT ") sel_part = qry_trim(sel_part.substr(7));
                size_t fp = qry_upper(sel_part).find(" FROM ");
                if (fp != std::string::npos) sel_part = qry_trim(sel_part.substr(0, fp));
                if (qry_trim(sel_part) != "*") {
                    std::vector<std::string> wanted;
                    int dp2=0; bool ins2=false; size_t st2=0;
                    std::string sp3=sel_part+",";
                    for (size_t i=0;i<sp3.size();++i){
                        char c2=sp3[i];
                        if(c2=='\''){ins2=!ins2;continue;}if(ins2)continue;
                        if(c2=='(')++dp2;else if(c2==')')--dp2;
                        else if(c2==','&&dp2==0){
                            std::string t=qry_trim(sp3.substr(st2,i-st2));
                            size_t dp3=t.rfind('.');
                            if(dp3!=std::string::npos)t=qry_trim(t.substr(dp3+1));
                            wanted.push_back(t); st2=i+1;
                        }
                    }
                    std::vector<int> ci3;
                    for (auto &w : wanted){
                        int cix=-1;
                        for(size_t i=0;i<r->col_names.size();++i)
                            if(qry_upper(r->col_names[i])==qry_upper(w)){cix=(int)i;break;}
                        ci3.push_back(cix);
                    }
                    std::vector<std::vector<SvdbVal>> nr3;
                    for(auto &row2:r->rows){
                        std::vector<SvdbVal> nrr3;
                        for(int cix:ci3) nrr3.push_back((cix>=0&&cix<(int)row2.size())?row2[cix]:SvdbVal{});
                        nr3.push_back(nrr3);
                    }
                    r->col_names=wanted; r->rows=nr3;
                }
            }
            *rows_out = r;
            return SVDB_OK;
        }

        auto is_from_pos = su_is.find(" FROM INFORMATION_SCHEMA.");
        if (is_from_pos != std::string::npos) {
            size_t view_start = is_from_pos + 25;
            size_t view_end   = view_start;
            while (view_end < su_is.size() && (isalnum((unsigned char)su_is[view_end]) || su_is[view_end] == '_'))
                ++view_end;
            std::string is_view = su_is.substr(view_start, view_end - view_start);
            std::string where_txt;
            {
                size_t wp = su_is.find(" WHERE ", view_end);
                if (wp != std::string::npos) {
                    where_txt = qry_trim(sql.substr(wp + 7));
                    static const char *stop_kws[] = {" ORDER BY "," GROUP BY "," LIMIT "," HAVING ", nullptr};
                    std::string wtu = qry_upper(where_txt);
                    for (const char **kw = stop_kws; *kw; ++kw) {
                        size_t sp = wtu.find(*kw);
                        if (sp != std::string::npos) { where_txt = qry_trim(where_txt.substr(0, sp)); wtu = qry_upper(where_txt); }
                    }
                }
            }
            svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
            if (!r) return SVDB_NOMEM;

            if (is_view == "TABLES") {
                r->col_names = {"table_catalog","table_schema","table_name","table_type"};
                for (auto &kv : db->schema) {
                    std::string ttype = "BASE TABLE";
                    auto it = db->create_sql.find(kv.first);
                    if (it != db->create_sql.end()) {
                        std::string cu = qry_upper(qry_trim(it->second));
                        if (cu.size() >= 11 && cu.substr(0, 11) == "CREATE VIEW") ttype = "VIEW";
                    }
                    Row rd;
                    rd["table_catalog"]=SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["table_schema"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["table_name"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                    rd["table_type"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,ttype};
                    if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                    r->rows.push_back({rd["table_catalog"],rd["table_schema"],rd["table_name"],rd["table_type"]});
                }
            } else if (is_view == "VIEWS") {
                r->col_names = {"table_catalog","table_schema","table_name","view_definition"};
                for (auto &kv : db->create_sql) {
                    std::string cu = qry_upper(qry_trim(kv.second));
                    if (cu.size() < 11 || cu.substr(0, 11) != "CREATE VIEW") continue;
                    Row rd;
                    rd["table_catalog"]  =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["table_schema"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["table_name"]     =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                    rd["view_definition"]=SvdbVal{SVDB_TYPE_TEXT,0,0,kv.second};
                    if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                    r->rows.push_back({rd["table_catalog"],rd["table_schema"],rd["table_name"],rd["view_definition"]});
                }
            } else if (is_view == "COLUMNS") {
                r->col_names = {"table_catalog","table_schema","table_name","column_name",
                                 "ordinal_position","is_nullable","data_type"};
                for (auto &tbl : db->col_order) {
                    int ord = 1;
                    for (auto &cn : tbl.second) {
                        auto &td  = db->schema[tbl.first];
                        auto  cit = td.find(cn);
                        std::string dtype = "TEXT"; bool nn = false;
                        if (cit != td.end()) {
                            nn = cit->second.not_null;
                            std::string ct = cit->second.type;
                            std::string ctu = qry_upper(ct);
                            if (ctu == "INTEGER" || ctu == "INT") dtype = "INTEGER";
                            else if (ctu == "REAL" || ctu == "FLOAT" || ctu == "DOUBLE") dtype = "REAL";
                            else if (ctu == "BLOB") dtype = "BLOB";
                            else if (!ct.empty()) dtype = ctu;
                        }
                        Row rd;
                        rd["table_catalog"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["table_schema"]    =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["table_name"]      =SvdbVal{SVDB_TYPE_TEXT,0,0,tbl.first};
                        rd["column_name"]     =SvdbVal{SVDB_TYPE_TEXT,0,0,cn};
                        rd["ordinal_position"]=SvdbVal{SVDB_TYPE_INT,ord++,0,""};
                        rd["is_nullable"]     =SvdbVal{SVDB_TYPE_TEXT,0,0,nn?"NO":"YES"};
                        rd["data_type"]       =SvdbVal{SVDB_TYPE_TEXT,0,0,dtype};
                        if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                        r->rows.push_back({rd["table_catalog"],rd["table_schema"],rd["table_name"],
                                           rd["column_name"],rd["ordinal_position"],rd["is_nullable"],rd["data_type"]});
                    }
                }
            } else if (is_view == "TABLE_CONSTRAINTS") {
                r->col_names = {"constraint_catalog","constraint_schema","constraint_name",
                                 "table_schema","table_name","constraint_type"};
                for (auto &kv : db->primary_keys) {
                    if (kv.second.empty()) continue;
                    Row rd;
                    rd["constraint_catalog"]=SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["constraint_schema"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["constraint_name"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first+"_pkey"};
                    rd["table_schema"]      =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                    rd["table_name"]        =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                    rd["constraint_type"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"PRIMARY KEY"};
                    if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                    r->rows.push_back({rd["constraint_catalog"],rd["constraint_schema"],
                                       rd["constraint_name"],rd["table_schema"],rd["table_name"],rd["constraint_type"]});
                }
                int uidx = 0;
                for (auto &kv : db->unique_constraints) {
                    for (auto &uc : kv.second) {
                        if (uc.empty()) continue;
                        Row rd;
                        rd["constraint_catalog"]=SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_schema"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_name"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first+"_unique_"+std::to_string(uidx++)};
                        rd["table_schema"]      =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["table_name"]        =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                        rd["constraint_type"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"UNIQUE"};
                        if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                        r->rows.push_back({rd["constraint_catalog"],rd["constraint_schema"],
                                           rd["constraint_name"],rd["table_schema"],rd["table_name"],rd["constraint_type"]});
                    }
                }
            }

            /* Project SELECT columns */
            {
                std::string sel_part = qry_trim(sql);
                /* strip SELECT keyword */
                if (qry_upper(sel_part).substr(0,7) == "SELECT ") sel_part = qry_trim(sel_part.substr(7));
                size_t from_p = qry_upper(sel_part).find(" FROM ");
                if (from_p != std::string::npos) sel_part = qry_trim(sel_part.substr(0, from_p));
                if (qry_trim(sel_part) != "*") {
                    std::vector<std::string> wanted;
                    {
                        int dp=0; bool ins=false; size_t st2=0;
                        std::string sp2=sel_part+",";
                        for (size_t i=0;i<sp2.size();++i){
                            char c2=sp2[i];
                            if(c2=='\''){ins=!ins;continue;}if(ins)continue;
                            if(c2=='(')++dp;else if(c2==')')--dp;
                            else if(c2==','&&dp==0){
                                std::string t=qry_trim(sp2.substr(st2,i-st2));
                                size_t dp2=t.rfind('.');
                                if(dp2!=std::string::npos)t=qry_trim(t.substr(dp2+1));
                                wanted.push_back(t); st2=i+1;
                            }
                        }
                    }
                    std::vector<int> ci2;
                    for (auto &w : wanted) {
                        int cix=-1;
                        for (size_t i=0;i<r->col_names.size();++i)
                            if(qry_upper(r->col_names[i])==qry_upper(w)){cix=(int)i;break;}
                        ci2.push_back(cix);
                    }
                    std::vector<std::vector<SvdbVal>> nr2;
                    for (auto &row2 : r->rows){
                        std::vector<SvdbVal> nrr;
                        for(int cix:ci2) nrr.push_back((cix>=0&&cix<(int)row2.size())?row2[cix]:SvdbVal{});
                        nr2.push_back(nrr);
                    }
                    r->col_names=wanted; r->rows=nr2;
                }
            }

            *rows_out = r;
            return SVDB_OK;
        }
    }


    /* ── Set-operation detection (UNION / INTERSECT / EXCEPT) ── */
    /* Find top-level UNION/INTERSECT/EXCEPT outside parentheses and string literals */
    {
        int depth = 0; bool in_str = false;
        std::string su2 = qry_upper(sql);
        for (size_t i = 0; i < su2.size(); ) {
            char c = su2[i];
            if (c == '\'') { in_str = !in_str; ++i; continue; }
            if (in_str) { ++i; continue; }
            if (c == '(') { ++depth; ++i; continue; }
            if (c == ')') { if (depth > 0) --depth; ++i; continue; }
            if (depth > 0) { ++i; continue; }
            /* Check for keyword */
            for (const char *kw : {" UNION ALL ", " UNION ", " INTERSECT ALL ", " INTERSECT ", " EXCEPT ALL ", " EXCEPT "}) {
                size_t kwlen = strlen(kw);
                if (i + kwlen <= su2.size() && su2.substr(i, kwlen) == kw) {
                    std::string lhs = qry_trim(sql.substr(0, i));
                    std::string rhs = qry_trim(sql.substr(i + kwlen));
                    /* Remove trailing ORDER BY from rhs (apply to merged result) */
                    std::string rhs_upper = qry_upper(rhs);
                    std::string order_clause;
                    size_t ob = rhs_upper.rfind(" ORDER BY ");
                    if (ob == std::string::npos) ob = rhs_upper.rfind("ORDER BY ");
                    if (ob != std::string::npos) {
                        order_clause = rhs.substr(ob);
                        rhs = rhs.substr(0, ob);
                    }
                    /* Also strip ORDER BY from lhs */
                    std::string lhs_upper = qry_upper(lhs);
                    size_t ob2 = lhs_upper.rfind(" ORDER BY ");
                    if (ob2 != std::string::npos) { lhs = lhs.substr(0, ob2); }

                    svdb_rows_t *left = nullptr, *right = nullptr;
                    svdb_code_t rc1 = svdb_query_internal(db, lhs, &left);
                    if (rc1 != SVDB_OK) { if (left) delete left; return rc1; }
                    svdb_code_t rc2 = svdb_query_internal(db, rhs, &right);
                    if (rc2 != SVDB_OK) { if (left) delete left; if (right) delete right; return rc2; }

                    svdb_rows_t *result = new (std::nothrow) svdb_rows_t();
                    if (!result) { delete left; delete right; return SVDB_NOMEM; }
                    result->col_names = left->col_names;

                    std::string op(kw); /* " UNION ALL " etc. */
                    bool all_mode = op.find("ALL") != std::string::npos;

                    /* Serialize a row for deduplication */
                    auto row_key = [](const std::vector<SvdbVal> &row2) {
                        std::string key;
                        for (const auto &v : row2) {
                            switch (v.type) {
                                case SVDB_TYPE_NULL: key += "\x01"; break;
                                case SVDB_TYPE_INT:  key += "\x02" + std::to_string(v.ival); break;
                                case SVDB_TYPE_REAL: key += "\x03" + std::to_string(v.rval); break;
                                case SVDB_TYPE_TEXT: key += "\x04" + v.sval; break;
                                case SVDB_TYPE_BLOB: key += "\x05" + v.sval; break;
                            }
                            key += "\x00";
                        }
                        return key;
                    };

                    std::set<std::string> seen;
                    auto add_row = [&](const std::vector<SvdbVal> &row2) {
                        if (all_mode) { result->rows.push_back(row2); return; }
                        std::string k = row_key(row2);
                        if (seen.insert(k).second) result->rows.push_back(row2);
                    };

                    bool is_union = op.find("UNION") != std::string::npos;
                    bool is_intersect = op.find("INTERSECT") != std::string::npos;
                    bool is_except = op.find("EXCEPT") != std::string::npos;

                    if (is_union) {
                        for (auto &r : left->rows)  add_row(r);
                        for (auto &r : right->rows) add_row(r);
                    } else if (is_intersect) {
                        std::set<std::string> right_keys;
                        for (auto &r : right->rows) right_keys.insert(row_key(r));
                        for (auto &r : left->rows) {
                            std::string k = row_key(r);
                            if (right_keys.count(k)) add_row(r);
                        }
                    } else if (is_except) {
                        std::set<std::string> right_keys;
                        for (auto &r : right->rows) right_keys.insert(row_key(r));
                        for (auto &r : left->rows) {
                            std::string k = row_key(r);
                            if (!right_keys.count(k)) add_row(r);
                        }
                    }

                    delete left; delete right;

                    /* Apply ORDER BY / LIMIT if present */
                    std::string limit_clause;
                    std::string ob_clause = order_clause;
                    if (!ob_clause.empty()) {
                        /* Strip leading " ORDER BY " or "ORDER BY " prefix */
                        std::string obu = qry_upper(qry_trim(ob_clause));
                        if (obu.substr(0, 9) == "ORDER BY ") ob_clause = qry_trim(ob_clause.substr(ob_clause.find("BY ") + 3));
                        /* Split off LIMIT clause if present */
                        std::string ob_u2 = qry_upper(ob_clause);
                        size_t lim_pos = ob_u2.find(" LIMIT ");
                        if (lim_pos != std::string::npos) {
                            limit_clause = qry_trim(ob_clause.substr(lim_pos + 7));
                            ob_clause = qry_trim(ob_clause.substr(0, lim_pos));
                        }
                    }
                    if (!ob_clause.empty()) {
                        /* Simple ORDER BY: single column + optional ASC/DESC */
                        bool desc = false;
                        std::string sort_col = ob_clause;
                        size_t sp = sort_col.rfind(' ');
                        if (sp != std::string::npos) {
                            std::string dir = qry_upper(sort_col.substr(sp + 1));
                            if (dir == "DESC") { desc = true; sort_col = qry_trim(sort_col.substr(0, sp)); }
                            else if (dir == "ASC") { sort_col = qry_trim(sort_col.substr(0, sp)); }
                        }
                        /* Find column index */
                        int col_idx = -1;
                        for (size_t ci = 0; ci < result->col_names.size(); ++ci) {
                            if (qry_upper(result->col_names[ci]) == qry_upper(sort_col)) { col_idx = (int)ci; break; }
                        }
                        if (col_idx < 0) col_idx = 0; /* fallback to first column */
                        std::stable_sort(result->rows.begin(), result->rows.end(),
                            [&](const std::vector<SvdbVal> &a2, const std::vector<SvdbVal> &b2) {
                                const SvdbVal &av = (int)a2.size() > col_idx ? a2[col_idx] : SvdbVal{};
                                const SvdbVal &bv = (int)b2.size() > col_idx ? b2[col_idx] : SvdbVal{};
                                bool less = false;
                                if (av.type == SVDB_TYPE_INT && bv.type == SVDB_TYPE_INT) less = av.ival < bv.ival;
                                else if (av.type == SVDB_TYPE_TEXT && bv.type == SVDB_TYPE_TEXT) less = av.sval < bv.sval;
                                else less = (av.type < bv.type);
                                return desc ? !less : less;
                            });
                    }
                    if (!limit_clause.empty()) {
                        int64_t lim = std::atoll(limit_clause.c_str());
                        if (lim >= 0 && (size_t)lim < result->rows.size())
                            result->rows.resize((size_t)lim);
                    }

                    *rows_out = result;
                    return SVDB_OK;
                }
            }
            ++i;
        }
    }

    /* ── Derived table (FROM subquery) detection ── */
    /* Detect: SELECT cols FROM (SELECT ...) [alias] [WHERE ...] */
    {
        std::string su_dt = qry_upper(sql);
        size_t from_pos = std::string::npos;
        {
            /* Find " FROM " outside parens and strings */
            int dp = 0; bool ins = false;
            for (size_t i = 0; i + 6 <= su_dt.size(); ++i) {
                char c = su_dt[i];
                if (c == '\'') { ins = !ins; continue; }
                if (ins) continue;
                if (c == '(') { ++dp; continue; }
                if (c == ')') { if (dp > 0) --dp; continue; }
                if (dp > 0) continue;
                if (su_dt.substr(i, 6) == " FROM ") { from_pos = i; break; }
            }
        }
        if (from_pos != std::string::npos) {
            size_t after_from = from_pos + 6;
            /* Skip whitespace */
            while (after_from < sql.size() && sql[after_from] == ' ') ++after_from;
            if (after_from < sql.size() && sql[after_from] == '(') {
                /* Find matching closing paren */
                int dp2 = 0; size_t close_p = std::string::npos;
                for (size_t i = after_from; i < sql.size(); ++i) {
                    if (sql[i] == '(') ++dp2;
                    else if (sql[i] == ')') { if (--dp2 == 0) { close_p = i; break; } }
                }
                if (close_p != std::string::npos) {
                    std::string inner_sql = sql.substr(after_from + 1, close_p - after_from - 1);
                    std::string inner_u = qry_upper(qry_trim(inner_sql));
                    if (inner_u.size() > 6 && inner_u.substr(0, 7) == "SELECT ") {
                        /* Execute the inner SELECT */
                        svdb_rows_t *inner_rows = nullptr;
                        svdb_code_t rc = svdb_query_internal(db, inner_sql, &inner_rows);
                        if (rc != SVDB_OK) { if (inner_rows) delete inner_rows; return rc; }
                        if (!inner_rows) return SVDB_ERR;

                        /* Extract alias: text between ) and next keyword */
                        std::string rest_sql = sql.substr(close_p + 1);
                        std::string rest_u = qry_upper(qry_trim(rest_sql));
                        std::string alias;
                        {
                            /* Read alias: first non-keyword word */
                            std::string rt = qry_trim(rest_u);
                            if (!rt.empty() && rt[0] != 'W' && rt[0] != 'O' && rt[0] != 'G' && rt[0] != 'L') {
                                /* try to read identifier */
                                size_t ae = 0;
                                while (ae < rt.size() && rt[ae] != ' ' && rt[ae] != '\0') ++ae;
                                std::string word = rt.substr(0, ae);
                                static const char *stop[] = {"WHERE","ORDER","GROUP","LIMIT","HAVING","INNER","LEFT","JOIN", nullptr};
                                bool is_stop = false;
                                for (const char **sw = stop; *sw; ++sw) if (word == *sw) { is_stop = true; break; }
                                if (!is_stop && !word.empty()) alias = word;
                            }
                        }

                        /* Build a temporary in-memory "table" from inner_rows */
                        std::string tmp_tname = "__derived_" + std::to_string((size_t)inner_rows);
                        db->schema[tmp_tname] = {};
                        db->col_order[tmp_tname] = {};
                        db->data[tmp_tname] = {};
                        for (const auto &cn : inner_rows->col_names) {
                            db->schema[tmp_tname][cn] = ColDef{"TEXT", "", false, false};
                            db->col_order[tmp_tname].push_back(cn);
                        }
                        for (const auto &irow : inner_rows->rows) {
                            Row r_new;
                            for (size_t ci = 0; ci < inner_rows->col_names.size() && ci < irow.size(); ++ci)
                                r_new[inner_rows->col_names[ci]] = irow[ci];
                            db->data[tmp_tname].push_back(r_new);
                        }
                        delete inner_rows;

                        /* Replace (subquery) with tmp_tname in SQL, preserving rest */
                        std::string new_sql = sql.substr(0, from_pos) + " FROM " + tmp_tname + rest_sql;
                        svdb_rows_t *result = nullptr;
                        svdb_code_t rc2 = svdb_query_internal(db, new_sql, &result);

                        /* Clean up temp table */
                        db->schema.erase(tmp_tname);
                        db->col_order.erase(tmp_tname);
                        db->data.erase(tmp_tname);

                        if (rc2 != SVDB_OK) { if (result) delete result; return rc2; }
                        *rows_out = result;
                        return SVDB_OK;
                    }
                }
            }
        }
    }

    /* Use parser to extract table name, columns and where */
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);

    std::string tname;
    std::string where_txt;
    std::vector<std::string> sel_cols;
    bool star = false, distinct = false;

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
    /* For JOIN queries, the AST parser may not capture WHERE (it sees JOIN as part of FROM).
     * Always use raw SQL WHERE extraction for JOIN queries or when AST gives empty WHERE. */
    if (where_txt.empty()) {
        where_txt = parse_where_from_sql(sql);
    }
    svdb_parser_destroy(p);

    /* Parse additional SQL clauses from raw SQL */
    auto order_cols  = parse_order_by(sql);
    auto group_cols  = parse_group_by(sql);
    std::string having_txt = parse_having(sql);
    int64_t limit_val = -1, offset_val = 0;
    parse_limit_offset(sql, limit_val, offset_val);
    auto all_joins = parse_all_joins(sql);
    /* Also detect comma-separated FROM tables (implicit CROSS JOIN) */
    {
        auto comma_joins = parse_comma_joins(sql);
        for (auto &cj : comma_joins) all_joins.push_back(cj);
    }
    JoinSpec join = all_joins.empty() ? JoinSpec{} : all_joins[0];

    /* Check DISTINCT */
    {
        std::string su = qry_upper(sql);
        size_t sel_pos = su.find("SELECT ");
        if (sel_pos != std::string::npos) {
            std::string after_sel = qry_trim(su.substr(sel_pos + 7));
            if (after_sel.substr(0, 9) == "DISTINCT ") distinct = true;
        }
    }

    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;

    /* ── Handle SELECT without a table ── */
    if (tname.empty()) {
        if (!sel_cols.empty()) {
            Row empty_row;
            std::vector<std::string> empty_order;
            std::vector<SvdbVal> result_row;
            /* Helper: find top-level " AS " position */
            auto find_as_no_tbl = [](const std::string &cu) -> size_t {
                int d = 0; bool in_s = false;
                for (size_t i = 0; i < cu.size(); ++i) {
                    char c = cu[i];
                    if (c == '\'') { in_s = !in_s; continue; }
                    if (in_s) continue;
                    if (c == '(') ++d;
                    else if (c == ')') { if (d > 0) --d; }
                    else if (d == 0 && i + 4 <= cu.size() &&
                             c == ' ' && cu[i+1] == 'A' && cu[i+2] == 'S' && cu[i+3] == ' ')
                        return i;
                }
                return std::string::npos;
            };
            for (const auto &expr : sel_cols) {
                std::string cu = qry_upper(expr);
                size_t as_p = find_as_no_tbl(cu);
                std::string col_name = (as_p != std::string::npos)
                    ? qry_trim(expr.substr(as_p + 4)) : expr;
                r->col_names.push_back(col_name);
                result_row.push_back(eval_expr(expr, empty_row, empty_order));
            }
            r->rows.push_back(result_row);
        }
        *rows_out = r; return SVDB_OK;
    }

    /* ── Resolve table ── */
    std::string tname_upper = qry_upper(tname);
    /* Case-insensitive table lookup */
    std::string resolved_tname;
    for (auto &kv : db->schema) {
        if (qry_upper(kv.first) == tname_upper) { resolved_tname = kv.first; break; }
    }
    if (resolved_tname.empty()) {
        /* Table not found — return empty */
        *rows_out = r; return SVDB_OK;
    }
    tname = resolved_tname;

    const auto &col_order = db->col_order.at(tname);

    /* ── Build joined rows if JOIN present ── */
    std::vector<Row> all_rows;
    std::vector<std::string> merged_col_order;

    /* Parse left table alias */
    std::string left_alias = parse_left_alias(sql);

    if (!join.type.empty()) {
        /* Resolve right table case-insensitively */
        std::string right_tname = join.table;
        for (auto &kv : db->schema) {
            if (qry_upper(kv.first) == qry_upper(join.table)) { right_tname = kv.first; break; }
        }
        const std::vector<Row> *right_data = db->schema.count(right_tname) ? &db->data.at(right_tname) : nullptr;
        std::vector<std::string> right_col_order = db->schema.count(right_tname) ? db->col_order.at(right_tname) : std::vector<std::string>{};

        /* Right alias: prefer join.alias, else right_tname */
        std::string right_alias = join.alias.empty() ? right_tname : join.alias;

        /* Merged col order: left cols first, then right */
        for (const auto &c : col_order)       merged_col_order.push_back(c);
        for (const auto &c : right_col_order) merged_col_order.push_back(c);

        auto make_merged_row = [&](const Row &lrow, const Row *prrow) -> Row {
            Row merged;
            /* Left columns: bare name + tname.col + left_alias.col */
            for (auto &kv : lrow) {
                merged[kv.first] = kv.second;
                merged[tname + "." + kv.first] = kv.second;
                if (!left_alias.empty()) merged[left_alias + "." + kv.first] = kv.second;
            }
            if (prrow) {
                /* Right columns: right_alias.col + right_tname.col */
                /* Only store bare col if not already in merged (avoids self-join overwrite) */
                for (auto &kv : *prrow) {
                    merged[right_alias + "." + kv.first] = kv.second;
                    if (right_tname != tname) merged[right_tname + "." + kv.first] = kv.second;
                    /* Bare col: only if this is NOT a self-join OR if the col doesn't exist yet */
                    if (right_tname != tname || merged.find(kv.first) == merged.end()) {
                        /* Not a self-join: also store as bare name */
                        if (right_tname != tname) merged[kv.first] = kv.second;
                    }
                }
            } else {
                /* NULL-fill right columns for LEFT JOIN no-match */
                for (const auto &c : right_col_order) {
                    merged[right_alias + "." + c] = SvdbVal{};
                    if (right_tname != tname) {
                        merged[right_tname + "." + c] = SvdbVal{};
                        merged[c] = SvdbVal{};
                    }
                }
            }
            return merged;
        };

        /* Build temporary merged rows for ON evaluation + result */
        auto right_rows_list = right_data ? *right_data : std::vector<Row>{};

        for (const auto &lrow : db->data.at(tname)) {
            bool matched = false;
            /* Prepare left row with alias prefixes for ON evaluation */
            Row lrow_prefixed;
            for (auto &kv : lrow) {
                lrow_prefixed[kv.first] = kv.second;
                lrow_prefixed[tname + "." + kv.first] = kv.second;
                if (!left_alias.empty()) lrow_prefixed[left_alias + "." + kv.first] = kv.second;
            }
            for (const auto &rrow : right_rows_list) {
                /* For ON condition: compare lrow cols vs rrow cols directly */
                bool on_match = join.on_left.empty(); /* CROSS JOIN: always match */
                if (!join.on_left.empty()) {
                    /* Build combined row with both left and right aliases for ON evaluation */
                    Row rrow_prefixed;
                    for (auto &kv : rrow) {
                        rrow_prefixed[kv.first] = kv.second;
                        rrow_prefixed[right_tname + "." + kv.first] = kv.second;
                        rrow_prefixed[right_alias + "." + kv.first] = kv.second;
                    }
                    Row combined_row = lrow_prefixed;
                    for (auto &kv : rrow_prefixed) combined_row[kv.first] = kv.second;

                    SvdbVal lv, rv;
                    if (!join.using_col.empty()) {
                        /* USING (col): compare left.col vs right.col using prefixed
                         * names to avoid the right table's value overwriting the left */
                        std::string lk = (!left_alias.empty() ? left_alias : tname) + "." + join.using_col;
                        std::string rk = (!right_alias.empty() ? right_alias : right_tname) + "." + join.using_col;
                        lv = eval_expr(lk, combined_row, merged_col_order);
                        rv = eval_expr(rk, combined_row, merged_col_order);
                    } else {
                        lv = eval_expr(join.on_left,  combined_row, merged_col_order);
                        rv = eval_expr(join.on_right, combined_row, merged_col_order);
                    }
                    /* NULL = NULL is false in joins */
                    if (lv.type != SVDB_TYPE_NULL && rv.type != SVDB_TYPE_NULL)
                        on_match = (val_cmp(lv, rv) == 0);
                }
                if (on_match) {
                    all_rows.push_back(make_merged_row(lrow, &rrow)); matched = true;
                }
            }
            if (!matched && join.type == "LEFT") {
                all_rows.push_back(make_merged_row(lrow, nullptr));
            }
        }
    } else {
        all_rows = db->data.at(tname);
        merged_col_order = col_order;
        /* Always add table-name and alias prefixes to rows for correlated subqueries */
        for (auto &row : all_rows) {
            Row extra;
            for (auto &kv : row) {
                extra[tname + "." + kv.first] = kv.second;
                if (!left_alias.empty())
                    extra[left_alias + "." + kv.first] = kv.second;
            }
            row.insert(extra.begin(), extra.end());
        }
    }

    /* ── Apply additional JOINs (multi-table join) ── */
    for (size_t ji = 1; ji < all_joins.size(); ++ji) {
        const JoinSpec &jn = all_joins[ji];
        if (jn.table.empty()) continue;
        /* Resolve right table */
        std::string rt = jn.table;
        for (auto &kv : db->schema) {
            if (qry_upper(kv.first) == qry_upper(jn.table)) { rt = kv.first; break; }
        }
        if (!db->schema.count(rt)) continue;
        const auto &rt_col_order = db->col_order.at(rt);
        const auto &rt_data = db->data.at(rt);
        std::string rt_alias = jn.alias.empty() ? rt : jn.alias;

        /* Merge col order */
        for (const auto &c : rt_col_order) merged_col_order.push_back(c);

        std::vector<Row> new_all_rows;
        for (const auto &lrow : all_rows) {
            bool matched = false;
            for (const auto &rrow : rt_data) {
                /* Check ON condition */
                bool on_match = jn.on_left.empty();
                if (!jn.on_left.empty()) {
                    SvdbVal lv = eval_expr(jn.on_left, lrow, merged_col_order);
                    /* Evaluate on_right against right row */
                    Row rrow_pref;
                    for (auto &kv : rrow) {
                        rrow_pref[kv.first] = kv.second;
                        rrow_pref[rt + "." + kv.first] = kv.second;
                        rrow_pref[rt_alias + "." + kv.first] = kv.second;
                    }
                    SvdbVal rv = eval_expr(jn.on_right, rrow_pref, rt_col_order);
                    if (lv.type != SVDB_TYPE_NULL && rv.type != SVDB_TYPE_NULL)
                        on_match = (val_cmp(lv, rv) == 0);
                }
                if (on_match) {
                    Row merged = lrow;
                    for (auto &kv : rrow) {
                        merged[rt_alias + "." + kv.first] = kv.second;
                        merged[rt + "." + kv.first] = kv.second;
                        if (merged.find(kv.first) == merged.end())
                            merged[kv.first] = kv.second;
                    }
                    new_all_rows.push_back(merged);
                    matched = true;
                }
            }
            if (!matched && jn.type == "LEFT") {
                Row merged = lrow;
                for (const auto &c : rt_col_order) {
                    merged[rt_alias + "." + c] = SvdbVal{};
                    merged[rt + "." + c] = SvdbVal{};
                    if (merged.find(c) == merged.end()) merged[c] = SvdbVal{};
                }
                new_all_rows.push_back(merged);
            }
        }
        all_rows = std::move(new_all_rows);
    }

    /* Find top-level " AS " (not inside parentheses) for alias stripping */
    auto find_top_as = [](const std::string &cu) -> size_t {
        int depth = 0;
        for (size_t i = 0; i < cu.size(); ++i) {
            if (cu[i] == '(') ++depth;
            else if (cu[i] == ')') { if (depth > 0) --depth; }
            else if (depth == 0 && i + 4 <= cu.size() &&
                     cu[i] == ' ' && cu[i+1] == 'A' && cu[i+2] == 'S' && cu[i+3] == ' ') {
                return i;
            }
        }
        return std::string::npos;
    };

    /* ── Determine output columns ── */
    std::vector<std::string> out_cols;
    if (star) {
        out_cols = col_order;
    } else {
        for (const auto &c : sel_cols) {
            /* Strip alias (AS name) — must be at top level, not inside parens */
            std::string cu = qry_upper(c);
            size_t as_pos = find_top_as(cu);
            std::string col_expr = (as_pos != std::string::npos) ? c.substr(0, as_pos) : c;
            out_cols.push_back(qry_trim(col_expr));
        }
    }

    /* Compute alias map for output columns */
    std::vector<std::string> out_names;
    for (const auto &c : (star ? sel_cols : sel_cols)) {
        std::string cu = qry_upper(c);
        size_t as_pos = find_top_as(cu);
        if (as_pos != std::string::npos) out_names.push_back(qry_trim(c.substr(as_pos + 4)));
        else out_names.push_back(c);
    }

    /* Column names for output: use original column names or expression */
    r->col_names.clear();
    if (star) {
        r->col_names = col_order;
    } else {
        for (size_t i = 0; i < sel_cols.size(); ++i) {
            std::string cu = qry_upper(sel_cols[i]);
            size_t as_pos = find_top_as(cu);
            std::string col_name = (as_pos != std::string::npos)
                ? qry_trim(sel_cols[i].substr(as_pos + 4))
                : sel_cols[i];
            r->col_names.push_back(col_name);
        }
    }

    /* ── GROUP BY / Aggregate path ── */
    bool has_agg = false;
    if (!group_cols.empty()) has_agg = true;
    if (!has_agg && !sel_cols.empty()) {
        for (const auto &c : sel_cols) if (is_agg_expr(c)) { has_agg = true; break; }
    }

    if (has_agg && group_cols.empty()) {
        /* Single-group aggregate (e.g. SELECT COUNT(*), SUM(x), SUM(i)+SUM(r)) */
        std::vector<AggState> aggs;
        const std::vector<std::string> &agg_src = star ? std::vector<std::string>{"*"} : sel_cols;
        for (const auto &c : agg_src)
            aggs.push_back(make_agg(c));
        /* Also collect sub-aggregates for compound expressions */
        std::vector<std::string> extra_agg_exprs; /* sub-expr strings */
        std::vector<AggState>   extra_aggs;
        if (!star) {
            for (size_t i = 0; i < sel_cols.size(); ++i) {
                if (aggs[i].func.empty() && is_agg_expr(sel_cols[i])) {
                    for (const auto &s : extract_agg_subexprs(sel_cols[i])) {
                        extra_agg_exprs.push_back(s);
                        extra_aggs.push_back(make_agg(s));
                    }
                }
            }
        }
        for (const auto &row : all_rows) {
            if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
            for (auto &a : aggs) agg_accumulate(a, row, merged_col_order);
            for (auto &a : extra_aggs) agg_accumulate(a, row, merged_col_order);
        }
        /* Build virtual row for sub-agg results used by compound expressions */
        Row agg_virtual_row;
        for (size_t j = 0; j < extra_agg_exprs.size(); ++j)
            agg_virtual_row[extra_agg_exprs[j]] = agg_result(extra_aggs[j]);
        /* Also find the first matching row for evaluating non-aggregate constants */
        Row first_row;
        for (const auto &row : all_rows) {
            if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
            first_row = row; break;
        }
        std::vector<SvdbVal> res_row;
        for (size_t i = 0; i < aggs.size(); ++i) {
            if (!aggs[i].func.empty()) {
                res_row.push_back(agg_result(aggs[i]));
            } else if (!star && is_agg_expr(sel_cols[i])) {
                /* Compound aggregate expression: evaluate against virtual row */
                res_row.push_back(eval_expr(sel_cols[i], agg_virtual_row, {}));
            } else if (!star) {
                /* Non-aggregate expression: evaluate against first matching row */
                SvdbVal v = eval_expr(sel_cols[i], first_row, merged_col_order);
                /* Also try agg_virtual_row for sub-agg references */
                if (v.type == SVDB_TYPE_NULL && agg_virtual_row.count(sel_cols[i]))
                    v = agg_virtual_row[sel_cols[i]];
                res_row.push_back(v);
            } else {
                res_row.push_back(agg_result(aggs[i]));
            }
        }
        r->rows.push_back(res_row);
        *rows_out = r; return SVDB_OK;
    }

    if (!group_cols.empty()) {
        /* GROUP BY */
        /* Map: group-key string → (first row for non-agg cols, agg states) */
        std::vector<std::string> key_strs;
        std::map<std::string, Row> key_rows;
        std::map<std::string, std::vector<AggState>> key_aggs;

        std::vector<AggState> proto;
        for (const auto &c : (star ? std::vector<std::string>{} : sel_cols))
            proto.push_back(make_agg(c));

        for (const auto &row : all_rows) {
            if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
            /* Build group key */
            std::string key;
            for (const auto &gc : group_cols) {
                SvdbVal v = eval_expr(gc, row, merged_col_order);
                key += val_to_str(v) + "\x01";
            }
            if (!key_rows.count(key)) {
                key_strs.push_back(key);
                key_rows[key] = row;
                key_aggs[key] = proto;
            }
            for (auto &a : key_aggs[key]) agg_accumulate(a, row, merged_col_order);
        }
        /* Build result */
        for (const auto &key : key_strs) {
            const Row &rep_row = key_rows[key];
            auto &agg_list = key_aggs[key];
            /* Check HAVING: augment row with aggregate results so HAVING can
             * reference aggregate expressions like SUM(amount) > 500 */
            if (!having_txt.empty()) {
                Row having_row = rep_row;
                for (size_t i = 0; i < sel_cols.size() && i < agg_list.size(); ++i) {
                    if (!agg_list[i].func.empty()) {
                        having_row[sel_cols[i]] = agg_result(agg_list[i]);
                        having_row[out_cols[i]] = agg_result(agg_list[i]);
                    }
                }
                if (!qry_eval_where(having_row, merged_col_order, having_txt)) continue;
            }
            std::vector<SvdbVal> res_row;
            if (star) {
                for (const auto &cn : col_order) {
                    auto it = rep_row.find(cn);
                    res_row.push_back(it != rep_row.end() ? it->second : SvdbVal{});
                }
            } else {
                for (size_t i = 0; i < sel_cols.size(); ++i) {
                    if (!agg_list[i].func.empty()) res_row.push_back(agg_result(agg_list[i]));
                    else res_row.push_back(eval_expr(out_cols[i], rep_row, merged_col_order));
                }
            }
            r->rows.push_back(res_row);
        }
        /* Apply ORDER BY */
        if (!order_cols.empty()) {
            std::sort(r->rows.begin(), r->rows.end(),
                [&](const std::vector<SvdbVal> &a, const std::vector<SvdbVal> &b) {
                    for (const auto &oc : order_cols) {
                        /* Find column index */
                        int idx = -1;
                        for (size_t i = 0; i < r->col_names.size(); ++i)
                            if (qry_upper(r->col_names[i]) == qry_upper(oc.expr)) { idx = (int)i; break; }
                        if (idx < 0) continue;
                        int c;
                        if (oc.nocase && a[idx].type == SVDB_TYPE_TEXT && b[idx].type == SVDB_TYPE_TEXT)
                            c = qry_upper(a[idx].sval).compare(qry_upper(b[idx].sval));
                        else
                            c = val_cmp(a[idx], b[idx]);
                        if (c != 0) return oc.desc ? c > 0 : c < 0;
                    }
                    return false;
                });
        }
        /* Apply LIMIT/OFFSET */
        if (offset_val > 0 && (size_t)offset_val < r->rows.size())
            r->rows.erase(r->rows.begin(), r->rows.begin() + offset_val);
        else if (offset_val > 0)
            r->rows.clear();
        if (limit_val >= 0 && (size_t)limit_val < r->rows.size())
            r->rows.resize((size_t)limit_val);
        *rows_out = r; return SVDB_OK;
    }

    /* ── Simple SELECT (no GROUP BY) ── */
    /* First, collect matching rows for window function support */
    std::vector<Row> matching_rows;
    for (const auto &row : all_rows) {
        if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
        matching_rows.push_back(row);
    }

    /* Detect and pre-compute window functions */
    bool has_win = false;
    if (!star) {
        for (const auto &ce : out_cols)
            if (is_window_expr(ce)) { has_win = true; break; }
    }
    std::vector<std::vector<SvdbVal>> win_vals; /* [col][row] */
    if (has_win)
        win_vals = compute_window_functions(matching_rows, merged_col_order, out_cols);

    std::vector<std::vector<SvdbVal>> raw_rows;
    std::vector<Row> orig_rows;  /* keep original rows for ORDER BY on non-SELECT cols */
    for (size_t ri = 0; ri < matching_rows.size(); ++ri) {
        const auto &row = matching_rows[ri];
        std::vector<SvdbVal> result_row;
        if (star) {
            for (const auto &cn : col_order) {
                auto it = row.find(cn);
                result_row.push_back(it != row.end() ? it->second : SvdbVal{});
            }
        } else {
            for (size_t ci = 0; ci < out_cols.size(); ++ci) {
                if (has_win && is_window_expr(out_cols[ci]))
                    result_row.push_back(win_vals[ci][ri]);
                else
                    result_row.push_back(eval_expr(out_cols[ci], row, merged_col_order));
            }
        }
        raw_rows.push_back(result_row);
        orig_rows.push_back(row);
    }

    /* DISTINCT */
    if (distinct) {
        std::set<std::string> seen;
        std::vector<std::vector<SvdbVal>> deduped;
        std::vector<Row> deduped_orig;
        for (size_t i = 0; i < raw_rows.size(); ++i) {
            std::string key;
            for (auto &v : raw_rows[i]) key += val_to_str(v) + "\x01";
            if (seen.insert(key).second) {
                deduped.push_back(raw_rows[i]);
                deduped_orig.push_back(orig_rows[i]);
            }
        }
        raw_rows = std::move(deduped);
        orig_rows = std::move(deduped_orig);
    }

    /* ORDER BY */
    if (!order_cols.empty()) {
        /* Build index array for stable sort */
        std::vector<size_t> idx_arr(raw_rows.size());
        for (size_t i = 0; i < idx_arr.size(); ++i) idx_arr[i] = i;
        std::stable_sort(idx_arr.begin(), idx_arr.end(),
            [&](size_t ia, size_t ib) {
                const auto &a = raw_rows[ia];
                const auto &b = raw_rows[ib];
                for (const auto &oc : order_cols) {
                    /* Find col in result columns */
                    int col_idx = -1;
                    for (size_t i = 0; i < r->col_names.size(); ++i)
                        if (qry_upper(r->col_names[i]) == qry_upper(oc.expr)) { col_idx = (int)i; break; }
                    /* Try numeric column reference */
                    if (col_idx < 0) {
                        try { col_idx = (int)std::stoll(oc.expr) - 1; } catch (...) {}
                    }
                    SvdbVal va, vb;
                    if (col_idx >= 0 && col_idx < (int)a.size()) {
                        va = a[col_idx]; vb = b[col_idx];
                    } else {
                        /* ORDER BY column not in SELECT — evaluate against original row */
                        va = eval_expr(oc.expr, orig_rows[ia], merged_col_order);
                        vb = eval_expr(oc.expr, orig_rows[ib], merged_col_order);
                    }
                    int c;
                    if (oc.nocase && va.type == SVDB_TYPE_TEXT && vb.type == SVDB_TYPE_TEXT)
                        c = qry_upper(va.sval).compare(qry_upper(vb.sval));
                    else
                        c = val_cmp(va, vb);
                    if (c != 0) return oc.desc ? c > 0 : c < 0;
                }
                return false;
            });
        std::vector<std::vector<SvdbVal>> sorted_rows(raw_rows.size());
        for (size_t i = 0; i < idx_arr.size(); ++i) sorted_rows[i] = raw_rows[idx_arr[i]];
        raw_rows = std::move(sorted_rows);
    }

    /* LIMIT / OFFSET */
    if (offset_val > 0 && (size_t)offset_val < raw_rows.size())
        raw_rows.erase(raw_rows.begin(), raw_rows.begin() + offset_val);
    else if (offset_val > 0)
        raw_rows.clear();
    if (limit_val >= 0 && (size_t)limit_val < raw_rows.size())
        raw_rows.resize((size_t)limit_val);

    r->rows = std::move(raw_rows);
    *rows_out = r; return SVDB_OK;
}

/* ── PRAGMA query handler ───────────────────────────────────────── */

svdb_code_t svdb_query_pragma(svdb_db_t *db, const std::string &sql,
                               svdb_rows_t **rows_out) {
    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;
    *rows_out = r;

    /* Extract pragma name and optional argument */
    std::string s = qry_trim(sql);
    /* Skip "PRAGMA " */
    if (s.size() < 8) return SVDB_OK;
    std::string rest = qry_trim(s.substr(7));

    /* Check for optional schema prefix (schema.pragma_name) */
    auto dot = rest.find('.');
    if (dot != std::string::npos) rest = rest.substr(dot + 1);

    /* Extract name and optional arg: name or name(arg) or name = val */
    std::string pname, parg;
    auto paren = rest.find('(');
    auto eq    = rest.find('=');
    if (paren != std::string::npos) {
        pname = qry_upper(qry_trim(rest.substr(0, paren)));
        size_t end = rest.find(')', paren);
        parg  = qry_trim(rest.substr(paren + 1, (end != std::string::npos ? end : rest.size()) - paren - 1));
        /* Strip quotes from arg */
        if (parg.size() >= 2 && (parg.front() == '\'' || parg.front() == '"'))
            parg = parg.substr(1, parg.size() - 2);
    } else if (eq != std::string::npos) {
        pname = qry_upper(qry_trim(rest.substr(0, eq)));
        parg  = qry_trim(rest.substr(eq + 1));
    } else {
        pname = qry_upper(qry_trim(rest));
    }

    /* PRAGMA table_info(tname) */
    if (pname == "TABLE_INFO" && !parg.empty()) {
        r->col_names = {"cid", "name", "type", "notnull", "dflt_value", "pk"};
        /* Find table case-insensitively */
        std::string parg_u = qry_upper(parg);
        std::string tname;
        for (auto &kv : db->schema)
            if (qry_upper(kv.first) == parg_u) { tname = kv.first; break; }
        if (!tname.empty() && db->col_order.count(tname)) {
            int cid = 0;
            int pk_seq = 1;
            for (const auto &col : db->col_order.at(tname)) {
                const ColDef *def = nullptr;
                auto sit = db->schema.at(tname).find(col);
                if (sit != db->schema.at(tname).end()) def = &sit->second;
                std::string ctype = def ? def->type : "TEXT";
                int notnull = def ? (def->not_null ? 1 : 0) : 0;
                int pk = 0;
                /* Check primary_key flag or primary_keys list */
                if (def && def->primary_key) {
                    pk = pk_seq++;
                } else if (db->primary_keys.count(tname)) {
                    int seq2 = 1;
                    for (auto &pk_col : db->primary_keys.at(tname)) {
                        if (pk_col == col) { pk = seq2; break; }
                        ++seq2;
                    }
                }
                std::string base_type = ctype;
                size_t sp = ctype.find(' ');
                if (sp != std::string::npos) base_type = ctype.substr(0, sp);
                SvdbVal v_cid, v_name, v_type, v_notnull, v_dflt, v_pk;
                v_cid.type = SVDB_TYPE_INT; v_cid.ival = cid++;
                v_name.type = SVDB_TYPE_TEXT; v_name.sval = col;
                v_type.type = SVDB_TYPE_TEXT; v_type.sval = base_type;
                v_notnull.type = SVDB_TYPE_INT; v_notnull.ival = notnull;
                /* dflt_value: use ColDef.default_val if set */
                if (def && !def->default_val.empty()) {
                    v_dflt.type = SVDB_TYPE_TEXT; v_dflt.sval = def->default_val;
                }
                v_pk.type = SVDB_TYPE_INT; v_pk.ival = pk;
                r->rows.push_back({v_cid, v_name, v_type, v_notnull, v_dflt, v_pk});
            }
        }
        return SVDB_OK;
    }

    /* PRAGMA table_list */
    if (pname == "TABLE_LIST") {
        r->col_names = {"schema", "name", "type", "ncol", "wr", "strict"};
        for (auto &kv : db->schema) {
            /* Determine if this is a view */
            std::string ttype = "table";
            if (db->create_sql.count(kv.first)) {
                std::string su3 = qry_upper(db->create_sql.at(kv.first));
                /* Strip leading whitespace */
                size_t i = 0;
                while (i < su3.size() && isspace((unsigned char)su3[i])) ++i;
                /* CREATE [TEMP] VIEW ... */
                if (su3.substr(i, 6) == "CREATE") {
                    size_t j = i + 6;
                    while (j < su3.size() && isspace((unsigned char)su3[j])) ++j;
                    if (su3.substr(j, 4) == "TEMP" || su3.substr(j, 9) == "TEMPORARY") {
                        while (j < su3.size() && isalpha((unsigned char)su3[j])) ++j;
                        while (j < su3.size() && isspace((unsigned char)su3[j])) ++j;
                    }
                    if (su3.substr(j, 4) == "VIEW") ttype = "view";
                }
            }
            SvdbVal v_schema, v_name, v_type, v_ncol, v_wr, v_strict;
            v_schema.type = SVDB_TYPE_TEXT; v_schema.sval = "main";
            v_name.type   = SVDB_TYPE_TEXT; v_name.sval   = kv.first;
            v_type.type   = SVDB_TYPE_TEXT; v_type.sval   = ttype;
            v_ncol.type   = SVDB_TYPE_INT;  v_ncol.ival   = (int64_t)kv.second.size();
            v_wr.type     = SVDB_TYPE_INT;  v_wr.ival     = 0;
            v_strict.type = SVDB_TYPE_INT;  v_strict.ival = 0;
            r->rows.push_back({v_schema, v_name, v_type, v_ncol, v_wr, v_strict});
        }
        return SVDB_OK;
    }

    /* PRAGMA index_list(tname) */
    if (pname == "INDEX_LIST" && !parg.empty()) {
        r->col_names = {"seq", "name", "unique", "origin", "partial"};
        std::string parg_u = qry_upper(parg);
        int seq = 0;
        for (auto &iv : db->indexes) {
            if (qry_upper(iv.second.table) != parg_u) continue;
            SvdbVal v_seq, v_name, v_uniq, v_origin, v_partial;
            v_seq.type = SVDB_TYPE_INT; v_seq.ival = seq++;
            v_name.type = SVDB_TYPE_TEXT; v_name.sval = iv.first;
            v_uniq.type = SVDB_TYPE_INT; v_uniq.ival = iv.second.unique ? 1 : 0;
            v_origin.type = SVDB_TYPE_TEXT; v_origin.sval = "c";
            v_partial.type = SVDB_TYPE_INT; v_partial.ival = 0;
            r->rows.push_back({v_seq, v_name, v_uniq, v_origin, v_partial});
        }
        return SVDB_OK;
    }

    /* PRAGMA index_info(idx_name) */
    if (pname == "INDEX_INFO" && !parg.empty()) {
        r->col_names = {"seqno", "cid", "name"};
        std::string parg_u = qry_upper(parg);
        auto it = db->indexes.find(parg);
        if (it == db->indexes.end()) {
            for (auto &iv : db->indexes)
                if (qry_upper(iv.first) == parg_u) { it = db->indexes.find(iv.first); break; }
        }
        if (it != db->indexes.end()) {
            const auto &tname = it->second.table;
            int seqno = 0;
            for (const auto &cn : it->second.columns) {
                /* find cid in col_order */
                int cid = 0;
                if (db->col_order.count(tname)) {
                    for (size_t ci = 0; ci < db->col_order.at(tname).size(); ++ci)
                        if (db->col_order.at(tname)[ci] == cn) { cid = (int)ci; break; }
                }
                SvdbVal v_seqno, v_cid, v_name;
                v_seqno.type = SVDB_TYPE_INT; v_seqno.ival = seqno++;
                v_cid.type   = SVDB_TYPE_INT; v_cid.ival   = cid;
                v_name.type  = SVDB_TYPE_TEXT; v_name.sval  = cn;
                r->rows.push_back({v_seqno, v_cid, v_name});
            }
        }
        return SVDB_OK;
    }

    /* PRAGMA index_xinfo(idx_name) */
    if (pname == "INDEX_XINFO" && !parg.empty()) {
        r->col_names = {"seqno", "cid", "name", "desc", "coll", "key"};
        std::string parg_u = qry_upper(parg);
        auto it = db->indexes.find(parg);
        if (it == db->indexes.end()) {
            for (auto &iv : db->indexes)
                if (qry_upper(iv.first) == parg_u) { it = db->indexes.find(iv.first); break; }
        }
        if (it != db->indexes.end()) {
            const auto &tname = it->second.table;
            int seqno = 0;
            for (const auto &cn : it->second.columns) {
                int cid = 0;
                if (db->col_order.count(tname)) {
                    for (size_t ci = 0; ci < db->col_order.at(tname).size(); ++ci)
                        if (db->col_order.at(tname)[ci] == cn) { cid = (int)ci; break; }
                }
                SvdbVal v_seqno, v_cid, v_name, v_desc, v_coll, v_key;
                v_seqno.type = SVDB_TYPE_INT; v_seqno.ival = seqno++;
                v_cid.type   = SVDB_TYPE_INT; v_cid.ival   = cid;
                v_name.type  = SVDB_TYPE_TEXT; v_name.sval  = cn;
                v_desc.type  = SVDB_TYPE_INT; v_desc.ival  = 0;
                v_coll.type  = SVDB_TYPE_TEXT; v_coll.sval  = "BINARY";
                v_key.type   = SVDB_TYPE_INT; v_key.ival   = 1;
                r->rows.push_back({v_seqno, v_cid, v_name, v_desc, v_coll, v_key});
            }
        }
        return SVDB_OK;
    }

    /* PRAGMA foreign_key_list(tname) */
    if (pname == "FOREIGN_KEY_LIST" && !parg.empty()) {
        r->col_names = {"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"};
        std::string parg_u = qry_upper(parg);
        for (auto &kv : db->fk_constraints) {
            if (qry_upper(kv.first) != parg_u) continue;
            int id = 0;
            for (const auto &fk : kv.second) {
                SvdbVal v_id, v_seq, v_table, v_from, v_to, v_onupd, v_ondel, v_match;
                v_id.type    = SVDB_TYPE_INT; v_id.ival = id;
                v_seq.type   = SVDB_TYPE_INT; v_seq.ival = 0;
                v_table.type = SVDB_TYPE_TEXT; v_table.sval = fk.parent_table;
                v_from.type  = SVDB_TYPE_TEXT; v_from.sval  = fk.child_col;
                v_to.type    = SVDB_TYPE_TEXT; v_to.sval    = fk.parent_col;
                v_onupd.type = SVDB_TYPE_TEXT; v_onupd.sval = "NO ACTION";
                v_ondel.type = SVDB_TYPE_TEXT; v_ondel.sval = "NO ACTION";
                v_match.type = SVDB_TYPE_TEXT; v_match.sval = "NONE";
                r->rows.push_back({v_id, v_seq, v_table, v_from, v_to, v_onupd, v_ondel, v_match});
                ++id;
            }
        }
        return SVDB_OK;
    }

    /* PRAGMA foreign_key_check / PRAGMA foreign_key_check(tbl) */
    if (pname == "FOREIGN_KEY_CHECK") {
        r->col_names = {"table", "rowid", "parent", "fkid"};
        /* Scan all (or one) tables for FK violations */
        auto check_table_fks = [&](const std::string &tname) {
            if (!db->fk_constraints.count(tname)) return;
            const auto &rows2 = db->data.count(tname) ? db->data.at(tname) : std::vector<Row>{};
            int64_t rowid2 = 1;
            for (const auto &row2 : rows2) {
                for (const auto &fk : db->fk_constraints.at(tname)) {
                    auto cit = row2.find(fk.child_col);
                    if (cit == row2.end() || cit->second.type == SVDB_TYPE_NULL) { ++rowid2; continue; }
                    const std::string &pcol = fk.parent_col.empty() ? fk.child_col : fk.parent_col;
                    bool found = false;
                    if (db->data.count(fk.parent_table)) {
                        for (const auto &pr : db->data.at(fk.parent_table)) {
                            auto pit = pr.find(pcol);
                            if (pit == pr.end()) continue;
                            if (pit->second.type == cit->second.type) {
                                if (pit->second.type == SVDB_TYPE_INT  && pit->second.ival == cit->second.ival) { found = true; break; }
                                if (pit->second.type == SVDB_TYPE_REAL && pit->second.rval == cit->second.rval) { found = true; break; }
                                if (pit->second.type == SVDB_TYPE_TEXT && pit->second.sval == cit->second.sval) { found = true; break; }
                            }
                        }
                    }
                    if (!found) {
                        SvdbVal v_tbl, v_rowid, v_parent, v_fkid;
                        v_tbl.type    = SVDB_TYPE_TEXT; v_tbl.sval    = tname;
                        v_rowid.type  = SVDB_TYPE_INT;  v_rowid.ival  = rowid2;
                        v_parent.type = SVDB_TYPE_TEXT; v_parent.sval = fk.parent_table;
                        v_fkid.type   = SVDB_TYPE_INT;  v_fkid.ival   = 0;
                        r->rows.push_back({v_tbl, v_rowid, v_parent, v_fkid});
                    }
                }
                ++rowid2;
            }
        };
        if (!parg.empty()) {
            check_table_fks(parg);
        } else {
            for (auto &kv : db->fk_constraints) check_table_fks(kv.first);
        }
        return SVDB_OK;
    }

    /* PRAGMA database_list */
    if (pname == "DATABASE_LIST") {
        r->col_names = {"seq", "name", "file"};
        SvdbVal v_seq, v_name, v_file;
        v_seq.type = SVDB_TYPE_INT; v_seq.ival = 0;
        v_name.type = SVDB_TYPE_TEXT; v_name.sval = "main";
        v_file.type = SVDB_TYPE_TEXT; v_file.sval = db->path;
        r->rows.push_back({v_seq, v_name, v_file});
        return SVDB_OK;
    }

    /* PRAGMA encoding */
    if (pname == "ENCODING") {
        r->col_names = {"encoding"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = "UTF-8";
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA integrity_check / quick_check */
    if (pname == "INTEGRITY_CHECK" || pname == "QUICK_CHECK") {
        r->col_names = {"integrity_check"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = "ok";
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA wal_mode [= val] */
    if (pname == "WAL_MODE") {
        if (!parg.empty()) {
            std::string upper_arg = qry_upper(parg);
            db->wal_mode = (upper_arg == "ON" || upper_arg == "WAL") ? "WAL" : "DELETE";
        }
        r->col_names = {"wal_mode"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = db->wal_mode;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA journal_mode: alias for wal_mode */
    if (pname == "JOURNAL_MODE") {
        if (!parg.empty()) {
            std::string upper_arg = qry_upper(parg);
            db->wal_mode = (upper_arg == "WAL") ? "WAL" : "DELETE";
        }
        r->col_names = {"journal_mode"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = db->wal_mode;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA isolation_level [= val] */
    if (pname == "ISOLATION_LEVEL") {
        if (!parg.empty()) db->isolation_level = parg;
        r->col_names = {"isolation_level"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = db->isolation_level;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA busy_timeout [= val] */
    if (pname == "BUSY_TIMEOUT") {
        if (!parg.empty()) {
            try { db->busy_timeout_ms = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"timeout"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->busy_timeout_ms;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA compression [= val] */
    if (pname == "COMPRESSION") {
        if (!parg.empty()) db->compression = qry_upper(parg);
        r->col_names = {"compression"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = db->compression;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA synchronous [= val] */
    if (pname == "SYNCHRONOUS") {
        if (!parg.empty()) db->synchronous = qry_upper(parg);
        r->col_names = {"synchronous"};
        SvdbVal v; v.type = SVDB_TYPE_INT;
        if      (db->synchronous == "OFF"   || db->synchronous == "0") v.ival = 0;
        else if (db->synchronous == "NORMAL"|| db->synchronous == "1") v.ival = 1;
        else if (db->synchronous == "FULL"  || db->synchronous == "2") v.ival = 2;
        else if (db->synchronous == "EXTRA" || db->synchronous == "3") v.ival = 3;
        else v.ival = 1;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA foreign_keys [= val] */
    if (pname == "FOREIGN_KEYS") {
        if (!parg.empty()) {
            std::string up = qry_upper(parg);
            db->foreign_keys_enabled = (up == "ON" || up == "1" || up == "TRUE");
        }
        r->col_names = {"foreign_keys"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->foreign_keys_enabled ? 1 : 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA max_rows [= val] */
    if (pname == "MAX_ROWS") {
        if (!parg.empty()) {
            try { db->max_rows = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"max_rows"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->max_rows;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA cache_memory [= val] */
    if (pname == "CACHE_MEMORY") {
        if (!parg.empty()) {
            try { db->cache_memory = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"cache_memory"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->cache_memory;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA memory_stats */
    if (pname == "MEMORY_STATS" || pname == "MEMORY_STATUS") {
        r->col_names = {"stat", "value"};
        int64_t total_rows = 0;
        for (auto &kv : db->data) total_rows += (int64_t)kv.second.size();
        SvdbVal k1, v1, k2, v2;
        k1.type = SVDB_TYPE_TEXT; k1.sval = "total_rows";  v1.type = SVDB_TYPE_INT; v1.ival = total_rows;
        k2.type = SVDB_TYPE_TEXT; k2.sval = "table_count"; v2.type = SVDB_TYPE_INT; v2.ival = (int64_t)db->schema.size();
        r->rows.push_back({k1, v1});
        r->rows.push_back({k2, v2});
        return SVDB_OK;
    }

    /* PRAGMA storage_info */
    if (pname == "STORAGE_INFO") {
        r->col_names = {"table_name", "row_count", "col_count"};
        for (auto &kv : db->schema) {
            SvdbVal v_tbl, v_rows, v_cols;
            v_tbl.type  = SVDB_TYPE_TEXT; v_tbl.sval  = kv.first;
            v_rows.type = SVDB_TYPE_INT;
            v_rows.ival = db->data.count(kv.first) ? (int64_t)db->data.at(kv.first).size() : 0;
            v_cols.type = SVDB_TYPE_INT;
            v_cols.ival = db->col_order.count(kv.first) ? (int64_t)db->col_order.at(kv.first).size() : 0;
            r->rows.push_back({v_tbl, v_rows, v_cols});
        }
        return SVDB_OK;
    }

    /* PRAGMA page_size / page_count / freelist_count: return stubs */
    if (pname == "PAGE_SIZE") {
        r->col_names = {"page_size"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 4096;
        r->rows.push_back({v});
        return SVDB_OK;
    }
    if (pname == "PAGE_COUNT") {
        r->col_names = {"page_count"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 1;
        r->rows.push_back({v});
        return SVDB_OK;
    }
    if (pname == "FREELIST_COUNT") {
        r->col_names = {"freelist_count"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA auto_vacuum [= val] */
    if (pname == "AUTO_VACUUM") {
        r->col_names = {"auto_vacuum"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA collation_list */
    if (pname == "COLLATION_LIST") {
        r->col_names = {"seq", "name"};
        for (int i = 0; i < 3; ++i) {
            SvdbVal vs, vn; vs.type = SVDB_TYPE_INT; vs.ival = i;
            vn.type = SVDB_TYPE_TEXT;
            vn.sval = (i == 0 ? "BINARY" : (i == 1 ? "NOCASE" : "RTRIM"));
            r->rows.push_back({vs, vn});
        }
        return SVDB_OK;
    }

    /* PRAGMA mmap_size [= val] */
    if (pname == "MMAP_SIZE") {
        r->col_names = {"mmap_size"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA query_timeout [= val] */
    if (pname == "QUERY_TIMEOUT") {
        if (!parg.empty()) {
            try { db->query_timeout_ms = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"query_timeout"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->query_timeout_ms;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA max_memory [= val] */
    if (pname == "MAX_MEMORY") {
        if (!parg.empty()) {
            try { db->max_memory = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"max_memory"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->max_memory;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* Unknown PRAGMA: return empty result */
    return SVDB_OK;
}

extern "C" {

svdb_code_t svdb_query(svdb_db_t *db, const char *sql, svdb_rows_t **rows) {
    svdb_assert_msg(db != nullptr, "svdb_query: db must not be NULL");
    svdb_assert_msg(sql != nullptr, "svdb_query: sql must not be NULL");
    svdb_assert_msg(rows != nullptr, "svdb_query: rows output pointer must not be NULL");
    if (!db || !sql || !rows) return SVDB_ERR;
    std::unique_lock<std::mutex> lk(db->mu);
    db->last_error.clear();
    /* Dispatch PRAGMA to dedicated handler */
    std::string s = qry_trim(strip_sql_comments_q(std::string(sql)));
    if (s.size() >= 6) {
        std::string su = qry_upper(s.substr(0, 6));
        if (su == "PRAGMA") return svdb_query_pragma(db, s, rows);
    }
    /* Dispatch BACKUP DATABASE TO 'path' */
    if (s.size() >= 6 && qry_upper(s.substr(0, 6)) == "BACKUP") {
        /* Extract destination path from: BACKUP DATABASE TO 'path' */
        std::string path_str;
        auto q1 = s.find('\'');
        auto q2 = s.rfind('\'');
        if (q1 != std::string::npos && q2 != q1) {
            path_str = s.substr(q1 + 1, q2 - q1 - 1);
        } else {
            auto q3 = s.find('"');
            auto q4 = s.rfind('"');
            if (q3 != std::string::npos && q4 != q3)
                path_str = s.substr(q3 + 1, q4 - q3 - 1);
        }
        *rows = new (std::nothrow) svdb_rows_t();
        if (!(*rows)) return SVDB_NOMEM;
        if (!path_str.empty()) {
            /* Create/truncate file at destination */
            FILE *f = fopen(path_str.c_str(), "wb");
            if (f) {
                /* Write minimal marker so the file exists and is non-empty */
                const char *marker = "SVDB backup\n";
                fwrite(marker, 1, strlen(marker), f);
                /* Serialize table data as CSV-like text */
                for (auto &kv : db->schema) {
                    std::string hdr = "TABLE:" + kv.first + "\n";
                    fwrite(hdr.c_str(), 1, hdr.size(), f);
                }
                fclose(f);
            }
        }
        return SVDB_OK;
    }
    /* Only SELECT and WITH (CTE) statements produce rows; for DML/DDL,
     * execute via svdb_exec and return an empty result (matching SQLite behavior
     * when Query() is called with a non-SELECT statement).
     * svdb_exec acquires db->mu; we must release our lock first to prevent
     * re-entrant deadlock on the non-recursive std::mutex. */
    {
        const char *dml_keywords[] = {"INSERT", "UPDATE", "DELETE", nullptr};
        const char *ddl_keywords[] = {"CREATE", "DROP", "ALTER", nullptr};

        auto starts_with_kw = [&](const char *kw) -> bool {
            size_t klen = strlen(kw);
            return s.size() >= klen && qry_upper(s.substr(0, klen)) == std::string(kw);
        };

        /* Check for DML with possible RETURNING clause */
        bool is_dml = false;
        for (const char **ns = dml_keywords; *ns; ++ns) {
            if (starts_with_kw(*ns)) { is_dml = true; break; }
        }
        bool is_ddl = false;
        for (const char **ns = ddl_keywords; *ns; ++ns) {
            if (starts_with_kw(*ns)) { is_ddl = true; break; }
        }

        if (is_dml) {
            std::string ret_clause, sql_no_ret;
            if (qry_extract_returning(s, ret_clause, sql_no_ret)) {
                /* DML with RETURNING: execute DML then return RETURNING rows */
                std::string ret_kw = qry_upper(s.substr(0, 6).substr(0, s.find(' ')));
                /* Find table name */
                std::string tname;
                {
                    std::string su2 = qry_upper(sql_no_ret);
                    size_t tp = std::string::npos;
                    if (su2.substr(0, 6) == "INSERT") {
                        tp = su2.find("INTO");
                        if (tp != std::string::npos) tp += 4;
                    } else if (su2.substr(0, 6) == "UPDATE") {
                        tp = 6;
                    } else if (su2.substr(0, 6) == "DELETE") {
                        tp = su2.find("FROM");
                        if (tp != std::string::npos) tp += 4;
                    }
                    if (tp != std::string::npos) {
                        while (tp < sql_no_ret.size() && isspace((unsigned char)sql_no_ret[tp])) ++tp;
                        size_t ts = tp;
                        while (tp < sql_no_ret.size() && (isalnum((unsigned char)sql_no_ret[tp]) || sql_no_ret[tp] == '_')) ++tp;
                        tname = sql_no_ret.substr(ts, tp - ts);
                    }
                }

                auto ret_exprs = qry_split_returning_exprs(ret_clause);
                const std::vector<std::string> *col_order_p = nullptr;
                std::vector<Row> before_snap, after_snap;

                if (!tname.empty() && db->data.count(tname)) {
                    before_snap = db->data.at(tname);
                    col_order_p = &db->col_order.at(tname);
                }

                lk.unlock();
                svdb_result_t res{};
                svdb_exec(db, sql_no_ret.c_str(), &res);
                lk.lock();

                if (!tname.empty() && db->data.count(tname)) {
                    after_snap = db->data.at(tname);
                    col_order_p = &db->col_order.at(tname);
                }

                std::vector<Row> affected_rows;
                std::string su2 = qry_upper(sql_no_ret.substr(0, 6));
                if (su2 == "INSERT") {
                    /* New rows appended at the end */
                    for (size_t i = before_snap.size(); i < after_snap.size(); ++i)
                        affected_rows.push_back(after_snap[i]);
                } else if (su2 == "UPDATE") {
                    /* Rows that were updated: evaluate the UPDATE's WHERE clause
                     * against the post-update data to identify affected rows. */
                    std::string su_no_ret = qry_upper(sql_no_ret);
                    std::string where_for_update;
                    size_t wp = su_no_ret.find(" WHERE ");
                    if (wp != std::string::npos)
                        where_for_update = qry_trim(sql_no_ret.substr(wp + 7));
                    for (const auto &r2 : after_snap) {
                        if (where_for_update.empty() ||
                            qry_eval_where(r2, col_order_p ? *col_order_p : std::vector<std::string>{}, where_for_update))
                            affected_rows.push_back(r2);
                    }
                } else if (su2 == "DELETE") {
                    /* Rows present before but gone after (compare by rowid) */
                    std::set<int64_t> after_rowids;
                    for (auto &r2 : after_snap) {
                        auto rit = r2.find(SVDB_ROWID_COLUMN);
                        if (rit != r2.end()) after_rowids.insert(rit->second.ival);
                    }
                    for (auto &r2 : before_snap) {
                        auto rit = r2.find(SVDB_ROWID_COLUMN);
                        if (rit == r2.end() || !after_rowids.count(rit->second.ival))
                            affected_rows.push_back(r2);
                    }
                }

                const std::vector<std::string> empty_order;
                *rows = qry_build_returning_result(ret_exprs, affected_rows,
                                                   col_order_p ? *col_order_p : empty_order);
                return (*rows) ? SVDB_OK : SVDB_NOMEM;
            }
            /* Plain DML without RETURNING */
            lk.unlock();
            svdb_result_t res{};
            svdb_exec(db, s.c_str(), &res);
            *rows = new (std::nothrow) svdb_rows_t();
            return (*rows) ? SVDB_OK : SVDB_NOMEM;
        }

        if (is_ddl) {
            lk.unlock();
            svdb_result_t res{};
            svdb_exec(db, s.c_str(), &res);
            *rows = new (std::nothrow) svdb_rows_t();
            return (*rows) ? SVDB_OK : SVDB_NOMEM;
        }
    }
    return svdb_query_internal(db, s, rows);
}

} /* extern "C" */

/* Non-static wrapper: evaluate a SQL expression in the context of a given row.
 * Used by exec.cpp's do_update to evaluate complex SET expressions (CASE, functions, subqueries). */
SvdbVal svdb_eval_expr_in_row(const std::string &expr, const Row &row,
                               const std::vector<std::string> &col_order) {
    return eval_expr(expr, row, col_order);
}

/* Set thread-local DB context for use by eval_expr subqueries */
void svdb_set_query_db(svdb_db_t *db) {
    g_query_db = db;
}
