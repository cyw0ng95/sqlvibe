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
 *   - JSON functions (when SVDB_EXT_JSON is enabled): json, json_array, json_object,
 *     json_extract, json_type, json_length, json_valid, json_quote, json_remove, json_set
 */
#include "svdb.h"
#include "svdb_types.h"
#include "svdb_util.h"
#include "../SF/svdb_assert.h"
#include "QP/parser.h"

#ifdef SVDB_EXT_JSON
#include "../../ext/json/json.h"
#endif

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
#include <iomanip>
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

/* Collapse all whitespace sequences (tabs, newlines, etc.) into single spaces
 * outside of string literals. Used to normalise multi-line SQL so that all
 * keyword searches like " FROM ", " WHERE " always work with a single space. */
static std::string normalize_whitespace(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    bool in_str = false, last_space = false;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'') {
            in_str = !in_str;
            /* Handle escaped single-quote '' */
            if (!in_str && i + 1 < s.size() && s[i+1] == '\'') {
                out += c; out += s[++i]; last_space = false; continue;
            }
            out += c; last_space = false; continue;
        }
        if (in_str) { out += c; last_space = false; continue; }
        if (isspace((unsigned char)c)) {
            if (!last_space) { out += ' '; last_space = true; }
        } else { out += c; last_space = false; }
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
/* Thread-local eval error: set by eval_expr for fatal errors like unknown function */
static thread_local std::string g_eval_error;

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
        /* ROW(a, b, ...) — returns first argument (scalar context) */
        if (eu.substr(0, 4) == "ROW(" && fn_paren_ok(4-1)) {
            std::string args = e.substr(4, e.size()-5);
            /* Find first top-level comma */
            int depth = 0;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++depth;
                else if (args[i] == ')') --depth;
                else if (args[i] == ',' && depth == 0) {
                    return eval_expr(qry_trim(args.substr(0, i)), row, col_order);
                }
            }
            /* Single argument */
            return eval_expr(qry_trim(args), row, col_order);
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
        /* JSON functions (SVDB_EXT_JSON) */
#ifdef SVDB_EXT_JSON
        /* json_extract(json, path) */
        if (eu.substr(0, 13) == "JSON_EXTRACT(" && fn_paren_ok(13-1)) {
            std::string args = e.substr(13, e.size()-14);
            size_t comma = args.find(',');
            if (comma != std::string::npos) {
                SvdbVal json_val = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal path_val = eval_expr(args.substr(comma+1), row, col_order);
                if (json_val.type == SVDB_TYPE_NULL || path_val.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string json_str = val_to_str(json_val);
                std::string path_str = val_to_str(path_val);
                char *result = svdb_json_extract(json_str.c_str(), path_str.c_str());
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    /* Try to parse result as number */
                    try {
                        size_t pos;
                        double d = std::stod(result_str, &pos);
                        if (pos == result_str.size()) {
                            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = d; return v;
                        }
                    } catch (...) {}
                    /* Return as text */
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
                return SvdbVal{};
            }
        }
        /* json(json) - validate and return JSON */
        if (eu.substr(0, 5) == "JSON(" && fn_paren_ok(5-1)) {
            SvdbVal json_val = eval_expr(e.substr(5, e.size()-6), row, col_order);
            if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string json_str = val_to_str(json_val);
            if (svdb_json_validate(json_str.c_str())) {
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = json_str; return v;
            }
            return SvdbVal{};
        }
        /* json_array(...) */
        if (eu.substr(0, 11) == "JSON_ARRAY(" && fn_paren_ok(11-1)) {
            std::string args = e.substr(11, e.size()-12);
            std::vector<std::string> values;
            int depth = 0; size_t start = 0;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = (i < args.size()) ? args[i] : ',';
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) {
                    SvdbVal v = eval_expr(args.substr(start, i-start), row, col_order);
                    values.push_back(val_to_str(v));
                    start = i + 1;
                }
            }
            std::vector<const char*> c_values;
            for (auto &s : values) c_values.push_back(s.c_str());
            char *result = svdb_json_array(c_values.data(), (int)c_values.size());
            if (result) {
                std::string result_str(result);
                svdb_json_free(result);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
            }
            return SvdbVal{};
        }
        /* json_object(key1, val1, ...) */
        if (eu.substr(0, 12) == "JSON_OBJECT(" && fn_paren_ok(12-1)) {
            std::string args = e.substr(12, e.size()-13);
            std::vector<std::string> keys, values;
            int depth = 0; size_t start = 0; bool is_key = true;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = (i < args.size()) ? args[i] : ',';
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) {
                    SvdbVal v = eval_expr(args.substr(start, i-start), row, col_order);
                    if (is_key) keys.push_back(val_to_str(v));
                    else values.push_back(val_to_str(v));
                    is_key = !is_key;
                    start = i + 1;
                }
            }
            if (keys.size() == values.size()) {
                std::vector<const char*> c_keys, c_values;
                for (auto &s : keys) c_keys.push_back(s.c_str());
                for (auto &s : values) c_values.push_back(s.c_str());
                char *result = svdb_json_object(c_keys.data(), c_values.data(), (int)keys.size());
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
            }
            return SvdbVal{};
        }
        /* json_type(json, path) */
        if (eu.substr(0, 10) == "JSON_TYPE(" && fn_paren_ok(10-1)) {
            std::string args = e.substr(10, e.size()-11);
            /* Find comma outside of string literals */
            size_t comma = std::string::npos;
            int depth = 0; bool in_str = false;
            for (size_t i = 0; i < args.size(); ++i) {
                char c = args[i];
                if (c == '\'' && (i == 0 || args[i-1] != '\'')) { in_str = !in_str; continue; }
                if (in_str) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) { comma = i; break; }
            }
            if (comma != std::string::npos) {
                SvdbVal json_val = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal path_val = eval_expr(args.substr(comma+1), row, col_order);
                if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string json_str = val_to_str(json_val);
                std::string path_str = path_val.type == SVDB_TYPE_NULL ? "$" : val_to_str(path_val);
                char *result = svdb_json_type(json_str.c_str(), path_str.c_str());
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
                return SvdbVal{};
            }
            /* No comma - single argument json_type(json) */
            SvdbVal json_val = eval_expr(args, row, col_order);
            if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string json_str = val_to_str(json_val);
            char *result = svdb_json_type(json_str.c_str(), "$");
            if (result) {
                std::string result_str(result);
                svdb_json_free(result);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
            }
            return SvdbVal{};
        }
        /* json_length(json, path) */
        if (eu.substr(0, 12) == "JSON_LENGTH(" && fn_paren_ok(12-1)) {
            std::string args = e.substr(12, e.size()-13);
            /* Find comma outside of string literals */
            size_t comma = std::string::npos;
            int depth = 0; bool in_str = false;
            for (size_t i = 0; i < args.size(); ++i) {
                char c = args[i];
                if (c == '\'' && (i == 0 || args[i-1] != '\'')) { in_str = !in_str; continue; }
                if (in_str) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) { comma = i; break; }
            }
            SvdbVal json_val, path_val;
            if (comma != std::string::npos) {
                json_val = eval_expr(args.substr(0, comma), row, col_order);
                path_val = eval_expr(args.substr(comma+1), row, col_order);
            } else {
                json_val = eval_expr(args, row, col_order);
                path_val.type = SVDB_TYPE_NULL;
            }
            if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string json_str = val_to_str(json_val);
            std::string path_str = path_val.type == SVDB_TYPE_NULL ? "$" : val_to_str(path_val);
            int64_t len = svdb_json_length(json_str.c_str(), path_str.c_str());
            if (len < 0) return SvdbVal{}; /* Error case */
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = len; return v;
        }
        /* json_valid(json) / json_isvalid(json) */
        if ((eu.substr(0, 12) == "JSON_VALID(" || eu.substr(0, 13) == "JSON_ISVALID(") && fn_paren_ok(eu.substr(0, 12) == "JSON_VALID(" ? 11 : 12)) {
            size_t start = (eu.substr(0, 12) == "JSON_VALID(") ? 12 : 13;
            SvdbVal json_val = eval_expr(e.substr(start, e.size()-start-1), row, col_order);
            if (json_val.type == SVDB_TYPE_NULL) { SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0; return v; }
            std::string json_str = val_to_str(json_val);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = svdb_json_validate(json_str.c_str()); return v;
        }
        /* json_quote(value) */
        if (eu.substr(0, 11) == "JSON_QUOTE(" && fn_paren_ok(11-1)) {
            SvdbVal val = eval_expr(e.substr(11, e.size()-12), row, col_order);
            if (val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string val_str = val_to_str(val);
            char *result = svdb_json_quote(val_str.c_str());
            if (result) {
                std::string result_str(result);
                svdb_json_free(result);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
            }
            return SvdbVal{};
        }
        /* json_remove(json, path) */
        if (eu.substr(0, 12) == "JSON_REMOVE(" && fn_paren_ok(12-1)) {
            std::string args = e.substr(12, e.size()-13);
            /* Find comma outside of string literals */
            size_t comma = std::string::npos;
            int depth = 0; bool in_str = false;
            for (size_t i = 0; i < args.size(); ++i) {
                char c = args[i];
                if (c == '\'' && (i == 0 || args[i-1] != '\'')) { in_str = !in_str; continue; }
                if (in_str) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) { comma = i; break; }
            }
            if (comma != std::string::npos) {
                SvdbVal json_val = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal path_val = eval_expr(args.substr(comma+1), row, col_order);
                if (json_val.type == SVDB_TYPE_NULL || path_val.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string json_str = val_to_str(json_val);
                std::string path_str = val_to_str(path_val);
                const char *paths[] = {path_str.c_str()};
                char *result = svdb_json_remove(json_str.c_str(), paths, 1);
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
                return SvdbVal{};
            }
        }
        /* json_set(json, path, value) */
        if (eu.substr(0, 9) == "JSON_SET(" && fn_paren_ok(9-1)) {
            std::string args = e.substr(9, e.size()-10);
            /* Parse: json, path, value */
            std::vector<std::string> parts;
            int depth = 0; size_t start = 0;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = (i < args.size()) ? args[i] : ',';
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) {
                    SvdbVal v = eval_expr(args.substr(start, i-start), row, col_order);
                    parts.push_back(val_to_str(v));
                    start = i + 1;
                }
            }
            if (parts.size() >= 3) {
                const char *path_value_pairs[] = {parts[1].c_str(), parts[2].c_str()};
                char *result = svdb_json_set(parts[0].c_str(), path_value_pairs, 1);
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
            }
            return SvdbVal{};
        }
        /* jsonb(json) - return canonical JSON (same as json() for now) */
        if (eu.substr(0, 6) == "JSONB(" && fn_paren_ok(6-1)) {
            SvdbVal json_val = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string json_str = val_to_str(json_val);
            char *result = svdb_json_minify(json_str.c_str());
            if (result) {
                std::string result_str(result);
                svdb_json_free(result);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
            }
            return SvdbVal{};
        }
        /* json_array_insert(json, path, value) */
        if (eu.substr(0, 18) == "JSON_ARRAY_INSERT(" && fn_paren_ok(18-1)) {
            std::string args = e.substr(18, e.size()-19);
            std::vector<std::string> parts;
            int depth = 0; size_t start = 0; bool in_s = false;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = (i < args.size()) ? args[i] : ',';
                if (c == '\'' && depth == 0) { in_s = !in_s; continue; }
                if (in_s) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) {
                    SvdbVal v = eval_expr(args.substr(start, i-start), row, col_order);
                    parts.push_back(val_to_str(v));
                    start = i + 1;
                }
            }
            if (parts.size() >= 3) {
                char *result = svdb_json_array_insert(parts[0].c_str(), parts[1].c_str(), parts[2].c_str());
                if (result) {
                    std::string result_str(result);
                    svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
                }
            }
            return SvdbVal{};
        }
        /* json_pretty(json) */
        if (eu.substr(0, 12) == "JSON_PRETTY(" && fn_paren_ok(12-1)) {
            SvdbVal json_val = eval_expr(e.substr(12, e.size()-13), row, col_order);
            if (json_val.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string json_str = val_to_str(json_val);
            char *result = svdb_json_pretty(json_str.c_str());
            if (result) {
                std::string result_str(result);
                svdb_json_free(result);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result_str; return v;
            }
            return SvdbVal{};
        }
        /* json_patch(target, patch) */
        if (eu.substr(0, 11) == "JSON_PATCH(" && fn_paren_ok(11-1)) {
            std::string args = e.substr(11, e.size()-12);
            size_t comma = std::string::npos;
            int depth = 0; bool in_str = false;
            for (size_t i = 0; i < args.size(); ++i) {
                char c = args[i];
                if (c == '\'') { in_str = !in_str; continue; }
                if (in_str) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                else if (c == ',' && depth == 0) { comma = i; break; }
            }
            if (comma != std::string::npos) {
                SvdbVal tgt = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal pat = eval_expr(args.substr(comma+1), row, col_order);
                if (tgt.type != SVDB_TYPE_NULL && pat.type != SVDB_TYPE_NULL) {
                    std::string ts = val_to_str(tgt), ps = val_to_str(pat);
                    char buf[65536];
                    if (svdb_json_patch(buf, sizeof(buf), ts.c_str(), ps.c_str()) == 0) {
                        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
                    }
                }
            }
            return SvdbVal{};
        }
#endif /* SVDB_EXT_JSON */

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
                if (type_str == "INTEGER" || type_str == "INT" || type_str == "BIGINT") {
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = val_to_i64(sv); return v;
                } else if (type_str == "REAL" || type_str == "FLOAT" || type_str == "DOUBLE" ||
                           type_str == "NUMERIC" || type_str == "DECIMAL") {
                    /* NUMERIC/DECIMAL have numeric affinity - prefer REAL for decimal values */
                    SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = val_to_dbl(sv); return v;
                } else if (type_str == "DATE" || type_str == "TIME" || type_str == "TIMESTAMP"
                           || type_str == "DATETIME") {
                    /* SQLite: DATE/TIME/TIMESTAMP are NUMERIC affinity aliases.
                     * Try INTEGER first (read leading integer from string), then REAL, then TEXT. */
                    if (sv.type == SVDB_TYPE_INT)  return sv;
                    if (sv.type == SVDB_TYPE_REAL) { SvdbVal v; v.type=SVDB_TYPE_INT; v.ival=(int64_t)sv.rval; return v; }
                    /* TEXT: read leading integer */
                    const std::string &s2 = sv.sval;
                    if (!s2.empty()) {
                        char *endp = nullptr;
                        int64_t ival = strtoll(s2.c_str(), &endp, 10);
                        if (endp > s2.c_str()) { SvdbVal v; v.type=SVDB_TYPE_INT; v.ival=ival; return v; }
                        double dval = strtod(s2.c_str(), &endp);
                        if (endp > s2.c_str()) { SvdbVal v; v.type=SVDB_TYPE_REAL; v.rval=dval; return v; }
                    }
                    return sv; /* fallback: keep as text */
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
            bool in_sq = false;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = i < args.size() ? args[i] : ',';
                if (c == '\'' && !in_sq) { in_sq = true; continue; }
                if (c == '\'' && in_sq) { if (i+1<args.size()&&args[i+1]=='\''){++i;continue;} in_sq=false; continue; }
                if (in_sq) continue;
                if (c == '(') ++rd; else if (c == ')') --rd;
                else if (c == ',' && rd == 0) { parts.push_back(args.substr(start2, i-start2)); start2 = i+1; }
            }
            if (parts.size() >= 3) {
                SvdbVal sv = eval_expr(parts[0], row, col_order);
                SvdbVal ov = eval_expr(parts[1], row, col_order);
                SvdbVal nv = eval_expr(parts[2], row, col_order);
                /* Any NULL arg → NULL result (SQLite behavior) */
                if (sv.type == SVDB_TYPE_NULL || ov.type == SVDB_TYPE_NULL || nv.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string src = val_to_str(sv);
                std::string old2 = val_to_str(ov);
                std::string new2 = val_to_str(nv);
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
        /* PRINTF(fmt, ...) / FORMAT(fmt, ...) — SQLite printf-style formatting */
        if ((eu.substr(0, 7) == "PRINTF(" && fn_paren_ok(6)) ||
            (eu.substr(0, 7) == "FORMAT(" && fn_paren_ok(6))) {
            size_t pref = 7;
            std::string args_str = e.substr(pref, e.size() - pref - 1);
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            bool in_s = false;
            for (size_t i = 0; i <= args_str.size(); ++i) {
                char c = i < args_str.size() ? args_str[i] : ',';
                if (c == '\'' && !in_s) { in_s = true; }
                else if (c == '\'' && in_s) {
                    if (i+1 < args_str.size() && args_str[i+1] == '\'') { ++i; }
                    else in_s = false;
                }
                if (!in_s) {
                    if (c == '(') ++rd; else if (c == ')') --rd;
                    else if (c == ',' && rd == 0) { parts.push_back(qry_trim(args_str.substr(start2, i-start2))); start2 = i+1; }
                }
            }
            if (!parts.empty()) {
                SvdbVal fmt_v = eval_expr(parts[0], row, col_order);
                if (fmt_v.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string fmt_s = val_to_str(fmt_v);
                std::string result;
                size_t ai = 1;
                for (size_t fi = 0; fi < fmt_s.size(); ++fi) {
                    if (fmt_s[fi] != '%') { result += fmt_s[fi]; continue; }
                    ++fi;
                    if (fi >= fmt_s.size()) break;
                    if (fmt_s[fi] == '%') { result += '%'; continue; }
                    /* Collect flags, width, precision, specifier */
                    std::string spec = "%";
                    while (fi < fmt_s.size() && (fmt_s[fi] == '-' || fmt_s[fi] == '+' || fmt_s[fi] == ' ' || fmt_s[fi] == '0' || fmt_s[fi] == '#')) spec += fmt_s[fi++];
                    while (fi < fmt_s.size() && isdigit((unsigned char)fmt_s[fi])) spec += fmt_s[fi++];
                    if (fi < fmt_s.size() && fmt_s[fi] == '.') { spec += fmt_s[fi++]; while (fi < fmt_s.size() && isdigit((unsigned char)fmt_s[fi])) spec += fmt_s[fi++]; }
                    if (fi >= fmt_s.size()) break;
                    char sp = fmt_s[fi]; spec += sp;
                    SvdbVal arg_v;
                    if (ai < parts.size()) arg_v = eval_expr(parts[ai++], row, col_order);
                    char buf[256] = {};
                    if (sp == 'd' || sp == 'i') {
                        int64_t iv = (arg_v.type == SVDB_TYPE_INT) ? arg_v.ival : (arg_v.type == SVDB_TYPE_REAL ? (int64_t)arg_v.rval : 0);
                        std::string s2 = spec.substr(0, spec.size()-1) + "lld";
                        snprintf(buf, sizeof(buf), s2.c_str(), (long long)iv);
                    } else if (sp == 'f' || sp == 'e' || sp == 'g') {
                        double dv = (arg_v.type == SVDB_TYPE_REAL) ? arg_v.rval : (arg_v.type == SVDB_TYPE_INT ? (double)arg_v.ival : 0.0);
                        snprintf(buf, sizeof(buf), spec.c_str(), dv);
                    } else if (sp == 's') {
                        std::string sv = val_to_str(arg_v);
                        snprintf(buf, sizeof(buf), spec.c_str(), sv.c_str());
                    } else {
                        result += spec; continue;
                    }
                    result += std::string(buf);
                }
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result; return v;
            }
        }
        /* QUOTE(val) — SQLite-style quoting: TEXT → 'escaped', NULL → NULL, INT/REAL → number */
        if (eu.substr(0, 6) == "QUOTE(" && fn_paren_ok(5)) {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) {
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = "NULL"; return v;
            }
            if (inner.type == SVDB_TYPE_INT) {
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = std::to_string(inner.ival); return v;
            }
            if (inner.type == SVDB_TYPE_REAL) {
                char buf[64]; snprintf(buf, sizeof(buf), "%.17g", inner.rval);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = std::string(buf); return v;
            }
            /* TEXT: wrap in single quotes, escape embedded single quotes as '' */
            std::string s2 = val_to_str(inner);
            std::string out = "'";
            for (char c : s2) { if (c == '\'') out += "''"; else out += c; }
            out += "'";
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = out; return v;
        }
        /* CHAR(n1, n2, ...) — build string from Unicode code points */
        if (eu.substr(0, 5) == "CHAR(" && fn_paren_ok(4)) {
            std::string args_str = e.substr(5, e.size()-6);
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            for (size_t i = 0; i <= args_str.size(); ++i) {
                char c = i < args_str.size() ? args_str[i] : ',';
                if (c == '(') ++rd; else if (c == ')') --rd;
                else if (c == ',' && rd == 0) { parts.push_back(qry_trim(args_str.substr(start2, i-start2))); start2 = i+1; }
            }
            std::string result;
            for (auto &part : parts) {
                SvdbVal cv = eval_expr(part, row, col_order);
                int64_t cp = (cv.type == SVDB_TYPE_INT) ? cv.ival : (cv.type == SVDB_TYPE_REAL ? (int64_t)cv.rval : 0);
                /* Encode Unicode code point as UTF-8 */
                if (cp < 0x80) { result += (char)cp; }
                else if (cp < 0x800) { result += (char)(0xC0|(cp>>6)); result += (char)(0x80|(cp&0x3F)); }
                else if (cp < 0x10000) { result += (char)(0xE0|(cp>>12)); result += (char)(0x80|((cp>>6)&0x3F)); result += (char)(0x80|(cp&0x3F)); }
                else { result += (char)(0xF0|(cp>>18)); result += (char)(0x80|((cp>>12)&0x3F)); result += (char)(0x80|((cp>>6)&0x3F)); result += (char)(0x80|(cp&0x3F)); }
            }
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = result; return v;
        }
        /* UNICODE(str) — return Unicode code point of first character */
        if (eu.substr(0, 8) == "UNICODE(" && fn_paren_ok(7)) {
            SvdbVal inner = eval_expr(e.substr(8, e.size()-9), row, col_order);
            if (inner.type == SVDB_TYPE_NULL || val_to_str(inner).empty()) return SvdbVal{};
            const std::string &s2 = val_to_str(inner);
            unsigned char c = (unsigned char)s2[0];
            int64_t cp = 0;
            if      (c < 0x80)  cp = c;
            else if (c < 0xE0 && s2.size() >= 2) cp = ((c & 0x1F) << 6)  | ((unsigned char)s2[1] & 0x3F);
            else if (c < 0xF0 && s2.size() >= 3) cp = ((c & 0x0F) << 12) | (((unsigned char)s2[1] & 0x3F) << 6) | ((unsigned char)s2[2] & 0x3F);
            else if (s2.size() >= 4) cp = ((c & 0x07) << 18) | (((unsigned char)s2[1] & 0x3F) << 12) | (((unsigned char)s2[2] & 0x3F) << 6) | ((unsigned char)s2[3] & 0x3F);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = cp; return v;
        }
        /* ZEROBLOB(N) — returns a BLOB of N zero bytes */
        if (eu.substr(0, 9) == "ZEROBLOB(" && fn_paren_ok(9-1)) {
            SvdbVal nv = eval_expr(e.substr(9, e.size()-10), row, col_order);
            int64_t n = (nv.type == SVDB_TYPE_INT) ? nv.ival : 0;
            if (n < 0) n = 0;
            SvdbVal v; v.type = SVDB_TYPE_BLOB; v.sval = std::string((size_t)n, '\0'); return v;
        }
        /* UNHEX(str) — decode hex string to BLOB */
        if (eu.substr(0, 7) == "UNHEX(" && fn_paren_ok(5)) {
            /* Note: "UNHEX(" is 6 chars */
        }
        if (eu.substr(0, 6) == "UNHEX(" && fn_paren_ok(5)) {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string hs = val_to_str(inner);
            /* Strip spaces */
            std::string hex;
            for (char c : hs) if (!isspace((unsigned char)c)) hex += c;
            if (hex.size() % 2 != 0) return SvdbVal{}; /* NULL for odd-length */
            auto fh = [](char c2) -> int {
                if (c2>='0'&&c2<='9') return c2-'0';
                if (c2>='a'&&c2<='f') return c2-'a'+10;
                if (c2>='A'&&c2<='F') return c2-'A'+10;
                return -1;
            };
            std::string blob;
            for (size_t i = 0; i < hex.size(); i += 2) {
                int hi = fh(hex[i]), lo = fh(hex[i+1]);
                if (hi < 0 || lo < 0) return SvdbVal{}; /* NULL for invalid hex */
                blob += (char)((hi << 4) | lo);
            }
            SvdbVal v; v.type = SVDB_TYPE_BLOB; v.sval = blob; return v;
        }
        /* RANDOM() — random signed 64-bit integer */
        if (eu == "RANDOM()") {
            SvdbVal v; v.type = SVDB_TYPE_INT;
            v.ival = (int64_t)(((uint64_t)rand() << 33) ^ ((uint64_t)rand() << 2) ^ (uint64_t)(rand() & 3));
            return v;
        }
        /* RANDOMBLOB(N) — N random bytes as BLOB */
        if (eu.substr(0, 11) == "RANDOMBLOB(" && fn_paren_ok(10)) {
            SvdbVal nv = eval_expr(e.substr(11, e.size()-12), row, col_order);
            int64_t n = (nv.type == SVDB_TYPE_INT) ? nv.ival : 0;
            if (n < 1) n = 1;
            std::string blob;
            for (int64_t i = 0; i < n; ++i) blob += (char)(rand() & 0xFF);
            SvdbVal v; v.type = SVDB_TYPE_BLOB; v.sval = blob; return v;
        }
        /* IIF(cond, true_val, false_val) — inline if */
        if (eu.substr(0, 4) == "IIF(" && fn_paren_ok(3)) {
            std::string args_str = e.substr(4, e.size()-5);
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            bool in_sq = false;
            for (size_t i = 0; i <= args_str.size(); ++i) {
                char c = i < args_str.size() ? args_str[i] : ',';
                if (c == '\'' && !in_sq) { in_sq = true; continue; }
                if (c == '\'' && in_sq) { if (i+1<args_str.size()&&args_str[i+1]=='\''){++i;continue;} in_sq=false; continue; }
                if (in_sq) continue;
                if (c == '(') ++rd; else if (c == ')') --rd;
                else if (c == ',' && rd == 0) { parts.push_back(args_str.substr(start2, i-start2)); start2 = i+1; }
            }
            if (parts.size() >= 3) {
                SvdbVal cond = eval_expr(parts[0], row, col_order);
                bool is_true = (cond.type == SVDB_TYPE_INT && cond.ival != 0) ||
                               (cond.type == SVDB_TYPE_REAL && cond.rval != 0.0) ||
                               (cond.type == SVDB_TYPE_TEXT && !cond.sval.empty());
                return eval_expr(is_true ? parts[1] : parts[2], row, col_order);
            }
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
        /* Also handles DATE(col), TIME(col), DATETIME(col), STRFTIME(fmt, col) with real dates */
        auto is_now_arg = [](const std::string &arg) -> bool {
            std::string a = arg; if (a.size()>=2 && a.front()=='\'') a = a.substr(1, a.size()-2);
            std::string au; for (char c : a) au += (char)toupper((unsigned char)c);
            return au == "NOW" || au == "NOW()";
        };
        /* Parse a date/datetime string into struct tm (supports YYYY-MM-DD [HH:MM:SS]) */
        auto parse_datetime_str = [](const std::string &s, std::tm &out) -> bool {
            memset(&out, 0, sizeof(out));
            /* Try YYYY-MM-DD HH:MM:SS */
            if (sscanf(s.c_str(), "%d-%d-%d %d:%d:%d",
                       &out.tm_year, &out.tm_mon, &out.tm_mday,
                       &out.tm_hour, &out.tm_min, &out.tm_sec) >= 3) {
                out.tm_year -= 1900; out.tm_mon -= 1;
                return true;
            }
            /* Try YYYY-MM-DD */
            if (sscanf(s.c_str(), "%d-%d-%d", &out.tm_year, &out.tm_mon, &out.tm_mday) == 3) {
                out.tm_year -= 1900; out.tm_mon -= 1;
                return true;
            }
            /* Try HH:MM:SS */
            if (sscanf(s.c_str(), "%d:%d:%d", &out.tm_hour, &out.tm_min, &out.tm_sec) == 3)
                return true;
            return false;
        };
        /* Resolve argument: if it's a string literal strip quotes, else eval as expr */
        auto resolve_dt_arg = [&](const std::string &arg) -> std::string {
            std::string a = qry_trim(arg);
            if (a.size() >= 2 && a.front() == '\'') return a.substr(1, a.size()-2);
            /* Not a literal — evaluate as expression */
            SvdbVal v = eval_expr(a, row, col_order);
            if (v.type == SVDB_TYPE_NULL) return "";
            if (v.type == SVDB_TYPE_TEXT) return v.sval;
            if (v.type == SVDB_TYPE_INT)  return std::to_string(v.ival);
            char buf[64]; snprintf(buf, sizeof(buf), "%.17g", v.rval); return buf;
        };
        if (eu.size() >= 9 && eu.substr(0, 9) == "STRFTIME(" && fn_paren_ok(8)) {
            std::string args_str = qry_trim(e.substr(9, e.size()-10));
            /* Split at top-level comma: first arg = format, second = datetime */
            size_t comma_pos = std::string::npos;
            { int dp=0; bool ins=false;
              for (size_t i=0;i<args_str.size();++i) {
                char c2=args_str[i]; if(c2=='\''){ins=!ins;continue;} if(ins)continue;
                if(c2=='(')++dp;else if(c2==')')--dp;
                else if(c2==','&&dp==0){comma_pos=i;break;}
              }
            }
            if (comma_pos != std::string::npos) {
                std::string fmt_arg = qry_trim(args_str.substr(0, comma_pos));
                std::string dt_arg  = qry_trim(args_str.substr(comma_pos + 1));
                std::string fmt_str;
                if (fmt_arg.size()>=2 && fmt_arg.front()=='\'') fmt_str=fmt_arg.substr(1,fmt_arg.size()-2);
                else fmt_str=fmt_arg;
                if (is_now_arg(dt_arg)) {
                    char buf[64]; fmt_utc(fmt_str.c_str(), buf, sizeof(buf));
                    SvdbVal v; v.type=SVDB_TYPE_TEXT; v.sval=buf; return v;
                }
                std::string dt_str = resolve_dt_arg(dt_arg);
                if (!dt_str.empty()) {
                    std::tm tm_buf{}; if (parse_datetime_str(dt_str, tm_buf)) {
                        tm_buf.tm_isdst = -1; mktime(&tm_buf); /* fills tm_wday, tm_yday */
                        char buf[64]; strftime(buf, sizeof(buf), fmt_str.c_str(), &tm_buf);
                        SvdbVal v; v.type=SVDB_TYPE_TEXT; v.sval=buf; return v;
                    }
                }
            }
            return SvdbVal{}; /* NULL on failure */
        }
        /* Helper: apply SQLite-compatible date modifiers to a tm struct */
        auto apply_date_modifier = [](std::tm &tm_in, const std::string &mod) -> bool {
            std::string m = mod;
            /* strip leading/trailing whitespace and quotes */
            while (!m.empty() && isspace((unsigned char)m.front())) m.erase(m.begin());
            while (!m.empty() && isspace((unsigned char)m.back())) m.pop_back();
            if (m.size() >= 2 && m.front() == '\'') m = m.substr(1, m.size()-2);
            if (m.empty()) return true;
            std::string mu; for (char c : m) mu += (char)toupper((unsigned char)c);
            /* +N days/day, -N days/day, +N months/month, +N years/year */
            int sign = 1; size_t p = 0;
            if (!mu.empty() && (mu[0] == '+' || mu[0] == '-')) { sign = (mu[0] == '-') ? -1 : 1; ++p; }
            char *endp = nullptr;
            long n = strtol(mu.c_str() + p, &endp, 10);
            if (!endp || endp == mu.c_str() + p) return false;
            std::string unit;
            const char *up = endp;
            while (*up == ' ') ++up;
            unit = up; for (auto &c2 : unit) c2 = (char)toupper((unsigned char)c2);
            int delta = (int)(sign * n);
            if (unit == "DAYS" || unit == "DAY") {
                tm_in.tm_mday += delta;
            } else if (unit == "MONTHS" || unit == "MONTH") {
                tm_in.tm_mon += delta;
            } else if (unit == "YEARS" || unit == "YEAR") {
                tm_in.tm_year += delta;
            } else if (unit == "HOURS" || unit == "HOUR") {
                tm_in.tm_hour += delta;
            } else if (unit == "MINUTES" || unit == "MINUTE") {
                tm_in.tm_min += delta;
            } else if (unit == "SECONDS" || unit == "SECOND") {
                tm_in.tm_sec += delta;
            } else { return false; }
            /* Normalize */
            tm_in.tm_isdst = -1; mktime(&tm_in);
            return true;
        };
        /* Parse all args of a date function: first is base datetime/col, rest are modifiers */
        auto parse_dt_args = [&](const std::string &args_str, std::string &base_out,
                                  std::vector<std::string> &mods_out) {
            base_out.clear(); mods_out.clear();
            int dp=0; bool ins=false; size_t start=0;
            for (size_t i=0; i<=args_str.size(); ++i) {
                char c2 = (i<args_str.size()) ? args_str[i] : ',';
                if(c2=='\''){ins=!ins; continue;} if(ins) continue;
                if(c2=='(')++dp; else if(c2==')')--dp;
                else if(c2==','&&dp==0){
                    std::string tok=qry_trim(args_str.substr(start,i-start));
                    if(base_out.empty()) base_out=tok;
                    else mods_out.push_back(tok);
                    start=i+1;
                }
            }
        };
        /* Helper: compute Julian Day Number from tm (proleptic Gregorian) */
        auto tm_to_julianday = [](const std::tm &tm_in, double frac_day) -> double {
            int Y = tm_in.tm_year + 1900;
            int M = tm_in.tm_mon + 1;
            int D = tm_in.tm_mday;
            int a = (14 - M) / 12;
            int y = Y + 4800 - a;
            int m = M + 12 * a - 3;
            int JDN = D + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
            /* Julian Day starts at noon; a calendar date with time 0:00 is JDN - 0.5 */
            return (double)JDN - 0.5 + frac_day;
        };
        if ((eu.substr(0, 5) == "DATE(" && fn_paren_ok(4)) || (eu.substr(0, 10) == "JULIANDAY(" && fn_paren_ok(9))) {
            bool is_julian = (eu.size() >= 10 && eu.substr(0, 10) == "JULIANDAY(");
            std::string args_str = qry_trim(e.substr(is_julian?10:5, e.size()-(is_julian?10:5)-1));
            std::string base_arg; std::vector<std::string> mods;
            parse_dt_args(args_str, base_arg, mods);
            if (is_now_arg(base_arg)) {
                std::time_t now = std::time(nullptr); std::tm tm_buf{}; gmtime_r(&now, &tm_buf);
                for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                if (is_julian) {
                    double frac = (tm_buf.tm_hour * 3600 + tm_buf.tm_min * 60 + tm_buf.tm_sec) / 86400.0;
                    SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = tm_to_julianday(tm_buf, frac); return v;
                }
                char buf[12]; strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_buf);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
            std::string dt_str = resolve_dt_arg(base_arg);
            if (!dt_str.empty()) {
                std::tm tm_buf{}; if (parse_datetime_str(dt_str, tm_buf)) {
                    tm_buf.tm_isdst = -1; mktime(&tm_buf);
                    for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                    if (is_julian) {
                        double frac = (tm_buf.tm_hour * 3600 + tm_buf.tm_min * 60 + tm_buf.tm_sec) / 86400.0;
                        SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = tm_to_julianday(tm_buf, frac); return v;
                    }
                    char buf[12]; strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_buf);
                    SvdbVal v; v.type=SVDB_TYPE_TEXT; v.sval=buf; return v;
                }
            }
            return SvdbVal{};
        }
        /* UNIXEPOCH(datetime) — seconds since 1970-01-01 00:00:00 UTC */
        if (eu.substr(0, 10) == "UNIXEPOCH(" && fn_paren_ok(9)) {
            std::string args_str = qry_trim(e.substr(10, e.size()-11));
            std::string base_arg; std::vector<std::string> mods;
            parse_dt_args(args_str, base_arg, mods);
            auto to_epoch = [&](std::tm &tm_buf) -> int64_t {
                for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                /* Portable UTC→epoch: timegm or manual calculation */
                int Y = tm_buf.tm_year + 1900;
                int M = tm_buf.tm_mon + 1;
                int D = tm_buf.tm_mday;
                /* Days since 1970-01-01 via JDN method */
                int a = (14-M)/12, y2 = Y+4800-a, m2 = M+12*a-3;
                int64_t jdn = D + (153*m2+2)/5 + 365*y2 + y2/4 - y2/100 + y2/400 - 32045;
                int64_t jdn_epoch = 2440588; /* JDN of 1970-01-01 */
                int64_t days = jdn - jdn_epoch;
                return days * 86400 + tm_buf.tm_hour * 3600 + tm_buf.tm_min * 60 + tm_buf.tm_sec;
            };
            if (is_now_arg(base_arg)) {
                std::time_t now = std::time(nullptr); std::tm tm_buf{}; gmtime_r(&now, &tm_buf);
                SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = to_epoch(tm_buf); return v;
            }
            std::string dt_str = resolve_dt_arg(base_arg);
            if (!dt_str.empty()) {
                std::tm tm_buf{}; if (parse_datetime_str(dt_str, tm_buf)) {
                    SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = to_epoch(tm_buf); return v;
                }
            }
            return SvdbVal{};
        }
        if (eu.substr(0, 5) == "TIME(" && fn_paren_ok(4)) {
            std::string args_str = qry_trim(e.substr(5, e.size()-6));
            std::string base_arg; std::vector<std::string> mods;
            parse_dt_args(args_str, base_arg, mods);
            if (is_now_arg(base_arg)) {
                std::time_t now = std::time(nullptr); std::tm tm_buf{}; gmtime_r(&now, &tm_buf);
                for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                char buf[10]; strftime(buf, sizeof(buf), "%H:%M:%S", &tm_buf);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
            std::string dt_str = resolve_dt_arg(base_arg);
            if (!dt_str.empty()) {
                std::tm tm_buf{}; if (parse_datetime_str(dt_str, tm_buf)) {
                    tm_buf.tm_isdst = -1; mktime(&tm_buf);
                    for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                    char buf[10]; strftime(buf, sizeof(buf), "%H:%M:%S", &tm_buf);
                    SvdbVal v; v.type=SVDB_TYPE_TEXT; v.sval=buf; return v;
                }
            }
            return SvdbVal{};
        }
        if (eu.substr(0, 9) == "DATETIME(" && fn_paren_ok(8)) {
            std::string args_str = qry_trim(e.substr(9, e.size()-10));
            std::string base_arg; std::vector<std::string> mods;
            parse_dt_args(args_str, base_arg, mods);
            if (is_now_arg(base_arg)) {
                std::time_t now = std::time(nullptr); std::tm tm_buf{}; gmtime_r(&now, &tm_buf);
                for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                char buf[24]; strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
                SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = buf; return v;
            }
            std::string dt_str = resolve_dt_arg(base_arg);
            if (!dt_str.empty()) {
                std::tm tm_buf{}; if (parse_datetime_str(dt_str, tm_buf)) {
                    tm_buf.tm_isdst = -1; mktime(&tm_buf);
                    for (auto &mod : mods) apply_date_modifier(tm_buf, mod);
                    char buf[24]; strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
                    SvdbVal v; v.type=SVDB_TYPE_TEXT; v.sval=buf; return v;
                }
            }
            return SvdbVal{};
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
            /* Skip CASE...END blocks: when we see END, find matching CASE and skip to it */
            if (i >= 3) {
                char c0 = s[i], c1 = (i>=1) ? s[i-1] : ' ', c2 = (i>=2) ? s[i-2] : ' ', c3 = (i>=3) ? s[i-3] : ' ';
                if ((c0 == 'D' || c0 == 'd') && (c1 == 'N' || c1 == 'n') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == ' ' || c3 == '(' || c3 == ',' || c3 == '=' || c3 == '+' || c3 == '-' || c3 == '*' || c3 == '/' || c3 == '>' || c3 == '<' || c3 == '\t' || c3 == '\n' || c3 == '\r')) {
                    /* Found END, now find matching CASE by counting nested END/CASE pairs */
                    int end_count = 1;
                    int j = i - 3;
                    while (j >= 3 && end_count > 0) {
                        char jc0 = s[j], jc1 = s[j-1], jc2 = s[j-2], jc3 = s[j-3];
                        if ((jc0 == 'D' || jc0 == 'd') && (jc1 == 'N' || jc1 == 'n') && (jc2 == 'E' || jc2 == 'e') &&
                            (jc3 == ' ' || jc3 == '(' || jc3 == ',' || jc3 == '=' || jc3 == '+' || jc3 == '-' || jc3 == '*' || jc3 == '/' || jc3 == '>' || jc3 == '<')) {
                            ++end_count; j -= 3;
                        } else if ((jc0 == 'E' || jc0 == 'e') && (jc1 == 'S' || jc1 == 's') && (jc2 == 'A' || jc2 == 'a') && (jc3 == 'C' || jc3 == 'c') &&
                            (j-4 < 0 || s[j-4] == ' ' || s[j-4] == '(' || s[j-4] == ',' || s[j-4] == '=' || s[j-4] == '+' || s[j-4] == '-' || s[j-4] == '*' || s[j-4] == '/' || s[j-4] == '>' || s[j-4] == '<')) {
                            --end_count; j -= 3;
                        }
                        --j;
                    }
                    i = j + 1; continue; /* Skip to just after CASE */
                }
            }
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
                        /* Skip '>' that is part of '->' or '->>' (JSON arrow operator) */
                        if ((op == ">" || op == ">=") && i > 0 && e[i-1] == '-') ok = false;
                        if (op == ">" && i > 1 && e[i-1] == '>' && e[i-2] == '-') ok = false;
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

#ifdef SVDB_EXT_JSON
    /* JSON arrow operators: ->> (extract as text) and -> (extract as JSON) */
    {
        int depth_arrow = 0;
        bool in_str_arrow = false;
        /* Scan right-to-left to find ->> or -> at top level */
        for (int i = (int)e.size()-1; i >= 1; --i) {
            char c = e[i];
            if (e[i] == '\'') { in_str_arrow = !in_str_arrow; continue; }
            if (in_str_arrow) continue;
            if (c == ')') ++depth_arrow;
            else if (c == '(') { if (depth_arrow > 0) --depth_arrow; }
            if (depth_arrow > 0) continue;
            /* Check for ->> at position i-1, i, i+1 */
            if (i+1 < (int)e.size() && e[i-1] == '-' && e[i] == '>' && e[i+1] == '>') {
                SvdbVal lhs = eval_expr(e.substr(0, i-1), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(i+2), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string json_str = val_to_str(lhs);
                std::string path_str = val_to_str(rhs);
                char *result = svdb_json_extract(json_str.c_str(), path_str.c_str());
                if (result) {
                    std::string rs(result); svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = rs; return v;
                }
                return SvdbVal{};
            }
            /* Check for -> at position i-1, i (but not ->>) */
            if (i < (int)e.size() && e[i-1] == '-' && e[i] == '>' &&
                (i+1 >= (int)e.size() || e[i+1] != '>')) {
                SvdbVal lhs = eval_expr(e.substr(0, i-1), row, col_order);
                SvdbVal rhs = eval_expr(e.substr(i+1), row, col_order);
                if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                std::string json_str = val_to_str(lhs);
                std::string path_str = val_to_str(rhs);
                char *result = svdb_json_extract(json_str.c_str(), path_str.c_str());
                if (result) {
                    std::string rs(result); svdb_json_free(result);
                    SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = rs; return v;
                }
                return SvdbVal{};
            }
        }
    }
#endif /* SVDB_EXT_JSON */

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
    /* First try: full expression with any prefix (handles alias.col in merged rows,
     * and aggregate results like COUNT(*) stored in having_row) */
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

    /* If the expression looks like a function call (IDENT(...)) but wasn't matched
     * by any known function handler above, and wasn't found as a row key, report an
     * error rather than silently returning NULL. */
    {
        size_t paren = e.find('(');
        if (paren != std::string::npos && e.back() == ')') {
            std::string fn = qry_trim(e.substr(0, paren));
            bool looks_like_func = !fn.empty();
            for (char ch : fn) {
                if (!isalnum((unsigned char)ch) && ch != '_') { looks_like_func = false; break; }
            }
            if (looks_like_func && !fn.empty() && isalpha((unsigned char)fn[0])) {
                g_eval_error = "no such function: " + fn;
                return SvdbVal{};
            }
        }
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
        /* Helper: substitute outer row refs into a SQL string.
         * Handles both qualified (table.col) and unqualified (col) names.
         * Uses longest-match first to avoid partial substitutions.
         * Does NOT substitute if the column is qualified with a different table. */
        auto subst_outer = [&](std::string sub_sql) -> std::string {
            /* Build list of (key, value) pairs sorted by key length (longest first) */
            std::vector<std::pair<std::string, std::string>> subs;
            for (auto &kv : row) {
                if (kv.first.empty()) continue;
                std::string repl;
                if (kv.second.type == SVDB_TYPE_NULL) repl = "NULL";
                else if (kv.second.type == SVDB_TYPE_INT) repl = std::to_string(kv.second.ival);
                else if (kv.second.type == SVDB_TYPE_REAL) {
                    char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); repl = buf;
                } else { repl = "'" + kv.second.sval + "'"; }
                subs.push_back({kv.first, repl});
            }
            /* Sort by key length (longest first) to avoid partial matches */
            std::sort(subs.begin(), subs.end(),
                [](const auto &a, const auto &b) { return a.first.size() > b.first.size(); });

            /* Substitute each key */
            for (auto &sub : subs) {
                const std::string &key = sub.first;
                const std::string &repl = sub.second;
                /* Check if this is a qualified key (contains '.') */
                bool is_qualified = (key.find('.') != std::string::npos);
                
                for (size_t p = sub_sql.find(key); p != std::string::npos;
                     p = sub_sql.find(key, p)) {
                    /* Check word boundaries */
                    bool lb = (p == 0 || (!isalnum((unsigned char)sub_sql[p-1]) && sub_sql[p-1] != '_'));
                    bool rb = (p + key.size() >= sub_sql.size() ||
                               (!isalnum((unsigned char)sub_sql[p+key.size()]) && sub_sql[p+key.size()] != '_'));
                    
                    /* Additional check: if key is unqualified, make sure it's not preceded by 'table.' */
                    if (!is_qualified && lb && p > 0) {
                        /* Check if preceded by identifier and dot (e.g., "order_line.") */
                        size_t dot_pos = p - 1;
                        if (sub_sql[dot_pos] == '.') {
                            /* Skip this - it's part of a qualified reference */
                            p += key.size();
                            continue;
                        }
                    }
                    
                    if (lb && rb) { sub_sql.replace(p, key.size(), repl); p += repl.size(); }
                    else { p += key.size(); }
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
                    /* Substitute ONLY qualified outer row refs (table.col) to support
                     * correlated subqueries. Unqualified names are NOT substituted to
                     * avoid corrupting non-correlated subqueries where the inner table
                     * happens to share a column name with the outer row. */
                    std::string sub_sql = qry_trim(inside);
                    for (auto &kv : row) {
                        if (kv.first.empty()) continue;
                        /* Only substitute qualified references (key contains '.') */
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

struct OrderCol { std::string expr; bool desc; bool nocase = false; int nulls = 0; /* 0=default,-1=NULLS FIRST,+1=NULLS LAST */ };
struct JoinSpec  {
    std::string type;      /* INNER/LEFT/RIGHT/CROSS/NATURAL */
    std::string table;
    std::string alias;
    std::string on_left;   /* simple equality: left side */
    std::string on_right;  /* simple equality: right side */
    std::string using_col; /* USING (col) */
    std::string on_expr;   /* full ON expression (used for complex conditions) */
};

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
        /* Split table name and optional alias (handle "AS alias" syntax) */
        size_t sp = t.find(' ');
        if (sp != std::string::npos) {
            j.table = qry_trim(t.substr(0, sp));
            std::string rest = qry_trim(t.substr(sp + 1));
            std::string restu = qry_upper(rest);
            if (restu.size() >= 3 && restu.substr(0, 3) == "AS ")
                j.alias = qry_trim(rest.substr(3));
            else
                j.alias = rest;
        } else {
            j.table = t;
        }
        result.push_back(j);
    }
    return result;
}

/* Return the position of a FETCH FIRST/NEXT clause in an upper-cased string,
 * or std::string::npos if absent. */
static size_t find_fetch_clause(const std::string &su_upper, size_t from = 0) {
    size_t fp = su_upper.find("FETCH FIRST ", from);
    size_t fn = su_upper.find("FETCH NEXT ",  from);
    if (fp == std::string::npos) return fn;
    if (fn == std::string::npos) return fp;
    return (fp < fn) ? fp : fn;
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
    /* End at LIMIT / FETCH FIRST / FETCH NEXT or end of string */
    size_t end = su.find("LIMIT ", pos);
    if (end == std::string::npos) end = find_fetch_clause(su, pos);
    std::string ob_text = (end != std::string::npos)
        ? sql.substr(pos + 9, end - pos - 9)
        : sql.substr(pos + 9);
    /* Split by comma */
    std::istringstream ss(ob_text);
    std::string token;
    while (std::getline(ss, token, ',')) {
        token = qry_trim(token);
        bool desc = false; bool nocase = false; int nulls_opt = 0;
        std::string tu = qry_upper(token);
        /* Strip NULLS FIRST / NULLS LAST */
        if (tu.size() >= 12 && tu.substr(tu.size()-12) == " NULLS FIRST") {
            nulls_opt = -1;
            token = qry_trim(token.substr(0, token.size()-12));
        } else if (tu.size() >= 11 && tu.substr(tu.size()-11) == " NULLS LAST") {
            nulls_opt = +1;
            token = qry_trim(token.substr(0, token.size()-11));
        }
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
        result.push_back({token, desc, nocase, nulls_opt});
    }
    return result;
}

/* Extract LIMIT and OFFSET */
static void parse_limit_offset(const std::string &sql, int64_t &limit, int64_t &offset) {
    limit = -1; offset = 0;
    std::string su = qry_upper(sql);
    size_t lpos = su.find("LIMIT ");
    if (lpos == std::string::npos) {
        /* Also handle FETCH FIRST/NEXT n ROWS ONLY */
        size_t fpos = find_fetch_clause(su);
        if (fpos != std::string::npos) {
            /* Advance past the keyword ("FETCH FIRST " or "FETCH NEXT ") */
            size_t ns = fpos + (su.substr(fpos, 12) == "FETCH FIRST " ? 12 : 11);
            while (ns < su.size() && isspace((unsigned char)su[ns])) ++ns;
            size_t ne = ns;
            while (ne < su.size() && isdigit((unsigned char)su[ne])) ++ne;
            if (ne > ns) {
                try { limit = std::stoll(su.substr(ns, ne - ns)); } catch (...) {}
            }
        }
        return;
    }
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
    if (end == std::string::npos) end = find_fetch_clause(su, pos);
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
    if (end == std::string::npos) end = find_fetch_clause(su, pos);
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
    /* Read potential alias: handle optional "AS alias" syntax */
    size_t alias_start = ts + 1;
    while (alias_start < after.size() && after[alias_start] == ' ') ++alias_start;
    /* Skip optional AS keyword */
    std::string rest_check = qry_upper(after.substr(alias_start));
    if (rest_check.size() >= 3 && rest_check.substr(0,3) == "AS " ) {
        alias_start += 3;
        while (alias_start < after.size() && after[alias_start] == ' ') ++alias_start;
        rest_check = qry_upper(after.substr(alias_start));
    }
    /* Check it's not a keyword */
    std::string rest = rest_check;
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
    static const char *join_kws[] = {
        "NATURAL LEFT OUTER JOIN ", "NATURAL LEFT JOIN ",
        "NATURAL RIGHT OUTER JOIN ", "NATURAL RIGHT JOIN ",
        "NATURAL INNER JOIN ", "NATURAL JOIN ",
        "INNER JOIN ", "LEFT OUTER JOIN ", "LEFT JOIN ",
        "RIGHT OUTER JOIN ", "RIGHT JOIN ",
        "CROSS JOIN ", " JOIN ", nullptr
    };
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
                    else if (kwup == "RIGHT JOIN " || kwup == "RIGHT OUTER JOIN ") jtype = "RIGHT";
                    else if (kwup == "CROSS JOIN ") jtype = "CROSS";
                    else if (kwup == "NATURAL JOIN " || kwup == "NATURAL INNER JOIN ") jtype = "NATURAL";
                    else if (kwup == "NATURAL LEFT JOIN " || kwup == "NATURAL LEFT OUTER JOIN ") jtype = "NATURAL LEFT";
                    else if (kwup == "NATURAL RIGHT JOIN " || kwup == "NATURAL RIGHT OUTER JOIN ") jtype = "NATURAL RIGHT";
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
        /* Read optional alias (handle "AS alias" syntax) */
        if (te != std::string::npos) {
            size_t as = te + 1;
            while (as < frag_su.size() && frag_su[as] == ' ') ++as;
            /* Skip optional AS keyword */
            if (frag_su.size() >= as + 3 && frag_su.substr(as, 3) == "AS ") {
                as += 3;
                while (as < frag_su.size() && frag_su[as] == ' ') ++as;
            }
            static const char *stop_kws2[] = {"ON ", "USING ", "WHERE ", "ORDER ", "GROUP ", "LIMIT ", "INNER ", "LEFT ", "RIGHT ", "CROSS ", "JOIN ", nullptr};
            bool is_stop2 = false;
            for (const char **kw = stop_kws2; *kw; ++kw) {
                if (frag_su.size() >= as + strlen(*kw) && frag_su.substr(as, strlen(*kw)) == std::string(*kw)) { is_stop2 = true; break; }
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
            for (const char *kw2 : {" WHERE ", " ORDER ", " GROUP ", " LIMIT ", " HAVING "}) {
                size_t kp = on_up.find(kw2);
                if (kp != std::string::npos) on_expr = on_expr.substr(0, kp);
            }
            j.on_expr = qry_trim(on_expr); /* store full ON expression */
            /* Also parse simple equality for backward compat */
            size_t eq_pos = j.on_expr.find('=');
            if (eq_pos != std::string::npos && j.on_expr.find(' ') < eq_pos &&
                qry_upper(j.on_expr).find(" AND ") == std::string::npos &&
                qry_upper(j.on_expr).find(" OR ") == std::string::npos) {
                /* Simple "a = b" with no AND/OR — extract sides */
                j.on_left  = qry_trim(j.on_expr.substr(0, eq_pos));
                j.on_right = qry_trim(j.on_expr.substr(eq_pos + 1));
            } else if (eq_pos != std::string::npos &&
                       qry_upper(j.on_expr).find(" AND ") == std::string::npos &&
                       qry_upper(j.on_expr).find(" OR ") == std::string::npos) {
                j.on_left  = qry_trim(j.on_expr.substr(0, eq_pos));
                j.on_right = qry_trim(j.on_expr.substr(eq_pos + 1));
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
    std::string func;    /* COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/JSON_GROUP_ARRAY/JSON_GROUP_OBJECT */
    std::string arg;     /* column or * */
    std::string sep;     /* separator for GROUP_CONCAT */
    std::string wrapper; /* outer scalar function: ABS/UPPER/LOWER/etc. */
    std::string cast_type; /* type for CAST wrapper */
    int64_t count = 0;
    double  sum   = 0.0;
    int64_t isum  = 0;     /* integer-only accumulator (used when is_real is false) */
    SvdbVal min_val, max_val;
    bool has_min = false, has_max = false;
    bool is_real = false;
    bool distinct = false; /* COUNT(DISTINCT ...) */
    std::unordered_set<std::string> seen_vals; /* for DISTINCT counting */
    std::vector<std::string> concat_vals; /* for GROUP_CONCAT */
    std::vector<SvdbVal>    json_vals;    /* for JSON_GROUP_ARRAY values */
    std::vector<std::pair<std::string,SvdbVal>> json_kv_vals; /* for JSON_GROUP_OBJECT key-value pairs */
    std::string arg2;    /* second argument (JSON_GROUP_OBJECT value expr) */
};

static bool is_agg_expr(const std::string &e) {
    /* Window functions (e.g. SUM(...) OVER (...)) are NOT regular aggregates */
    if (is_window_expr(e)) return false;
    std::string eu = qry_upper(e);
    /* Check for aggregate functions at the TOP LEVEL (not inside a subquery) */
    static const char *agg_fns[] = {"COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT(", "GROUP CONCAT(", "JSON_GROUP_ARRAY(", "JSON_GROUP_OBJECT(", nullptr};
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
    /* Also check for CAST wrapper around aggregate: CAST(AVG(x) AS INT) */
    if (eu.substr(0, 5) == "CAST(") {
        /* Find the inner expression */
        size_t inner_start = 5;
        size_t inner_end = e.size() - 1;
        std::string inner = qry_trim(e.substr(inner_start, inner_end - inner_start));
        return is_agg_expr(inner);
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
    /* Check for outer scalar wrapper: e.g. ABS(MIN(a)), UPPER(MAX(s)), CAST(AVG(x) AS INT) */
    static const char *scalar_funcs[] = {"ABS", "UPPER", "LOWER", "ROUND", "CEIL", "FLOOR", "LENGTH", "CAST", nullptr};
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
                /* For CAST wrapper, extract the type */
                if (std::string(*sf) == "CAST") {
                    size_t as_pos = inner_eu.find(" AS ");
                    if (as_pos != std::string::npos) {
                        a.cast_type = qry_trim(inner_expr.substr(as_pos + 4));
                        /* Remove trailing ')' if present */
                        if (!a.cast_type.empty() && a.cast_type.back() == ')') {
                            a.cast_type.pop_back();
                        }
                    }
                }
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
    /* JSON_GROUP_ARRAY(expr) */
    if (a.func.empty() && eu.substr(0, 17) == "JSON_GROUP_ARRAY(" && eu.back() == ')') {
        a.func = "JSON_GROUP_ARRAY";
        a.arg  = qry_trim(e_orig.substr(17, e_orig.size() - 18));
    }
    /* JSON_GROUP_OBJECT(key_expr, val_expr) */
    if (a.func.empty() && eu.substr(0, 18) == "JSON_GROUP_OBJECT(" && eu.back() == ')') {
        std::string inner = e_orig.substr(18, e_orig.size() - 19);
        int d2 = 0; size_t cp = std::string::npos;
        for (size_t i = 0; i < inner.size(); ++i) {
            if (inner[i] == '(') ++d2; else if (inner[i] == ')') --d2;
            else if (inner[i] == ',' && d2 == 0) { cp = i; break; }
        }
        a.func = "JSON_GROUP_OBJECT";
        if (cp != std::string::npos) {
            a.arg  = qry_trim(inner.substr(0, cp));
            a.arg2 = qry_trim(inner.substr(cp+1));
        } else {
            a.arg = qry_trim(inner);
        }
    }
    return a;
}

/* Extract top-level aggregate sub-expressions from a compound expression.
 * E.g. "SUM(i) + SUM(r)" → ["SUM(i)", "SUM(r)"] */
static std::vector<std::string> extract_agg_subexprs(const std::string &expr) {
    std::vector<std::string> result;
    std::string eu = qry_upper(expr);
    static const char *agg_names[] = {"COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT", nullptr};
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
    } else if (a.func == "JSON_GROUP_ARRAY") {
        a.json_vals.push_back(v);
    } else if (a.func == "JSON_GROUP_OBJECT") {
        /* v is the key; evaluate arg2 for the value */
        SvdbVal v2 = a.arg2.empty() ? SvdbVal{} : eval_expr(a.arg2, row, col_order);
        a.json_kv_vals.push_back({val_to_str(v), v2});
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
    } else if (a.func == "JSON_GROUP_ARRAY") {
        /* Build JSON array from collected values */
        std::string res = "[";
        for (size_t i = 0; i < a.json_vals.size(); ++i) {
            if (i > 0) res += ",";
            const SvdbVal &sv = a.json_vals[i];
            if (sv.type == SVDB_TYPE_NULL)       res += "null";
            else if (sv.type == SVDB_TYPE_INT)   res += std::to_string(sv.ival);
            else if (sv.type == SVDB_TYPE_REAL) {
                std::ostringstream oss;
                if (sv.rval == (int64_t)sv.rval) oss << (int64_t)sv.rval;
                else oss << std::setprecision(15) << sv.rval;
                res += oss.str();
            } else {
                /* TEXT: JSON-encode the string */
                res += "\"";
                for (char c : sv.sval) {
                    if (c == '"')  res += "\\\"";
                    else if (c == '\\') res += "\\\\";
                    else if (c == '\n') res += "\\n";
                    else if (c == '\r') res += "\\r";
                    else if (c == '\t') res += "\\t";
                    else res += c;
                }
                res += "\"";
            }
        }
        res += "]";
        base_result.type = SVDB_TYPE_TEXT; base_result.sval = res;
    } else if (a.func == "JSON_GROUP_OBJECT") {
        /* Build JSON object from collected key-value pairs */
        std::string res = "{";
        for (size_t i = 0; i < a.json_kv_vals.size(); ++i) {
            if (i > 0) res += ",";
            /* Key (always a string) */
            res += "\"";
            for (char c : a.json_kv_vals[i].first) {
                if (c == '"')  res += "\\\"";
                else if (c == '\\') res += "\\\\";
                else res += c;
            }
            res += "\":";
            /* Value */
            const SvdbVal &sv = a.json_kv_vals[i].second;
            if (sv.type == SVDB_TYPE_NULL)       res += "null";
            else if (sv.type == SVDB_TYPE_INT)   res += std::to_string(sv.ival);
            else if (sv.type == SVDB_TYPE_REAL) {
                std::ostringstream oss;
                if (sv.rval == (int64_t)sv.rval) oss << (int64_t)sv.rval;
                else oss << std::setprecision(15) << sv.rval;
                res += oss.str();
            } else {
                res += "\"";
                for (char c : sv.sval) {
                    if (c == '"')  res += "\\\"";
                    else if (c == '\\') res += "\\\\";
                    else if (c == '\n') res += "\\n";
                    else if (c == '\r') res += "\\r";
                    else if (c == '\t') res += "\\t";
                    else res += c;
                }
                res += "\"";
            }
        }
        res += "}";
        base_result.type = SVDB_TYPE_TEXT; base_result.sval = res;
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
        } else if (a.wrapper == "CAST") {
            /* For CAST wrapper, apply type conversion using stored cast_type */
            if (!a.cast_type.empty()) {
                std::string type_str = qry_upper(a.cast_type);
                /* Handle NUMERIC/DECIMAL as REAL */
                if (type_str.find("NUMERIC") != std::string::npos || 
                    type_str.find("DECIMAL") != std::string::npos) {
                    type_str = "REAL";
                }
                /* Apply type conversion */
                if (type_str == "INTEGER" || type_str == "INT") {
                    if (base_result.type == SVDB_TYPE_REAL) {
                        base_result.ival = (int64_t)base_result.rval;
                        base_result.type = SVDB_TYPE_INT;
                    } else if (base_result.type == SVDB_TYPE_TEXT) {
                        char *endp = nullptr;
                        int64_t iv = strtoll(base_result.sval.c_str(), &endp, 10);
                        if (endp != base_result.sval.c_str() && *endp == '\0') {
                            base_result.ival = iv;
                            base_result.type = SVDB_TYPE_INT;
                        }
                    }
                } else if (type_str == "REAL" || type_str == "FLOAT" || type_str == "DOUBLE") {
                    if (base_result.type == SVDB_TYPE_INT) {
                        base_result.rval = (double)base_result.ival;
                        base_result.type = SVDB_TYPE_REAL;
                    } else if (base_result.type == SVDB_TYPE_TEXT) {
                        char *endp = nullptr;
                        double dv = strtod(base_result.sval.c_str(), &endp);
                        if (endp != base_result.sval.c_str() && *endp == '\0') {
                            base_result.rval = dv;
                            base_result.type = SVDB_TYPE_REAL;
                        }
                    }
                } else if (type_str == "TEXT") {
                    if (base_result.type == SVDB_TYPE_INT) {
                        base_result.sval = std::to_string(base_result.ival);
                        base_result.type = SVDB_TYPE_TEXT;
                    } else if (base_result.type == SVDB_TYPE_REAL) {
                        char buf[64];
                        snprintf(buf, sizeof(buf), "%.17g", base_result.rval);
                        base_result.sval = buf;
                        base_result.type = SVDB_TYPE_TEXT;
                    }
                }
            }
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

    /* Clear any leftover eval error from a previous call */
    g_eval_error.clear();

    /* Set thread-local DB context for subquery support */
    svdb_db_t *prev_db = g_query_db;
    g_query_db = db;
    struct DbGuard { svdb_db_t **p; svdb_db_t *v; ~DbGuard() { *p = v; } } db_guard{&g_query_db, prev_db};

    /* ── information_schema / sqlite_master / sqlite_sequence / sqlite_stat1 intercept ── */
    {
        std::string su_is = qry_upper(qry_trim(sql));

        /* sqlite_stat1 virtual table */
        if (su_is.find(" FROM SQLITE_STAT1") != std::string::npos) {
            svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
            if (!r) return SVDB_NOMEM;
            r->col_names = {"tbl", "idx", "stat"};
            for (auto &entry : db->stat1) {
                SvdbVal v_tbl, v_idx, v_stat;
                v_tbl.type = SVDB_TYPE_TEXT; v_tbl.sval = std::get<0>(entry);
                v_idx.type = SVDB_TYPE_TEXT; v_idx.sval = std::get<1>(entry);
                v_stat.type = SVDB_TYPE_TEXT; v_stat.sval = std::get<2>(entry);
                r->rows.push_back({v_tbl, v_idx, v_stat});
            }
            *rows_out = r;
            return SVDB_OK;
        }

        /* sqlvibe_extensions virtual table */
        auto se_pos = su_is.find(" FROM SQLVIBE_EXTENSIONS");
        if (se_pos != std::string::npos) {
            /* Parse WHERE clause */
            std::string where_txt;
            {
                size_t wp = su_is.find(" WHERE ", se_pos);
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
            r->col_names = {"name", "description"};
            /* Built-in extensions */
            struct ExtInfo { const char *name; const char *desc; };
            ExtInfo exts[] = {
                {"json", "JSON functions"},
                {"math", "Math functions"},
                {"fts5", "Full-text search"}
            };
            for (auto &ext : exts) {
                Row rd;
                rd["name"] = SvdbVal{SVDB_TYPE_TEXT,0,0,ext.name};
                rd["description"] = SvdbVal{SVDB_TYPE_TEXT,0,0,ext.desc};
                if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                r->rows.push_back({rd["name"], rd["description"]});
            }
            *rows_out = r;
            return SVDB_OK;
        }

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
            /* Apply ORDER BY if present */
            {
                auto order_cols2 = parse_order_by(sql);
                if (!order_cols2.empty()) {
                    std::stable_sort(r->rows.begin(), r->rows.end(),
                        [&](const std::vector<SvdbVal> &a, const std::vector<SvdbVal> &b) {
                            for (auto &oc : order_cols2) {
                                int ci4 = -1;
                                for (size_t i = 0; i < r->col_names.size(); ++i)
                                    if (qry_upper(r->col_names[i]) == qry_upper(oc.expr)) { ci4 = (int)i; break; }
                                if (ci4 < 0 || ci4 >= (int)a.size() || ci4 >= (int)b.size()) continue;
                                const SvdbVal &av = a[ci4], &bv = b[ci4];
                                std::string as2 = (av.type==SVDB_TYPE_TEXT)?av.sval:std::to_string(av.ival);
                                std::string bs2 = (bv.type==SVDB_TYPE_TEXT)?bv.sval:std::to_string(bv.ival);
                                int cmp = as2.compare(bs2);
                                if (cmp != 0) return oc.desc ? cmp > 0 : cmp < 0;
                            }
                            return false;
                        });
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

            } else if (is_view == "KEY_COLUMN_USAGE") {
                r->col_names = {"constraint_catalog","constraint_schema","constraint_name",
                                 "table_schema","table_name","column_name","ordinal_position"};
                /* Primary key columns */
                for (auto &kv : db->primary_keys) {
                    if (kv.second.empty()) continue;
                    int ord = 1;
                    for (auto &col : kv.second) {
                        Row rd;
                        rd["constraint_catalog"]=SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_schema"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_name"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"PRIMARY"};
                        rd["table_schema"]      =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["table_name"]        =SvdbVal{SVDB_TYPE_TEXT,0,0,kv.first};
                        rd["column_name"]       =SvdbVal{SVDB_TYPE_TEXT,0,0,col};
                        rd["ordinal_position"]  =SvdbVal{SVDB_TYPE_INT,ord++,0,""};
                        if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                        r->rows.push_back({rd["constraint_catalog"],rd["constraint_schema"],
                                           rd["constraint_name"],rd["table_schema"],rd["table_name"],
                                           rd["column_name"],rd["ordinal_position"]});
                    }
                }
                /* Column-level primary keys (single-col PK stored in schema) */
                for (auto &tbl : db->col_order) {
                    if (db->primary_keys.count(tbl.first)) continue; /* already handled */
                    int ord = 1;
                    for (auto &cn : tbl.second) {
                        auto &td = db->schema[tbl.first];
                        auto cit = td.find(cn);
                        if (cit == td.end() || !cit->second.primary_key) continue;
                        Row rd;
                        rd["constraint_catalog"]=SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_schema"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_name"]   =SvdbVal{SVDB_TYPE_TEXT,0,0,"PRIMARY"};
                        rd["table_schema"]      =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["table_name"]        =SvdbVal{SVDB_TYPE_TEXT,0,0,tbl.first};
                        rd["column_name"]       =SvdbVal{SVDB_TYPE_TEXT,0,0,cn};
                        rd["ordinal_position"]  =SvdbVal{SVDB_TYPE_INT,ord++,0,""};
                        if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                        r->rows.push_back({rd["constraint_catalog"],rd["constraint_schema"],
                                           rd["constraint_name"],rd["table_schema"],rd["table_name"],
                                           rd["column_name"],rd["ordinal_position"]});
                    }
                }
            } else if (is_view == "REFERENTIAL_CONSTRAINTS") {
                r->col_names = {"constraint_catalog","constraint_schema","constraint_name",
                                 "unique_constraint_catalog","unique_constraint_schema",
                                 "unique_constraint_name","match_option","update_rule","delete_rule"};
                for (auto &kv : db->fk_constraints) {
                    int fidx = 0;
                    for (auto &fk : kv.second) {
                        std::string cname = kv.first + "_fk_" + std::to_string(fidx++);
                        Row rd;
                        rd["constraint_catalog"]        =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_schema"]         =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["constraint_name"]           =SvdbVal{SVDB_TYPE_TEXT,0,0,cname};
                        rd["unique_constraint_catalog"] =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["unique_constraint_schema"]  =SvdbVal{SVDB_TYPE_TEXT,0,0,"main"};
                        rd["unique_constraint_name"]    =SvdbVal{SVDB_TYPE_TEXT,0,0,fk.parent_table+"_pkey"};
                        rd["match_option"]              =SvdbVal{SVDB_TYPE_TEXT,0,0,"NONE"};
                        rd["update_rule"]               =SvdbVal{SVDB_TYPE_TEXT,0,0,"NO ACTION"};
                        rd["delete_rule"]               =SvdbVal{SVDB_TYPE_TEXT,0,0,"NO ACTION"};
                        if (!where_txt.empty() && !qry_eval_where(rd, r->col_names, where_txt)) continue;
                        r->rows.push_back({rd["constraint_catalog"],rd["constraint_schema"],
                                           rd["constraint_name"],rd["unique_constraint_catalog"],
                                           rd["unique_constraint_schema"],rd["unique_constraint_name"],
                                           rd["match_option"],rd["update_rule"],rd["delete_rule"]});
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


    /* ── CTE (WITH clause) handling ── */
    /* Detect: WITH name AS (SELECT ...) [, name2 AS (...)] SELECT ... */
    {
        std::string su_cte = qry_upper(qry_trim(sql));
        if (su_cte.size() >= 5 && su_cte.substr(0, 5) == "WITH ") {
            /* Parse one or more CTEs: WITH n1 AS (q1), n2 AS (q2) SELECT ... */
            struct CTEDef { std::string name; std::string query; std::vector<std::string> col_names; };
            std::vector<CTEDef> cte_list;
            size_t pos = 5; /* after "WITH " */
            bool parse_ok = true;
            while (parse_ok) {
                /* Skip whitespace */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                /* Read CTE name */
                size_t ns = pos;
                while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                if (pos == ns) { parse_ok = false; break; }
                std::string cte_name = sql.substr(ns, pos - ns);
                /* Skip RECURSIVE keyword */
                if (qry_upper(cte_name) == "RECURSIVE") {
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    ns = pos;
                    while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                    if (pos == ns) { parse_ok = false; break; }
                    cte_name = sql.substr(ns, pos - ns);
                }
                /* Optional column list: name(col1, col2, ...) */
                std::vector<std::string> cte_cols;
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                if (pos < sql.size() && sql[pos] == '(') {
                    size_t cl_end = pos + 1; int cl_depth = 1;
                    while (cl_end < sql.size() && cl_depth > 0) {
                        if (sql[cl_end] == '(') ++cl_depth;
                        else if (sql[cl_end] == ')') --cl_depth;
                        ++cl_end;
                    }
                    std::string col_list_str = sql.substr(pos + 1, cl_end - pos - 2);
                    std::istringstream css(col_list_str);
                    std::string col_tok;
                    while (std::getline(css, col_tok, ',')) cte_cols.push_back(qry_trim(col_tok));
                    pos = cl_end;
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                }
                /* Expect AS */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                std::string su_next = qry_upper(sql.substr(pos, 2));
                if (su_next != "AS") { parse_ok = false; break; }
                pos += 2;
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                /* Expect opening ( */
                if (pos >= sql.size() || sql[pos] != '(') { parse_ok = false; break; }
                /* Find matching ) */
                int depth2 = 0; size_t close_p = std::string::npos;
                for (size_t i = pos; i < sql.size(); ++i) {
                    if (sql[i] == '(') ++depth2;
                    else if (sql[i] == ')') { if (--depth2 == 0) { close_p = i; break; } }
                }
                if (close_p == std::string::npos) { parse_ok = false; break; }
                std::string cte_query = qry_trim(sql.substr(pos + 1, close_p - pos - 1));
                cte_list.push_back({cte_name, cte_query, cte_cols});
                pos = close_p + 1;
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                if (pos < sql.size() && sql[pos] == ',') { ++pos; continue; }
                break; /* next token should be SELECT */
            }
            if (parse_ok && !cte_list.empty()) {
                /* Remaining SQL is the main query */
                std::string main_query = qry_trim(sql.substr(pos));
                std::string mqu = qry_upper(main_query);
                if (mqu.size() >= 7 && mqu.substr(0, 7) == "SELECT ") {
                    /* Execute each CTE and store as temp table */
                    std::vector<std::string> tmp_names;
                    for (auto &cte : cte_list) {
                        svdb_rows_t *cte_rows = nullptr;
                        /* Check for recursive CTE: query references itself AND has UNION ALL.
                         * Use word-boundary check to avoid substring false positives
                         * (e.g. CTE named 'a' matching 'table_a'). */
                        static const size_t UNION_ALL_LEN = 10; /* length of " UNION ALL" */
                        std::string cte_q_upper = qry_upper(cte.query);
                        std::string cte_name_upper = qry_upper(cte.name);
                        /* Verify " FROM <name><boundary>" (boundary = space, comma, ), or end) */
                        bool has_self_ref = false;
                        {
                            std::string from_kw = " FROM " + cte_name_upper;
                            size_t fp = cte_q_upper.find(from_kw);
                            while (fp != std::string::npos) {
                                size_t after = fp + from_kw.size();
                                if (after >= cte_q_upper.size() ||
                                    !isalnum((unsigned char)cte_q_upper[after]) &&
                                    cte_q_upper[after] != '_') {
                                    has_self_ref = true; break;
                                }
                                fp = cte_q_upper.find(from_kw, fp + 1);
                            }
                        }
                        size_t ua_pos = std::string::npos;
                        if (has_self_ref) {
                            int ua_d = 0; bool ua_s = false;
                            for (size_t i2 = 0; i2 + UNION_ALL_LEN <= cte_q_upper.size(); ++i2) {
                                char c2 = cte_q_upper[i2];
                                if (c2 == '\'') { ua_s = !ua_s; continue; }
                                if (ua_s) continue;
                                if (c2 == '(') { ++ua_d; continue; }
                                if (c2 == ')') { if (ua_d > 0) --ua_d; continue; }
                                if (ua_d == 0 && cte_q_upper.substr(i2, UNION_ALL_LEN) == " UNION ALL") { ua_pos = i2; break; }
                            }
                        }
                        if (has_self_ref && ua_pos != std::string::npos) {
                            /* Recursive CTE execution */
                            std::string anchor_q = qry_trim(cte.query.substr(0, ua_pos));
                            std::string recur_q  = qry_trim(cte.query.substr(ua_pos + 10));
                            svdb_code_t rc_a = svdb_query_internal(db, anchor_q, &cte_rows);
                            if (rc_a != SVDB_OK) { if (cte_rows) delete cte_rows; return rc_a; }
                            /* Apply column renaming to anchor result BEFORE seeding,
                             * so the recursive part can reference columns by declared names */
                            if (!cte.col_names.empty() && cte_rows) {
                                for (size_t ci = 0; ci < cte.col_names.size() && ci < cte_rows->col_names.size(); ++ci)
                                    cte_rows->col_names[ci] = cte.col_names[ci];
                            }
                            /* Seed temp table with anchor results so recursive part can read it */
                            std::string tmp_rname = cte.name;
                            db->schema[tmp_rname] = {};
                            db->col_order[tmp_rname] = {};
                            db->data[tmp_rname] = {};
                            if (cte_rows) {
                                for (const auto &cn : cte_rows->col_names) {
                                    db->schema[tmp_rname][cn] = ColDef{"TEXT", "", false, false};
                                    db->col_order[tmp_rname].push_back(cn);
                                }
                                for (const auto &irow : cte_rows->rows) {
                                    Row r_seed;
                                    for (size_t ci = 0; ci < cte_rows->col_names.size() && ci < irow.size(); ++ci)
                                        r_seed[cte_rows->col_names[ci]] = irow[ci];
                                    db->data[tmp_rname].push_back(r_seed);
                                }
                            }
                            /* Iterative recursion — cap at 1000 iterations to prevent
                             * infinite loops; matches SQLite's compile-time default. */
                            int max_iter = 1000;
                            while (max_iter-- > 0) {
                                svdb_rows_t *new_rows = nullptr;
                                svdb_code_t rc_r = svdb_query_internal(db, recur_q, &new_rows);
                                if (rc_r != SVDB_OK || !new_rows || new_rows->rows.empty()) {
                                    if (new_rows) delete new_rows;
                                    break;
                                }
                                /* Rename new_rows columns so next iteration can reference them */
                                if (!cte.col_names.empty()) {
                                    for (size_t ci = 0; ci < cte.col_names.size() && ci < new_rows->col_names.size(); ++ci)
                                        new_rows->col_names[ci] = cte.col_names[ci];
                                }
                                for (auto &irow : new_rows->rows) cte_rows->rows.push_back(irow);
                                db->data[tmp_rname].clear();
                                for (const auto &irow : new_rows->rows) {
                                    Row r_next;
                                    for (size_t ci = 0; ci < new_rows->col_names.size() && ci < irow.size(); ++ci)
                                        r_next[new_rows->col_names[ci]] = irow[ci];
                                    db->data[tmp_rname].push_back(r_next);
                                }
                                delete new_rows;
                            }
                            /* Remove temporary recursive seed table; main setup below rebuilds it */
                            db->schema.erase(tmp_rname);
                            db->col_order.erase(tmp_rname);
                            db->data.erase(tmp_rname);
                        } else {
                            svdb_code_t rc = svdb_query_internal(db, cte.query, &cte_rows);
                            if (rc != SVDB_OK) { if (cte_rows) delete cte_rows; return rc; }
                        }
                        /* Apply column renaming if a column list was specified (non-recursive case) */
                        if (!cte.col_names.empty() && cte_rows && ua_pos == std::string::npos) {
                            for (size_t ci = 0; ci < cte.col_names.size() && ci < cte_rows->col_names.size(); ++ci)
                                cte_rows->col_names[ci] = cte.col_names[ci];
                        }
                        /* Store as temp table with CTE name */
                        std::string tmp_name = cte.name;
                        db->schema[tmp_name] = {};
                        db->col_order[tmp_name] = {};
                        db->data[tmp_name] = {};
                        if (cte_rows) {
                            for (const auto &cn : cte_rows->col_names) {
                                db->schema[tmp_name][cn] = ColDef{"TEXT", "", false, false};
                                db->col_order[tmp_name].push_back(cn);
                            }
                            for (const auto &irow : cte_rows->rows) {
                                Row r_new;
                                for (size_t ci = 0; ci < cte_rows->col_names.size() && ci < irow.size(); ++ci)
                                    r_new[cte_rows->col_names[ci]] = irow[ci];
                                db->data[tmp_name].push_back(r_new);
                            }
                            delete cte_rows;
                        }
                        tmp_names.push_back(tmp_name);
                    }
                    /* Execute main query with CTE tables available */
                    svdb_rows_t *result = nullptr;
                    svdb_code_t rc2 = svdb_query_internal(db, main_query, &result);
                    /* Clean up temp CTE tables */
                    for (const auto &tn : tmp_names) {
                        db->schema.erase(tn);
                        db->col_order.erase(tn);
                        db->data.erase(tn);
                    }
                    if (rc2 != SVDB_OK) { if (result) delete result; return rc2; }
                    *rows_out = result;
                    return SVDB_OK;
                }
            }
        }
    }

    /* ── Standalone VALUES statement ── */
    {
        std::string su_v = qry_upper(qry_trim(sql));
        if (su_v.size() >= 7 && su_v.substr(0, 7) == "VALUES ") {
            svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
            if (!r) return SVDB_NOMEM;
            const std::string &raw = sql;
            /* Skip leading whitespace then "VALUES" (6 chars) */
            size_t pos = raw.find_first_not_of(" \t\r\n");
            pos += 6; /* len("VALUES") */
            while (pos < raw.size() && isspace((unsigned char)raw[pos])) ++pos;
            bool first_row = true;
            while (pos < raw.size()) {
                if (raw[pos] != '(') break;
                int depth = 0;
                size_t row_end = std::string::npos;
                for (size_t i = pos; i < raw.size(); ++i) {
                    if (raw[i] == '(') ++depth;
                    else if (raw[i] == ')') { if (--depth == 0) { row_end = i; break; } }
                }
                if (row_end == std::string::npos) break;
                std::string row_str = raw.substr(pos + 1, row_end - pos - 1);
                /* Split by commas (respecting nesting and string literals) */
                std::vector<std::string> elems;
                std::string cur; int nesting_depth = 0; bool in_string_literal = false;
                for (char c : row_str) {
                    if (c == '\'') { in_string_literal = !in_string_literal; cur += c; continue; }
                    if (in_string_literal) { cur += c; continue; }
                    if (c == '(') { ++nesting_depth; cur += c; }
                    else if (c == ')') { --nesting_depth; cur += c; }
                    else if (c == ',' && nesting_depth == 0) { elems.push_back(qry_trim(cur)); cur.clear(); }
                    else cur += c;
                }
                if (!qry_trim(cur).empty()) elems.push_back(qry_trim(cur));
                if (first_row) {
                    for (size_t i = 0; i < elems.size(); ++i)
                        r->col_names.push_back("column" + std::to_string(i + 1));
                    first_row = false;
                }
                std::vector<SvdbVal> data_row;
                Row empty_row; std::vector<std::string> empty_order;
                for (const auto &e : elems)
                    data_row.push_back(eval_expr(e, empty_row, empty_order));
                r->rows.push_back(data_row);
                pos = row_end + 1;
                while (pos < raw.size() && isspace((unsigned char)raw[pos])) ++pos;
                if (pos < raw.size() && raw[pos] == ',') { ++pos; while (pos < raw.size() && isspace((unsigned char)raw[pos])) ++pos; }
                else break;
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
                    /* Validate: ORDER BY/GROUP BY/HAVING not allowed in individual SELECTs of UNION/INTERSECT/EXCEPT.
                     * They must appear only at the end of the entire set operation. */
                    std::string lhs_upper = qry_upper(lhs);
                    std::string rhs_upper = qry_upper(rhs);
                    /* Check LHS for invalid clauses */
                    if (lhs_upper.find(" ORDER BY ") != std::string::npos ||
                        lhs_upper.find(" GROUP BY ") != std::string::npos ||
                        lhs_upper.find(" HAVING ") != std::string::npos) {
                        db->last_error = "SQL logic error: ORDER BY clause should come after " + 
                            std::string(kw, 1, kwlen-2) + " not before";
                        return SVDB_ERR;
                    }
                    /* Check RHS for trailing clauses (these will be applied to the combined result) */
                    std::string order_clause;
                    size_t ob = rhs_upper.rfind(" ORDER BY ");
                    if (ob == std::string::npos) ob = rhs_upper.rfind("ORDER BY ");
                    if (ob != std::string::npos) {
                        /* Check if there are other clauses before ORDER BY that shouldn't be there */
                        std::string before_ob = rhs_upper.substr(0, ob);
                        if (before_ob.find(" GROUP BY ") != std::string::npos ||
                            before_ob.find(" HAVING ") != std::string::npos) {
                            db->last_error = "SQL logic error: ORDER BY clause should come after " + 
                                std::string(kw, 1, kwlen-2);
                            return SVDB_ERR;
                        }
                        order_clause = rhs.substr(ob);
                        rhs = rhs.substr(0, ob);
                    }

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
                        if (all_mode) {
                            /* INTERSECT ALL: emit min(count_left, count_right) occurrences */
                            std::map<std::string, int64_t> right_counts;
                            for (auto &r : right->rows) right_counts[row_key(r)]++;
                            for (auto &r : left->rows) {
                                std::string k = row_key(r);
                                auto it = right_counts.find(k);
                                if (it != right_counts.end() && it->second > 0) {
                                    --it->second;
                                    result->rows.push_back(r);
                                }
                            }
                        } else {
                            std::set<std::string> right_keys;
                            for (auto &r : right->rows) right_keys.insert(row_key(r));
                            for (auto &r : left->rows) {
                                std::string k = row_key(r);
                                if (right_keys.count(k)) add_row(r);
                            }
                        }
                    } else if (is_except) {
                        if (all_mode) {
                            /* EXCEPT ALL: multiset – subtract one occurrence per right-side match */
                            std::map<std::string, int64_t> right_counts;
                            for (auto &r : right->rows) right_counts[row_key(r)]++;
                            for (auto &r : left->rows) {
                                std::string k = row_key(r);
                                auto it = right_counts.find(k);
                                if (it != right_counts.end() && it->second > 0) {
                                    --it->second;
                                } else {
                                    result->rows.push_back(r);
                                }
                            }
                        } else {
                            std::set<std::string> right_keys;
                            for (auto &r : right->rows) right_keys.insert(row_key(r));
                            for (auto &r : left->rows) {
                                std::string k = row_key(r);
                                if (!right_keys.count(k)) add_row(r);
                            }
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

#ifdef SVDB_EXT_JSON
    /* ── JSON Table-Valued Functions (json_each, json_tree, jsonb_each, jsonb_tree) ── */
    {
        std::string su_tvf = qry_upper(sql);
        size_t from_pos_tvf = std::string::npos;
        /* Find top-level " FROM " */
        {
            int dp = 0; bool ins = false;
            for (size_t i = 0; i + 6 <= su_tvf.size(); ++i) {
                char c = su_tvf[i];
                if (c == '\'') { ins = !ins; continue; }
                if (ins) continue;
                if (c == '(') { ++dp; continue; }
                if (c == ')') { if (dp > 0) --dp; continue; }
                if (dp > 0) continue;
                if (su_tvf.substr(i, 6) == " FROM ") { from_pos_tvf = i; break; }
            }
        }
        if (from_pos_tvf != std::string::npos) {
            size_t after_from = from_pos_tvf + 6;
            while (after_from < su_tvf.size() && su_tvf[after_from] == ' ') ++after_from;
            /* Check if FROM is followed by a JSON TVF name */
            static const char *tvf_names[] = {"JSON_EACH(", "JSON_TREE(", "JSONB_EACH(", "JSONB_TREE(", nullptr};
            bool is_tree[] = {false, true, false, true};
            int tvf_idx = -1;
            size_t tvf_name_len = 0;
            for (int ti = 0; tvf_names[ti]; ++ti) {
                size_t nl = strlen(tvf_names[ti]);
                if (after_from + nl <= su_tvf.size() &&
                    su_tvf.substr(after_from, nl) == tvf_names[ti]) {
                    tvf_idx = ti;
                    tvf_name_len = nl;
                    break;
                }
            }
            if (tvf_idx >= 0) {
                /* Find closing paren of the TVF call */
                size_t arg_start = after_from + tvf_name_len - 1; /* points to '(' */
                int dp2 = 0; size_t close_p = std::string::npos;
                for (size_t i = arg_start; i < sql.size(); ++i) {
                    if (sql[i] == '(') ++dp2;
                    else if (sql[i] == ')') { if (--dp2 == 0) { close_p = i; break; } }
                }
                if (close_p != std::string::npos) {
                    /* Extract the JSON argument */
                    std::string tvf_arg_expr = sql.substr(arg_start + 1, close_p - arg_start - 1);
                    /* Evaluate the argument */
                    Row empty_row; std::vector<std::string> empty_order;
                    SvdbVal arg_val = eval_expr(tvf_arg_expr, empty_row, empty_order);
                    std::string json_str = arg_val.type == SVDB_TYPE_NULL ? "" : val_to_str(arg_val);

                    /* Generate TVF rows */
                    svdb_json_tvf_rows_t *tvf_rows = nullptr;
                    if (!json_str.empty()) {
                        if (is_tree[tvf_idx])
                            tvf_rows = svdb_json_tree(json_str.c_str());
                        else
                            tvf_rows = svdb_json_each(json_str.c_str());
                    }

                    /* Extract optional alias */
                    std::string rest_sql = sql.substr(close_p + 1);
                    std::string rest_u = qry_upper(qry_trim(rest_sql));
                    std::string tvf_alias;
                    {
                        std::string rt = qry_trim(rest_u);
                        if (!rt.empty() && rt[0] != 'W' && rt[0] != 'O' && rt[0] != 'G' &&
                            rt[0] != 'L' && rt[0] != 'J' && rt[0] != 'I' && rt[0] != ',') {
                            size_t ae = 0;
                            while (ae < rt.size() && rt[ae] != ' ' && rt[ae] != ',') ++ae;
                            std::string word = rt.substr(0, ae);
                            static const char *stop[] = {"WHERE","ORDER","GROUP","LIMIT","HAVING",
                                                          "INNER","LEFT","JOIN","ON", nullptr};
                            bool is_stop = false;
                            for (const char **sw = stop; *sw; ++sw)
                                if (word == *sw) { is_stop = true; break; }
                            if (!is_stop && !word.empty()) tvf_alias = word;
                        }
                    }

                    /* Create temp table */
                    static const char *tvf_cols[] = {"key","value","type","atom","id","parent","fullkey","path"};
                    std::string tmp_tname = "__tvf_" + std::to_string((size_t)tvf_rows) + "_" + std::to_string(tvf_idx);
                    db->schema[tmp_tname] = {};
                    db->col_order[tmp_tname] = {};
                    db->data[tmp_tname] = {};
                    for (const char *cn : tvf_cols) {
                        db->schema[tmp_tname][cn] = ColDef{"TEXT", "", false, false};
                        db->col_order[tmp_tname].push_back(cn);
                    }

                    if (tvf_rows) {
                        for (int ri = 0; ri < tvf_rows->count; ++ri) {
                            svdb_json_tvf_row_t &tr = tvf_rows->rows[ri];
                            Row r_new;
                            r_new["key"]     = tr.key ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.key} : SvdbVal{};
                            r_new["value"]   = tr.value ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.value} : SvdbVal{};
                            r_new["type"]    = tr.type ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.type} : SvdbVal{};
                            r_new["atom"]    = tr.atom ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.atom} : SvdbVal{};
                            r_new["id"]      = SvdbVal{SVDB_TYPE_INT, tr.id, 0, ""};
                            r_new["parent"]  = tr.parent >= 0 ? SvdbVal{SVDB_TYPE_INT, tr.parent, 0, ""} : SvdbVal{};
                            r_new["fullkey"] = tr.fullkey ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.fullkey} : SvdbVal{};
                            r_new["path"]    = tr.path ? SvdbVal{SVDB_TYPE_TEXT, 0, 0, tr.path} : SvdbVal{};
                            db->data[tmp_tname].push_back(r_new);
                        }
                        svdb_json_tvf_rows_free(tvf_rows);
                    }

                    /* Rewrite SQL: replace TVF call with tmp table name */
                    std::string new_sql = sql.substr(0, from_pos_tvf) + " FROM " + tmp_tname;
                    if (!tvf_alias.empty()) {
                        /* Skip alias in rest_sql since we're using the table name */
                        size_t skip_alias = rest_sql.find(tvf_alias);
                        if (skip_alias != std::string::npos)
                            new_sql += rest_sql.substr(skip_alias + tvf_alias.size());
                        else
                            new_sql += rest_sql;
                    } else {
                        new_sql += rest_sql;
                    }

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
#endif /* SVDB_EXT_JSON */

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
                    svdb_rows_t *inner_rows = nullptr;
                    
                    /* Check if it's a VALUES table constructor */
                    if (inner_u.size() >= 7 && inner_u.substr(0, 7) == "VALUES ") {
                        /* Execute VALUES as standalone statement */
                        svdb_code_t rc = svdb_query_internal(db, inner_sql, &inner_rows);
                        if (rc != SVDB_OK) { if (inner_rows) delete inner_rows; return rc; }
                        if (!inner_rows) return SVDB_ERR;
                    } else if (inner_u.size() > 6 && inner_u.substr(0, 7) == "SELECT ") {
                        /* Execute the inner SELECT */
                        svdb_code_t rc = svdb_query_internal(db, inner_sql, &inner_rows);
                        if (rc != SVDB_OK) { if (inner_rows) delete inner_rows; return rc; }
                        if (!inner_rows) return SVDB_ERR;
                    } else {
                        /* Not a supported subquery type */
                    }
                    
                    if (inner_rows) {

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
            /* Check for evaluation errors (e.g., unknown function) */
            if (!g_eval_error.empty()) {
                db->last_error = g_eval_error;
                g_eval_error.clear();
                delete r;
                return SVDB_ERR;
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

    /* ── View resolution: redirect SELECT FROM view to its underlying query ── */
    if (db->create_sql.count(tname)) {
        std::string cs_up = qry_upper(db->create_sql.at(tname));
        size_t view_kw = cs_up.find("VIEW");
        if (view_kw != std::string::npos && view_kw < 20) {
            size_t as_pos = cs_up.find(" AS ");
            if (as_pos != std::string::npos) {
                std::string view_select = db->create_sql.at(tname).substr(as_pos + 4);
                view_select = qry_trim(view_select);
                std::string sql_up = qry_upper(sql);
                size_t from_p = sql_up.find(" FROM ");
                if (from_p != std::string::npos) {
                    size_t ts = from_p + 6;
                    while (ts < sql_up.size() && isspace((unsigned char)sql_up[ts])) ++ts;
                    size_t te = ts;
                    while (te < sql_up.size() && (isalnum((unsigned char)sql_up[te]) || sql_up[te] == '_')) ++te;
                    if (qry_upper(sql.substr(ts, te - ts)) == tname_upper) {
                        delete r;
                        std::string new_sql = sql.substr(0, ts) + "(" + view_select + ") AS " + tname + sql.substr(te);
                        svdb_rows_t *view_result = nullptr;
                        svdb_code_t rc2 = svdb_query_internal(db, new_sql, &view_result);
                        if (rc2 != SVDB_OK) { if (view_result) delete view_result; return rc2; }
                        *rows_out = view_result;
                        return SVDB_OK;
                    }
                }
            }
        }
    }

    /* Safe access to col_order — return empty result if metadata is missing */
    auto col_order_it = db->col_order.find(tname);
    if (col_order_it == db->col_order.end()) {
        /* Table exists in schema but col_order is missing (e.g., incomplete CTAS) */
        *rows_out = r;
        return SVDB_OK;
    }
    const auto &col_order = col_order_it->second;

    /* ── Build joined rows if JOIN present ── */
    std::vector<Row> all_rows;
    std::vector<std::string> merged_col_order;

    /* Parse left table alias */
    std::string left_alias = parse_left_alias(sql);

    /* Qualified lookup keys for SELECT * (supports JOINs with overlapping column names) */
    std::vector<std::string> star_lookup_keys;

    if (!join.type.empty()) {
        /* Resolve right table case-insensitively */
        std::string right_tname = join.table;
        for (auto &kv : db->schema) {
            if (qry_upper(kv.first) == qry_upper(join.table)) { right_tname = kv.first; break; }
        }
        /* Safe access to right table data and col_order */
        const std::vector<Row> *right_data = nullptr;
        std::vector<std::string> right_col_order;
        if (db->schema.count(right_tname)) {
            auto data_it = db->data.find(right_tname);
            auto col_it = db->col_order.find(right_tname);
            if (data_it != db->data.end()) right_data = &data_it->second;
            if (col_it != db->col_order.end()) right_col_order = col_it->second;
        }

        /* Right alias: prefer join.alias, else right_tname */
        std::string right_alias = join.alias.empty() ? right_tname : join.alias;

        bool is_natural   = (join.type == "NATURAL" || join.type == "NATURAL LEFT" || join.type == "NATURAL RIGHT");
        bool is_right_jn  = (join.type == "RIGHT"   || join.type == "NATURAL RIGHT");
        bool is_left_jn   = (join.type == "LEFT"    || join.type == "NATURAL LEFT");

        /* For NATURAL JOIN: find common column names */
        std::vector<std::string> natural_cols;
        if (is_natural) {
            for (const auto &lc : col_order) {
                for (const auto &rc : right_col_order)
                    if (qry_upper(lc) == qry_upper(rc)) { natural_cols.push_back(lc); break; }
            }
        }

        /* Merged col order: left cols first, then right (NATURAL JOIN deduplicates common cols) */
        for (const auto &c : col_order) {
            merged_col_order.push_back(c);
            star_lookup_keys.push_back((!left_alias.empty() ? left_alias : tname) + "." + c);
        }
        for (const auto &c : right_col_order) {
            if (is_natural) {
                bool is_common = false;
                for (const auto &nc : natural_cols) if (qry_upper(c) == qry_upper(nc)) { is_common = true; break; }
                if (is_common) continue;
            }
            /* For USING join: skip the join column(s) (already in left side of merged_col_order) */
            if (!join.using_col.empty()) {
                bool is_using_col = false;
                std::istringstream uss(join.using_col);
                std::string uc;
                while (std::getline(uss, uc, ',')) {
                    if (qry_upper(c) == qry_upper(qry_trim(uc))) { is_using_col = true; break; }
                }
                if (is_using_col) continue;
            }
            merged_col_order.push_back(c);
            star_lookup_keys.push_back(right_alias + "." + c);
        }

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
                for (auto &kv : *prrow) {
                    merged[right_alias + "." + kv.first] = kv.second;
                    if (right_tname != tname) merged[right_tname + "." + kv.first] = kv.second;
                    /* Bare col for non-self-joins (do NOT overwrite left's value for common cols) */
                    if (right_tname != tname && merged.find(kv.first) == merged.end())
                        merged[kv.first] = kv.second;
                }
            } else {
                /* NULL-fill right columns for LEFT/NATURAL LEFT JOIN no-match */
                for (const auto &c : right_col_order) {
                    merged[right_alias + "." + c] = SvdbVal{};
                    if (right_tname != tname) { merged[right_tname + "." + c] = SvdbVal{}; merged[c] = SvdbVal{}; }
                }
            }
            return merged;
        };

        /* Helper: build NULL-left + right-row merged row (for RIGHT JOIN unmatched) */
        auto make_right_unmatched_row = [&](const Row &rrow) -> Row {
            Row merged;
            for (const auto &c : col_order) {
                merged[c] = SvdbVal{};
                merged[tname + "." + c] = SvdbVal{};
                if (!left_alias.empty()) merged[left_alias + "." + c] = SvdbVal{};
            }
            for (auto &kv : rrow) {
                merged[right_alias + "." + kv.first] = kv.second;
                if (right_tname != tname) merged[right_tname + "." + kv.first] = kv.second;
                merged[kv.first] = kv.second;
            }
            /* For NATURAL/USING joins: join column value comes from right table,
             * so update the left-prefixed keys for join columns to the right value */
            auto set_join_col = [&](const std::string &col) {
                auto it = rrow.find(col);
                if (it != rrow.end()) {
                    merged[tname + "." + col] = it->second;
                    if (!left_alias.empty()) merged[left_alias + "." + col] = it->second;
                }
            };
            for (const auto &nc : natural_cols) set_join_col(nc);
            if (!join.using_col.empty()) {
                /* Split using_col by comma for multi-column USING */
                std::istringstream uss(join.using_col);
                std::string uc;
                while (std::getline(uss, uc, ',')) set_join_col(qry_trim(uc));
            }
            return merged;
        };

        /* Helper: evaluate ON match for a pair of left/right rows */
        auto eval_on_match = [&](const Row &lrow_prefixed, const Row &rrow) -> bool {
            if (is_natural) {
                if (natural_cols.empty()) return true; /* no common cols → cross join */
                Row combined_row = lrow_prefixed;
                for (auto &kv : rrow) {
                    combined_row[right_tname + "." + kv.first] = kv.second;
                    combined_row[right_alias + "." + kv.first] = kv.second;
                }
                for (const auto &nc : natural_cols) {
                    std::string lk = (!left_alias.empty() ? left_alias : tname) + "." + nc;
                    std::string rk = right_alias + "." + nc;
                    SvdbVal lv = eval_expr(lk, combined_row, merged_col_order);
                    SvdbVal rv = eval_expr(rk, combined_row, merged_col_order);
                    if (lv.type == SVDB_TYPE_NULL || rv.type == SVDB_TYPE_NULL || val_cmp(lv, rv) != 0)
                        return false;
                }
                return true;
            }
            if (join.using_col.empty() && join.on_expr.empty()) return true; /* CROSS JOIN */

            /* Build combined row with both left and right prefixed cols */
            Row rrow_prefixed;
            for (auto &kv : rrow) {
                rrow_prefixed[kv.first] = kv.second;
                rrow_prefixed[right_tname + "." + kv.first] = kv.second;
                rrow_prefixed[right_alias + "." + kv.first] = kv.second;
            }
            Row combined_row = lrow_prefixed;
            for (auto &kv : rrow_prefixed) combined_row[kv.first] = kv.second;

            if (!join.using_col.empty()) {
                /* Handle comma-separated USING columns (USING (a, b, ...)) */
                std::istringstream uss(join.using_col);
                std::string uc;
                while (std::getline(uss, uc, ',')) {
                    uc = qry_trim(uc);
                    std::string lk = (!left_alias.empty() ? left_alias : tname) + "." + uc;
                    std::string rk = right_alias + "." + uc;
                    SvdbVal lv = eval_expr(lk, combined_row, merged_col_order);
                    SvdbVal rv = eval_expr(rk, combined_row, merged_col_order);
                    if (lv.type == SVDB_TYPE_NULL || rv.type == SVDB_TYPE_NULL) return false;
                    if (val_cmp(lv, rv) != 0) return false;
                }
                return true;
            }
            /* Complex ON condition: evaluate full expression with qry_eval_where */
            return qry_eval_where(combined_row, merged_col_order, join.on_expr);
        };

        auto right_rows_list = right_data ? *right_data : std::vector<Row>{};
        std::vector<bool> right_matched(right_rows_list.size(), false);

        /* Safe access to db->data.at(tname) */
        auto data_it = db->data.find(tname);
        const std::vector<Row> &left_rows = (data_it != db->data.end()) ? data_it->second : std::vector<Row>{};
        
        for (const auto &lrow : left_rows) {
            bool matched = false;
            Row lrow_prefixed;
            for (auto &kv : lrow) {
                lrow_prefixed[kv.first] = kv.second;
                lrow_prefixed[tname + "." + kv.first] = kv.second;
                if (!left_alias.empty()) lrow_prefixed[left_alias + "." + kv.first] = kv.second;
            }
            for (size_t ri = 0; ri < right_rows_list.size(); ++ri) {
                if (eval_on_match(lrow_prefixed, right_rows_list[ri])) {
                    all_rows.push_back(make_merged_row(lrow, &right_rows_list[ri]));
                    matched = true;
                    right_matched[ri] = true;
                }
            }
            if (!matched && (is_left_jn)) {
                all_rows.push_back(make_merged_row(lrow, nullptr));
            }
        }

        /* RIGHT JOIN: add unmatched right rows with NULL-filled left columns */
        if (is_right_jn) {
            for (size_t ri = 0; ri < right_rows_list.size(); ++ri)
                if (!right_matched[ri])
                    all_rows.push_back(make_right_unmatched_row(right_rows_list[ri]));
        }
    } else {
        /* Safe access to db->data.at(tname) */
        auto data_it = db->data.find(tname);
        if (data_it != db->data.end()) {
            all_rows = data_it->second;
        }
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
        /* Safe access to rt_col_order and rt_data */
        auto rt_col_it = db->col_order.find(rt);
        auto rt_data_it = db->data.find(rt);
        if (rt_col_it == db->col_order.end() || rt_data_it == db->data.end()) continue;
        const auto &rt_col_order = rt_col_it->second;
        const auto &rt_data = rt_data_it->second;
        std::string rt_alias = jn.alias.empty() ? rt : jn.alias;

        /* Merge col order */
        for (const auto &c : rt_col_order) merged_col_order.push_back(c);

        std::vector<Row> new_all_rows;
        for (const auto &lrow : all_rows) {
            bool matched = false;
            for (const auto &rrow : rt_data) {
                /* Check ON condition: use qry_eval_where for full expression */
                bool on_match = jn.on_expr.empty() && jn.on_left.empty() && jn.using_col.empty();
                if (!on_match) {
                    Row rrow_pref;
                    for (auto &kv : rrow) {
                        rrow_pref[kv.first] = kv.second;
                        rrow_pref[rt + "." + kv.first] = kv.second;
                        rrow_pref[rt_alias + "." + kv.first] = kv.second;
                    }
                    Row combined = lrow;
                    for (auto &kv : rrow_pref) combined[kv.first] = kv.second;
                    if (!jn.using_col.empty()) {
                        SvdbVal lv = eval_expr(rt_alias + "." + jn.using_col, combined, merged_col_order);
                        SvdbVal rv = eval_expr(rt_alias + "." + jn.using_col, rrow_pref, rt_col_order);
                        if (lv.type != SVDB_TYPE_NULL && rv.type != SVDB_TYPE_NULL)
                            on_match = (val_cmp(lv, rv) == 0);
                    } else if (!jn.on_expr.empty()) {
                        on_match = qry_eval_where(combined, merged_col_order, jn.on_expr);
                    } else {
                        on_match = false;
                    }
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
        /* Update star_lookup_keys for additional JOIN columns */
        for (const auto &c : rt_col_order)
            star_lookup_keys.push_back(rt_alias + "." + c);
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
    
    /* Find implicit column alias (without AS keyword)
     * e.g., "name employee_name" → alias is "employee_name"
     * Looks for trailing identifier that's not a SQL keyword */
    auto find_implicit_alias = [](const std::string &expr) -> std::string {
        std::string cu = qry_upper(expr);
        /* Find last space-separated token */
        size_t last_space = cu.rfind(' ');
        if (last_space == std::string::npos || last_space >= expr.size() - 1) return "";
        
        std::string last_token = cu.substr(last_space + 1);
        /* Check if it's a simple identifier (alphanumeric + underscore) */
        for (char c : last_token) {
            if (!isalnum((unsigned char)c) && c != '_') return "";
        }
        /* Check it's not a SQL keyword */
        static const char *reserved_kws[] = {
            "FROM", "WHERE", "ORDER", "GROUP", "HAVING", "LIMIT", "UNION", "INTERSECT",
            "EXCEPT", "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "ON", "ASC", "DESC",
            "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST",
            "AS", "DISTINCT", "ALL", "BETWEEN", "IN", "LIKE", "IS", "NOT", "AND", "OR",
            "NULLS", "FIRST", "LAST", "OFFSET", "FETCH", "OVER", "PARTITION", "BY",
            "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW",
            nullptr
        };
        for (const char **kw = reserved_kws; *kw; ++kw) {
            if (last_token == *kw) return "";
        }
        /* Make sure the expression before the alias is valid */
        std::string expr_part = qry_trim(expr.substr(0, last_space));
        if (expr_part.empty()) return "";

        /* Reject implicit alias if expr_part contains any top-level space
         * (outside parentheses/string literals). A top-level space means the
         * expression is compound (e.g. "a + b", "a AND b"), not a simple
         * column reference or function call. */
        {
            int dep = 0; bool ins = false;
            for (char ch : expr_part) {
                if (ch == '\'') { ins = !ins; continue; }
                if (ins) continue;
                if (ch == '(') { ++dep; continue; }
                if (ch == ')') { if (dep > 0) --dep; continue; }
                if (dep == 0 && ch == ' ') return "";
            }
        }

        return expr.substr(last_space + 1);
    };

    /* ── Expand qualified stars (e.g. "e.*", "o.*") in sel_cols ── */
    {
        std::vector<std::string> expanded;
        for (auto &c : sel_cols) {
            /* Check if it's a qualified star: identifier.* */
            if (c.size() >= 3 && c.back() == '*' && c[c.size()-2] == '.') {
                std::string prefix = c.substr(0, c.size()-2);
                std::string prefix_upper = qry_upper(prefix);
                /* Find the table that this prefix/alias refers to */
                std::string target_tbl;
                if (prefix_upper == qry_upper(left_alias) || prefix_upper == qry_upper(tname))
                    target_tbl = tname;
                else {
                    for (auto &jn : all_joins) {
                        if (prefix_upper == qry_upper(jn.alias) || prefix_upper == qry_upper(jn.table))
                            { target_tbl = jn.table.empty() ? jn.alias : jn.table; break; }
                    }
                }
                /* Resolve table name to its stored column order */
                if (!target_tbl.empty()) {
                    /* Case-insensitive lookup in db->col_order */
                    std::string resolved_tbl;
                    for (auto &kv : db->col_order) {
                        if (qry_upper(kv.first) == qry_upper(target_tbl))
                            { resolved_tbl = kv.first; break; }
                    }
                    if (!resolved_tbl.empty()) {
                        for (const auto &col : db->col_order[resolved_tbl])
                            expanded.push_back(prefix + "." + col);
                        continue;
                    }
                }
                /* Fallback: not resolved, keep original */
                expanded.push_back(c);
            } else {
                expanded.push_back(c);
            }
        }
        sel_cols = std::move(expanded);
    }

    /* ── Determine output columns ── */
    std::vector<std::string> out_cols;
    if (star) {
        out_cols = merged_col_order; /* use merged for JOIN, col_order for simple */
    } else {
        for (const auto &c : sel_cols) {
            /* Strip alias (AS name) — must be at top level, not inside parens */
            std::string cu = qry_upper(c);
            size_t as_pos = find_top_as(cu);
            std::string col_expr;
            if (as_pos != std::string::npos) {
                col_expr = c.substr(0, as_pos);
            } else {
                /* Try implicit alias (without AS keyword) */
                std::string implicit = find_implicit_alias(c);
                col_expr = implicit.empty() ? c : c.substr(0, c.rfind(' '));
            }
            out_cols.push_back(qry_trim(col_expr));
        }
    }

    /* Compute alias map for output columns */
    std::vector<std::string> out_names;
    for (const auto &c : (star ? sel_cols : sel_cols)) {
        std::string cu = qry_upper(c);
        size_t as_pos = find_top_as(cu);
        if (as_pos != std::string::npos) {
            out_names.push_back(qry_trim(c.substr(as_pos + 4)));
        } else {
            std::string implicit = find_implicit_alias(c);
            out_names.push_back(implicit.empty() ? c : implicit);
        }
    }

    /* Column names for output: use original column names or expression */
    r->col_names.clear();
    if (star) {
        r->col_names = merged_col_order; /* JOINs show all merged columns */
    } else {
        for (size_t i = 0; i < sel_cols.size(); ++i) {
            std::string cu = qry_upper(sel_cols[i]);
            size_t as_pos = find_top_as(cu);
            std::string col_name;
            if (as_pos != std::string::npos) {
                /* Explicit AS alias */
                col_name = qry_trim(sel_cols[i].substr(as_pos + 4));
            } else {
                /* Try implicit alias (without AS keyword) */
                std::string implicit = find_implicit_alias(sel_cols[i]);
                col_name = implicit.empty() ? sel_cols[i] : implicit;
            }
            /* Strip table/alias prefix from simple qualified column references:
             * "t.colname" → "colname" (matches SQLite behavior). Only strip when
             * col_name is a plain "prefix.ident" with no spaces or parens. */
            {
                bool simple_qual = false;
                size_t dot = col_name.find('.');
                if (dot != std::string::npos && dot + 1 < col_name.size()) {
                    bool all_ident = true;
                    for (char ch : col_name) {
                        if (ch == '.') continue;
                        if (!isalnum((unsigned char)ch) && ch != '_' && ch != '"' && ch != '`')
                            { all_ident = false; break; }
                    }
                    if (all_ident && col_name.find('.', dot+1) == std::string::npos)
                        simple_qual = true;
                }
                if (simple_qual) col_name = col_name.substr(dot + 1);
            }
            r->col_names.push_back(col_name);
        }
    }

    /* ── Resolve SELECT aliases in WHERE / HAVING texts ── */
    /* Allows: SELECT name AS n FROM t WHERE n LIKE 'A%'  (alias used in WHERE)
     * and:    SELECT COUNT(*) AS cnt FROM t HAVING cnt > 5 */
    if (!star && !sel_cols.empty()) {
        /* Build alias → underlying-expression map (longest alias first) */
        std::vector<std::pair<std::string, std::string>> alias_subs; /* (upper_alias, expr) */
        for (size_t i = 0; i < sel_cols.size() && i < out_names.size() && i < out_cols.size(); ++i) {
            std::string au = qry_upper(out_names[i]);
            std::string eu = qry_upper(out_cols[i]);
            if (!au.empty() && au != eu)
                alias_subs.push_back({au, out_cols[i]});
        }
        /* Sort by alias length (longest first) to avoid partial substitutions */
        std::sort(alias_subs.begin(), alias_subs.end(),
            [](const auto &a, const auto &b){ return a.first.size() > b.first.size(); });

        auto subst_aliases = [&](const std::string &txt) -> std::string {
            if (alias_subs.empty() || txt.empty()) return txt;
            std::string result = txt;
            std::string result_u = qry_upper(result);
            for (auto &sub : alias_subs) {
                const std::string &key = sub.first; /* upper-case alias */
                const std::string &repl = sub.second; /* underlying expression */
                size_t klen = key.size();
                for (size_t p = result_u.find(key); p != std::string::npos;) {
                    bool lb = (p == 0 || (!isalnum((unsigned char)result_u[p-1]) && result_u[p-1] != '_'));
                    bool rb = (p + klen >= result_u.size() ||
                               (!isalnum((unsigned char)result_u[p+klen]) && result_u[p+klen] != '_'));
                    if (lb && rb) {
                        result.replace(p, klen, repl);
                        result_u.replace(p, klen, qry_upper(repl));
                        p = result_u.find(key, p + repl.size());
                    } else {
                        p = result_u.find(key, p + 1);
                    }
                }
            }
            return result;
        };
        where_txt = subst_aliases(where_txt);
        having_txt = subst_aliases(having_txt);
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

        /* Build alias → underlying-expression map for GROUP BY alias resolution */
        std::map<std::string, std::string> sel_alias_map;
        for (size_t i = 0; i < sel_cols.size() && i < out_cols.size(); ++i) {
            std::string cu = qry_upper(sel_cols[i]);
            size_t as_pos = find_top_as(cu);
            if (as_pos != std::string::npos) {
                std::string alias = qry_trim(sel_cols[i].substr(as_pos + 4));
                sel_alias_map[qry_upper(alias)] = out_cols[i];
            }
        }

        for (const auto &row : all_rows) {
            if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
            /* Build group key */
            std::string key;
            for (const auto &gc : group_cols) {
                /* Resolve alias references in GROUP BY */
                auto alias_it = sel_alias_map.find(qry_upper(gc));
                const std::string &gc_expr = (alias_it != sel_alias_map.end()) ? alias_it->second : gc;
                SvdbVal v = eval_expr(gc_expr, row, merged_col_order);
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
            /* Use qualified lookup keys when available (JOINs with overlapping column names) */
            if (!star_lookup_keys.empty() && star_lookup_keys.size() == merged_col_order.size()) {
                for (size_t ki = 0; ki < star_lookup_keys.size(); ++ki) {
                    auto it = row.find(star_lookup_keys[ki]);
                    if (it != row.end()) { result_row.push_back(it->second); continue; }
                    /* Fallback to bare name */
                    auto it2 = row.find(merged_col_order[ki]);
                    result_row.push_back(it2 != row.end() ? it2->second : SvdbVal{});
                }
            } else {
                for (const auto &cn : merged_col_order) {
                    auto it = row.find(cn);
                    result_row.push_back(it != row.end() ? it->second : SvdbVal{});
                }
            }
        } else {
            for (size_t ci = 0; ci < out_cols.size(); ++ci) {
                if (has_win && is_window_expr(out_cols[ci]))
                    result_row.push_back(win_vals[ci][ri]);
                else
                    result_row.push_back(eval_expr(out_cols[ci], row, merged_col_order));
            }
        }
        /* Check for evaluation errors (e.g., unknown function) */
        if (!g_eval_error.empty()) {
            db->last_error = g_eval_error;
            g_eval_error.clear();
            delete r;
            return SVDB_ERR;
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
                    /* Handle NULLs: SQLite treats NULL as smallest value, so:
                     *   ASC  → NULLS FIRST (smallest values sort first)
                     *   DESC → NULLS LAST  (smallest values sort last)
                     * NULLS FIRST/LAST override: nulls<0 = NULLS FIRST, nulls>0 = NULLS LAST */
                    bool a_null = (va.type == SVDB_TYPE_NULL);
                    bool b_null = (vb.type == SVDB_TYPE_NULL);
                    if (a_null || b_null) {
                        if (a_null && b_null) continue;
                        bool nulls_first;
                        if (oc.nulls < 0) nulls_first = true;        /* explicit NULLS FIRST */
                        else if (oc.nulls > 0) nulls_first = false;  /* explicit NULLS LAST */
                        else nulls_first = !oc.desc; /* default: SQLite NULL=smallest → FIRST for ASC, LAST for DESC */
                        if (a_null) return nulls_first;
                        return !nulls_first;
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
    svdb_assert(db != nullptr);
    svdb_assert(rows_out != nullptr);
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
            /* Safe access to col_order, schema, and primary_keys */
            auto col_order_it = db->col_order.find(tname);
            auto schema_it = db->schema.find(tname);
            if (col_order_it == db->col_order.end() || schema_it == db->schema.end()) {
                return SVDB_OK;
            }
            for (const auto &col : col_order_it->second) {
                const ColDef *def = nullptr;
                auto sit = schema_it->second.find(col);
                if (sit != schema_it->second.end()) def = &sit->second;
                std::string ctype = def ? def->type : "TEXT";
                int notnull = def ? (def->not_null ? 1 : 0) : 0;
                int pk = 0;
                /* Check primary_key flag or primary_keys list */
                if (def && def->primary_key) {
                    pk = pk_seq++;
                } else if (db->primary_keys.count(tname)) {
                    int seq2 = 1;
                    auto pk_it = db->primary_keys.find(tname);
                    if (pk_it != db->primary_keys.end()) {
                        for (auto &pk_col : pk_it->second) {
                            if (pk_col == col) { pk = seq2; break; }
                            ++seq2;
                        }
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
            auto create_it = db->create_sql.find(kv.first);
            if (create_it != db->create_sql.end()) {
                std::string su3 = qry_upper(create_it->second);
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
                v_to.type    = SVDB_TYPE_TEXT; v_to.sval    = fk.parent_col.empty() ? fk.child_col : fk.parent_col;
                v_onupd.type = SVDB_TYPE_TEXT; v_onupd.sval = fk.on_update.empty() ? "NO ACTION" : fk.on_update;
                v_ondel.type = SVDB_TYPE_TEXT; v_ondel.sval = fk.on_delete.empty() ? "NO ACTION" : fk.on_delete;
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
            auto fk_it = db->fk_constraints.find(tname);
            if (fk_it == db->fk_constraints.end()) return;
            auto data_it = db->data.find(tname);
            if (data_it == db->data.end()) return;
            const auto &rows2 = data_it->second;
            int64_t rowid2 = 1;
            for (const auto &row2 : rows2) {
                for (const auto &fk : fk_it->second) {
                    auto cit = row2.find(fk.child_col);
                    if (cit == row2.end() || cit->second.type == SVDB_TYPE_NULL) { ++rowid2; continue; }
                    const std::string &pcol = fk.parent_col.empty() ? fk.child_col : fk.parent_col;
                    bool found = false;
                    auto parent_data_it = db->data.find(fk.parent_table);
                    if (parent_data_it != db->data.end()) {
                        for (const auto &pr : parent_data_it->second) {
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
        else v.ival = 2; /* default FULL=2 */
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA foreign_keys [= val] */
    if (pname == "FOREIGN_KEYS") {
        if (!parg.empty()) {
            std::string up = qry_upper(parg);
            db->foreign_keys_enabled = (up == "ON" || up == "1" || up == "TRUE");
            return SVDB_OK; /* setter returns no rows (SQLite behavior) */
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
            auto data_it = db->data.find(kv.first);
            v_rows.ival = (data_it != db->data.end()) ? (int64_t)data_it->second.size() : 0;
            v_cols.type = SVDB_TYPE_INT;
            auto col_it = db->col_order.find(kv.first);
            v_cols.ival = (col_it != db->col_order.end()) ? (int64_t)col_it->second.size() : 0;
            r->rows.push_back({v_tbl, v_rows, v_cols});
        }
        return SVDB_OK;
    }

    /* PRAGMA page_size / page_count / freelist_count: return stubs */
    if (pname == "PAGE_SIZE") {
        if (!parg.empty()) { try { db->page_size_val = std::stoll(parg); } catch (...) {} }
        r->col_names = {"page_size"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->page_size_val;
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
        if (!parg.empty()) { try { db->auto_vacuum_val = std::stoll(parg); } catch (...) {} }
        r->col_names = {"auto_vacuum"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->auto_vacuum_val;
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
        if (!parg.empty()) { try { db->mmap_size_val = std::stoll(parg); } catch (...) {} }
        r->col_names = {"mmap_size"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->mmap_size_val;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA temp_store [= val] */
    if (pname == "TEMP_STORE") {
        if (!parg.empty()) { try { db->temp_store_val = std::stoll(parg); } catch (...) {} }
        r->col_names = {"temp_store"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->temp_store_val;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA query_only [= val] */
    if (pname == "QUERY_ONLY") {
        if (!parg.empty()) {
            std::string pu = qry_upper(parg);
            db->query_only_val = (pu == "1" || pu == "ON" || pu == "TRUE" || pu == "YES");
        }
        r->col_names = {"query_only"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->query_only_val ? 1 : 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA locking_mode [= val] */
    if (pname == "LOCKING_MODE") {
        if (!parg.empty()) { db->locking_mode_val = qry_upper(parg) == "EXCLUSIVE" ? "exclusive" : "normal"; }
        r->col_names = {"locking_mode"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = db->locking_mode_val;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA read_uncommitted [= val] */
    if (pname == "READ_UNCOMMITTED") {
        if (!parg.empty()) {
            std::string pu = qry_upper(parg);
            db->read_uncommitted_val = (pu == "1" || pu == "ON" || pu == "TRUE" || pu == "YES");
        }
        r->col_names = {"read_uncommitted"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->read_uncommitted_val ? 1 : 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA cache_spill [= val] */
    if (pname == "CACHE_SPILL") {
        if (!parg.empty()) {
            std::string pu = qry_upper(parg);
            db->cache_spill_val = (pu == "0" || pu == "OFF" || pu == "FALSE") ? 0 : 1;
        }
        r->col_names = {"cache_spill"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->cache_spill_val;
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

    /* PRAGMA sqlite_sequence — return auto-increment counter table */
    if (pname == "SQLITE_SEQUENCE") {
        r->col_names = {"name", "seq"};
        for (auto &kv : db->rowid_counter) {
            /* Only include tables that use AUTOINCREMENT */
            bool has_autoincrement = false;
            auto schema_it = db->schema.find(kv.first);
            if (schema_it != db->schema.end()) {
                for (auto &col : schema_it->second) {
                    if (col.second.auto_increment) { has_autoincrement = true; break; }
                }
            }
            if (!has_autoincrement) continue;
            SvdbVal v_name, v_seq;
            v_name.type = SVDB_TYPE_TEXT; v_name.sval = kv.first;
            v_seq.type  = SVDB_TYPE_INT;  v_seq.ival  = kv.second;
            r->rows.push_back({v_name, v_seq});
        }
        return SVDB_OK;
    }

    /* PRAGMA shrink_memory — release unused memory, return 0 */
    if (pname == "SHRINK_MEMORY") {
        r->col_names = {"shrink_memory"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA optimize [= val] — return 'ok' (no-op for in-memory DB) */
    if (pname == "OPTIMIZE") {
        r->col_names = {"optimize"};
        SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = "ok";
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA journal_size_limit [= val] */
    if (pname == "JOURNAL_SIZE_LIMIT") {
        if (!parg.empty()) {
            try { db->journal_size_limit_val = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"journal_size_limit"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->journal_size_limit_val;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA cache_grind — return page-cache statistics stub */
    if (pname == "CACHE_GRIND") {
        r->col_names = {"pages_cached", "pages_free", "hits", "misses"};
        SvdbVal v0, v1, v2, v3;
        v0.type = SVDB_TYPE_INT; v0.ival = 0;
        v1.type = SVDB_TYPE_INT; v1.ival = 0;
        v2.type = SVDB_TYPE_INT; v2.ival = 0;
        v3.type = SVDB_TYPE_INT; v3.ival = 0;
        r->rows.push_back({v0, v1, v2, v3});
        return SVDB_OK;
    }

    /* PRAGMA function_list — return list of built-in SQL functions */
    if (pname == "FUNCTION_LIST") {
        r->col_names = {"name", "narg", "type"};
        /* Scalar functions */
        const char* scalar_funcs[] = {
            /* Math */
            "abs", "round", "ceil", "ceiling", "floor", "pow", "power", "sqrt",
            "exp", "log", "log10", "ln", "sin", "cos", "tan", "asin", "acos",
            "atan", "atan2", "sinh", "cosh", "tanh", "deg_to_rad", "rad_to_deg",
            "mod", "random",
            /* String */
            "length", "upper", "lower", "trim", "ltrim", "rtrim", "substr",
            "substring", "instr", "replace", "quote", "char", "unicode",
            "group_concat", "string_agg", "repeat", "reverse", "format",
            /* Type conversion */
            "typeof", "cast",
            /* Conditional */
            "coalesce", "ifnull", "nullif", "iif",
            /* Date/Time */
            "date", "time", "datetime", "julianday", "strftime",
            /* Aggregate */
            "count", "sum", "avg", "min", "max", "total", "group_concat",
            /* JSON (extension) */
            "json", "json_array", "json_object", "json_extract", "json_set",
            "json_insert", "json_replace", "json_remove", "json_type",
            "json_valid", "json_patch", "json_quote",
            /* Full-text search (extension) */
            "highlight", "snippet", "offsets", "optimize",
            /* Other */
            "likelihood", "likely", "unlikely", "last_insert_rowid",
            "sqlite_version", "changes", "total_changes",
            nullptr
        };
        for (int i = 0; scalar_funcs[i] != nullptr; ++i) {
            SvdbVal v_name, v_narg, v_type;
            v_name.type = SVDB_TYPE_TEXT; v_name.sval = scalar_funcs[i];
            v_narg.type = SVDB_TYPE_INT; v_narg.ival = -1; /* variable args */
            v_type.type = SVDB_TYPE_TEXT; v_type.sval = "scalar";
            r->rows.push_back({v_name, v_narg, v_type});
        }
        return SVDB_OK;
    }

    /* PRAGMA wal_autocheckpoint [= N] — get/set WAL auto-checkpoint threshold */
    if (pname == "WAL_AUTOCKPT" || pname == "WAL_AUTOCHECKPOINT") {
        if (!parg.empty()) {
            try { db->wal_autocheckpoint_val = std::stoll(parg); } catch (...) {}
        }
        r->col_names = {"wal_autocheckpoint"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = db->wal_autocheckpoint_val;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA wal_checkpoint [(mode)] — perform WAL checkpoint
     * Modes: passive, full, truncate (default: passive)
     * Returns: (busy, log, checkpointed) — stub values for in-memory DB
     */
    if (pname == "WAL_CKPT" || pname == "WAL_CHECKPOINT") {
        std::string mode = qry_upper(parg);
        if (mode.empty()) mode = "PASSIVE";
        /* In delete mode (no WAL), checkpoint is a no-op but returns (0,0,0) */
        r->col_names = {"busy", "log", "checkpointed"};
        SvdbVal v_busy, v_log, v_ckpt;
        v_busy.type = SVDB_TYPE_INT; v_busy.ival = 0;
        v_log.type  = SVDB_TYPE_INT; v_log.ival  = 0;
        v_ckpt.type = SVDB_TYPE_INT; v_ckpt.ival = 0;
        r->rows.push_back({v_busy, v_log, v_ckpt});
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
    std::string s = qry_trim(normalize_whitespace(strip_sql_comments_q(std::string(sql))));
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

                if (!tname.empty()) {
                    auto data_it = db->data.find(tname);
                    auto col_it = db->col_order.find(tname);
                    if (data_it != db->data.end() && col_it != db->col_order.end()) {
                        before_snap = data_it->second;
                        col_order_p = &col_it->second;
                    }
                }

                lk.unlock();
                svdb_result_t res{};
                svdb_exec(db, sql_no_ret.c_str(), &res);
                lk.lock();

                if (!tname.empty()) {
                    auto data_it = db->data.find(tname);
                    auto col_it = db->col_order.find(tname);
                    if (data_it != db->data.end() && col_it != db->col_order.end()) {
                        after_snap = data_it->second;
                        col_order_p = &col_it->second;
                    }
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
            /* Check for multi-statement SQL ending with SELECT (e.g., CREATE ... ; SELECT ...) */
            /* Split on top-level semicolons to find trailing SELECT */
            {
                std::vector<std::string> stmts;
                std::string cur_stmt; int paren_depth = 0; bool in_string = false;
                for (size_t i = 0; i < s.size(); ++i) {
                    char ch = s[i];
                    if (ch == '\'') { in_string = !in_string; cur_stmt += ch; continue; }
                    if (in_string) { cur_stmt += ch; continue; }
                    if (ch == '(') ++paren_depth; else if (ch == ')') --paren_depth;
                    if (ch == ';' && paren_depth == 0) {
                        std::string stmt = qry_trim(cur_stmt);
                        if (!stmt.empty()) stmts.push_back(stmt);
                        cur_stmt.clear();
                    } else { cur_stmt += ch; }
                }
                std::string last_stmt = qry_trim(cur_stmt);
                if (!last_stmt.empty()) stmts.push_back(last_stmt);
                if (stmts.size() > 1) {
                    std::string last_upper = qry_upper(stmts.back().substr(0, std::min((size_t)7, stmts.back().size())));
                    if (last_upper.substr(0,6) == "SELECT" || last_upper.substr(0,4) == "WITH") {
                        /* Execute non-SELECT statements (release lock to avoid deadlock),
                         * then return SELECT result */
                        lk.unlock();
                        for (size_t i = 0; i < stmts.size()-1; ++i) {
                            svdb_result_t res{};
                            svdb_exec(db, stmts[i].c_str(), &res);
                        }
                        lk.lock();
                        return svdb_query_internal(db, stmts.back(), rows);
                    }
                }
            }
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
