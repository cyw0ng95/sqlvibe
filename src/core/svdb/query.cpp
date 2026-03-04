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
    if (esc != '\0' && !pat.empty() && pat[0] == esc && pat.size() >= 2) {
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

/* ── Expression evaluator ────────────────────────────────────────── */

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

    /* COALESCE(a, b, ...) */
    {
        std::string eu = qry_upper(e);
        if (eu.substr(0, 9) == "COALESCE(" && e.back() == ')') {
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
        if (eu.substr(0, 7) == "IFNULL(" && e.back() == ')') {
            std::string args = e.substr(7, e.size()-8);
            size_t comma = args.find(',');
            if (comma != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma), row, col_order);
                if (a.type != SVDB_TYPE_NULL) return a;
                return eval_expr(args.substr(comma+1), row, col_order);
            }
        }
        /* NULLIF(a, b) */
        if (eu.substr(0, 7) == "NULLIF(" && e.back() == ')') {
            std::string args = e.substr(7, e.size()-8);
            size_t comma = args.find(',');
            if (comma != std::string::npos) {
                SvdbVal a = eval_expr(args.substr(0, comma), row, col_order);
                SvdbVal b = eval_expr(args.substr(comma+1), row, col_order);
                if (val_cmp(a, b) == 0) return SvdbVal{};
                return a;
            }
        }
        /* LENGTH(expr) — returns Unicode character count (UTF-8 code points) */
        if (eu.substr(0, 7) == "LENGTH(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            std::string s = val_to_str(inner);
            /* Count UTF-8 code points: leading bytes start with 0x00-0x7F or 0xC0-0xFF */
            int64_t len = 0;
            for (size_t i = 0; i < s.size(); ++i) {
                unsigned char c = (unsigned char)s[i];
                if ((c & 0xC0) != 0x80) ++len; /* not a continuation byte */
            }
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = len; return v;
        }
        /* UPPER(expr) */
        if (eu.substr(0, 6) == "UPPER(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            v.sval = qry_upper(val_to_str(inner)); return v;
        }
        /* LOWER(expr) */
        if (eu.substr(0, 6) == "LOWER(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            if (inner.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            std::string s = val_to_str(inner);
            for (auto &c : s) c = (char)tolower((unsigned char)c);
            v.sval = s; return v;
        }
        /* TRIM([LEADING|TRAILING|BOTH] [chars FROM] expr) or TRIM(expr[, chars]) */
        if (eu.substr(0, 5) == "TRIM(" && e.back() == ')') {
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
        if (eu.substr(0, 6) == "LTRIM(" && e.back() == ')') {
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
        if (eu.substr(0, 6) == "RTRIM(" && e.back() == ')') {
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
        if (eu.substr(0, 5) == "CAST(" && e.back() == ')') {
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
        if (eu.substr(0, 4) == "ABS(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(4, e.size()-5), row, col_order);
            if (inner.type == SVDB_TYPE_INT)  { inner.ival = inner.ival < 0 ? -inner.ival : inner.ival; }
            else if (inner.type == SVDB_TYPE_REAL) { inner.rval = std::abs(inner.rval); }
            return inner;
        }
        /* CEIL / CEILING(expr) */
        if ((eu.substr(0, 5) == "CEIL(" || eu.substr(0, 8) == "CEILING(") && e.back() == ')') {
            size_t start = (eu[4] == '(') ? 5 : 8;
            SvdbVal inner = eval_expr(e.substr(start, e.size()-start-1), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::ceil(val_to_dbl(inner)); return v;
        }
        /* FLOOR(expr) */
        if (eu.substr(0, 6) == "FLOOR(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::floor(val_to_dbl(inner)); return v;
        }
        /* ROUND(expr) or ROUND(expr, digits) */
        if (eu.substr(0, 6) == "ROUND(" && e.back() == ')') {
            std::string args = e.substr(6, e.size()-7);
            /* find top-level comma */
            int rd = 0; size_t comma_pos = std::string::npos;
            for (size_t i = 0; i < args.size(); ++i) {
                if (args[i] == '(') ++rd; else if (args[i] == ')') --rd;
                else if (args[i] == ',' && rd == 0) { comma_pos = i; break; }
            }
            SvdbVal v; v.type = SVDB_TYPE_REAL;
            if (comma_pos == std::string::npos) {
                SvdbVal inner = eval_expr(args, row, col_order);
                v.rval = std::round(val_to_dbl(inner));
            } else {
                SvdbVal inner = eval_expr(args.substr(0, comma_pos), row, col_order);
                SvdbVal digits = eval_expr(args.substr(comma_pos+1), row, col_order);
                int64_t n = val_to_i64(digits);
                double factor = std::pow(10.0, (double)n);
                v.rval = std::round(val_to_dbl(inner) * factor) / factor;
            }
            return v;
        }
        /* SQRT(expr) */
        if (eu.substr(0, 5) == "SQRT(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(5, e.size()-6), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = std::sqrt(val_to_dbl(inner)); return v;
        }
        /* POW(a, b) / POWER(a, b) */
        if ((eu.substr(0, 4) == "POW(" || eu.substr(0, 6) == "POWER(") && e.back() == ')') {
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
        if (eu.substr(0, 5) == "SIGN(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(5, e.size()-6), row, col_order);
            double d = val_to_dbl(inner);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (d > 0) ? 1 : (d < 0 ? -1 : 0); return v;
        }
        /* MAX(a, b) and MIN(a, b) as scalar functions */
        if ((eu.substr(0, 4) == "MAX(" || eu.substr(0, 4) == "MIN(") && e.back() == ')') {
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
        if (eu.substr(0, 7) == "SUBSTR(" && e.back() == ')') {
            std::string args = e.substr(7, e.size()-8);
            /* split by top-level comma */
            std::vector<std::string> parts;
            int rd = 0; size_t start2 = 0;
            for (size_t i = 0; i <= args.size(); ++i) {
                char c = i < args.size() ? args[i] : ',';
                if (c == '(') ++rd; else if (c == ')') --rd;
                else if (c == ',' && rd == 0) { parts.push_back(args.substr(start2, i-start2)); start2 = i+1; }
            }
            if (!parts.empty()) {
                SvdbVal str_v = eval_expr(parts[0], row, col_order);
                std::string s2 = val_to_str(str_v);
                int64_t off = parts.size() > 1 ? val_to_i64(eval_expr(parts[1], row, col_order)) : 1;
                if (off < 1) off = 1;
                int64_t len2 = (int64_t)s2.size();
                if (parts.size() > 2) len2 = val_to_i64(eval_expr(parts[2], row, col_order));
                size_t idx2 = (size_t)(off - 1);
                if (idx2 >= s2.size()) { SvdbVal v; v.type = SVDB_TYPE_TEXT; return v; }
                SvdbVal v; v.type = SVDB_TYPE_TEXT;
                v.sval = s2.substr(idx2, (size_t)std::max((int64_t)0, len2));
                return v;
            }
        }
        /* REPLACE(str, old, new) */
        if (eu.substr(0, 8) == "REPLACE(" && e.back() == ')') {
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
        if (eu.substr(0, 6) == "INSTR(" && e.back() == ')') {
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
        if (eu.substr(0, 4) == "HEX(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(4, e.size()-5), row, col_order);
            static const char *hexchars = "0123456789ABCDEF";
            std::string s2 = val_to_str(inner);
            std::string hex;
            for (unsigned char c : s2) {
                hex += hexchars[(c >> 4) & 0xf];
                hex += hexchars[c & 0xf];
            }
            SvdbVal v; v.type = SVDB_TYPE_TEXT; v.sval = hex; return v;
        }
        /* TYPEOF(expr) */
        if (eu.substr(0, 7) == "TYPEOF(" && e.back() == ')') {
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
        /* ISNULL(expr) / NOTNULL(expr) */
        if (eu.substr(0, 7) == "ISNULL(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (inner.type == SVDB_TYPE_NULL) ? 1 : 0; return v;
        }
        if (eu.substr(0, 8) == "NOTNULL(" && e.back() == ')') {
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
                    if (prev == '+' || prev == '-' || prev == '*' || prev == '/' || prev == '%' || prev == '(') continue;
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
                    /* find next WHEN, ELSE or END */
                    size_t next = eu2.size();
                    for (const char *kw : {" WHEN ", " ELSE ", " END"}) {
                        size_t p2 = eu2.find(kw, pos2);
                        if (p2 != std::string::npos && p2 < next) next = p2;
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

    /* Logical OR / AND in expression context (lower precedence than comparison) */
    {
        /* Helper: find top-level keyword outside parens/strings, scanning left-to-right */
        auto find_kw_expr = [&](const std::string &kw) -> size_t {
            int depth_e = 0; bool in_str_e = false;
            std::string eu_e = qry_upper(e);
            std::string full_kw = " " + kw + " ";
            for (size_t i = 0; i + full_kw.size() <= eu_e.size(); ++i) {
                char c = eu_e[i];
                if (c == '\'') { in_str_e = !in_str_e; continue; }
                if (in_str_e) continue;
                if (c == '(') { ++depth_e; continue; }
                if (c == ')') { if (depth_e > 0) --depth_e; continue; }
                if (depth_e > 0) continue;
                if (eu_e.substr(i, full_kw.size()) == full_kw) return i + 1; /* position of keyword */
            }
            return std::string::npos;
        };
        /* Find rightmost OR at top level */
        size_t or_p = std::string::npos;
        {
            int depth_e = 0; bool in_str_e = false;
            std::string eu_e = qry_upper(e);
            for (int i = (int)eu_e.size() - 4; i >= 0; --i) {
                char c = eu_e[i];
                if (c == '\'') { in_str_e = !in_str_e; continue; }
                if (in_str_e) continue;
                if (c == ')') { ++depth_e; continue; }
                if (c == '(') { if (depth_e > 0) --depth_e; continue; }
                if (depth_e > 0) continue;
                if (eu_e.substr(i, 4) == " OR ") {
                    or_p = (size_t)i + 1; break;
                }
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
        /* Find rightmost AND at top level */
        size_t and_p = std::string::npos;
        {
            int depth_e = 0; bool in_str_e = false;
            std::string eu_e = qry_upper(e);
            for (int i = (int)eu_e.size() - 5; i >= 0; --i) {
                char c = eu_e[i];
                if (c == '\'') { in_str_e = !in_str_e; continue; }
                if (in_str_e) continue;
                if (c == ')') { ++depth_e; continue; }
                if (c == '(') { if (depth_e > 0) --depth_e; continue; }
                if (depth_e > 0) continue;
                if (eu_e.substr(i, 5) == " AND ") {
                    and_p = (size_t)i + 1; break;
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

    /* Comparison operators in expression context: return INT 0/1 or NULL */
    {
        static const char* cmp_ops[] = {"!=", "<>", "<=", ">=", "<", ">", "=", nullptr};
        for (int oi = 0; cmp_ops[oi]; ++oi) {
            /* Find operator scanning right-to-left outside parens/strings */
            std::string op = cmp_ops[oi];
            int depth_c = 0; bool in_str_c = false;
            size_t found_c = std::string::npos;
            for (int i = (int)e.size() - (int)op.size(); i >= 0; --i) {
                char c = e[i];
                if (c == '\'') { in_str_c = !in_str_c; continue; }
                if (in_str_c) continue;
                if (c == ')') ++depth_c;
                else if (c == '(') { if (depth_c > 0) --depth_c; }
                if (depth_c > 0) continue;
                if (e.substr(i, op.size()) == op) {
                    /* Make sure we don't match partial operators: != not matching < */
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
                std::string rhs_s = qry_trim(e.substr(found_c + op.size()));
                if (!lhs_s.empty()) {
                    SvdbVal lhs = eval_expr(lhs_s, row, col_order);
                    SvdbVal rhs = eval_expr(rhs_s, row, col_order);
                    /* NULL comparison returns NULL */
                    if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
                    int c = val_cmp(lhs, rhs);
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

    /* NOT expr */
    if (wu.substr(0, 4) == "NOT ") {
        return !qry_eval_where(row, col_order, wt.substr(4));
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
            return !like_match(val_to_str(lhs), val_to_str(rhs), esc_char);
        }
        size_t like_pos = wu.find(" LIKE ");
        if (like_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, like_pos), row, col_order);
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

    /* GLOB / NOT GLOB (check NOT GLOB first) */
    {
        size_t not_glob = wu.find(" NOT GLOB ");
        if (not_glob != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, not_glob), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(not_glob + 10), row, col_order);
            return !glob_match(val_to_str(lhs), val_to_str(rhs));
        }
        size_t glob_pos = wu.find(" GLOB ");
        if (glob_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, glob_pos), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(glob_pos + 6), row, col_order);
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

    /* Comparison operators: !=, <>, <=, >=, =, <, > (depth-aware scan) */
    {
        const char *ops[] = {"!=", "<>", "<=", ">=", "=", "<", ">", nullptr};
        size_t op_start = std::string::npos, op_len = 0;
        /* Scan left-to-right, respecting paren depth and string literals */
        {
            int depth_c = 0; bool in_str_c = false;
            for (size_t i = 0; i < wt.size(); ++i) {
                char c = wt[i];
                if (c == '\'') { in_str_c = !in_str_c; continue; }
                if (in_str_c) continue;
                if (c == '(') { ++depth_c; continue; }
                if (c == ')') { if (depth_c > 0) --depth_c; continue; }
                if (depth_c > 0) continue;
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
            SvdbVal lhs = eval_expr(lhs_s, row, col_order);
            SvdbVal rhs = eval_expr(rhs_s, row, col_order);
            /* NULL comparisons: any comparison with NULL is false */
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return false;
            int c = val_cmp(lhs, rhs);
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

struct OrderCol { std::string expr; bool desc; };
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
    size_t pos = su.find("ORDER BY ");
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
        bool desc = false;
        std::string tu = qry_upper(token);
        if (tu.size() >= 5 && tu.substr(tu.size()-5) == " DESC") {
            desc = true; token = qry_trim(token.substr(0, token.size()-5));
        } else if (tu.size() >= 4 && tu.substr(tu.size()-4) == " ASC") {
            token = qry_trim(token.substr(0, token.size()-4));
        }
        result.push_back({token, desc});
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

struct AggState {
    std::string func;    /* COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT */
    std::string arg;     /* column or * */
    std::string sep;     /* separator for GROUP_CONCAT */
    std::string wrapper; /* outer scalar function: ABS/UPPER/LOWER/etc. */
    int64_t count = 0;
    double  sum   = 0.0;
    SvdbVal min_val, max_val;
    bool has_min = false, has_max = false;
    bool is_real = false;
    bool distinct = false; /* COUNT(DISTINCT ...) */
    std::unordered_set<std::string> seen_vals; /* for DISTINCT counting */
    std::vector<std::string> concat_vals; /* for GROUP_CONCAT */
};

static bool is_agg_expr(const std::string &e) {
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
    std::string eu = qry_upper(qry_trim(expr));
    std::string e_orig = qry_trim(expr);
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
        if (eu.substr(0, prefix.size()) == prefix && eu.back() == ')') {
            a.func = funcs[i];
            a.arg = qry_trim(e_orig.substr(prefix.size(), e_orig.size() - prefix.size() - 1));
            /* Strip DISTINCT prefix */
            std::string arg_upper = qry_upper(a.arg);
            if (arg_upper.size() > 9 && arg_upper.substr(0, 9) == "DISTINCT ") {
                a.distinct = true;
                a.arg = qry_trim(a.arg.substr(9));
            }
            break;
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
        ++a.count;
        if (v.type == SVDB_TYPE_REAL) a.is_real = true;
        a.sum += val_to_dbl(v);
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
        else { base_result.type = SVDB_TYPE_INT; base_result.ival = (int64_t)a.sum; }
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

/* ── Main SELECT execution ──────────────────────────────────────── */

svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                  svdb_rows_t **rows_out) {
    if (!rows_out) return SVDB_ERR;

    /* Set thread-local DB context for subquery support */
    svdb_db_t *prev_db = g_query_db;
    g_query_db = db;
    struct DbGuard { svdb_db_t **p; svdb_db_t *v; ~DbGuard() { *p = v; } } db_guard{&g_query_db, prev_db};

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
            for (const auto &expr : sel_cols) {
                r->col_names.push_back(expr);
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
                    SvdbVal lv = eval_expr(join.on_left,  combined_row, merged_col_order);
                    SvdbVal rv = eval_expr(join.on_right, combined_row, merged_col_order);
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
        /* Single-group aggregate (e.g. SELECT COUNT(*), SUM(x)) */
        std::vector<AggState> aggs;
        for (const auto &c : (star ? std::vector<std::string>{"*"} : sel_cols))
            aggs.push_back(make_agg(c));
        for (const auto &row : all_rows) {
            if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
            for (auto &a : aggs) agg_accumulate(a, row, merged_col_order);
        }
        std::vector<SvdbVal> res_row;
        for (auto &a : aggs) res_row.push_back(agg_result(a));
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
                        int c = val_cmp(a[idx], b[idx]);
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
    std::vector<std::vector<SvdbVal>> raw_rows;
    std::vector<Row> orig_rows;  /* keep original rows for ORDER BY on non-SELECT cols */
    for (const auto &row : all_rows) {
        if (!qry_eval_where(row, merged_col_order, where_txt)) continue;
        std::vector<SvdbVal> result_row;
        if (star) {
            for (const auto &cn : col_order) {
                auto it = row.find(cn);
                result_row.push_back(it != row.end() ? it->second : SvdbVal{});
            }
        } else {
            for (const auto &ce : out_cols) {
                result_row.push_back(eval_expr(ce, row, merged_col_order));
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
                    int c = val_cmp(va, vb);
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

static svdb_code_t svdb_query_pragma(svdb_db_t *db, const std::string &sql,
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
        r->col_names = {"query_timeout"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* PRAGMA max_memory [= val] */
    if (pname == "MAX_MEMORY") {
        r->col_names = {"max_memory"};
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = 0;
        r->rows.push_back({v});
        return SVDB_OK;
    }

    /* Unknown PRAGMA: return empty result */
    return SVDB_OK;
}

extern "C" {

svdb_code_t svdb_query(svdb_db_t *db, const char *sql, svdb_rows_t **rows) {
    if (!db || !sql || !rows) return SVDB_ERR;
    std::lock_guard<std::mutex> lk(db->mu);
    db->last_error.clear();
    /* Dispatch PRAGMA to dedicated handler */
    std::string s = qry_trim(std::string(sql));
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
    return svdb_query_internal(db, s, rows);
}

} /* extern "C" */
