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
#include <cstring>
#include <cmath>
#include <sstream>
#include <functional>

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
static bool like_match(const std::string &text, const std::string &pat) {
    /* Simple % and _ wildcard matching */
    if (pat.empty()) return text.empty();
    if (pat[0] == '%') {
        for (size_t i = 0; i <= text.size(); ++i)
            if (like_match(text.substr(i), pat.substr(1))) return true;
        return false;
    }
    if (text.empty()) return false;
    if (pat[0] == '_' || pat[0] == text[0])
        return like_match(text.substr(1), pat.substr(1));
    return false;
}

/* ── Expression evaluator ────────────────────────────────────────── */

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

    /* Parenthesised expression */
    if (e.front() == '(' && e.back() == ')') {
        int depth = 0;
        bool balanced = true;
        for (size_t i = 0; i < e.size() - 1; ++i) {
            if (e[i] == '(') ++depth;
            else if (e[i] == ')') { if (--depth < 0) { balanced = false; break; } }
        }
        if (balanced && depth == 0)
            return eval_expr(e.substr(1, e.size()-2), row, col_order);
    }

    /* String literal */
    if (e[0] == '\'' ) {
        SvdbVal v; v.type = SVDB_TYPE_TEXT;
        v.sval = e.size() >= 2 ? e.substr(1, e.size()-2) : "";
        std::string out; out.reserve(v.sval.size());
        for (size_t i = 0; i < v.sval.size(); ++i) {
            if (v.sval[i] == '\'' && i+1 < v.sval.size() && v.sval[i+1] == '\'')
                { out += '\''; ++i; }
            else out += v.sval[i];
        }
        v.sval = out; return v;
    }

    /* Numeric literal */
    bool starts_num = isdigit((unsigned char)e[0]) || e[0] == '.' ||
                      ((e[0] == '-' || e[0] == '+') && e.size() > 1 &&
                       (isdigit((unsigned char)e[1]) || e[1] == '.'));
    if (starts_num) {
        bool is_real = false;
        for (char c : e) if (c == '.' || c == 'e' || c == 'E') is_real = true;
        SvdbVal v;
        try {
            if (is_real) { v.type = SVDB_TYPE_REAL; v.rval = std::stod(e); }
            else          { v.type = SVDB_TYPE_INT;  v.ival = std::stoll(e); }
        } catch (...) { v.type = SVDB_TYPE_TEXT; v.sval = e; }
        return v;
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
                    SvdbVal v = eval_expr(args.substr(start, i - start), row, col_order);
                    if (v.type != SVDB_TYPE_NULL) return v;
                    start = i + 1;
                }
            }
            return SvdbVal{};
        }
        /* IFNULL(a, b) */
        if (eu.substr(0, 8) == "IFNULL(" && e.back() == ')') {
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
        /* LENGTH(expr) */
        if (eu.substr(0, 7) == "LENGTH(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(7, e.size()-8), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_INT;
            v.ival = (int64_t)val_to_str(inner).size();
            return v;
        }
        /* UPPER(expr) */
        if (eu.substr(0, 6) == "UPPER(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            v.sval = qry_upper(val_to_str(inner)); return v;
        }
        /* LOWER(expr) */
        if (eu.substr(0, 6) == "LOWER(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(6, e.size()-7), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            std::string s = val_to_str(inner);
            for (auto &c : s) c = (char)tolower((unsigned char)c);
            v.sval = s; return v;
        }
        /* TRIM(expr) */
        if (eu.substr(0, 5) == "TRIM(" && e.back() == ')') {
            SvdbVal inner = eval_expr(e.substr(5, e.size()-6), row, col_order);
            SvdbVal v; v.type = SVDB_TYPE_TEXT;
            v.sval = qry_trim(val_to_str(inner)); return v;
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
            if (ops_str.find(c) != std::string::npos && i > 0) {
                /* Make sure it's a real operator, not unary minus */
                if ((c == '-' || c == '+') && i == 0) continue;
                return (size_t)i;
            }
        }
        return std::string::npos;
    };

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

    /* High-precedence: * and / */
    op_pos = find_binary_op(e, "*/");
    if (op_pos != std::string::npos && op_pos > 0) {
        char op = e[op_pos];
        SvdbVal lhs = eval_expr(e.substr(0, op_pos), row, col_order);
        SvdbVal rhs = eval_expr(e.substr(op_pos+1), row, col_order);
        if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
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

    /* Column reference (possibly table.col or "col") */
    std::string col = e;
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
        for (size_t i = 0; i < wt.size()-1; ++i) {
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

    /* LIKE */
    {
        size_t like_pos = wu.find(" LIKE ");
        if (like_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, like_pos), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(like_pos + 6), row, col_order);
            std::string pattern = val_to_str(rhs);
            return like_match(val_to_str(lhs), pattern);
        }
        size_t not_like = wu.find(" NOT LIKE ");
        if (not_like != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, not_like), row, col_order);
            SvdbVal rhs = eval_expr(wt.substr(not_like + 10), row, col_order);
            return !like_match(val_to_str(lhs), val_to_str(rhs));
        }
    }

    /* BETWEEN ... AND ... */
    {
        size_t bet_pos = wu.find(" BETWEEN ");
        if (bet_pos != std::string::npos) {
            size_t and2 = wu.find(" AND ", bet_pos + 9);
            if (and2 != std::string::npos) {
                SvdbVal val  = eval_expr(wt.substr(0, bet_pos), row, col_order);
                SvdbVal low  = eval_expr(wt.substr(bet_pos+9, and2-(bet_pos+9)), row, col_order);
                SvdbVal high = eval_expr(wt.substr(and2+5), row, col_order);
                return val_cmp(val, low) >= 0 && val_cmp(val, high) <= 0;
            }
        }
    }

    /* IN (v1, v2, ...) / NOT IN */
    {
        size_t in_pos  = wu.find(" IN (");
        size_t nin_pos = wu.find(" NOT IN (");
        size_t use_pos = std::string::npos; bool negated = false;
        if (nin_pos != std::string::npos) { use_pos = nin_pos; negated = true; }
        else if (in_pos != std::string::npos) use_pos = in_pos;
        if (use_pos != std::string::npos) {
            SvdbVal lhs = eval_expr(wt.substr(0, use_pos), row, col_order);
            size_t paren_start = wt.find('(', use_pos);
            size_t paren_end   = wt.rfind(')');
            if (paren_start != std::string::npos && paren_end != std::string::npos) {
                std::string inside = wt.substr(paren_start+1, paren_end-paren_start-1);
                /* Split by comma */
                bool found = false;
                std::istringstream ss(inside);
                std::string token;
                while (std::getline(ss, token, ',')) {
                    SvdbVal rv = eval_expr(qry_trim(token), row, col_order);
                    if (val_cmp(lhs, rv) == 0) { found = true; break; }
                }
                return negated ? !found : found;
            }
        }
    }

    /* Comparison operators: !=, <>, <=, >=, =, <, > */
    const char *ops[] = {"!=", "<>", "<=", ">=", "=", "<", ">", nullptr};
    size_t op_start = std::string::npos, op_len = 0;
    for (int i = 0; ops[i]; ++i) {
        size_t pos = wt.find(ops[i]);
        if (pos != std::string::npos && (op_start == std::string::npos || pos < op_start)) {
            op_start = pos; op_len = strlen(ops[i]);
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

    return true;
}

/* ── SQL clause parsing helpers ─────────────────────────────────── */

struct OrderCol { std::string expr; bool desc; };
struct JoinSpec  { std::string type; /* INNER/LEFT */ std::string table; std::string on_left; std::string on_right; };

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

/* Extract simple JOIN spec (one JOIN at a time) */
static JoinSpec parse_join(const std::string &sql) {
    JoinSpec j; j.type = "";
    std::string su = qry_upper(sql);
    /* Look for INNER JOIN or LEFT JOIN or LEFT OUTER JOIN */
    size_t pos = std::string::npos;
    std::string join_type;
    {
        size_t inner = su.find("INNER JOIN ");
        size_t left  = su.find("LEFT JOIN ");
        size_t louter= su.find("LEFT OUTER JOIN ");
        if (louter != std::string::npos) { pos = louter; join_type = "LEFT"; }
        else if (inner != std::string::npos && (left == std::string::npos || inner < left))
            { pos = inner; join_type = "INNER"; }
        else if (left != std::string::npos)
            { pos = left; join_type = "LEFT"; }
    }
    if (pos == std::string::npos) return j;
    j.type = join_type;
    /* Skip "INNER JOIN " or "LEFT JOIN " or "LEFT OUTER JOIN " */
    size_t after = (join_type == "LEFT" && su.find("LEFT OUTER JOIN ") == pos)
        ? pos + 16 : pos + (join_type == "INNER" ? 11 : 10);
    /* Read table name */
    size_t ts = after, te = su.find(' ', ts);
    j.table = qry_trim(sql.substr(ts, te - ts));
    /* Find ON clause */
    size_t on_pos = su.find(" ON ", te);
    if (on_pos == std::string::npos) return j; /* cross join - no ON */
    std::string on_expr = qry_trim(sql.substr(on_pos + 4));
    /* End at WHERE/ORDER BY/GROUP BY/LIMIT */
    for (const char *kw : {" WHERE ", " ORDER ", " GROUP ", " LIMIT ", " HAVING "}) {
        size_t kp = qry_upper(on_expr).find(kw);
        if (kp != std::string::npos) on_expr = on_expr.substr(0, kp);
    }
    /* Parse ON left.col = right.col */
    size_t eq_pos = on_expr.find('=');
    if (eq_pos != std::string::npos) {
        j.on_left  = qry_trim(on_expr.substr(0, eq_pos));
        j.on_right = qry_trim(on_expr.substr(eq_pos + 1));
    }
    return j;
}

/* ── Aggregate function evaluation ──────────────────────────────── */

struct AggState {
    std::string func; /* COUNT/SUM/AVG/MIN/MAX */
    std::string arg;  /* column or * */
    int64_t count = 0;
    double  sum   = 0.0;
    SvdbVal min_val, max_val;
    bool has_min = false, has_max = false;
    bool is_real = false;
};

static bool is_agg_expr(const std::string &e) {
    std::string eu = qry_upper(e);
    return eu.find("COUNT(") != std::string::npos ||
           eu.find("SUM(")   != std::string::npos ||
           eu.find("AVG(")   != std::string::npos ||
           eu.find("MIN(")   != std::string::npos ||
           eu.find("MAX(")   != std::string::npos;
}

static AggState make_agg(const std::string &expr) {
    AggState a; a.func = "";
    std::string eu = qry_upper(qry_trim(expr));
    const char *funcs[] = {"COUNT", "SUM", "AVG", "MIN", "MAX", nullptr};
    for (int i = 0; funcs[i]; ++i) {
        std::string prefix = std::string(funcs[i]) + "(";
        if (eu.substr(0, prefix.size()) == prefix && eu.back() == ')') {
            a.func = funcs[i];
            a.arg = qry_trim(expr.substr(prefix.size(), expr.size() - prefix.size() - 1));
            break;
        }
    }
    return a;
}

static void agg_accumulate(AggState &a, const Row &row,
                            const std::vector<std::string> &col_order) {
    if (a.func == "COUNT") {
        if (a.arg == "*") { ++a.count; return; }
        SvdbVal v = eval_expr(a.arg, row, col_order);
        if (v.type != SVDB_TYPE_NULL) ++a.count;
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
    }
}

static SvdbVal agg_result(const AggState &a) {
    if (a.func == "COUNT") {
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = a.count; return v;
    }
    if (a.func == "SUM") {
        if (a.count == 0) return SvdbVal{};
        if (a.is_real) { SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = a.sum; return v; }
        SvdbVal v; v.type = SVDB_TYPE_INT; v.ival = (int64_t)a.sum; return v;
    }
    if (a.func == "AVG") {
        if (a.count == 0) return SvdbVal{};
        SvdbVal v; v.type = SVDB_TYPE_REAL; v.rval = a.sum / (double)a.count; return v;
    }
    if (a.func == "MIN") return a.has_min ? a.min_val : SvdbVal{};
    if (a.func == "MAX") return a.has_max ? a.max_val : SvdbVal{};
    return SvdbVal{};
}

/* ── Main SELECT execution ──────────────────────────────────────── */

svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                  svdb_rows_t **rows_out) {
    if (!rows_out) return SVDB_ERR;

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
    svdb_parser_destroy(p);

    /* Parse additional SQL clauses from raw SQL */
    auto order_cols  = parse_order_by(sql);
    auto group_cols  = parse_group_by(sql);
    std::string having_txt = parse_having(sql);
    int64_t limit_val = -1, offset_val = 0;
    parse_limit_offset(sql, limit_val, offset_val);
    JoinSpec join = parse_join(sql);

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

    if (!join.type.empty() && db->schema.count(join.table)) {
        /* Resolve right table case-insensitively */
        std::string right_tname = join.table;
        for (auto &kv : db->schema) {
            if (qry_upper(kv.first) == qry_upper(join.table)) { right_tname = kv.first; break; }
        }
        const auto &right_col_order = db->col_order.at(right_tname);
        /* Merged col order: left cols first, then right (prefixed) */
        for (const auto &c : col_order)       merged_col_order.push_back(c);
        for (const auto &c : right_col_order) merged_col_order.push_back(c);

        for (const auto &lrow : db->data.at(tname)) {
            bool matched = false;
            for (const auto &rrow : db->data.at(right_tname)) {
                /* Evaluate ON condition by merging row */
                Row merged;
                for (auto &kv : lrow) merged[kv.first] = kv.second;
                for (auto &kv : rrow) {
                    /* Prefix right cols with table name to avoid collision */
                    merged[kv.first] = kv.second;
                    merged[right_tname + "." + kv.first] = kv.second;
                }
                /* Check ON condition */
                bool on_match = true;
                if (!join.on_left.empty() && !join.on_right.empty()) {
                    SvdbVal lv = eval_expr(join.on_left, merged, merged_col_order);
                    SvdbVal rv = eval_expr(join.on_right, merged, merged_col_order);
                    on_match = (val_cmp(lv, rv) == 0);
                }
                if (on_match) {
                    all_rows.push_back(merged); matched = true;
                }
            }
            if (!matched && join.type == "LEFT") {
                Row merged;
                for (auto &kv : lrow) merged[kv.first] = kv.second;
                for (const auto &c : right_col_order) {
                    merged[c] = SvdbVal{};
                    merged[right_tname + "." + c] = SvdbVal{};
                }
                all_rows.push_back(merged);
            }
        }
    } else {
        all_rows = db->data.at(tname);
        merged_col_order = col_order;
    }

    /* ── Determine output columns ── */
    std::vector<std::string> out_cols;
    if (star) {
        out_cols = col_order;
    } else {
        for (const auto &c : sel_cols) {
            /* Strip alias (AS name) */
            std::string cu = qry_upper(c);
            size_t as_pos = cu.find(" AS ");
            std::string col_expr = (as_pos != std::string::npos) ? c.substr(0, as_pos) : c;
            out_cols.push_back(qry_trim(col_expr));
        }
    }

    /* Compute alias map for output columns */
    std::vector<std::string> out_names;
    for (const auto &c : (star ? sel_cols : sel_cols)) {
        std::string cu = qry_upper(c);
        size_t as_pos = cu.find(" AS ");
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
            size_t as_pos = cu.find(" AS ");
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
            /* Check HAVING */
            if (!having_txt.empty()) {
                /* Evaluate having against the representative row */
                if (!qry_eval_where(rep_row, merged_col_order, having_txt)) continue;
            }
            std::vector<SvdbVal> res_row;
            auto &agg_list = key_aggs[key];
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
    }

    /* DISTINCT */
    if (distinct) {
        std::set<std::string> seen;
        std::vector<std::vector<SvdbVal>> deduped;
        for (auto &row : raw_rows) {
            std::string key;
            for (auto &v : row) key += val_to_str(v) + "\x01";
            if (seen.insert(key).second) deduped.push_back(row);
        }
        raw_rows = std::move(deduped);
    }

    /* ORDER BY */
    if (!order_cols.empty()) {
        std::sort(raw_rows.begin(), raw_rows.end(),
            [&](const std::vector<SvdbVal> &a, const std::vector<SvdbVal> &b) {
                for (const auto &oc : order_cols) {
                    /* Find col index */
                    int idx = -1;
                    for (size_t i = 0; i < r->col_names.size(); ++i)
                        if (qry_upper(r->col_names[i]) == qry_upper(oc.expr)) { idx = (int)i; break; }
                    /* Try numeric index */
                    if (idx < 0) {
                        try { idx = (int)std::stoll(oc.expr) - 1; } catch (...) {}
                    }
                    if (idx < 0 || idx >= (int)a.size()) continue;
                    int c = val_cmp(a[idx], b[idx]);
                    if (c != 0) return oc.desc ? c > 0 : c < 0;
                }
                return false;
            });
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
            for (const auto &col : db->col_order.at(tname)) {
                const ColDef *def = nullptr;
                auto sit = db->schema.at(tname).find(col);
                if (sit != db->schema.at(tname).end()) def = &sit->second;
                std::string ctype = def ? def->type : "TEXT";
                int notnull = def ? (def->not_null ? 1 : 0) : 0;
                int pk = 0;
                /* Check primary_keys */
                if (db->primary_keys.count(tname)) {
                    for (auto &pk_col : db->primary_keys.at(tname))
                        if (pk_col == col) { pk = 1; break; }
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
    return svdb_query_internal(db, s, rows);
}

} /* extern "C" */
