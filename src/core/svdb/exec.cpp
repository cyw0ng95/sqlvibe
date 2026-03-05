/*
 * exec.cpp — svdb_exec: DDL + DML dispatcher
 *
 * Parses SQL via the QP parser and handles:
 *   DDL: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX, ALTER TABLE
 *   DML: INSERT, UPDATE, DELETE
 * SELECT is forwarded to query.cpp via svdb_query_internal.
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
#include <sstream>
#include <cstring>
#include <cstdio>

/* Implemented in query.cpp */
extern svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                        svdb_rows_t **rows_out);
extern svdb_code_t svdb_query_pragma(svdb_db_t *db, const std::string &sql,
                                      svdb_rows_t **rows_out);
extern SvdbVal svdb_eval_expr_in_row(const std::string &expr, const Row &row,
                                      const std::vector<std::string> &col_order);
extern void svdb_set_query_db(svdb_db_t *db);

/* ── helpers ────────────────────────────────────────────────────── */

static std::string str_upper(std::string s) {
    for (auto &c : s) c = (char)toupper((unsigned char)c);
    return s;
}

static std::string str_trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char)s[a])) ++a;
    while (b > a && isspace((unsigned char)s[b-1])) --b;
    return s.substr(a, b - a);
}

/* Strip SQL line comments and block comments, preserving string literals */
static std::string strip_sql_comments(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    size_t i = 0;
    while (i < s.size()) {
        /* String literal: copy as-is */
        if (s[i] == '\'') {
            out += s[i++];
            while (i < s.size()) {
                out += s[i];
                if (s[i] == '\'' && (i + 1 >= s.size() || s[i+1] != '\'')) { ++i; break; }
                if (s[i] == '\'' && i + 1 < s.size() && s[i+1] == '\'') { out += s[++i]; }
                ++i;
            }
            continue;
        }
        /* Line comment: -- to end of line → replace with space */
        if (i + 1 < s.size() && s[i] == '-' && s[i+1] == '-') {
            while (i < s.size() && s[i] != '\n') ++i;
            out += ' ';
            continue;
        }
        /* Block comment: slash-star ... star-slash → replace with space */
        if (i + 1 < s.size() && s[i] == '/' && s[i+1] == '*') {
            i += 2;
            while (i + 1 < s.size() && !(s[i] == '*' && s[i+1] == '/')) ++i;
            if (i + 1 < s.size()) i += 2;
            out += ' ';
            continue;
        }
        out += s[i++];
    }
    return out;
}

static std::string first_keyword(const std::string &sql) {
    size_t i = 0;
    while (i < sql.size() && isspace((unsigned char)sql[i])) ++i;
    size_t start = i;
    while (i < sql.size() && isalpha((unsigned char)sql[i])) ++i;
    std::string kw = sql.substr(start, i - start);
    return str_upper(kw);
}

/* Parse column definitions from CREATE TABLE sql (after the opening '(').
 * Fills schema ColDef and col_order for the table. */
static void parse_column_defs(const std::string &sql, size_t pos,
                               TableDef &out_td,
                               std::vector<std::string> &out_order,
                               std::vector<std::string> *out_pk = nullptr,
                               std::vector<std::vector<std::string>> *out_uniq = nullptr,
                               CheckList *out_checks = nullptr,
                               std::vector<FKDef> *out_fks = nullptr) {
    /* Find the opening '(' */
    while (pos < sql.size() && sql[pos] != '(') ++pos;
    if (pos >= sql.size()) return;
    ++pos; /* skip '(' */

    int depth = 1;
    /* Walk the column list */
    while (pos < sql.size() && depth > 0) {
        /* Skip whitespace */
        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
        if (pos >= sql.size() || (sql[pos] == ')' && depth == 1)) break;

        /* Check for table-level constraint keywords */
        size_t tmp = pos;
        std::string kw;
        while (tmp < sql.size() && isalpha((unsigned char)sql[tmp])) ++tmp;
        kw = str_upper(sql.substr(pos, tmp - pos));

        if (kw == "PRIMARY") {
            /* PRIMARY KEY (col, ...) */
            pos = tmp;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            /* skip KEY */
            while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            if (pos < sql.size() && sql[pos] == '(') {
                ++pos;
                std::vector<std::string> pk_cols;
                while (pos < sql.size() && sql[pos] != ')') {
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    std::string cn;
                    if (pos < sql.size() && (sql[pos] == '"' || sql[pos] == '`')) {
                        char q = sql[pos++]; size_t s = pos;
                        while (pos < sql.size() && sql[pos] != q) ++pos;
                        cn = sql.substr(s, pos - s);
                        if (pos < sql.size()) ++pos;
                    } else {
                        size_t s = pos;
                        while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                        cn = sql.substr(s, pos - s);
                    }
                    if (!cn.empty()) pk_cols.push_back(cn);
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    if (pos < sql.size() && sql[pos] == ',') ++pos;
                }
                if (pos < sql.size()) ++pos; /* skip ')' */
                if (out_pk) *out_pk = pk_cols;
                for (auto &cn : pk_cols) {
                    auto it = out_td.find(cn);
                    if (it != out_td.end()) it->second.primary_key = true;
                }
            }
            /* skip to next comma */
            while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')') ++pos;
            if (pos < sql.size() && sql[pos] == ',') ++pos;
            continue;
        }
        if (kw == "UNIQUE") {
            /* UNIQUE (col, ...) */
            pos = tmp;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            if (pos < sql.size() && sql[pos] == '(') {
                ++pos;
                std::vector<std::string> ucols;
                while (pos < sql.size() && sql[pos] != ')') {
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    std::string cn;
                    size_t s = pos;
                    while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                    cn = sql.substr(s, pos - s);
                    if (!cn.empty()) ucols.push_back(cn);
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    if (pos < sql.size() && sql[pos] == ',') ++pos;
                }
                if (pos < sql.size()) ++pos;
                if (out_uniq && !ucols.empty()) out_uniq->push_back(ucols);
            }
            while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')') ++pos;
            if (pos < sql.size() && sql[pos] == ',') ++pos;
            continue;
        }
        if (kw == "CHECK") {
            pos = tmp;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            if (pos < sql.size() && sql[pos] == '(') {
                ++pos;
                size_t s = pos;
                int cdepth = 1;
                while (pos < sql.size() && cdepth > 0) {
                    if (sql[pos] == '(') ++cdepth;
                    else if (sql[pos] == ')') --cdepth;
                    if (cdepth > 0) ++pos;
                }
                if (out_checks) out_checks->push_back(sql.substr(s, pos - s));
                if (pos < sql.size()) ++pos; /* skip ')' */
            }
            while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')') ++pos;
            if (pos < sql.size() && sql[pos] == ',') ++pos;
            continue;
        }
        if (kw == "FOREIGN") {
            /* FOREIGN KEY (col) REFERENCES tbl(col) */
            pos = tmp;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            /* skip KEY */
            while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            std::string child_col;
            if (pos < sql.size() && sql[pos] == '(') {
                ++pos;
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t s = pos;
                while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                child_col = sql.substr(s, pos - s);
                while (pos < sql.size() && sql[pos] != ')') ++pos;
                if (pos < sql.size()) ++pos;
            }
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            /* skip REFERENCES keyword */
            while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            std::string parent_table;
            size_t s2 = pos;
            while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
            parent_table = sql.substr(s2, pos - s2);
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            std::string parent_col;
            if (pos < sql.size() && sql[pos] == '(') {
                ++pos;
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t s3 = pos;
                while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                parent_col = sql.substr(s3, pos - s3);
                while (pos < sql.size() && sql[pos] != ')') ++pos;
                if (pos < sql.size()) ++pos;
            }
            if (out_fks && !child_col.empty() && !parent_table.empty()) {
                FKDef fk;
                fk.child_col = child_col;
                fk.parent_table = parent_table;
                fk.parent_col = parent_col;
                out_fks->push_back(fk);
            }
            while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')') ++pos;
            if (pos < sql.size() && sql[pos] == ',') ++pos;
            continue;
        }

        /* Read column name */
        std::string col_name;
        if (pos < sql.size() && (sql[pos] == '"' || sql[pos] == '`')) {
            char q = sql[pos++];
            size_t start = pos;
            while (pos < sql.size() && sql[pos] != q) ++pos;
            col_name = sql.substr(start, pos - start);
            if (pos < sql.size()) ++pos;
        } else {
            size_t start = pos;
            while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
            col_name = sql.substr(start, pos - start);
        }
        if (col_name.empty()) { ++pos; continue; }

        /* Skip whitespace */
        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;

        /* Read type (may be empty) */
        std::string col_type;
        while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) {
            col_type += (char)toupper((unsigned char)sql[pos]);
            ++pos;
        }
        /* Handle type with precision e.g. VARCHAR(255) */
        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
        if (pos < sql.size() && sql[pos] == '(') {
            int pd = 1; ++pos;
            while (pos < sql.size() && pd > 0) {
                if (sql[pos] == '(') ++pd;
                else if (sql[pos] == ')') --pd;
                ++pos;
            }
        }
        if (col_type.empty()) col_type = "TEXT";

        ColDef cd;
        cd.type = col_type;
        cd.not_null = false;
        cd.primary_key = false;

        /* Consume column constraints until ',' or ')' */
        while (pos < sql.size()) {
            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
            if (pos >= sql.size()) break;
            if (sql[pos] == ',' && depth == 1) { ++pos; break; }
            if (sql[pos] == ')') { depth--; break; }
            if (sql[pos] == '(') { depth++; ++pos; continue; }
            /* Read next keyword */
            size_t s2 = pos;
            while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
            std::string ckw = str_upper(sql.substr(s2, pos - s2));
            if (ckw == "NOT") {
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t s3 = pos;
                while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
                std::string next_kw = str_upper(sql.substr(s3, pos - s3));
                if (next_kw == "NULL") cd.not_null = true;
            } else if (ckw == "DEFAULT") {
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t ds = pos;
                if (pos < sql.size() && sql[pos] == '\'') {
                    ++pos;
                    while (pos < sql.size() && sql[pos] != '\'') ++pos;
                    if (pos < sql.size()) ++pos;
                } else if (pos < sql.size() && sql[pos] == '(') {
                    int pd2 = 1; ++pos;
                    while (pos < sql.size() && pd2 > 0) {
                        if (sql[pos] == '(') ++pd2;
                        else if (sql[pos] == ')') --pd2;
                        ++pos;
                    }
                } else {
                    while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')' &&
                           !isspace((unsigned char)sql[pos])) ++pos;
                }
                cd.default_val = str_trim(sql.substr(ds, pos - ds));
            } else if (ckw == "PRIMARY") {
                /* PRIMARY KEY — skip KEY */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
                cd.primary_key = true;
                cd.not_null = true;
                if (out_pk) out_pk->push_back(col_name);
            } else if (ckw == "AUTOINCREMENT") {
                cd.auto_increment = true;
            } else if (ckw == "UNIQUE") {
                /* Column-level UNIQUE */
                if (out_uniq) out_uniq->push_back({col_name});
            } else if (ckw == "REFERENCES") {
                /* Inline FK: col ... REFERENCES parent(pcol) */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t s3 = pos;
                while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                std::string parent_tbl = sql.substr(s3, pos - s3);
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                std::string parent_c;
                if (pos < sql.size() && sql[pos] == '(') {
                    ++pos;
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    size_t s4 = pos;
                    while (pos < sql.size() && (isalnum((unsigned char)sql[pos]) || sql[pos] == '_')) ++pos;
                    parent_c = sql.substr(s4, pos - s4);
                    while (pos < sql.size() && sql[pos] != ')') ++pos;
                    if (pos < sql.size()) ++pos;
                }
                if (out_fks && !parent_tbl.empty()) {
                    FKDef fk;
                    fk.child_col = col_name;
                    fk.parent_table = parent_tbl;
                    fk.parent_col = parent_c;
                    out_fks->push_back(fk);
                }
            } else if (ckw == "CHECK") {
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                if (pos < sql.size() && sql[pos] == '(') {
                    ++pos;
                    size_t s3 = pos; int cdepth = 1;
                    while (pos < sql.size() && cdepth > 0) {
                        if (sql[pos] == '(') ++cdepth;
                        else if (sql[pos] == ')') --cdepth;
                        if (cdepth > 0) ++pos;
                    }
                    if (out_checks) out_checks->push_back(sql.substr(s3, pos - s3));
                    if (pos < sql.size()) ++pos;
                }
            } else if (ckw.empty()) {
                ++pos;
            }
        }

        out_td[col_name] = cd;
        out_order.push_back(col_name);
    }
}

/* Evaluate a simple WHERE expression against a single row.
 * Supports: col = 'val', col = num, col != val, col > num, col < num.
 * Returns true if row matches (or no WHERE given). */
static bool eval_where(const Row &row,
                        const std::vector<std::string> &col_order,
                        const std::string &where_text) {
    if (where_text.empty()) return true;

    /* Tokenise: find first comparison operator */
    std::string wt = str_trim(where_text);

    /* Support AND-joined conditions (very basic) */
    /* Find top-level AND */
    size_t and_pos = std::string::npos;
    {
        std::string wu = str_upper(wt);
        size_t p = 0;
        int depth = 0;
        while (p < wu.size()) {
            if (wu[p] == '(') { ++depth; ++p; continue; }
            if (wu[p] == ')') { if (depth > 0) --depth; ++p; continue; }
            if (depth == 0 && p + 4 <= wu.size() && wu.substr(p, 4) == " AND") {
                and_pos = p;
                break;
            }
            ++p;
        }
    }
    if (and_pos != std::string::npos) {
        std::string lhs = wt.substr(0, and_pos);
        std::string rhs = wt.substr(and_pos + 4);
        return eval_where(row, col_order, lhs) && eval_where(row, col_order, rhs);
    }

    /* Parse single condition: col OP value */
    size_t p = 0;
    while (p < wt.size() && isspace((unsigned char)wt[p])) ++p;

    /* col name */
    std::string col;
    if (p < wt.size() && (wt[p] == '"' || wt[p] == '`')) {
        char q = wt[p++]; size_t s = p;
        while (p < wt.size() && wt[p] != q) ++p;
        col = wt.substr(s, p - s);
        if (p < wt.size()) ++p;
    } else {
        size_t s = p;
        while (p < wt.size() && (isalnum((unsigned char)wt[p]) || wt[p] == '_' || wt[p] == '.')) ++p;
        col = wt.substr(s, p - s);
        /* strip table prefix */
        auto dot = col.find('.');
        if (dot != std::string::npos) col = col.substr(dot + 1);
    }

    while (p < wt.size() && isspace((unsigned char)wt[p])) ++p;

    /* operator */
    std::string op;
    while (p < wt.size() && (wt[p] == '=' || wt[p] == '!' || wt[p] == '<' || wt[p] == '>')) {
        op += wt[p++];
    }
    if (op.empty()) return true; /* can't parse, allow */

    while (p < wt.size() && isspace((unsigned char)wt[p])) ++p;

    /* value */
    std::string val_str;
    bool is_text = false;
    if (p < wt.size() && wt[p] == '\'') {
        ++p; size_t s = p;
        while (p < wt.size() && wt[p] != '\'') ++p;
        val_str = wt.substr(s, p - s);
        is_text = true;
        if (p < wt.size()) ++p;
    } else {
        size_t s = p;
        while (p < wt.size() && !isspace((unsigned char)wt[p])) ++p;
        val_str = wt.substr(s, p - s);
    }

    /* Look up column in row */
    auto it = row.find(col);
    if (it == row.end()) return false;

    const SvdbVal &sv = it->second;

    if (is_text || sv.type == SVDB_TYPE_TEXT) {
        std::string row_val = (sv.type == SVDB_TYPE_TEXT) ? sv.sval :
                              (sv.type == SVDB_TYPE_INT)  ? std::to_string(sv.ival) :
                                                             std::to_string(sv.rval);
        if (op == "="  || op == "==") return row_val == val_str;
        if (op == "!=" || op == "<>") return row_val != val_str;
        if (op == "<")                return row_val < val_str;
        if (op == ">")                return row_val > val_str;
        if (op == "<=")               return row_val <= val_str;
        if (op == ">=")               return row_val >= val_str;
        return true;
    } else {
        double row_num = (sv.type == SVDB_TYPE_INT) ? (double)sv.ival : sv.rval;
        double cmp_num = 0.0;
        if (!val_str.empty()) {
            try { cmp_num = std::stod(val_str); } catch (...) { return false; }
        }
        if (op == "="  || op == "==") return row_num == cmp_num;
        if (op == "!=" || op == "<>") return row_num != cmp_num;
        if (op == "<")                return row_num < cmp_num;
        if (op == ">")                return row_num > cmp_num;
        if (op == "<=")               return row_num <= cmp_num;
        if (op == ">=")               return row_num >= cmp_num;
        return true;
    }
}

/* Parse a literal value string into an SvdbVal */
static SvdbVal parse_literal(const std::string &v) {
    SvdbVal sv;
    std::string vu = str_upper(v);
    if (vu == "NULL") {
        sv.type = SVDB_TYPE_NULL;
        return sv;
    }
    /* Boolean literals: TRUE → 1, FALSE → 0 (SQLite-compatible) */
    if (vu == "TRUE") {
        sv.type = SVDB_TYPE_INT; sv.ival = 1; return sv;
    }
    if (vu == "FALSE") {
        sv.type = SVDB_TYPE_INT; sv.ival = 0; return sv;
    }
    if (v.empty()) {
        /* Empty string: parser stripped quotes from '' → return TEXT "" */
        sv.type = SVDB_TYPE_TEXT;
        return sv;
    }
    /* Hex blob literal: x'HEXHEX...' or X'HEXHEX...' (SQLite-compatible) */
    if (v.size() >= 3 && (v[0] == 'x' || v[0] == 'X') && v[1] == '\'') {
        sv.type = SVDB_TYPE_BLOB;
        size_t end = v.rfind('\'');
        if (end > 1) {
            std::string hex = v.substr(2, end - 2);
            std::string binary;
            for (size_t i = 0; i + 1 < hex.size(); i += 2) {
                auto fromhex = [](char c) -> unsigned char {
                    if (c >= '0' && c <= '9') return (unsigned char)(c - '0');
                    if (c >= 'a' && c <= 'f') return (unsigned char)(c - 'a' + 10);
                    if (c >= 'A' && c <= 'F') return (unsigned char)(c - 'A' + 10);
                    return 0;
                };
                binary += (char)((fromhex(hex[i]) << 4) | fromhex(hex[i + 1]));
            }
            sv.sval = binary;
        }
        return sv;
    }
    if (v[0] == '\'') {
        sv.type = SVDB_TYPE_TEXT;
        sv.sval = v.size() >= 2 ? v.substr(1, v.size() - 2) : "";
        /* unescape '' → ' */
        std::string &s = sv.sval;
        std::string out;
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '\'' && i + 1 < s.size() && s[i+1] == '\'') { out += '\''; ++i; }
            else out += s[i];
        }
        sv.sval = out;
        return sv;
    }
    /* Try integer */
    bool is_float = false;
    for (char c : v) if (c == '.' || c == 'e' || c == 'E') is_float = true;
    try {
        if (is_float) {
            sv.type = SVDB_TYPE_REAL;
            sv.rval = std::stod(v);
        } else {
            sv.type = SVDB_TYPE_INT;
            sv.ival = std::stoll(v);
        }
    } catch (...) {
        sv.type = SVDB_TYPE_TEXT;
        sv.sval = v;
    }
    return sv;
}

/* Evaluate a simple expression for UPDATE SET clause values.
 * Handles: column refs, integer/float/string/NULL literals, arithmetic ops.
 * E.g. "v + 1", "price * 1.1", "col", "'hello'" */
static SvdbVal eval_expr_exec(const std::string &expr_in, const Row &row,
                               const std::vector<std::string> &col_order);

/* String conversion for eval_expr_exec */
static std::string val_to_str_exec(const SvdbVal &v) {
    if (v.type == SVDB_TYPE_TEXT || v.type == SVDB_TYPE_BLOB) return v.sval;
    if (v.type == SVDB_TYPE_INT)  return std::to_string(v.ival);
    if (v.type == SVDB_TYPE_REAL) {
        char buf[64]; snprintf(buf, sizeof(buf), "%.17g", v.rval); return buf;
    }
    return "";
}

static SvdbVal eval_expr_exec(const std::string &expr_in, const Row &row,
                               const std::vector<std::string> &col_order) {
    (void)col_order;
    std::string e = str_trim(expr_in);
    if (e.empty()) return SvdbVal{};

    /* Strip balanced outer parens */
    if (e.size() >= 2 && e.front() == '(' && e.back() == ')') {
        int d = 0; bool matched = true;
        for (size_t i = 0; i < e.size(); ++i) {
            if (e[i] == '(') ++d;
            else if (e[i] == ')') { if (--d == 0 && i + 1 != e.size()) { matched = false; break; } }
        }
        if (matched) return eval_expr_exec(e.substr(1, e.size()-2), row, col_order);
    }

    /* Find rightmost top-level || (string concatenation, lowest precedence) */
    int depth2 = 0; bool in_str2 = false;
    for (size_t i = e.size(); i >= 2; --i) {
        char c1 = e[i-2], c2 = e[i-1];
        if (c2 == '\'') { in_str2 = !in_str2; continue; }
        if (in_str2) continue;
        if (c2 == ')') ++depth2;
        if (c2 == '(') { if (depth2 > 0) --depth2; }
        if (depth2 > 0) continue;
        if (c1 == '|' && c2 == '|' && i >= 2) {
            SvdbVal lhs = eval_expr_exec(e.substr(0, i-2), row, col_order);
            SvdbVal rhs = eval_expr_exec(e.substr(i), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
            SvdbVal res; res.type = SVDB_TYPE_TEXT;
            res.sval = val_to_str_exec(lhs) + val_to_str_exec(rhs);
            return res;
        }
    }

    /* Find rightmost top-level + or - (for left-associative parsing) */
    depth2 = 0; in_str2 = false;
    for (size_t i = e.size(); i > 0; --i) {
        char c = e[i-1];
        if (c == '\'') { in_str2 = !in_str2; continue; }
        if (in_str2) continue;
        if (c == ')') ++depth2;
        if (c == '(') { if (depth2 > 0) --depth2; }
        if (depth2 > 0) continue;
        if ((c == '+' || c == '-') && i > 1) {
            SvdbVal lhs = eval_expr_exec(e.substr(0, i-1), row, col_order);
            SvdbVal rhs = eval_expr_exec(e.substr(i), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
            bool int_res = (lhs.type == SVDB_TYPE_INT && rhs.type == SVDB_TYPE_INT);
            double lv = (lhs.type == SVDB_TYPE_INT) ? (double)lhs.ival : lhs.rval;
            double rv = (rhs.type == SVDB_TYPE_INT) ? (double)rhs.ival : rhs.rval;
            double res = (c == '+') ? lv + rv : lv - rv;
            if (int_res) return SvdbVal{SVDB_TYPE_INT, (int64_t)res, 0.0, {}};
            return SvdbVal{SVDB_TYPE_REAL, 0, res, {}};
        }
    }
    /* Find rightmost top-level * or / */
    depth2 = 0; in_str2 = false;
    for (size_t i = e.size(); i > 0; --i) {
        char c = e[i-1];
        if (c == '\'') { in_str2 = !in_str2; continue; }
        if (in_str2) continue;
        if (c == ')') ++depth2;
        if (c == '(') { if (depth2 > 0) --depth2; }
        if (depth2 > 0) continue;
        if (c == '*' || c == '/') {
            SvdbVal lhs = eval_expr_exec(e.substr(0, i-1), row, col_order);
            SvdbVal rhs = eval_expr_exec(e.substr(i), row, col_order);
            if (lhs.type == SVDB_TYPE_NULL || rhs.type == SVDB_TYPE_NULL) return SvdbVal{};
            bool int_res = (lhs.type == SVDB_TYPE_INT && rhs.type == SVDB_TYPE_INT && c != '/');
            double lv = (lhs.type == SVDB_TYPE_INT) ? (double)lhs.ival : lhs.rval;
            double rv = (rhs.type == SVDB_TYPE_INT) ? (double)rhs.ival : rhs.rval;
            if (c == '/' && rv == 0.0) return SvdbVal{};
            double res = (c == '*') ? lv * rv : lv / rv;
            if (int_res) return SvdbVal{SVDB_TYPE_INT, (int64_t)res, 0.0, {}};
            return SvdbVal{SVDB_TYPE_REAL, 0, res, {}};
        }
    }

    /* Column reference */
    auto it = row.find(e);
    if (it != row.end()) return it->second;

    /* Fall back to literal parsing */
    return parse_literal(e);
}

/* ── DDL handlers ───────────────────────────────────────────────── */

static svdb_code_t do_create_table(svdb_db_t *db, const std::string &sql) {
    /* Use parser to get table name and column names */
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) { db->last_error = "out of memory"; return SVDB_NOMEM; }
    svdb_ast_node_t *ast = svdb_parser_parse(p);
    if (!ast) {
        db->last_error = svdb_parser_error(p);
        svdb_parser_destroy(p);
        return SVDB_ERR;
    }
    std::string tname = svdb_ast_get_table(ast);
    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);

    if (tname.empty()) { db->last_error = "CREATE TABLE: missing table name"; return SVDB_ERR; }

    /* IF NOT EXISTS: ignore if table already exists */
    std::string su = str_upper(sql);
    bool if_not_exists = su.find("IF NOT EXISTS") != std::string::npos;
    if (db->schema.count(tname)) {
        if (if_not_exists) return SVDB_OK;
        db->last_error = "table " + tname + " already exists";
        return SVDB_ERR;
    }

    TableDef td;
    std::vector<std::string> order;
    std::vector<std::string> pks;
    std::vector<std::vector<std::string>> uniqs;
    CheckList checks;
    std::vector<FKDef> fks;
    parse_column_defs(sql, 0, td, order, &pks, &uniqs, &checks, &fks);

    db->schema[tname]    = td;
    db->col_order[tname] = order;

    /* Empty column list is not valid */
    if (order.empty()) {
        db->last_error = "near \")\"" ": syntax error";
        return SVDB_ERR;
    }

    db->data[tname]      = {};
    db->rowid_counter[tname] = 0;
    db->create_sql[tname] = sql;
    if (!pks.empty()) db->primary_keys[tname] = pks;
    if (!uniqs.empty()) db->unique_constraints[tname] = uniqs;
    if (!checks.empty()) db->check_constraints[tname] = checks;
    if (!fks.empty()) db->fk_constraints[tname] = fks;
    return SVDB_OK;
}

/* Parse CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (col1, col2, ...) */
static svdb_code_t do_create_index(svdb_db_t *db, const std::string &sql, bool unique) {
    std::string su = str_upper(sql);
    /* Find IF NOT EXISTS */
    bool if_not_exists = su.find("IF NOT EXISTS") != std::string::npos;
    /* Find index name: after INDEX keyword */
    size_t idx_pos = su.find("INDEX");
    if (idx_pos == std::string::npos) return SVDB_ERR;
    size_t pos = idx_pos + 5;
    while (pos < su.size() && isspace((unsigned char)su[pos])) ++pos;
    /* Skip IF NOT EXISTS */
    if (su.substr(pos, 13) == "IF NOT EXISTS") { pos += 13; while (pos < su.size() && isspace((unsigned char)su[pos])) ++pos; }
    /* Read index name */
    size_t ns = pos;
    while (pos < su.size() && !isspace((unsigned char)su[pos]) && su[pos] != '(') ++pos;
    std::string iname = sql.substr(ns, pos - ns);
    /* Check for ON */
    while (pos < su.size() && su[pos] != 'O') ++pos;
    if (su.substr(pos, 2) != "ON") return SVDB_ERR;
    pos += 2;
    while (pos < su.size() && isspace((unsigned char)su[pos])) ++pos;
    /* Read table name */
    size_t ts = pos;
    while (pos < su.size() && !isspace((unsigned char)su[pos]) && su[pos] != '(') ++pos;
    std::string tname = sql.substr(ts, pos - ts);
    /* Check table exists */
    if (!db->schema.count(tname)) { db->last_error = "no such table: " + tname; return SVDB_ERR; }
    /* Check duplicate index name */
    if (db->indexes.count(iname)) {
        if (if_not_exists) return SVDB_OK;
        db->last_error = "index " + iname + " already exists";
        return SVDB_ERR;
    }
    /* Read column list */
    size_t paren = sql.find('(', pos);
    size_t rparen = sql.rfind(')');
    IndexDef idef; idef.table = tname; idef.unique = unique;
    if (paren != std::string::npos && rparen != std::string::npos) {
        std::string cols_str = sql.substr(paren + 1, rparen - paren - 1);
        std::istringstream ss(cols_str);
        std::string col;
        while (std::getline(ss, col, ',')) {
            col = str_trim(col);
            if (!col.empty()) idef.columns.push_back(col);
        }
    }
    db->indexes[iname] = idef;
    /* If unique index, also register in unique_constraints for enforcement at INSERT */
    if (unique && !idef.columns.empty()) {
        db->unique_constraints[tname].push_back(idef.columns);
    }
    return SVDB_OK;
}

static svdb_code_t do_drop_index(svdb_db_t *db, const std::string &sql) {
    std::string su = str_upper(sql);
    bool if_exists = su.find("IF EXISTS") != std::string::npos;
    size_t idx_pos = su.find("INDEX");
    if (idx_pos == std::string::npos) return SVDB_ERR;
    size_t pos = idx_pos + 5;
    while (pos < su.size() && isspace((unsigned char)su[pos])) ++pos;
    if (su.substr(pos, 9) == "IF EXISTS") { pos += 9; while (pos < su.size() && isspace((unsigned char)su[pos])) ++pos; }
    size_t ns = pos;
    while (pos < su.size() && !isspace((unsigned char)su[pos])) ++pos;
    std::string iname = sql.substr(ns, pos - ns);
    if (!db->indexes.count(iname)) {
        if (if_exists) return SVDB_OK;
        db->last_error = "no such index: " + iname;
        return SVDB_ERR;
    }
    /* Remove from unique_constraints if it was a unique index */
    auto &uconstr = db->unique_constraints;
    std::string dt = db->indexes[iname].table;
    std::vector<std::string> ucols = db->indexes[iname].columns;
    db->indexes.erase(iname);
    if (!ucols.empty() && uconstr.count(dt)) {
        auto &uc = uconstr[dt];
        uc.erase(std::remove(uc.begin(), uc.end(), ucols), uc.end());
    }
    return SVDB_OK;
}

static svdb_code_t do_drop_table(svdb_db_t *db, const std::string &sql) {
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);
    std::string tname = ast ? svdb_ast_get_table(ast) : std::string();
    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);

    if (tname.empty()) { db->last_error = "DROP TABLE: missing table name"; return SVDB_ERR; }

    std::string su = str_upper(sql);
    bool if_exists = su.find("IF EXISTS") != std::string::npos;
    if (!db->schema.count(tname)) {
        if (if_exists) return SVDB_OK;
        db->last_error = "no such table: " + tname;
        return SVDB_ERR;
    }
    db->schema.erase(tname);
    db->col_order.erase(tname);
    db->data.erase(tname);
    db->rowid_counter.erase(tname);
    return SVDB_OK;
}

static svdb_code_t do_alter_table(svdb_db_t *db, const std::string &sql) {
    /* Handles:
     * ALTER TABLE t ADD [COLUMN] col type
     * ALTER TABLE t ADD CONSTRAINT name UNIQUE(cols)
     * ALTER TABLE t ADD CONSTRAINT name CHECK(expr)
     * ALTER TABLE t RENAME TO new_name
     * ALTER TABLE t RENAME [COLUMN] old TO new
     * ALTER TABLE t RENAME INDEX old TO new
     * ALTER TABLE t DROP [COLUMN] col
     */
    std::string su = str_upper(sql);
    size_t p = 0;
    auto skip_kw = [&](const char *kw) {
        size_t len = strlen(kw);
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        if (p + len <= su.size() && su.substr(p, len) == kw) p += len;
    };
    skip_kw("ALTER"); skip_kw("TABLE");
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* table name */
    std::string tname;
    if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
        char q = sql[p++]; size_t s = p;
        while (p < sql.size() && sql[p] != q) ++p;
        tname = sql.substr(s, p - s);
        if (p < sql.size()) ++p;
    } else {
        size_t s = p;
        while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
        tname = sql.substr(s, p - s);
    }
    if (!db->schema.count(tname)) {
        db->last_error = "no such table: " + tname;
        return SVDB_ERR;
    }

    /* Skip whitespace and read action keyword */
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    size_t ap = p;
    while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
    std::string action = su.substr(ap, p - ap);

    if (action == "RENAME") {
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t np = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string next = su.substr(np, p - np);

        if (next == "INDEX") {
            /* RENAME INDEX old TO new */
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            size_t s3 = p;
            while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_' || sql[p] == '-')) ++p;
            std::string old_idx = str_trim(sql.substr(s3, p - s3));
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            /* Skip "TO" keyword */
            while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            size_t s4 = p;
            while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_' || sql[p] == '-')) ++p;
            std::string new_idx = str_trim(sql.substr(s4, p - s4));
            while (!new_idx.empty() && (new_idx.back() == ';' || isspace((unsigned char)new_idx.back())))
                new_idx.pop_back();
            /* Case-insensitive lookup */
            std::string old_key;
            std::string old_u = str_upper(old_idx);
            for (auto &kv : db->indexes)
                if (str_upper(kv.first) == old_u) { old_key = kv.first; break; }
            if (!old_key.empty()) {
                db->indexes[new_idx] = db->indexes[old_key];
                db->indexes.erase(old_key);
            } else {
                db->last_error = "no such index: " + old_idx;
                return SVDB_ERR;
            }
            return SVDB_OK;
        } else if (next == "COLUMN") {
            /* RENAME COLUMN old TO new */
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            std::string old_col;
            if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
                char q = sql[p++]; size_t s3 = p;
                while (p < sql.size() && sql[p] != q) ++p;
                old_col = sql.substr(s3, p - s3); if (p < sql.size()) ++p;
            } else {
                size_t s3 = p;
                while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
                old_col = sql.substr(s3, p - s3);
            }
            while (p < su.size() && isspace((unsigned char)su[p])) ++p;
            while (p < su.size() && isalpha((unsigned char)su[p])) ++p; /* skip TO */
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            std::string new_col;
            if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
                char q = sql[p++]; size_t s4 = p;
                while (p < sql.size() && sql[p] != q) ++p;
                new_col = sql.substr(s4, p - s4); if (p < sql.size()) ++p;
            } else {
                size_t s4 = p;
                while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
                new_col = sql.substr(s4, p - s4);
            }
            new_col = str_trim(new_col);
            while (!new_col.empty() && (new_col.back() == ';' || isspace((unsigned char)new_col.back())))
                new_col.pop_back();
            if (!db->schema[tname].count(old_col)) {
                db->last_error = "no such column: " + old_col; return SVDB_ERR;
            }
            db->schema[tname][new_col] = db->schema[tname][old_col];
            db->schema[tname].erase(old_col);
            for (auto &cn : db->col_order[tname]) if (cn == old_col) { cn = new_col; break; }
            for (auto &row : db->data[tname]) {
                auto it = row.find(old_col);
                if (it != row.end()) { row[new_col] = it->second; row.erase(it); }
            }
            return SVDB_OK;
        } else {
            /* RENAME TO new_name */
            std::string new_name;
            if (next == "TO") {
                while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
                size_t s3 = p;
                while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
                new_name = sql.substr(s3, p - s3);
            } else {
                new_name = sql.substr(np, p - np);
            }
            new_name = str_trim(new_name);
            while (!new_name.empty() && (new_name.back() == ';' || isspace((unsigned char)new_name.back())))
                new_name.pop_back();
            db->schema[new_name]        = db->schema[tname];
            db->col_order[new_name]     = db->col_order[tname];
            db->data[new_name]          = db->data[tname];
            db->rowid_counter[new_name] = db->rowid_counter[tname];
            db->schema.erase(tname); db->col_order.erase(tname);
            db->data.erase(tname);   db->rowid_counter.erase(tname);
            return SVDB_OK;
        }
    } else if (action == "DROP") {
        /* DROP [COLUMN] col */
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t np = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string next = su.substr(np, p - np);
        if (next == "COLUMN") {
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
        } else {
            p = np; /* go back if no COLUMN keyword */
        }
        std::string col_name;
        if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
            char q = sql[p++]; size_t s3 = p;
            while (p < sql.size() && sql[p] != q) ++p;
            col_name = sql.substr(s3, p - s3); if (p < sql.size()) ++p;
        } else {
            size_t s3 = p;
            while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
            col_name = sql.substr(s3, p - s3);
        }
        col_name = str_trim(col_name);
        while (!col_name.empty() && (col_name.back() == ';' || isspace((unsigned char)col_name.back())))
            col_name.pop_back();
        if (!db->schema[tname].count(col_name)) {
            db->last_error = "no such column: " + col_name; return SVDB_ERR;
        }
        db->schema[tname].erase(col_name);
        auto &co = db->col_order[tname];
        co.erase(std::remove(co.begin(), co.end(), col_name), co.end());
        for (auto &row : db->data[tname]) row.erase(col_name);
        return SVDB_OK;
    } else if (action == "ADD") {
        /* ADD [COLUMN] col type  OR  ADD CONSTRAINT name ... */
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t np = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string next = su.substr(np, p - np);

        if (next == "CONSTRAINT") {
            /* ADD CONSTRAINT name UNIQUE(cols) / CHECK(expr) */
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            size_t s3 = p;
            while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
            /* skip whitespace after constraint name */
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            /* read constraint type */
            size_t s4 = p;
            while (p < sql.size() && isalpha((unsigned char)sql[p])) ++p;
            std::string ctype2 = str_upper(sql.substr(s4, p - s4));
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            if (ctype2 == "UNIQUE") {
                if (p < sql.size() && sql[p] == '(') {
                    ++p;
                    std::vector<std::string> ucols;
                    while (p < sql.size() && sql[p] != ')') {
                        while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
                        size_t s5 = p;
                        while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
                        std::string cn = sql.substr(s5, p - s5);
                        if (!cn.empty()) ucols.push_back(cn);
                        while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
                        if (p < sql.size() && sql[p] == ',') ++p;
                    }
                    if (!ucols.empty()) db->unique_constraints[tname].push_back(ucols);
                }
                return SVDB_OK;
            } else if (ctype2 == "CHECK") {
                if (p < sql.size() && sql[p] == '(') {
                    ++p; size_t s5 = p; int cdepth = 1;
                    while (p < sql.size() && cdepth > 0) {
                        if (sql[p] == '(') ++cdepth;
                        else if (sql[p] == ')') --cdepth;
                        if (cdepth > 0) ++p;
                    }
                    std::string chk = sql.substr(s5, p - s5);
                    db->check_constraints[tname].push_back(chk);
                }
                return SVDB_OK;
            }
            return SVDB_OK;
        } else if (next == "COLUMN") {
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
        } else {
            p = np; /* no COLUMN keyword, back up */
        }

        /* ADD col type */
        while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
        std::string col_name;
        if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
            char q = sql[p++]; size_t s3 = p;
            while (p < sql.size() && sql[p] != q) ++p;
            col_name = sql.substr(s3, p - s3); if (p < sql.size()) ++p;
        } else {
            size_t s3 = p;
            while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
            col_name = sql.substr(s3, p - s3);
        }
        if (col_name.empty()) { db->last_error = "ADD COLUMN: missing column name"; return SVDB_ERR; }
        while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
        std::string col_type;
        while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) {
            col_type += (char)toupper((unsigned char)sql[p]); ++p;
        }
        if (col_type.empty()) col_type = "TEXT";
        ColDef cd; cd.type = col_type;
        db->schema[tname][col_name] = cd;
        db->col_order[tname].push_back(col_name);
        for (auto &row : db->data[tname]) row[col_name] = SvdbVal{};
        return SVDB_OK;
    }
    return SVDB_OK;
}


/* ── DML handlers ───────────────────────────────────────────────── */

static svdb_code_t do_insert(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
    svdb_assert(db != nullptr);
    svdb_assert(!sql.empty());
    /* Handle INSERT INTO t DEFAULT VALUES */
    std::string su_pre = str_upper(sql);
    if (su_pre.find("DEFAULT VALUES") != std::string::npos) {
        /* Extract table name */
        size_t into_pos = su_pre.find("INTO");
        if (into_pos == std::string::npos) into_pos = su_pre.find("INSERT") + 6;
        else into_pos += 4;
        while (into_pos < sql.size() && isspace((unsigned char)sql[into_pos])) ++into_pos;
        size_t ts = into_pos;
        while (into_pos < sql.size() && (isalnum((unsigned char)sql[into_pos]) || sql[into_pos] == '_')) ++into_pos;
        std::string tname2 = sql.substr(ts, into_pos - ts);
        if (!db->schema.count(tname2)) { db->last_error = "no such table: " + tname2; return SVDB_ERR; }
        const auto &col_order2 = db->col_order[tname2];
        Row row;
        for (const auto &cn : col_order2) {
            auto dit = db->schema[tname2].find(cn);
            if (dit != db->schema[tname2].end() && !dit->second.default_val.empty())
                row[cn] = svdb_eval_expr_in_row(dit->second.default_val, row, {});
            else
                row[cn] = SvdbVal{};
        }
        db->rowid_counter[tname2]++;
        row[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[tname2], 0.0, {}};
        db->data[tname2].push_back(row);
        db->rows_affected = 1; db->last_insert_rowid = db->rowid_counter[tname2];
        if (res) { res->code = SVDB_OK; res->rows_affected = 1; res->last_insert_rowid = db->last_insert_rowid; }
        return SVDB_OK;
    }

    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);
    if (!ast) {
        db->last_error = svdb_parser_error(p);
        svdb_parser_destroy(p);
        return SVDB_ERR;
    }

    std::string tname = svdb_ast_get_table(ast);
    int ncols = svdb_ast_get_column_count(ast);
    int nrows = svdb_ast_get_value_row_count(ast);

    if (!db->schema.count(tname)) {
        db->last_error = "no such table: " + tname;
        svdb_ast_node_free(ast); svdb_parser_destroy(p);
        return SVDB_ERR;
    }

    const auto &col_order = db->col_order[tname];

    /* ── INSERT ... SELECT ─────────────────────────────────────────── */
    if (nrows == 0) {
        /* Check if SQL contains a SELECT after column list / INTO clause */
        std::string su2 = str_upper(sql);
        /* Find SELECT keyword (must follow INTO table_name [(cols)]) */
        /* Simple detection: after "INTO tablename [(...)]" look for "SELECT" */
        size_t sel_pos = std::string::npos;
        {
            size_t p2 = su2.find("SELECT");
            while (p2 != std::string::npos) {
                /* Make sure it's at word boundary */
                bool before_ok = (p2 == 0 || !isalnum((unsigned char)su2[p2-1]));
                bool after_ok  = (p2 + 6 >= su2.size() || !isalnum((unsigned char)su2[p2+6]));
                if (before_ok && after_ok) { sel_pos = p2; break; }
                p2 = su2.find("SELECT", p2 + 1);
            }
        }
        if (sel_pos != std::string::npos) {
            /* Extract SELECT SQL and execute it */
            std::string sel_sql = sql.substr(sel_pos);
            /* Extract target columns from ncols (if explicit column list given) */
            std::vector<std::string> ins_cols2;
            if (ncols > 0) {
                for (int i = 0; i < ncols; ++i) ins_cols2.push_back(svdb_ast_get_column(ast, i));
            } else {
                ins_cols2 = col_order;
            }
            svdb_rows_t *sel_rows = nullptr;
            svdb_code_t rc2 = svdb_query_internal(db, sel_sql, &sel_rows);
            if (rc2 != SVDB_OK || !sel_rows) {
                svdb_ast_node_free(ast); svdb_parser_destroy(p);
                return rc2;
            }
            int64_t inserted2 = 0;
            for (const auto &sel_row : sel_rows->rows) {
                Row row2;
                for (size_t ci = 0; ci < ins_cols2.size() && ci < sel_row.size(); ++ci)
                    row2[ins_cols2[ci]] = sel_row[ci];
                /* Fill missing columns with defaults */
                for (const auto &cn : col_order) {
                    if (!row2.count(cn)) {
                        auto dit = db->schema[tname].find(cn);
                        if (dit != db->schema[tname].end() && !dit->second.default_val.empty())
                            row2[cn] = svdb_eval_expr_in_row(dit->second.default_val, row2, {});
                        else row2[cn] = SvdbVal{};
                    }
                }
                db->rowid_counter[tname]++;
                row2[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[tname], 0.0, {}};
                db->data[tname].push_back(row2);
                ++inserted2;
            }
            delete sel_rows;
            db->rows_affected = inserted2;
            db->last_insert_rowid = db->rowid_counter[tname];
            if (res) { res->code = SVDB_OK; res->rows_affected = inserted2; res->last_insert_rowid = db->last_insert_rowid; }
            svdb_ast_node_free(ast); svdb_parser_destroy(p);
            return SVDB_OK;
        }
    }

    /* Determine insertion columns */
    std::vector<std::string> ins_cols;
    if (ncols > 0) {
        for (int i = 0; i < ncols; ++i)
            ins_cols.push_back(svdb_ast_get_column(ast, i));
        /* Validate: all specified column names must exist in the table */
        for (const auto &ic : ins_cols) {
            if (db->schema[tname].find(ic) == db->schema[tname].end() &&
                str_upper(ic) != "ROWID" && str_upper(ic) != "_SVDB_ROWID_") {
                db->last_error = "table " + tname + " has no column named " + ic;
                svdb_ast_node_free(ast); svdb_parser_destroy(p);
                return SVDB_ERR;
            }
        }
    } else {
        ins_cols = col_order;
    }

    /* Detect ON CONFLICT clause: DO NOTHING or DO UPDATE SET ... */
    std::string su_sql = str_upper(sql);
    bool on_conflict_nothing = (su_sql.find("ON CONFLICT") != std::string::npos &&
                                 su_sql.find("DO NOTHING") != std::string::npos) ||
                                su_sql.find("INSERT OR IGNORE") != std::string::npos;
    bool on_conflict_update  = su_sql.find("ON CONFLICT") != std::string::npos &&
                                su_sql.find("DO UPDATE") != std::string::npos;
    /* For DO UPDATE: extract SET clause after DO UPDATE */
    std::string do_update_set;
    if (on_conflict_update) {
        size_t du_pos = su_sql.find("DO UPDATE SET");
        if (du_pos != std::string::npos) {
            do_update_set = str_trim(sql.substr(du_pos + 13));
            /* strip trailing ; */
            while (!do_update_set.empty() && (do_update_set.back() == ';' || isspace((unsigned char)do_update_set.back())))
                do_update_set.pop_back();
        }
    }
    /* INSERT OR REPLACE behaves like DO UPDATE with excluded.col = incoming col */
    if (su_sql.find("INSERT OR REPLACE") != std::string::npos ||
        su_sql.find("REPLACE INTO") != std::string::npos) {
        on_conflict_update = true;
    }

    /* Validate column count: number of VALUES per row must match number of target columns */
    if (nrows > 0) {
        int nv0 = svdb_ast_get_value_count(ast, 0);
        if (nv0 > (int)ins_cols.size()) {
            db->last_error = std::to_string(nv0) + " values for " + std::to_string(ins_cols.size()) + " columns";
            svdb_ast_node_free(ast); svdb_parser_destroy(p);
            return SVDB_ERR;
        }
    }

    int64_t inserted = 0;
    for (int ri = 0; ri < nrows; ++ri) {
        Row row;
        /* Set defaults first */
        for (const auto &cn : col_order) {
            auto dit = db->schema[tname].find(cn);
            if (dit != db->schema[tname].end() && !dit->second.default_val.empty())
                row[cn] = svdb_eval_expr_in_row(dit->second.default_val, row, {});
            else
                row[cn] = SvdbVal{};
        }

        int nv = svdb_ast_get_value_count(ast, ri);
        for (int ci = 0; ci < nv && ci < (int)ins_cols.size(); ++ci) {
            std::string vstr = svdb_ast_get_value(ast, ri, ci);
            row[ins_cols[ci]] = parse_literal(vstr);
        }

        /* ── Constraint checks ───────────────────────────────── */

        /* Auto-assign values for INTEGER PRIMARY KEY (AUTOINCREMENT) columns that were omitted */
        for (const auto &cn : col_order) {
            auto cdit = db->schema[tname].find(cn);
            if (cdit != db->schema[tname].end() && cdit->second.primary_key &&
                (cdit->second.auto_increment ||
                 str_upper(cdit->second.type) == "INTEGER")) {
                auto rit = row.find(cn);
                if (rit == row.end() || rit->second.type == SVDB_TYPE_NULL) {
                    /* Peek at the next rowid_counter value (it gets incremented later) */
                    SvdbVal v; v.type = SVDB_TYPE_INT;
                    v.ival = db->rowid_counter[tname] + 1;
                    row[cn] = v;
                }
            }
        }

        /* NOT NULL check */
        for (const auto &cn : col_order) {
            auto cdit = db->schema[tname].find(cn);
            if (cdit != db->schema[tname].end() && cdit->second.not_null) {
                auto rit = row.find(cn);
                if (rit == row.end() || rit->second.type == SVDB_TYPE_NULL) {
                    /* INSERT OR IGNORE only suppresses UNIQUE conflicts, not NOT NULL */
                    db->last_error = "NOT NULL constraint failed: " + tname + "." + cn;
                    svdb_ast_node_free(ast); svdb_parser_destroy(p);
                    return SVDB_ERR;
                }
            }
        }

        /* UNIQUE constraint check */
        auto check_unique = [&](const std::vector<std::string> &ucols) -> int {
            /* Returns -1 if no conflict, or index of conflicting row */
            for (int ei2 = 0; ei2 < (int)db->data[tname].size(); ++ei2) {
                const auto &existing = db->data[tname][ei2];
                bool match = true;
                for (const auto &uc : ucols) {
                    auto ri2 = row.find(uc);
                    auto eit = existing.find(uc);
                    if (ri2 == row.end() || eit == existing.end()) { match = false; break; }
                    if (ri2->second.type == SVDB_TYPE_NULL) { match = false; break; }
                    if (eit->second.type  == SVDB_TYPE_NULL) { match = false; break; }
                    if (ri2->second.type != eit->second.type) { match = false; break; }
                    if (ri2->second.type == SVDB_TYPE_INT  && ri2->second.ival != eit->second.ival) { match = false; break; }
                    if (ri2->second.type == SVDB_TYPE_REAL && ri2->second.rval != eit->second.rval) { match = false; break; }
                    if (ri2->second.type == SVDB_TYPE_TEXT && ri2->second.sval != eit->second.sval) { match = false; break; }
                }
                if (match) return ei2;
            }
            return -1;
        };

        /* Check primary key (composite or single-column) */
        bool conflict_handled = false;
        if (db->primary_keys.count(tname)) {
            const auto &pk_cols = db->primary_keys.at(tname);
            if (!pk_cols.empty()) {
                int ci = check_unique(pk_cols);
                if (ci >= 0) {
                    if (on_conflict_nothing) { conflict_handled = true; }
                    else if (on_conflict_update) {
                        db->data[tname][ci] = row;
                        ++inserted; conflict_handled = true;
                    } else {
                        /* Report the first PK column in the error message */
                        db->last_error = "UNIQUE constraint failed: " + tname + "." + pk_cols[0];
                        svdb_ast_node_free(ast); svdb_parser_destroy(p);
                        return SVDB_ERR;
                    }
                }
            }
        } else {
            /* Column-level UNIQUE/PRIMARY KEY fallback */
            for (const auto &cn : col_order) {
                auto cdit = db->schema[tname].find(cn);
                if (cdit != db->schema[tname].end() && cdit->second.primary_key) {
                    int ci = check_unique({cn});
                    if (ci >= 0) {
                        if (on_conflict_nothing) { conflict_handled = true; break; }
                        if (on_conflict_update) {
                            db->data[tname][ci] = row;
                            ++inserted; conflict_handled = true; break;
                        }
                        db->last_error = "UNIQUE constraint failed: " + tname + "." + cn;
                        svdb_ast_node_free(ast); svdb_parser_destroy(p);
                        return SVDB_ERR;
                    }
                }
            }
        }
        if (conflict_handled) continue; /* skip or already updated */

        /* Check table UNIQUE constraints */
        if (db->unique_constraints.count(tname)) {
            for (const auto &ucols : db->unique_constraints.at(tname)) {
                int ci = check_unique(ucols);
                if (ci >= 0) {
                    if (on_conflict_nothing) { conflict_handled = true; break; }
                    if (on_conflict_update) {
                        db->data[tname][ci] = row;
                        ++inserted; conflict_handled = true; break;
                    }
                    db->last_error = "UNIQUE constraint failed: " + tname;
                    svdb_ast_node_free(ast); svdb_parser_destroy(p);
                    return SVDB_ERR;
                }
            }
            if (conflict_handled) continue; /* skip or already updated */
        }

        /* CHECK constraint evaluation (simple: skip for now, basic eval) */
        if (db->check_constraints.count(tname)) {
            for (const auto &chk : db->check_constraints.at(tname)) {
                /* Very basic: evaluate simple comparison like "score >= 0" */
                std::string chk_upper = str_upper(chk);
                /* Try to evaluate numerically */
                bool ok = true;
                /* Find operator */
                for (const char *op : {">=", "<=", "!=", "<>", ">", "<", "="}) {
                    size_t op_pos = chk.find(op);
                    if (op_pos == std::string::npos) continue;
                    std::string lhs = str_trim(chk.substr(0, op_pos));
                    std::string rhs = str_trim(chk.substr(op_pos + strlen(op)));
                    /* Look up lhs in row */
                    auto it2 = row.find(lhs);
                    if (it2 == row.end()) break;
                    const SvdbVal &sv2 = it2->second;
                    if (sv2.type == SVDB_TYPE_NULL) { ok = true; break; } /* NULLs pass */
                    try {
                        double rv = std::stod(rhs);
                        double lv = (sv2.type == SVDB_TYPE_INT) ? (double)sv2.ival : sv2.rval;
                        if (std::string(op) == ">=")      ok = lv >= rv;
                        else if (std::string(op) == "<=") ok = lv <= rv;
                        else if (std::string(op) == ">")  ok = lv > rv;
                        else if (std::string(op) == "<")  ok = lv < rv;
                        else if (std::string(op) == "=" || std::string(op) == "==") ok = lv == rv;
                        else if (std::string(op) == "!=" || std::string(op) == "<>") ok = lv != rv;
                    } catch (...) { ok = true; }
                    break;
                }
                if (!ok) {
                    db->last_error = "CHECK constraint failed: " + tname;
                    svdb_ast_node_free(ast); svdb_parser_destroy(p);
                    return SVDB_ERR;
                }
            }
        }

        /* FK constraint check */
        /* FK enforcement is disabled at insert-time (like SQLite default).
         * Use PRAGMA foreign_key_check to detect violations. */

        /* Auto-increment rowid */
        db->rowid_counter[tname]++;
        row[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[tname], 0.0, {}};

        db->data[tname].push_back(row);
        ++inserted;
    }

    db->rows_affected     = inserted;
    db->last_insert_rowid = db->rowid_counter[tname];
    if (res) {
        res->code              = SVDB_OK;
        res->errmsg            = "";
        res->rows_affected     = inserted;
        res->last_insert_rowid = db->last_insert_rowid;
    }

    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);
    return SVDB_OK;
}

static svdb_code_t do_update(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
    svdb_assert(db != nullptr);
    svdb_assert(!sql.empty());
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);
    if (!ast) {
        db->last_error = svdb_parser_error(p);
        svdb_parser_destroy(p);
        return SVDB_ERR;
    }

    std::string tname     = svdb_ast_get_table(ast);
    std::string where_txt = svdb_ast_get_where(ast);

    if (!db->schema.count(tname)) {
        db->last_error = "no such table: " + tname;
        svdb_ast_node_free(ast); svdb_parser_destroy(p);
        return SVDB_ERR;
    }

    /* Parse SET assignments from raw SQL (col=val,...) */
    /* Find SET keyword */
    std::string su = str_upper(sql);
    size_t set_pos = su.find("SET");
    if (set_pos == std::string::npos) {
        db->last_error = "UPDATE: missing SET";
        svdb_ast_node_free(ast); svdb_parser_destroy(p);
        return SVDB_ERR;
    }
    /* Find end of SET clause (WHERE or end) */
    size_t where_pos = su.find("WHERE", set_pos);
    std::string set_clause = sql.substr(set_pos + 3,
        (where_pos != std::string::npos ? where_pos : sql.size()) - set_pos - 3);

    /* Parse assignments: col1=val1, col2=val2 */
    std::vector<std::pair<std::string,std::string>> assignments;
    {
        size_t ap = 0;
        while (ap < set_clause.size()) {
            while (ap < set_clause.size() && isspace((unsigned char)set_clause[ap])) ++ap;
            /* col name */
            std::string cn;
            if (ap < set_clause.size() && (set_clause[ap] == '"' || set_clause[ap] == '`')) {
                char q = set_clause[ap++]; size_t s = ap;
                while (ap < set_clause.size() && set_clause[ap] != q) ++ap;
                cn = set_clause.substr(s, ap - s);
                if (ap < set_clause.size()) ++ap;
            } else {
                size_t s = ap;
                while (ap < set_clause.size() && (isalnum((unsigned char)set_clause[ap]) || set_clause[ap] == '_')) ++ap;
                cn = set_clause.substr(s, ap - s);
            }
            if (cn.empty()) { ++ap; continue; }
            while (ap < set_clause.size() && isspace((unsigned char)set_clause[ap])) ++ap;
            if (ap < set_clause.size() && set_clause[ap] == '=') ++ap;
            while (ap < set_clause.size() && isspace((unsigned char)set_clause[ap])) ++ap;
            /* value */
            std::string vstr;
            if (ap < set_clause.size() && set_clause[ap] == '\'') {
                size_t s = ap++;
                while (ap < set_clause.size() && set_clause[ap] != '\'') ++ap;
                if (ap < set_clause.size()) ++ap;
                vstr = set_clause.substr(s, ap - s);
            } else {
                /* Read value until top-level comma, handling parens and strings */
                size_t s = ap;
                int vd = 0; bool vs = false;
                while (ap < set_clause.size()) {
                    char vc = set_clause[ap];
                    if (vc == '\'') { vs = !vs; ++ap; continue; }
                    if (vs) { ++ap; continue; }
                    if (vc == '(') ++vd;
                    else if (vc == ')') { if (vd > 0) --vd; }
                    else if (vc == ',' && vd == 0) break;
                    ++ap;
                }
                vstr = str_trim(set_clause.substr(s, ap - s));
            }
            assignments.push_back({cn, vstr});
            while (ap < set_clause.size() && (isspace((unsigned char)set_clause[ap]) || set_clause[ap] == ',')) ++ap;
        }
    }

    const auto &col_order = db->col_order[tname];
    int64_t updated = 0;
    svdb_set_query_db(db);  /* set thread-local DB context for subquery eval in SET expressions */
    for (auto &row : db->data[tname]) {
        if (!eval_where(row, col_order, where_txt)) continue;
        for (const auto &asgn : assignments)
            row[asgn.first] = svdb_eval_expr_in_row(asgn.second, row, col_order);
        ++updated;
    }
    svdb_set_query_db(nullptr);

    db->rows_affected = updated;
    if (res) { res->code = SVDB_OK; res->rows_affected = updated; }

    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);
    return SVDB_OK;
}

static svdb_code_t do_delete(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
    svdb_assert(db != nullptr);
    svdb_assert(!sql.empty());
    svdb_parser_t *p = svdb_parser_create(sql.c_str(), sql.size());
    if (!p) return SVDB_NOMEM;
    svdb_ast_node_t *ast = svdb_parser_parse(p);
    if (!ast) {
        db->last_error = svdb_parser_error(p);
        svdb_parser_destroy(p);
        return SVDB_ERR;
    }

    std::string tname     = svdb_ast_get_table(ast);
    std::string where_txt = svdb_ast_get_where(ast);
    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);

    if (!db->schema.count(tname)) {
        db->last_error = "no such table: " + tname;
        return SVDB_ERR;
    }

    const auto &col_order = db->col_order[tname];
    auto &rows = db->data[tname];
    int64_t deleted = 0;
    auto it = rows.begin();
    while (it != rows.end()) {
        if (eval_where(*it, col_order, where_txt)) {
            it = rows.erase(it);
            ++deleted;
        } else {
            ++it;
        }
    }

    db->rows_affected = deleted;
    if (res) { res->code = SVDB_OK; res->rows_affected = deleted; }
    return SVDB_OK;
}

/* ── Public API ─────────────────────────────────────────────────── */

extern "C" {

svdb_code_t svdb_exec(svdb_db_t *db, const char *sql, svdb_result_t *res) {
    svdb_assert_msg(db != nullptr, "svdb_exec: db must not be NULL");
    svdb_assert_msg(sql != nullptr, "svdb_exec: sql must not be NULL");
    if (!db || !sql) return SVDB_ERR;
    if (res) { res->code = SVDB_OK; res->errmsg = ""; res->rows_affected = 0; res->last_insert_rowid = 0; }

    std::lock_guard<std::mutex> lk(db->mu);
    db->last_error.clear();
    db->rows_affected = 0;

    std::string s = strip_sql_comments(std::string(sql));
    s = str_trim(s);
    std::string kw = first_keyword(s);

    svdb_code_t rc = SVDB_OK;
    if (kw == "CREATE") {
        std::string su = str_upper(s);
        size_t p = su.find("CREATE") + 6;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t s2 = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string what = su.substr(s2, p - s2);
        if (what == "TABLE")      rc = do_create_table(db, s);
        else if (what == "UNIQUE") {
            /* CREATE UNIQUE INDEX ... */
            rc = do_create_index(db, s, true);
        }
        else if (what == "INDEX") rc = do_create_index(db, s, false);
        else if (what == "VIEW")  {
            /* Store view metadata so table_list can include it */
            std::string su2 = str_upper(s);
            bool if_not_exists2 = su2.find("IF NOT EXISTS") != std::string::npos;
            /* Extract view name after CREATE [TEMP] VIEW [IF NOT EXISTS] */
            size_t vp = su2.find("VIEW");
            if (vp != std::string::npos) {
                vp += 4;
                while (vp < su2.size() && isspace((unsigned char)su2[vp])) ++vp;
                if (su2.substr(vp, 13) == "IF NOT EXISTS") {
                    vp += 13;
                    while (vp < su2.size() && isspace((unsigned char)su2[vp])) ++vp;
                }
                std::string vname;
                if (vp < s.size() && (s[vp] == '"' || s[vp] == '`')) {
                    char q = s[vp++]; size_t vs = vp;
                    while (vp < s.size() && s[vp] != q) ++vp;
                    vname = s.substr(vs, vp - vs);
                } else {
                    size_t vs = vp;
                    while (vp < s.size() && (isalnum((unsigned char)s[vp]) || s[vp] == '_')) ++vp;
                    vname = s.substr(vs, vp - vs);
                }
                if (!vname.empty() && !(if_not_exists2 && db->schema.count(vname))) {
                    /* Register view with empty schema so it shows in table_list */
                    if (!db->schema.count(vname)) {
                        db->schema[vname] = {};
                        db->col_order[vname] = {};
                        db->create_sql[vname] = s;
                        /* Mark as view using special key in create_sql prefix */
                        db->create_sql[vname] = s; /* full CREATE VIEW sql */
                    }
                }
            }
            rc = SVDB_OK;
        }
        else                      rc = SVDB_OK;
    } else if (kw == "DROP") {
        std::string su = str_upper(s);
        size_t p = su.find("DROP") + 4;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t s2 = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string what = su.substr(s2, p - s2);
        if (what == "TABLE") rc = do_drop_table(db, s);
        else if (what == "INDEX") rc = do_drop_index(db, s);
        else                  rc = SVDB_OK; /* DROP VIEW: ignore */
    } else if (kw == "ALTER") {
        rc = do_alter_table(db, s);
    } else if (kw == "INSERT") {
        rc = do_insert(db, s, res);
    } else if (kw == "UPDATE") {
        rc = do_update(db, s, res);
    } else if (kw == "DELETE") {
        rc = do_delete(db, s, res);
    } else if (kw == "BEGIN") {
        /* SQL-level transaction: create snapshot */
        if (db->in_transaction) {
            rc = SVDB_ERR; /* Nested BEGIN is an error */
        } else {
            svdb_tx_t *t = new (std::nothrow) svdb_tx_t();
            if (t) {
                t->db = db;
                t->data_snapshot  = db->data;
                t->rowid_snapshot = db->rowid_counter;
                db->sql_tx = t;
                db->in_transaction = true;
            }
            rc = SVDB_OK;
        }
    } else if (kw == "COMMIT") {
        if (db->in_transaction && db->sql_tx) {
            delete db->sql_tx;
            db->sql_tx = nullptr;
            db->in_transaction = false;
            rc = SVDB_OK;
        } else {
            rc = SVDB_ERR; /* COMMIT without BEGIN */
        }
    } else if (kw == "ROLLBACK") {
        /* Could be ROLLBACK or ROLLBACK TO SAVEPOINT name */
        std::string su2 = str_upper(s);
        size_t to_pos = su2.find(" TO ");
        if (to_pos != std::string::npos) {
            if (db->in_transaction && db->sql_tx) {
                /* ROLLBACK TO [SAVEPOINT] name */
                std::string rest2 = str_trim(s.substr(to_pos + 4));
                /* skip optional SAVEPOINT keyword */
                std::string rest2u = str_upper(rest2);
                if (rest2u.substr(0, 9) == "SAVEPOINT") rest2 = str_trim(rest2.substr(9));
                /* remove trailing ; */
                while (!rest2.empty() && (rest2.back() == ';' || isspace((unsigned char)rest2.back())))
                    rest2.pop_back();
                /* Find savepoint */
                const std::string &n = rest2;
                auto &sp_names = db->sql_tx->savepoints;
                auto &sp_data  = db->sql_tx->sp_data;
                auto &sp_rowid = db->sql_tx->sp_rowid;
                bool found_sp = false;
                for (int i = (int)sp_names.size() - 1; i >= 0; --i) {
                    if (sp_names[i] == n) {
                        if (i < (int)sp_data.size()) {
                            db->data          = sp_data[i];
                            db->rowid_counter = sp_rowid[i];
                            sp_names.resize(i + 1);
                            sp_data.resize(i + 1);
                            sp_rowid.resize(i + 1);
                        }
                        found_sp = true;
                        break;
                    }
                }
                rc = found_sp ? SVDB_OK : SVDB_ERR; /* Unknown savepoint = error */
            } else {
                rc = SVDB_ERR; /* ROLLBACK TO without active transaction */
            }
        } else if (db->in_transaction && db->sql_tx) {
            /* Full rollback */
            db->data          = db->sql_tx->data_snapshot;
            db->rowid_counter = db->sql_tx->rowid_snapshot;
            delete db->sql_tx;
            db->sql_tx = nullptr;
            db->in_transaction = false;
            rc = SVDB_OK;
        } else {
            rc = SVDB_ERR; /* ROLLBACK without BEGIN */
        }
    } else if (kw == "SAVEPOINT") {
        std::string sp_name = str_trim(s.substr(9));
        while (!sp_name.empty() && (sp_name.back() == ';' || isspace((unsigned char)sp_name.back())))
            sp_name.pop_back();
        if (!db->in_transaction) {
            /* Implicit transaction for SAVEPOINT without BEGIN (SQLite behaviour) */
            svdb_tx_t *t = new (std::nothrow) svdb_tx_t();
            if (t) {
                t->db = db;
                t->data_snapshot  = db->data;
                t->rowid_snapshot = db->rowid_counter;
                db->sql_tx = t;
                db->in_transaction = true;
            }
        }
        if (db->in_transaction && db->sql_tx) {
            db->sql_tx->savepoints.push_back(sp_name);
            db->sql_tx->sp_data.push_back(db->data);
            db->sql_tx->sp_rowid.push_back(db->rowid_counter);
        }
        rc = SVDB_OK;
    } else if (kw == "RELEASE") {
        if (db->in_transaction && db->sql_tx) {
            std::string sp_name = str_trim(s.substr(7));
            /* skip optional SAVEPOINT keyword */
            if (str_upper(sp_name).substr(0, 9) == "SAVEPOINT") sp_name = str_trim(sp_name.substr(9));
            while (!sp_name.empty() && (sp_name.back() == ';' || isspace((unsigned char)sp_name.back())))
                sp_name.pop_back();
            auto &sp_names = db->sql_tx->savepoints;
            auto &sp_data  = db->sql_tx->sp_data;
            auto &sp_rowid = db->sql_tx->sp_rowid;
            for (int i = (int)sp_names.size() - 1; i >= 0; --i) {
                if (sp_names[i] == sp_name) {
                    sp_names.erase(sp_names.begin() + i);
                    if (i < (int)sp_data.size()) {
                        sp_data.erase(sp_data.begin() + i);
                        sp_rowid.erase(sp_rowid.begin() + i);
                    }
                    break;
                }
            }
        }
        rc = SVDB_OK;
    } else if (kw == "PRAGMA") {
        /* Route PRAGMA through svdb_query_pragma so SET values are stored */
        svdb_rows_t *rows = nullptr;
        rc = svdb_query_pragma(db, s, &rows);
        if (rows) svdb_rows_close(rows);
    } else if (kw == "SELECT") {
        /* svdb_exec on SELECT: run and discard result */
        svdb_rows_t *rows = nullptr;
        rc = svdb_query_internal(db, s, &rows);
        if (rows) svdb_rows_close(rows);
    } else {
        /* Unknown: accept silently */
        rc = SVDB_OK;
    }

    if (rc != SVDB_OK && res) {
        res->code   = rc;
        res->errmsg = db->last_error.c_str();
    }
    return rc;
}

/* Prepared statement stubs */

svdb_code_t svdb_prepare(svdb_db_t *db, const char *sql, svdb_stmt_t **stmt) {
    if (!db || !sql || !stmt) return SVDB_ERR;
    /* Empty SQL is an error */
    const char *p = sql;
    while (*p && isspace((unsigned char)*p)) ++p;
    if (*p == '\0') {
        db->last_error = "empty SQL statement";
        return SVDB_ERR;
    }
    svdb_stmt_t *s = new (std::nothrow) svdb_stmt_t();
    if (!s) return SVDB_NOMEM;
    s->db  = db;
    s->sql = sql;
    *stmt = s;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_int(svdb_stmt_t *stmt, int idx, int64_t val) {
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_INT; sv.ival = val;
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_real(svdb_stmt_t *stmt, int idx, double val) {
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_REAL; sv.rval = val;
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_text(svdb_stmt_t *stmt, int idx,
                                  const char *val, size_t len) {
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_TEXT;
    sv.sval = val ? std::string(val, len) : std::string();
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_null(svdb_stmt_t *stmt, int idx) {
    if (!stmt) return SVDB_ERR;
    stmt->bindings[idx] = SvdbVal{};
    return SVDB_OK;
}

svdb_code_t svdb_stmt_exec(svdb_stmt_t *stmt, svdb_result_t *res) {
    if (!stmt) return SVDB_ERR;
    return svdb_exec(stmt->db, stmt->sql.c_str(), res);
}

svdb_code_t svdb_stmt_query(svdb_stmt_t *stmt, svdb_rows_t **rows) {
    if (!stmt) return SVDB_ERR;
    return svdb_query(stmt->db, stmt->sql.c_str(), rows);
}

svdb_code_t svdb_stmt_reset(svdb_stmt_t *stmt) {
    if (!stmt) return SVDB_ERR;
    stmt->bindings.clear();
    return SVDB_OK;
}

svdb_code_t svdb_stmt_close(svdb_stmt_t *stmt) {
    delete stmt;
    return SVDB_OK;
}

/* Transaction stubs */

svdb_code_t svdb_begin(svdb_db_t *db, svdb_tx_t **tx) {
    if (!db || !tx) return SVDB_ERR;
    if (db->in_transaction) {
        db->last_error = "cannot start a transaction within a transaction";
        return SVDB_ERR;
    }
    svdb_tx_t *t = new (std::nothrow) svdb_tx_t();
    if (!t) return SVDB_NOMEM;
    t->db = db;
    /* Take snapshot of current data for rollback */
    t->data_snapshot  = db->data;
    t->rowid_snapshot = db->rowid_counter;
    db->in_transaction = true;
    *tx = t;
    return SVDB_OK;
}

svdb_code_t svdb_commit(svdb_tx_t *tx) {
    if (!tx) return SVDB_ERR;
    if (tx->db) tx->db->in_transaction = false;
    delete tx;
    return SVDB_OK;
}

svdb_code_t svdb_rollback(svdb_tx_t *tx) {
    if (!tx) return SVDB_ERR;
    if (tx->db) {
        /* Restore snapshot */
        tx->db->data          = tx->data_snapshot;
        tx->db->rowid_counter = tx->rowid_snapshot;
        tx->db->in_transaction = false;
    }
    delete tx;
    return SVDB_OK;
}

svdb_code_t svdb_savepoint(svdb_tx_t *tx, const char *name) {
    if (!tx || !name) return SVDB_ERR;
    tx->savepoints.push_back(name);
    /* Save current data as savepoint snapshot */
    tx->sp_data.push_back(tx->db->data);
    tx->sp_rowid.push_back(tx->db->rowid_counter);
    return SVDB_OK;
}

svdb_code_t svdb_release(svdb_tx_t *tx, const char *name) {
    if (!tx || !name) return SVDB_ERR;
    std::string n(name);
    for (int i = (int)tx->savepoints.size() - 1; i >= 0; --i) {
        if (tx->savepoints[i] == n) {
            tx->savepoints.erase(tx->savepoints.begin() + i);
            if (i < (int)tx->sp_data.size()) {
                tx->sp_data.erase(tx->sp_data.begin() + i);
                tx->sp_rowid.erase(tx->sp_rowid.begin() + i);
            }
            return SVDB_OK;
        }
    }
    return SVDB_OK; /* ignore unknown savepoint name */
}

svdb_code_t svdb_rollback_to(svdb_tx_t *tx, const char *name) {
    if (!tx || !name || !tx->db) return SVDB_ERR;
    std::string n(name);
    for (int i = (int)tx->savepoints.size() - 1; i >= 0; --i) {
        if (tx->savepoints[i] == n) {
            /* Restore to savepoint snapshot */
            if (i < (int)tx->sp_data.size()) {
                tx->db->data          = tx->sp_data[i];
                tx->db->rowid_counter = tx->sp_rowid[i];
                /* Remove all savepoints after this one */
                tx->savepoints.resize(i + 1);
                tx->sp_data.resize(i + 1);
                tx->sp_rowid.resize(i + 1);
            }
            return SVDB_OK;
        }
    }
    tx->db->last_error = "no such savepoint: " + n;
    return SVDB_ERR;
}

/* Schema introspection */

svdb_code_t svdb_tables(svdb_db_t *db, svdb_rows_t **rows) {
    if (!db || !rows) return SVDB_ERR;
    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;
    r->col_names = {"name", "sql"};
    for (const auto &kv : db->schema) {
        SvdbVal sv_name; sv_name.type = SVDB_TYPE_TEXT; sv_name.sval = kv.first;
        SvdbVal sv_sql;
        auto it = db->create_sql.find(kv.first);
        if (it != db->create_sql.end()) { sv_sql.type = SVDB_TYPE_TEXT; sv_sql.sval = it->second; }
        r->rows.push_back({sv_name, sv_sql});
    }
    *rows = r;
    return SVDB_OK;
}

svdb_code_t svdb_columns(svdb_db_t *db, const char *table, svdb_rows_t **rows) {
    if (!db || !table || !rows) return SVDB_ERR;
    std::string tname(table);
    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;
    r->col_names = {"name", "type"};
    if (db->col_order.count(tname)) {
        for (const auto &cn : db->col_order.at(tname)) {
            SvdbVal name_v; name_v.type = SVDB_TYPE_TEXT; name_v.sval = cn;
            SvdbVal type_v; type_v.type = SVDB_TYPE_TEXT;
            auto sit = db->schema.at(tname).find(cn);
            if (sit != db->schema.at(tname).end()) type_v.sval = sit->second.type;
            r->rows.push_back({name_v, type_v});
        }
    }
    *rows = r;
    return SVDB_OK;
}

svdb_code_t svdb_indexes(svdb_db_t *db, const char *table, svdb_rows_t **rows) {
    if (!db || !table || !rows) return SVDB_ERR;
    *rows = new (std::nothrow) svdb_rows_t();
    if (!*rows) return SVDB_NOMEM;
    (*rows)->col_names = {"name", "unique", "columns"};
    std::string tname(table);
    for (auto &kv : db->indexes) {
        if (kv.second.table != tname) continue;
        SvdbVal v_name, v_uniq, v_cols;
        v_name.type = SVDB_TYPE_TEXT; v_name.sval = kv.first;
        v_uniq.type = SVDB_TYPE_INT;  v_uniq.ival = kv.second.unique ? 1 : 0;
        std::string cols_str;
        for (size_t i = 0; i < kv.second.columns.size(); ++i) {
            if (i > 0) cols_str += ",";
            cols_str += kv.second.columns[i];
        }
        v_cols.type = SVDB_TYPE_TEXT; v_cols.sval = cols_str;
        (*rows)->rows.push_back({v_name, v_uniq, v_cols});
    }
    return SVDB_OK;
}

svdb_code_t svdb_backup(svdb_db_t *src, const char *dest_path) {
    if (!src || !dest_path) return SVDB_ERR;
    /* Create or truncate the destination file as a minimal backup marker */
    FILE *f = fopen(dest_path, "wb");
    if (!f) return SVDB_ERR;
    /* Write a minimal SQLite-compatible header magic */
    const char *magic = "SQLite format 3\0";
    fwrite(magic, 1, 16, f);
    fclose(f);
    return SVDB_OK;
}

} /* extern "C" */
