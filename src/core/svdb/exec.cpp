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

/* ── Helper: case-insensitive table lookup ───────────────────────────────── */

/* Resolve table name case-insensitively (for unquoted identifiers).
 * Returns the canonical table name from schema, or empty string if not found.
 * Supports schema prefixes: temp., temporary., main. */
static std::string resolve_table_name(svdb_db_t *db, const std::string &tname) {
    /* Strip schema prefix (temp., temporary., main.) */
    std::string actual_name = tname;
    if (tname.size() > 5) {
        std::string prefix = svdb_str_upper(tname.substr(0, 5));
        if (prefix == "TEMP.") {
            actual_name = tname.substr(5);
        }
    }
    if (actual_name == tname && tname.size() > 10) {
        std::string prefix = svdb_str_upper(tname.substr(0, 10));
        if (prefix == "TEMPORARY.") {
            actual_name = tname.substr(10);
        }
    }
    if (actual_name == tname && tname.size() > 5) {
        std::string prefix = svdb_str_upper(tname.substr(0, 5));
        if (prefix == "MAIN.") {
            actual_name = tname.substr(5);
        }
    }
    
    /* Exact match first */
    if (db->schema.count(actual_name)) return actual_name;

    /* For unquoted identifiers, do case-insensitive lookup */
    if (!is_quoted_identifier(actual_name)) {
        std::string actual_upper = svdb_str_upper(actual_name);
        for (auto &kv : db->schema) {
            if (svdb_str_upper(kv.first) == actual_upper) {
                return kv.first;
            }
        }
    }
    return "";
}

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
 * Fills schema ColDef and col_order for the table.
 * Returns number of column-level PRIMARY KEY declarations (for validation). */
static int parse_column_defs(const std::string &sql, size_t pos,
                               TableDef &out_td,
                               std::vector<std::string> &out_order,
                               std::vector<std::string> *out_pk = nullptr,
                               std::vector<std::vector<std::string>> *out_uniq = nullptr,
                               CheckList *out_checks = nullptr,
                               std::vector<FKDef> *out_fks = nullptr) {
    int column_pk_count = 0;  /* Count column-level PRIMARY KEY declarations */
    /* Find the opening '(' */
    while (pos < sql.size() && sql[pos] != '(') ++pos;
    if (pos >= sql.size()) return 0;
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
                /* Parse optional ON DELETE / ON UPDATE action(s) */
                auto parse_fk_action = [&](const std::string &clause, std::string &target) {
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    size_t peek = pos;
                    while (peek < sql.size() && isalpha((unsigned char)sql[peek])) ++peek;
                    std::string action = str_upper(sql.substr(pos, peek - pos));
                    pos = peek;
                    if (action == "SET" || action == "NO") {
                        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                        peek = pos;
                        while (peek < sql.size() && isalpha((unsigned char)sql[peek])) ++peek;
                        std::string action2 = str_upper(sql.substr(pos, peek - pos));
                        pos = peek;
                        action = action + " " + action2;
                    }
                    target = action;
                };
                while (true) {
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    size_t peek = pos;
                    while (peek < sql.size() && isalpha((unsigned char)sql[peek])) ++peek;
                    std::string w1 = str_upper(sql.substr(pos, peek - pos));
                    if (w1 != "ON") break;
                    pos = peek;
                    while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                    peek = pos;
                    while (peek < sql.size() && isalpha((unsigned char)sql[peek])) ++peek;
                    std::string w2 = str_upper(sql.substr(pos, peek - pos));
                    pos = peek;
                    if (w2 == "DELETE") parse_fk_action(w2, fk.on_delete);
                    else if (w2 == "UPDATE") parse_fk_action(w2, fk.on_update);
                    else break;
                }
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
                column_pk_count++;  /* Track column-level PRIMARY KEY declarations */
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
                    /* Parse optional ON DELETE / ON UPDATE action(s) */
                    while (true) {
                        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                        size_t peek2 = pos;
                        while (peek2 < sql.size() && isalpha((unsigned char)sql[peek2])) ++peek2;
                        std::string kw2 = str_upper(sql.substr(pos, peek2 - pos));
                        if (kw2 != "ON") break;
                        pos = peek2;
                        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                        peek2 = pos;
                        while (peek2 < sql.size() && isalpha((unsigned char)sql[peek2])) ++peek2;
                        std::string evt = str_upper(sql.substr(pos, peek2 - pos));
                        pos = peek2;
                        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                        peek2 = pos;
                        while (peek2 < sql.size() && isalpha((unsigned char)sql[peek2])) ++peek2;
                        std::string act = str_upper(sql.substr(pos, peek2 - pos));
                        pos = peek2;
                        if (act == "SET" || act == "NO") {
                            while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                            peek2 = pos;
                            while (peek2 < sql.size() && isalpha((unsigned char)sql[peek2])) ++peek2;
                            std::string act2 = str_upper(sql.substr(pos, peek2 - pos));
                            pos = peek2;
                            act = act + " " + act2;
                        }
                        if (evt == "DELETE") fk.on_delete = act;
                        else if (evt == "UPDATE") fk.on_update = act;
                        else break;
                    }
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

        /* Check for duplicate column name */
        if (out_td.find(col_name) != out_td.end()) {
            return -1;  /* Signal duplicate column error */
        }

        out_td[col_name] = cd;
        out_order.push_back(col_name);
    }

    return column_pk_count;
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

    /* Look up LHS column in row */
    auto it = row.find(col);
    if (it == row.end()) {
        /* Case-insensitive fallback */
        std::string col_up = str_upper(col);
        for (auto &kv : row) {
            if (str_upper(kv.first) == col_up) { it = row.find(kv.first); break; }
        }
    }
    if (it == row.end()) return false;

    const SvdbVal &sv = it->second;

    /* Check if val_str is a column reference (not a quoted string, not a number).
     * A column reference looks like an identifier (letters/digits/_) with optional
     * table prefix (table.col). When found in the row, compare column-to-column. */
    if (!is_text) {
        /* Try to look val_str up as a column (qualified or unqualified) */
        auto vit = row.find(val_str);
        if (vit == row.end()) {
            /* Try stripping table qualifier */
            auto vdot = val_str.find('.');
            std::string val_col = (vdot != std::string::npos) ? val_str.substr(vdot + 1) : val_str;
            std::string val_col_up = str_upper(val_col);
            for (auto &kv : row) {
                std::string k = kv.first;
                auto kdot = k.find('.');
                if (kdot != std::string::npos) k = k.substr(kdot + 1);
                if (str_upper(k) == val_col_up) { vit = row.find(kv.first); break; }
            }
        }
        /* Only treat as column ref if it looks like an identifier (letters/digits/underscore, not a number) */
        bool looks_like_ident = !val_str.empty() &&
            (isalpha((unsigned char)val_str[0]) || val_str[0] == '_');
        if (looks_like_ident && vit != row.end()) {
            /* Column-to-column comparison */
            const SvdbVal &sv2 = vit->second;
            if (sv.type == SVDB_TYPE_NULL || sv2.type == SVDB_TYPE_NULL) return false;
            double lv = (sv.type == SVDB_TYPE_INT) ? (double)sv.ival : sv.rval;
            double rv = (sv2.type == SVDB_TYPE_INT) ? (double)sv2.ival : sv2.rval;
            if (sv.type == SVDB_TYPE_TEXT || sv2.type == SVDB_TYPE_TEXT) {
                std::string ls = (sv.type == SVDB_TYPE_TEXT) ? sv.sval : std::to_string(sv.ival);
                std::string rs = (sv2.type == SVDB_TYPE_TEXT) ? sv2.sval : std::to_string(sv2.ival);
                if (op == "="  || op == "==") return ls == rs;
                if (op == "!=" || op == "<>") return ls != rs;
                if (op == "<")                return ls < rs;
                if (op == ">")                return ls > rs;
                if (op == "<=")               return ls <= rs;
                if (op == ">=")               return ls >= rs;
            } else {
                if (op == "="  || op == "==") return lv == rv;
                if (op == "!=" || op == "<>") return lv != rv;
                if (op == "<")                return lv < rv;
                if (op == ">")                return lv > rv;
                if (op == "<=")               return lv <= rv;
                if (op == ">=")               return lv >= rv;
            }
            return true;
        }
    }

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

/* Evaluate a boolean CHECK constraint expression against a row.
 * Handles AND/OR, comparison operators, and arithmetic via eval_expr_exec. */
static bool eval_check_constraint(const std::string &expr, const Row &row,
                                   const std::vector<std::string> &col_order) {
    std::string e = str_trim(expr);
    if (e.empty()) return true;

    /* Strip balanced outer parens */
    if (e.size() >= 2 && e.front() == '(' && e.back() == ')') {
        int d = 0; bool matched = true;
        for (size_t i = 0; i < e.size(); ++i) {
            if (e[i] == '(') ++d;
            else if (e[i] == ')') { if (--d == 0 && i + 1 != e.size()) { matched = false; break; } }
        }
        if (matched) return eval_check_constraint(e.substr(1, e.size()-2), row, col_order);
    }

    /* Handle top-level AND */
    {
        std::string eu = str_upper(e);
        int depth = 0;
        for (size_t i = 0; i < eu.size(); ++i) {
            if (eu[i] == '(') { ++depth; continue; }
            if (eu[i] == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && i + 4 <= eu.size() && eu.substr(i, 4) == " AND") {
                size_t after = i + 4;
                if (after < eu.size() && (isalnum((unsigned char)eu[after]) || eu[after] == '_'))
                    continue; /* not a word boundary — e.g. " ANDROID" */
                return eval_check_constraint(e.substr(0, i), row, col_order) &&
                       eval_check_constraint(e.substr(i + 4), row, col_order);
            }
        }
    }

    /* Handle top-level OR */
    {
        std::string eu = str_upper(e);
        int depth = 0;
        for (size_t i = 0; i < eu.size(); ++i) {
            if (eu[i] == '(') { ++depth; continue; }
            if (eu[i] == ')') { if (depth > 0) --depth; continue; }
            if (depth == 0 && i + 3 <= eu.size() && eu.substr(i, 3) == " OR") {
                size_t after = i + 3;
                if (after < eu.size() && (isalnum((unsigned char)eu[after]) || eu[after] == '_'))
                    continue; /* not a word boundary — e.g. " ORDER" */
                return eval_check_constraint(e.substr(0, i), row, col_order) ||
                       eval_check_constraint(e.substr(i + 3), row, col_order);
            }
        }
    }

    /* Find first top-level comparison operator (check 2-char ops before 1-char) */
    {
        int depth = 0; bool in_str = false;
        for (size_t i = 0; i < e.size(); ++i) {
            char c = e[i];
            if (c == '\'') { in_str = !in_str; continue; }
            if (in_str) continue;
            if (c == '(') { ++depth; continue; }
            if (c == ')') { if (depth > 0) --depth; continue; }
            if (depth > 0) continue;
            /* 2-char operators */
            if (i + 1 < e.size()) {
                std::string two = e.substr(i, 2);
                if (two == ">=" || two == "<=" || two == "!=" || two == "<>") {
                    SvdbVal lv = eval_expr_exec(str_trim(e.substr(0, i)), row, col_order);
                    SvdbVal rv = eval_expr_exec(str_trim(e.substr(i + 2)), row, col_order);
                    if (lv.type == SVDB_TYPE_NULL || rv.type == SVDB_TYPE_NULL) return true;
                    if (lv.type == SVDB_TYPE_TEXT || rv.type == SVDB_TYPE_TEXT) {
                        std::string ls = val_to_str_exec(lv), rs = val_to_str_exec(rv);
                        if (two == ">=") return ls >= rs;
                        if (two == "<=") return ls <= rs;
                        return ls != rs; /* != or <> */
                    }
                    double l = (lv.type == SVDB_TYPE_INT) ? (double)lv.ival : lv.rval;
                    double r = (rv.type == SVDB_TYPE_INT) ? (double)rv.ival : rv.rval;
                    if (two == ">=") return l >= r;
                    if (two == "<=") return l <= r;
                    return l != r; /* != or <> */
                }
            }
            /* 1-char operators (only if not part of a 2-char op) */
            if (c == '>' || c == '<' || c == '=') {
                if (i + 1 < e.size() && (e[i+1] == '=' || e[i+1] == '>')) continue;
                SvdbVal lv = eval_expr_exec(str_trim(e.substr(0, i)), row, col_order);
                SvdbVal rv = eval_expr_exec(str_trim(e.substr(i + 1)), row, col_order);
                if (lv.type == SVDB_TYPE_NULL || rv.type == SVDB_TYPE_NULL) return true;
                if (lv.type == SVDB_TYPE_TEXT || rv.type == SVDB_TYPE_TEXT) {
                    std::string ls = val_to_str_exec(lv), rs = val_to_str_exec(rv);
                    if (c == '>') return ls > rs;
                    if (c == '<') return ls < rs;
                    return ls == rs; /* = */
                }
                double l = (lv.type == SVDB_TYPE_INT) ? (double)lv.ival : lv.rval;
                double r = (rv.type == SVDB_TYPE_INT) ? (double)rv.ival : rv.rval;
                if (c == '>') return l > r;
                if (c == '<') return l < r;
                return l == r; /* = */
            }
        }
    }
    return true; /* unparseable expression — allow by default */
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
    
    /* Check for existing table (case-insensitive for unquoted identifiers) */
    bool table_exists = contains_table_case_insensitive(db->schema, tname);
    if (table_exists) {
        if (if_not_exists) return SVDB_OK;
        db->last_error = "table " + tname + " already exists";
        return SVDB_ERR;
    }

    /* Check for CREATE TABLE ... AS SELECT syntax */
    size_t as_pos = su.find(" AS ");
    size_t select_pos = su.find("SELECT");
    bool is_ctas = (as_pos != std::string::npos && select_pos != std::string::npos && 
                    select_pos > as_pos && as_pos < 50);
    
    TableDef td;
    std::vector<std::string> order;
    std::vector<std::string> pks;
    std::vector<std::vector<std::string>> uniqs;
    CheckList checks;
    std::vector<FKDef> fks;
    int column_pk_count = 0;
    
    if (!is_ctas) {
        /* Standard CREATE TABLE with column definitions */
        column_pk_count = parse_column_defs(sql, 0, td, order, &pks, &uniqs, &checks, &fks);
        if (column_pk_count == -1) {
            db->last_error = "duplicate column name: " + order.back();
            return SVDB_ERR;
        }
    }
    /* For CREATE TABLE AS SELECT, td and order remain empty - will be populated by executing the SELECT */

    db->schema[tname]    = td;
    db->col_order[tname] = order;

    /* For CTAS, empty column list is OK - will be populated when SELECT is executed */
    if (order.empty() && !is_ctas) {
        db->last_error = "near \")\"" ": syntax error";
        return SVDB_ERR;
    }

    /* Enforce single PRIMARY KEY constraint (SQLite compatibility)
     * Multiple separate PRIMARY KEY column declarations are not allowed
     * e.g., "x INTEGER PRIMARY KEY, y INTEGER PRIMARY KEY" is invalid
     * But composite PK "PRIMARY KEY (a, b)" is valid */
    if (column_pk_count > 1) {
        db->last_error = "table \"" + tname + "\" has more than one primary key";
        return SVDB_ERR;
    }

    db->data[tname]      = {};
    db->rowid_counter[tname] = 0;
    db->create_sql[tname] = sql;
    if (!pks.empty()) db->primary_keys[tname] = pks;
    if (!uniqs.empty()) db->unique_constraints[tname] = uniqs;
    if (!checks.empty()) db->check_constraints[tname] = checks;
    if (!fks.empty()) db->fk_constraints[tname] = fks;
    
    /* For CREATE TABLE AS SELECT, execute the SELECT and populate the table */
    if (is_ctas) {
        std::string select_sql = sql.substr(as_pos + 4);
        select_sql = svdb_str_trim(select_sql);
        svdb_rows_t *rows = nullptr;
        svdb_code_t rc = svdb_query_internal(db, select_sql, &rows);
        if (rc != SVDB_OK || !rows) {
            /* Clean up partial table definition on error */
            db->schema.erase(tname);
            db->col_order.erase(tname);
            db->data.erase(tname);
            db->rowid_counter.erase(tname);
            db->create_sql.erase(tname);
            if (rows) svdb_rows_close(rows);
            return rc;
        }
        
        /* Populate column order from result */
        db->col_order[tname] = rows->col_names;
        for (size_t ci = 0; ci < rows->col_names.size(); ++ci) {
            ColDef col;
            const std::string &cname = rows->col_names[ci];
            /* Try to find declared type from source table schema (SQLite-compatible:
             * expressions/aliases that don't match any source column get empty type) */
            col.type = "";
            for (auto &src_tbl : db->schema) {
                if (src_tbl.first == tname) continue; /* skip the new table itself */
                auto cit = src_tbl.second.find(cname);
                if (cit != src_tbl.second.end() && !cit->second.type.empty()) {
                    std::string ct = str_upper(cit->second.type);
                    /* Normalize INTEGER -> INT to match SQLite CTAS behavior */
                    if (ct == "INTEGER") col.type = "INT";
                    else col.type = cit->second.type;
                    break;
                }
            }
            /* Fall back to value-inferred type only for direct column matches */
            if (col.type.empty()) {
                /* Leave empty for expression columns (matches SQLite behavior) */
            }
            db->schema[tname][cname] = col;
        }
        
        /* Insert all rows - convert from vector<SvdbVal> to Row (map) */
        for (auto &row_vec : rows->rows) {
            Row row_map;
            for (size_t ci = 0; ci < rows->col_names.size() && ci < row_vec.size(); ++ci) {
                row_map[rows->col_names[ci]] = row_vec[ci];
            }
            db->data[tname].push_back(row_map);
        }
        
        svdb_rows_close(rows);
    }
    
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
    /* Check table exists (case-insensitive for unquoted identifiers) */
    std::string resolved_tname = resolve_table_name(db, tname);
    if (resolved_tname.empty()) { db->last_error = "no such table: " + tname; return SVDB_ERR; }
    /* Check duplicate index name */
    if (db->indexes.count(iname)) {
        if (if_not_exists) return SVDB_OK;
        db->last_error = "index " + iname + " already exists";
        return SVDB_ERR;
    }
    /* Read column list */
    size_t paren = sql.find('(', pos);
    size_t rparen = sql.rfind(')');
    IndexDef idef; idef.table = resolved_tname; idef.unique = unique;
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
    
    /* Case-insensitive table lookup */
    std::string resolved_tname = resolve_table_name(db, tname);
    if (resolved_tname.empty()) {
        if (if_exists) return SVDB_OK;
        db->last_error = "no such table: " + tname;
        return SVDB_ERR;
    }
    db->schema.erase(resolved_tname);
    db->col_order.erase(resolved_tname);
    db->data.erase(resolved_tname);
    db->rowid_counter.erase(resolved_tname);
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
        if (db->schema[tname][col_name].primary_key) {
            db->last_error = "cannot drop PRIMARY KEY column: " + col_name; return SVDB_ERR;
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
        /* Parse optional constraints: NOT NULL, DEFAULT */
        while (p < sql.size()) {
            while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
            if (p >= sql.size()) break;
            size_t kw_start = p;
            while (p < sql.size() && isalpha((unsigned char)sql[p])) ++p;
            std::string ckw = str_upper(sql.substr(kw_start, p - kw_start));
            if (ckw == "NOT") {
                while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
                size_t s3 = p;
                while (p < sql.size() && isalpha((unsigned char)sql[p])) ++p;
                std::string nkw = str_upper(sql.substr(s3, p - s3));
                if (nkw == "NULL") cd.not_null = true;
            } else if (ckw == "DEFAULT") {
                while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
                size_t ds = p;
                if (p < sql.size() && sql[p] == '\'') {
                    ++p;
                    while (p < sql.size() && sql[p] != '\'') ++p;
                    if (p < sql.size()) ++p;
                } else if (p < sql.size() && sql[p] == '(') {
                    int pd2 = 1; ++p;
                    while (p < sql.size() && pd2 > 0) {
                        if (sql[p] == '(') ++pd2;
                        else if (sql[p] == ')') --pd2;
                        ++p;
                    }
                } else {
                    while (p < sql.size() && sql[p] != ',' && sql[p] != ')' &&
                           !isspace((unsigned char)sql[p])) ++p;
                }
                cd.default_val = str_trim(sql.substr(ds, p - ds));
            } else if (ckw.empty()) {
                break;
            } else {
                /* Skip unknown constraint keyword token */
            }
        }
        db->schema[tname][col_name] = cd;
        db->col_order[tname].push_back(col_name);
        /* Set existing rows: use default value if provided, otherwise NULL */
        for (auto &row : db->data[tname]) {
            if (!cd.default_val.empty()) {
                row[col_name] = svdb_eval_expr_in_row(cd.default_val, row, {});
            } else {
                row[col_name] = SvdbVal{};
            }
        }
        return SVDB_OK;
    }
    return SVDB_OK;
}


/* ── Trigger handlers ───────────────────────────────────────────── */

/* Parse and store a CREATE TRIGGER statement */
static svdb_code_t do_create_trigger(svdb_db_t *db, const std::string &sql) {
    std::string su = str_upper(sql);

    /* Check IF NOT EXISTS */
    bool if_not_exists = su.find("IF NOT EXISTS") != std::string::npos;

    /* Find trigger name: CREATE TRIGGER [IF NOT EXISTS] <name> */
    size_t p = su.find("TRIGGER");
    if (p == std::string::npos) return SVDB_ERR;
    p += 7;
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    if (su.substr(p, 13) == "IF NOT EXISTS") {
        p += 13;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    }
    /* Extract trigger name (possibly quoted) */
    std::string trig_name;
    if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
        char q = sql[p++]; size_t s = p;
        while (p < sql.size() && sql[p] != q) ++p;
        trig_name = sql.substr(s, p - s);
        if (p < sql.size()) ++p;
    } else {
        size_t s = p;
        while (p < su.size() && (isalnum((unsigned char)su[p]) || su[p] == '_')) ++p;
        trig_name = sql.substr(s, p - s);
    }
    if (trig_name.empty()) { db->last_error = "CREATE TRIGGER: missing trigger name"; return SVDB_ERR; }

    /* Check duplicate */
    if (db->triggers.count(trig_name)) {
        if (if_not_exists) return SVDB_OK;
        db->last_error = "trigger " + trig_name + " already exists";
        return SVDB_ERR;
    }

    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* Parse timing: BEFORE | AFTER | INSTEAD OF */
    TriggerTiming timing = TRIGGER_AFTER;
    size_t s2 = p;
    while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
    std::string timing_kw = su.substr(s2, p - s2);
    if (timing_kw == "BEFORE") {
        timing = TRIGGER_BEFORE;
    } else if (timing_kw == "AFTER") {
        timing = TRIGGER_AFTER;
    } else if (timing_kw == "INSTEAD") {
        timing = TRIGGER_INSTEAD_OF;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p; /* skip OF */
    }
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* Parse event: INSERT | UPDATE | DELETE */
    TriggerEvent event = TRIGGER_INSERT;
    s2 = p;
    while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
    std::string event_kw = su.substr(s2, p - s2);
    if (event_kw == "INSERT")      event = TRIGGER_INSERT;
    else if (event_kw == "UPDATE") event = TRIGGER_UPDATE;
    else if (event_kw == "DELETE") event = TRIGGER_DELETE;

    /* Skip optional "OF col" for UPDATE triggers */
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    if (su.substr(p, 2) == "OF") {
        p += 2;
        while (p < su.size() && (isalnum((unsigned char)su[p]) || su[p] == '_' || isspace((unsigned char)su[p]) || su[p] == ',')) ++p;
    }

    /* Skip ON keyword */
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    while (p < su.size() && isalpha((unsigned char)su[p])) ++p; /* ON */
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* Extract table name */
    std::string trig_table;
    if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
        char q = sql[p++]; size_t s = p;
        while (p < sql.size() && sql[p] != q) ++p;
        trig_table = sql.substr(s, p - s);
        if (p < sql.size()) ++p;
    } else {
        size_t s = p;
        while (p < su.size() && (isalnum((unsigned char)su[p]) || su[p] == '_')) ++p;
        trig_table = sql.substr(s, p - s);
    }
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* Skip optional FOR EACH ROW */
    if (su.substr(p, 3) == "FOR") {
        while (p < su.size() && (isalpha((unsigned char)su[p]) || isspace((unsigned char)su[p]))) ++p;
    }
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;

    /* Optional WHEN condition */
    std::string when_expr;
    if (su.substr(p, 4) == "WHEN") {
        p += 4;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        /* Read until BEGIN */
        size_t begin_pos = su.find("BEGIN", p);
        if (begin_pos != std::string::npos) {
            when_expr = str_trim(sql.substr(p, begin_pos - p));
            p = begin_pos;
        }
    }

    /* Find BEGIN...END body */
    size_t begin_pos = su.find("BEGIN", p);
    if (begin_pos == std::string::npos) { db->last_error = "CREATE TRIGGER: missing BEGIN"; return SVDB_ERR; }
    begin_pos += 5; /* skip BEGIN */

    /* Find the matching END: track nested BEGIN...END */
    size_t end_pos = std::string::npos;
    {
        int depth = 1;
        size_t q = begin_pos;
        while (q < su.size() && depth > 0) {
            if (su.substr(q, 5) == "BEGIN") { ++depth; q += 5; continue; }
            if (su.substr(q, 3) == "END" &&
                (q + 3 >= su.size() || (!isalnum((unsigned char)su[q+3]) && su[q+3] != '_'))) {
                --depth;
                if (depth == 0) { end_pos = q; break; }
                q += 3; continue;
            }
            ++q;
        }
    }
    if (end_pos == std::string::npos) { db->last_error = "CREATE TRIGGER: missing END"; return SVDB_ERR; }

    std::string body = str_trim(sql.substr(begin_pos, end_pos - begin_pos));

    TriggerDef td;
    td.name      = trig_name;
    td.timing    = timing;
    td.event     = event;
    td.table     = trig_table;
    td.when_expr = when_expr;
    td.body      = body;
    db->triggers[trig_name] = td;
    return SVDB_OK;
}

/* DROP TRIGGER [IF EXISTS] <name> */
static svdb_code_t do_drop_trigger(svdb_db_t *db, const std::string &sql) {
    std::string su = str_upper(sql);
    bool if_exists = su.find("IF EXISTS") != std::string::npos;
    size_t p = su.find("TRIGGER");
    if (p == std::string::npos) return SVDB_ERR;
    p += 7;
    while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    if (su.substr(p, 9) == "IF EXISTS") {
        p += 9;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
    }
    std::string name;
    if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
        char q = sql[p++]; size_t s = p;
        while (p < sql.size() && sql[p] != q) ++p;
        name = sql.substr(s, p - s);
    } else {
        size_t s = p;
        while (p < su.size() && (isalnum((unsigned char)su[p]) || su[p] == '_')) ++p;
        name = sql.substr(s, p - s);
    }
    /* strip trailing semicolons/spaces */
    while (!name.empty() && (name.back() == ';' || isspace((unsigned char)name.back())))
        name.pop_back();
    if (name.empty()) { db->last_error = "DROP TRIGGER: missing name"; return SVDB_ERR; }

    auto it = db->triggers.find(name);
    if (it == db->triggers.end()) {
        if (if_exists) return SVDB_OK;
        db->last_error = "no such trigger: " + name;
        return SVDB_ERR;
    }
    db->triggers.erase(it);
    return SVDB_OK;
}

/* Substitute NEW.col and OLD.col references in trigger body SQL */
static std::string trigger_substitute_row(const std::string &sql_tpl,
                                           const Row *new_row,
                                           const Row *old_row) {
    std::string out = sql_tpl;
    /* Replace NEW.col and OLD.col with their literal values */
    auto substitute = [&](const std::string &prefix, const Row *row) {
        if (!row) return;
        /* For each col in the row, replace prefix.col with its value */
        for (auto &kv : *row) {
            const std::string &col = kv.first;
            /* skip rowid column */
            if (col.empty() || col[0] == '_') continue;
            std::string placeholder = prefix + "." + col;
            std::string val_str;
            if (kv.second.type == SVDB_TYPE_NULL)      val_str = "NULL";
            else if (kv.second.type == SVDB_TYPE_INT)  val_str = std::to_string(kv.second.ival);
            else if (kv.second.type == SVDB_TYPE_REAL) {
                char buf[64]; snprintf(buf, sizeof(buf), "%.17g", kv.second.rval); val_str = buf;
            } else {
                /* TEXT/BLOB: escape single quotes */
                std::string escaped;
                for (char c : kv.second.sval) {
                    if (c == '\'') escaped += "''";
                    else escaped += c;
                }
                val_str = "'" + escaped + "'";
            }
            /* Replace all occurrences (case-insensitive for NEW/OLD prefix) */
            std::string ph_upper = str_upper(placeholder);
            std::string out_upper = str_upper(out);
            for (size_t pos = out_upper.find(ph_upper); pos != std::string::npos;
                 pos = out_upper.find(ph_upper, pos)) {
                bool lb = (pos == 0 || !isalnum((unsigned char)out[pos-1]) && out[pos-1] != '_');
                bool rb = (pos + placeholder.size() >= out.size() ||
                           (!isalnum((unsigned char)out[pos+placeholder.size()]) &&
                            out[pos+placeholder.size()] != '_'));
                if (lb && rb) {
                    out.replace(pos, placeholder.size(), val_str);
                    out_upper.replace(pos, placeholder.size(), str_upper(val_str));
                    pos += val_str.size();
                } else {
                    pos += placeholder.size();
                }
            }
        }
    };
    substitute("NEW", new_row);
    substitute("OLD", old_row);
    return out;
}

/* Forward declaration */
static svdb_code_t svdb_exec_internal(svdb_db_t *db, const std::string &sql, svdb_result_t *res);

/* Fire triggers for a given event on a table.
 * new_row: the new row (for INSERT/UPDATE), null for DELETE
 * old_row: the old row (for DELETE/UPDATE), null for INSERT */
static svdb_code_t fire_triggers(svdb_db_t *db, TriggerTiming timing, TriggerEvent event,
                                   const std::string &tname,
                                   const Row *new_row, const Row *old_row) {
    for (auto &kv : db->triggers) {
        const TriggerDef &td = kv.second;
        if (td.timing != timing || td.event != event) continue;
        if (str_upper(td.table) != str_upper(tname)) continue;

        /* Evaluate WHEN condition if present.
         * After substituting NEW/OLD refs, the expression is a pure value comparison
         * (e.g. "1 != 0" or "'foo' != 'inserts'"). eval_where handles simple cases. */
        if (!td.when_expr.empty()) {
            std::string when_sub = trigger_substitute_row(td.when_expr, new_row, old_row);
            /* Use a dummy row; after substitution the condition only contains literals */
            Row dummy_row;
            std::vector<std::string> dummy_order;
            if (!eval_where(dummy_row, dummy_order, when_sub))
                continue; /* WHEN is false: skip this trigger */
        }

        /* Substitute NEW/OLD refs and execute the body */
        std::string body = trigger_substitute_row(td.body, new_row, old_row);

        /* Execute each statement in the body (split on ;) */
        size_t pos = 0;
        while (pos < body.size()) {
            while (pos < body.size() && isspace((unsigned char)body[pos])) ++pos;
            if (pos >= body.size()) break;
            /* Find statement end (;) at depth 0 */
            size_t stmt_start = pos;
            int depth = 0; bool in_str = false;
            while (pos < body.size()) {
                char c = body[pos];
                if (c == '\'') { in_str = !in_str; ++pos; continue; }
                if (in_str) { ++pos; continue; }
                if (c == '(') ++depth;
                else if (c == ')') { if (depth > 0) --depth; }
                else if (c == ';' && depth == 0) { ++pos; break; }
                ++pos;
            }
            std::string stmt = str_trim(body.substr(stmt_start, pos - stmt_start - (pos > 0 && body[pos-1] == ';' ? 1 : 0)));
            if (!stmt.empty()) {
                svdb_code_t rc = svdb_exec_internal(db, stmt, nullptr);
                if (rc != SVDB_OK) return rc;
            }
        }
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

        /* Case-insensitive table lookup with schema prefix support */
        std::string resolved_tname2 = resolve_table_name(db, tname2);
        if (resolved_tname2.empty()) { db->last_error = "no such table: " + tname2; return SVDB_ERR; }
        
        const auto &col_order2 = db->col_order[resolved_tname2];
        Row row;
        for (const auto &cn : col_order2) {
            auto dit = db->schema[resolved_tname2].find(cn);
            if (dit != db->schema[resolved_tname2].end() && !dit->second.default_val.empty())
                row[cn] = svdb_eval_expr_in_row(dit->second.default_val, row, {});
            else
                row[cn] = SvdbVal{};
        }
        /* Auto-assign INTEGER PRIMARY KEY if not set */
        for (const auto &cn2 : col_order2) {
            auto cdit = db->schema[resolved_tname2].find(cn2);
            if (cdit != db->schema[resolved_tname2].end() && cdit->second.primary_key &&
                str_upper(cdit->second.type) == "INTEGER") {
                auto rit = row.find(cn2);
                if (rit == row.end() || rit->second.type == SVDB_TYPE_NULL) {
                    SvdbVal v; v.type = SVDB_TYPE_INT;
                    v.ival = db->rowid_counter[resolved_tname2] + 1;
                    row[cn2] = v;
                }
            }
        }
        db->rowid_counter[resolved_tname2]++;
        row[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[resolved_tname2], 0.0, {}};
        db->data[resolved_tname2].push_back(row);
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

    /* Case-insensitive table lookup */
    std::string resolved_tname = resolve_table_name(db, tname);
    if (resolved_tname.empty()) {
        db->last_error = "no such table: " + tname;
        svdb_ast_node_free(ast); svdb_parser_destroy(p);
        return SVDB_ERR;
    }
    tname = resolved_tname; /* use canonical name for all subsequent operations */

    const auto &col_order = db->col_order[resolved_tname];

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
                        auto dit = db->schema[resolved_tname].find(cn);
                        if (dit != db->schema[resolved_tname].end() && !dit->second.default_val.empty())
                            row2[cn] = svdb_eval_expr_in_row(dit->second.default_val, row2, {});
                        else row2[cn] = SvdbVal{};
                    }
                }
                db->rowid_counter[resolved_tname]++;
                row2[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[resolved_tname], 0.0, {}};
                db->data[resolved_tname].push_back(row2);
                ++inserted2;
            }
            delete sel_rows;
            db->rows_affected = inserted2;
            db->last_insert_rowid = db->rowid_counter[resolved_tname];
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
            if (db->schema[resolved_tname].find(ic) == db->schema[resolved_tname].end() &&
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
        /* VALUES() with empty parentheses is a syntax error; use DEFAULT VALUES */
        if (nv0 == 0) {
            db->last_error = "near ')': syntax error";
            svdb_ast_node_free(ast); svdb_parser_destroy(p);
            return SVDB_ERR;
        }
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
            /* Handle DEFAULT keyword — use column's default value */
            std::string vstr_upper = str_upper(vstr);
            if (vstr_upper == "DEFAULT") {
                auto dit = db->schema[tname].find(ins_cols[ci]);
                if (dit != db->schema[tname].end() && !dit->second.default_val.empty()) {
                    row[ins_cols[ci]] = svdb_eval_expr_in_row(dit->second.default_val, row, {});
                } else {
                    row[ins_cols[ci]] = SvdbVal{};
                }
            } else {
                row[ins_cols[ci]] = parse_literal(vstr);
            }
        }

        /* Apply column type affinity coercion (SQLite compatible) */
        for (int ci = 0; ci < (int)ins_cols.size(); ++ci) {
            const std::string &col_name = ins_cols[ci];
            auto cit = db->schema[tname].find(col_name);
            if (cit == db->schema[tname].end()) continue;
            std::string ct = str_upper(cit->second.type);
            auto rit = row.find(col_name);
            if (rit == row.end() || rit->second.type == SVDB_TYPE_NULL) continue;
            /* REAL affinity: coerce integer to real; coerce text-number to real */
            if ((ct == "REAL" || ct == "FLOAT" || ct == "DOUBLE") && rit->second.type == SVDB_TYPE_INT) {
                rit->second.rval = (double)rit->second.ival;
                rit->second.type = SVDB_TYPE_REAL;
            }
            if ((ct == "REAL" || ct == "FLOAT" || ct == "DOUBLE") && rit->second.type == SVDB_TYPE_TEXT) {
                const std::string &sv = rit->second.sval;
                char *endp = nullptr;
                double dv = strtod(sv.c_str(), &endp);
                if (endp && endp != sv.c_str() && *endp == '\0') {
                    rit->second.rval = dv;
                    rit->second.type = SVDB_TYPE_REAL;
                }
            }
            /* INTEGER affinity: coerce real with no fractional part to integer */
            if ((ct == "INTEGER" || ct == "INT") && rit->second.type == SVDB_TYPE_REAL) {
                if (rit->second.rval == (double)(int64_t)rit->second.rval) {
                    rit->second.ival = (int64_t)rit->second.rval;
                    rit->second.type = SVDB_TYPE_INT;
                }
            }
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

        /* CHECK constraint evaluation */
        if (db->check_constraints.count(tname)) {
            for (const auto &chk : db->check_constraints.at(tname)) {
                if (!eval_check_constraint(chk, row, col_order)) {
                    db->last_error = "CHECK constraint failed: " + tname;
                    svdb_ast_node_free(ast); svdb_parser_destroy(p);
                    return SVDB_ERR;
                }
            }
        }

        /* FK constraint check */
        if (db->foreign_keys_enabled && db->fk_constraints.count(tname)) {
            for (const auto &fk : db->fk_constraints.at(tname)) {
                auto child_it = row.find(fk.child_col);
                if (child_it == row.end() || child_it->second.type == SVDB_TYPE_NULL)
                    continue; /* NULL values don't violate FK */
                std::string parent_up = str_upper(fk.parent_table);
                std::string resolved_parent;
                for (auto &kv : db->schema)
                    if (str_upper(kv.first) == parent_up) { resolved_parent = kv.first; break; }
                if (resolved_parent.empty() || !db->data.count(resolved_parent))
                    continue;
                bool found = false;
                for (const auto &prow : db->data.at(resolved_parent)) {
                    auto pit = prow.find(fk.parent_col);
                    if (pit == prow.end()) continue;
                    if (pit->second.type == child_it->second.type) {
                        bool match = false;
                        switch (pit->second.type) {
                            case SVDB_TYPE_INT:  match = (pit->second.ival == child_it->second.ival); break;
                            case SVDB_TYPE_REAL: match = (pit->second.rval == child_it->second.rval); break;
                            case SVDB_TYPE_TEXT: match = (pit->second.sval == child_it->second.sval); break;
                            default: break;
                        }
                        if (match) { found = true; break; }
                    }
                }
                if (!found) {
                    db->last_error = "FOREIGN KEY constraint failed";
                    svdb_ast_node_free(ast); svdb_parser_destroy(p);
                    return SVDB_ERR;
                }
            }
        }

        /* Auto-increment rowid */
        db->rowid_counter[tname]++;
        row[SVDB_ROWID_COLUMN] = SvdbVal{SVDB_TYPE_INT, db->rowid_counter[tname], 0.0, {}};

        /* Fire BEFORE INSERT triggers */
        if (!db->triggers.empty())
            fire_triggers(db, TRIGGER_BEFORE, TRIGGER_INSERT, tname, &row, nullptr);

        db->data[tname].push_back(row);
        ++inserted;

        /* Fire AFTER INSERT triggers */
        if (!db->triggers.empty())
            fire_triggers(db, TRIGGER_AFTER, TRIGGER_INSERT, tname, &row, nullptr);
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

/* Helper: compare two SvdbVal for FK matching (ignoring NULL) */
static bool fk_vals_equal(const SvdbVal &a, const SvdbVal &b) {
    if (a.type == SVDB_TYPE_NULL || b.type == SVDB_TYPE_NULL) return false;
    if (a.type != b.type) return false;
    switch (a.type) {
        case SVDB_TYPE_INT:  return a.ival == b.ival;
        case SVDB_TYPE_REAL: return a.rval == b.rval;
        case SVDB_TYPE_TEXT: return a.sval == b.sval;
        default: return false;
    }
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

    /* Case-insensitive table lookup */
    std::string resolved_tname = resolve_table_name(db, tname);
    if (resolved_tname.empty()) {
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
    /* Check for UPDATE ... FROM (PostgreSQL-style): detect top-level FROM after SET */
    /* Find FROM at top level between SET and WHERE */
    std::string from_table;
    size_t update_from_pos = std::string::npos;
    {
        int depth = 0; bool in_s = false;
        size_t search_start = set_pos + 3;
        for (size_t i = search_start; i < sql.size(); ++i) {
            char c = sql[i];
            if (c == '\'' && !in_s) { in_s = true; continue; }
            if (c == '\'' && in_s) { if (i+1<sql.size()&&sql[i+1]=='\''){++i;} else in_s=false; continue; }
            if (in_s) continue;
            if (c == '(') ++depth; else if (c == ')') --depth;
            if (depth == 0 && i+4 <= sql.size() && str_upper(sql.substr(i,4)) == "FROM" &&
                (i == 0 || !isalnum((unsigned char)sql[i-1]) && sql[i-1]!='_') &&
                (i+4 >= sql.size() || (!isalnum((unsigned char)sql[i+4]) && sql[i+4]!='_'))) {
                update_from_pos = i;
                /* Extract table name after FROM */
                size_t tp = i + 4;
                while (tp < sql.size() && isspace((unsigned char)sql[tp])) ++tp;
                size_t ts = tp;
                while (tp < sql.size() && (isalnum((unsigned char)sql[tp]) || sql[tp]=='_')) ++tp;
                from_table = sql.substr(ts, tp - ts);
                break;
            }
        }
    }
    if (!from_table.empty() && db->schema.count(from_table)) {
        /* UPDATE t SET col=expr FROM other WHERE cond — join-based update */
        size_t where_pos2 = std::string::npos;
        { int depth=0; bool in_s=false;
          for (size_t i=update_from_pos+4; i<sql.size(); ++i) {
            char c=sql[i]; if(c=='\''){in_s=!in_s;continue;} if(in_s)continue;
            if(c=='(')++depth;else if(c==')')--depth;
            if(depth==0&&i+5<=sql.size()&&str_upper(sql.substr(i,5))=="WHERE") { where_pos2=i; break; }
          }
        }
        std::string where_clause = (where_pos2!=std::string::npos) ? str_trim(sql.substr(where_pos2+5)) : "";
        std::string set_clause2 = str_trim(sql.substr(set_pos+3, update_from_pos - set_pos - 3));
        /* Parse assignments */
        std::vector<std::pair<std::string,std::string>> assignments;
        size_t ap = 0;
        while (ap < set_clause2.size()) {
            while (ap < set_clause2.size() && isspace((unsigned char)set_clause2[ap])) ++ap;
            std::string cn;
            if (ap < set_clause2.size() && (set_clause2[ap]=='"'||set_clause2[ap]=='`')) {
                char q=set_clause2[ap++]; size_t s=ap;
                while(ap<set_clause2.size()&&set_clause2[ap]!=q)++ap;
                cn=set_clause2.substr(s,ap-s); if(ap<set_clause2.size())++ap;
            } else {
                size_t s=ap;
                while(ap<set_clause2.size()&&(isalnum((unsigned char)set_clause2[ap])||set_clause2[ap]=='_'))++ap;
                cn=set_clause2.substr(s,ap-s);
            }
            if(cn.empty()){++ap;continue;}
            while(ap<set_clause2.size()&&isspace((unsigned char)set_clause2[ap]))++ap;
            if(ap<set_clause2.size()&&set_clause2[ap]=='=')++ap;
            while(ap<set_clause2.size()&&isspace((unsigned char)set_clause2[ap]))++ap;
            std::string vstr; size_t vs=ap; int vd=0; bool vsq=false;
            while(ap<set_clause2.size()){
                char vc=set_clause2[ap];
                if(vc=='\''){vsq=!vsq;++ap;continue;} if(vsq){++ap;continue;}
                if(vc=='(')++vd; else if(vc==')'){if(vd>0)--vd;} else if(vc==','&&vd==0)break;
                ++ap;
            }
            vstr=str_trim(set_clause2.substr(vs,ap-vs));
            assignments.push_back({cn,vstr});
            while(ap<set_clause2.size()&&(isspace((unsigned char)set_clause2[ap])||set_clause2[ap]==','))++ap;
        }
        /* Build combined column order: target cols first, then from_table cols (prefixed) */
        auto &tcols = db->col_order[resolved_tname];
        auto &fcols = db->col_order[from_table];
        Row combined_cols_order; /* Use string list */
        std::vector<std::string> combined_order;
        for (auto &cn : tcols) combined_order.push_back(cn);
        for (auto &cn : fcols) {
            combined_order.push_back(from_table + "." + cn);
            combined_order.push_back(cn); /* also unqualified */
        }
        svdb_set_query_db(db);
        int64_t updated = 0;
        for (auto &trow : db->data[resolved_tname]) {
            for (auto &frow : db->data[from_table]) {
                /* Build combined row */
                Row combined = trow;
                /* Add target table's columns with qualified names */
                for (auto &cn : tcols) combined[resolved_tname + "." + cn] = trow.count(cn) ? trow.at(cn) : SvdbVal{};
                for (auto &cn : fcols) {
                    combined[from_table + "." + cn] = frow.count(cn) ? frow.at(cn) : SvdbVal{};
                    if (!combined.count(cn)) combined[cn] = frow.count(cn) ? frow.at(cn) : SvdbVal{};
                }
                std::vector<std::string> col_order_vec = combined_order;
                if (where_clause.empty() || eval_where(combined, col_order_vec, where_clause)) {
                    for (auto &asgn : assignments) {
                        std::string simple_col = asgn.first;
                        /* Strip table qualifier if present */
                        auto dot = simple_col.find('.');
                        if (dot != std::string::npos) simple_col = simple_col.substr(dot+1);
                        if (trow.count(simple_col))
                            trow[simple_col] = svdb_eval_expr_in_row(asgn.second, combined, col_order_vec);
                    }
                    ++updated;
                    break; /* update target row only once (first match) */
                }
            }
        }
        svdb_set_query_db(nullptr);
        db->rows_affected = updated;
        if (res) { res->code = SVDB_OK; res->rows_affected = updated; }
        svdb_ast_node_free(ast); svdb_parser_destroy(p);
        return SVDB_OK;
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

    const auto &col_order = db->col_order[resolved_tname];
    int64_t updated = 0;
    /* Collect old and new row values (needed for FK ON UPDATE actions) */
    std::vector<std::pair<Row,Row>> updated_pairs; /* {old_row, new_row} */
    svdb_set_query_db(db);  /* set thread-local DB context for subquery eval in SET expressions */
    for (auto &row : db->data[resolved_tname]) {
        if (!eval_where(row, col_order, where_txt)) continue;
        Row old_row = row;
        for (const auto &asgn : assignments)
            row[asgn.first] = svdb_eval_expr_in_row(asgn.second, row, col_order);
        updated_pairs.push_back({old_row, row});
        ++updated;
    }
    svdb_set_query_db(nullptr);

    /* FK ON UPDATE actions */
    if (db->foreign_keys_enabled && !updated_pairs.empty()) {
        for (auto &kv : db->fk_constraints) {
            const std::string &child_tname = kv.first;
            if (!db->data.count(child_tname)) continue;
            for (const auto &fk : kv.second) {
                if (str_upper(fk.parent_table) != str_upper(resolved_tname)) continue;
                std::string action = str_upper(fk.on_update);
                if (action.empty() || action == "NO ACTION" || action == "RESTRICT") continue;
                if (action == "CASCADE") {
                    for (const auto &pr : updated_pairs) {
                        const Row &old_row = pr.first;
                        const Row &new_row = pr.second;
                        auto old_it = old_row.find(fk.parent_col);
                        auto new_it = new_row.find(fk.parent_col);
                        if (old_it == old_row.end() || new_it == new_row.end()) continue;
                        if (fk_vals_equal(old_it->second, new_it->second)) continue; /* no change */
                        for (auto &crow : db->data[child_tname]) {
                            auto cit = crow.find(fk.child_col);
                            if (cit == crow.end()) continue;
                            if (fk_vals_equal(cit->second, old_it->second))
                                cit->second = new_it->second;
                        }
                    }
                } else if (action == "SET NULL") {
                    for (const auto &pr : updated_pairs) {
                        const Row &old_row = pr.first;
                        const Row &new_row = pr.second;
                        auto old_it = old_row.find(fk.parent_col);
                        auto new_it = new_row.find(fk.parent_col);
                        if (old_it == old_row.end() || new_it == new_row.end()) continue;
                        if (fk_vals_equal(old_it->second, new_it->second)) continue;
                        for (auto &crow : db->data[child_tname]) {
                            auto cit = crow.find(fk.child_col);
                            if (cit == crow.end()) continue;
                            if (fk_vals_equal(cit->second, old_it->second))
                                cit->second = SvdbVal{};
                        }
                    }
                }
            }
        }
    }

    db->rows_affected = updated;
    if (res) { res->code = SVDB_OK; res->rows_affected = updated; }

    /* Fire AFTER UPDATE triggers */
    if (!db->triggers.empty()) {
        for (const auto &pr : updated_pairs)
            fire_triggers(db, TRIGGER_AFTER, TRIGGER_UPDATE, resolved_tname, &pr.second, &pr.first);
    }

    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);
    return SVDB_OK;
}

/* Recursive FK ON DELETE handler.
 * Checks/applies FK actions when rows are deleted from 'tname'.
 * Returns SVDB_ERR (with db->last_error set) if RESTRICT/NO ACTION is violated.
 * depth prevents infinite recursion in self-referencing tables. */
static svdb_code_t fk_on_delete(svdb_db_t *db,
                                  const std::string &tname,
                                  const std::vector<Row> &deleted_rows,
                                  int depth = 0) {
    if (depth > 64 || deleted_rows.empty()) return SVDB_OK;

    for (auto &kv : db->fk_constraints) {
        const std::string &child_tname = kv.first;
        if (!db->data.count(child_tname)) continue;
        for (const auto &fk : kv.second) {
            if (str_upper(fk.parent_table) != str_upper(tname)) continue;

            std::string action = str_upper(fk.on_delete);
            /* Empty on_delete defaults to NO ACTION (= RESTRICT at statement level) */
            if (action.empty() || action == "NO ACTION") action = "RESTRICT";

            if (action == "RESTRICT") {
                for (const auto &drow : deleted_rows) {
                    auto pit = drow.find(fk.parent_col);
                    if (pit == drow.end() || pit->second.type == SVDB_TYPE_NULL) continue;
                    for (const auto &crow : db->data.at(child_tname)) {
                        auto cit = crow.find(fk.child_col);
                        if (cit == crow.end()) continue;
                        if (fk_vals_equal(cit->second, pit->second)) {
                            db->last_error = "FOREIGN KEY constraint failed";
                            return SVDB_ERR;
                        }
                    }
                }
            } else if (action == "CASCADE") {
                std::vector<Row> cascade_deleted;
                auto &crows = db->data[child_tname];
                for (const auto &drow : deleted_rows) {
                    auto pit = drow.find(fk.parent_col);
                    if (pit == drow.end() || pit->second.type == SVDB_TYPE_NULL) continue;
                    auto new_end = std::remove_if(crows.begin(), crows.end(),
                        [&](const Row &crow) {
                            auto cit = crow.find(fk.child_col);
                            if (cit == crow.end()) return false;
                            if (fk_vals_equal(cit->second, pit->second)) {
                                cascade_deleted.push_back(crow);
                                return true;
                            }
                            return false;
                        });
                    crows.erase(new_end, crows.end());
                }
                if (!cascade_deleted.empty()) {
                    svdb_code_t rc = fk_on_delete(db, child_tname, cascade_deleted, depth + 1);
                    if (rc != SVDB_OK) return rc;
                }
            } else if (action == "SET NULL") {
                for (const auto &drow : deleted_rows) {
                    auto pit = drow.find(fk.parent_col);
                    if (pit == drow.end() || pit->second.type == SVDB_TYPE_NULL) continue;
                    for (auto &crow : db->data[child_tname]) {
                        auto cit = crow.find(fk.child_col);
                        if (cit == crow.end()) continue;
                        if (fk_vals_equal(cit->second, pit->second))
                            cit->second = SvdbVal{};
                    }
                }
            }
        }
    }
    return SVDB_OK;
}

static svdb_code_t do_delete(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
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

    /* Case-insensitive table lookup */
    std::string resolved_tname = resolve_table_name(db, tname);
    if (resolved_tname.empty()) {
        db->last_error = "no such table: " + tname;
        return SVDB_ERR;
    }
    tname = resolved_tname; /* use canonical name for all subsequent operations */

    /* Check for DELETE FROM t USING other_table WHERE ... (PostgreSQL style) */
    {
        std::string su2 = str_upper(sql);
        size_t using_pos = std::string::npos;
        { int depth=0; bool in_s=false;
          for (size_t i=0; i<sql.size(); ++i) {
            char c=sql[i]; if(c=='\''){in_s=!in_s;continue;} if(in_s)continue;
            if(c=='(')++depth; else if(c==')')--depth;
            if(depth==0&&i+5<=sql.size()&&str_upper(sql.substr(i,5))=="USING"&&
               (i==0||!isalnum((unsigned char)sql[i-1]))&&
               (i+5>=sql.size()||!isalnum((unsigned char)sql[i+5]))) { using_pos=i; break; }
          }
        }
        if (using_pos != std::string::npos) {
            size_t tp = using_pos + 5;
            while (tp < sql.size() && isspace((unsigned char)sql[tp])) ++tp;
            size_t ts = tp;
            while (tp < sql.size() && (isalnum((unsigned char)sql[tp])||sql[tp]=='_')) ++tp;
            std::string using_table = sql.substr(ts, tp - ts);
            if (!using_table.empty() && db->schema.count(using_table)) {
                /* Find WHERE */
                size_t where_pos2 = std::string::npos;
                { int depth=0; bool in_s=false;
                  for (size_t i=tp; i<sql.size(); ++i) {
                    char c=sql[i]; if(c=='\''){in_s=!in_s;continue;} if(in_s)continue;
                    if(c=='(')++depth; else if(c==')')--depth;
                    if(depth==0&&i+5<=sql.size()&&str_upper(sql.substr(i,5))=="WHERE") { where_pos2=i; break; }
                  }
                }
                std::string where_clause = (where_pos2!=std::string::npos)?str_trim(sql.substr(where_pos2+5)):"";
                auto &tcols = db->col_order[tname];
                auto &ucols = db->col_order[using_table];
                auto &trows = db->data[tname];
                int64_t deleted = 0;
                /* Build set of target row indexes to delete */
                std::vector<bool> to_delete(trows.size(), false);
                for (size_t ti = 0; ti < trows.size(); ++ti) {
                    for (auto &urow : db->data[using_table]) {
                        Row combined = trows[ti];
                        /* Add target table's columns with qualified names */
                        for (auto &cn : tcols) combined[tname + "." + cn] = trows[ti].count(cn) ? trows[ti].at(cn) : SvdbVal{};
                        for (auto &cn : ucols) {
                            combined[using_table + "." + cn] = urow.count(cn) ? urow.at(cn) : SvdbVal{};
                            if (!combined.count(cn)) combined[cn] = urow.count(cn) ? urow.at(cn) : SvdbVal{};
                        }
                        std::vector<std::string> co_vec; for (auto &cn:tcols) co_vec.push_back(cn);
                        for (auto &cn:ucols) { co_vec.push_back(using_table+"."+cn); if(!combined.count(cn)) co_vec.push_back(cn); }
                        if (where_clause.empty() || eval_where(combined, co_vec, where_clause)) {
                            to_delete[ti] = true; break;
                        }
                    }
                }
                std::vector<Row> new_rows;
                for (size_t i = 0; i < trows.size(); ++i) {
                    if (to_delete[i]) ++deleted;
                    else new_rows.push_back(trows[i]);
                }
                db->data[resolved_tname] = std::move(new_rows);
                db->rows_affected = deleted;
                if (res) { res->code = SVDB_OK; res->rows_affected = deleted; }
                return SVDB_OK;
            }
        }
    }

    const auto &col_order = db->col_order[resolved_tname];
    auto &rows = db->data[resolved_tname];
    int64_t deleted = 0;

    /* Collect deleted rows before erasing (needed for FK cascade) */
    std::vector<Row> deleted_rows;
    auto it = rows.begin();
    while (it != rows.end()) {
        if (eval_where(*it, col_order, where_txt)) {
            deleted_rows.push_back(*it);
            it = rows.erase(it);
            ++deleted;
        } else {
            ++it;
        }
    }

    /* FK ON DELETE actions: CASCADE, SET NULL, RESTRICT/NO ACTION (recursive) */
    if (db->foreign_keys_enabled && !deleted_rows.empty()) {
        /* RESTRICT check: must happen before modifications; scan first */
        for (auto &kv : db->fk_constraints) {
            const std::string &child_tname = kv.first;
            if (!db->data.count(child_tname)) continue;
            for (const auto &fk : kv.second) {
                if (str_upper(fk.parent_table) != str_upper(resolved_tname)) continue;
                std::string action = str_upper(fk.on_delete);
                if (!action.empty() && action != "RESTRICT" && action != "NO ACTION") continue;
                for (const auto &drow : deleted_rows) {
                    auto pit = drow.find(fk.parent_col);
                    if (pit == drow.end() || pit->second.type == SVDB_TYPE_NULL) continue;
                    for (const auto &crow : db->data.at(child_tname)) {
                        auto cit = crow.find(fk.child_col);
                        if (cit == crow.end()) continue;
                        if (fk_vals_equal(cit->second, pit->second)) {
                            /* Undo deletes and return error */
                            for (auto &dr : deleted_rows) db->data[resolved_tname].push_back(dr);
                            db->last_error = "FOREIGN KEY constraint failed";
                            return SVDB_ERR;
                        }
                    }
                }
            }
        }
        /* Apply CASCADE / SET NULL recursively (skip RESTRICT - already checked) */
        svdb_code_t fk_rc = fk_on_delete(db, resolved_tname, deleted_rows);
        if (fk_rc != SVDB_OK) {
            /* Undo deletes */
            for (auto &dr : deleted_rows) db->data[resolved_tname].push_back(dr);
            return fk_rc;
        }
    }

    db->rows_affected = deleted;
    if (res) { res->code = SVDB_OK; res->rows_affected = deleted; }

    /* Fire AFTER DELETE triggers */
    if (!db->triggers.empty()) {
        for (const auto &drow : deleted_rows)
            fire_triggers(db, TRIGGER_AFTER, TRIGGER_DELETE, resolved_tname, nullptr, &drow);
    }

    return SVDB_OK;
}

/* ── Public API ─────────────────────────────────────────────────── */

/* Internal exec (no lock - must be called with lock held) */
static svdb_code_t svdb_exec_internal(svdb_db_t *db, const std::string &sql_in, svdb_result_t *res) {
    std::string s = strip_sql_comments(sql_in);
    s = str_trim(s);
    std::string kw = first_keyword(s);
    if (kw == "INSERT")       return do_insert(db, s, res);
    if (kw == "UPDATE")       return do_update(db, s, res);
    if (kw == "DELETE")       return do_delete(db, s, res);
    if (kw == "SELECT") {
        svdb_rows_t *rows = nullptr;
        svdb_code_t rc = svdb_query_internal(db, s, &rows);
        if (rows) svdb_rows_close(rows);
        return rc;
    }
    /* Silently accept anything else in trigger bodies */
    return SVDB_OK;
}

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
        else if (what == "INDEX")   rc = do_create_index(db, s, false);
        else if (what == "TRIGGER") rc = do_create_trigger(db, s);
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
                    /* Error if view already exists and no IF NOT EXISTS */
                    if (db->schema.count(vname) && !if_not_exists2) {
                        db->last_error = "view " + vname + " already exists";
                        rc = SVDB_ERR;
                    } else if (!db->schema.count(vname)) {
                        /* Register view with empty schema so it shows in table_list */
                        db->schema[vname] = {};
                        db->col_order[vname] = {};
                        db->create_sql[vname] = s;
                        rc = SVDB_OK;
                    } else {
                        rc = SVDB_OK; /* IF NOT EXISTS and view exists: silent success */
                    }
                } else {
                    rc = SVDB_OK;
                }
            } else {
                rc = SVDB_OK;
            }
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
        else if (what == "TRIGGER") rc = do_drop_trigger(db, s);
        else if (what == "VIEW") {
            /* DROP [TEMP] VIEW [IF EXISTS] vname [RESTRICT|CASCADE] */
            /* Note: RESTRICT and CASCADE are SQL standard but SQLite doesn't support them.
             * We reject them to match SQLite behavior. */
            std::string su2 = str_upper(s);
            if (su2.find(" RESTRICT") != std::string::npos || su2.find(" CASCADE") != std::string::npos) {
                db->last_error = "near \"RESTRICT\": syntax error";
                rc = SVDB_ERR;
            } else {
                bool if_exists2 = su2.find("IF EXISTS") != std::string::npos;
                size_t vp2 = su2.find("VIEW");
                if (vp2 != std::string::npos) {
                    vp2 += 4;
                    while (vp2 < su2.size() && isspace((unsigned char)su2[vp2])) ++vp2;
                    if (su2.substr(vp2, 9) == "IF EXISTS") {
                        vp2 += 9;
                        while (vp2 < su2.size() && isspace((unsigned char)su2[vp2])) ++vp2;
                    }
                    std::string vname2;
                    if (vp2 < s.size() && (s[vp2] == '"' || s[vp2] == '`')) {
                        char q2 = s[vp2++]; size_t vs2 = vp2;
                        while (vp2 < s.size() && s[vp2] != q2) ++vp2;
                        vname2 = s.substr(vs2, vp2 - vs2);
                    } else {
                        size_t vs2 = vp2;
                        while (vp2 < s.size() && (isalnum((unsigned char)s[vp2]) || s[vp2] == '_')) ++vp2;
                        vname2 = s.substr(vs2, vp2 - vs2);
                    }
                    if (!vname2.empty()) {
                        /* Case-insensitive lookup */
                        std::string resolved_vname;
                        auto vit = db->schema.find(vname2);
                        if (vit != db->schema.end()) {
                            resolved_vname = vname2;
                        } else {
                            std::string vu = str_upper(vname2);
                            for (auto &kv : db->schema) {
                                if (str_upper(kv.first) == vu) { resolved_vname = kv.first; break; }
                            }
                        }
                        if (!resolved_vname.empty()) {
                            db->schema.erase(resolved_vname);
                            db->col_order.erase(resolved_vname);
                            db->create_sql.erase(resolved_vname);
                            db->data.erase(resolved_vname);
                            rc = SVDB_OK;
                        } else if (if_exists2) {
                            rc = SVDB_OK;
                        } else {
                            db->last_error = "no such view: " + vname2;
                            rc = SVDB_ERR;
                        }
                    } else {
                        rc = SVDB_OK;
                    }
                } else {
                    rc = SVDB_OK;
                }
            }
        }
        else                  rc = SVDB_OK;
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
        /* Check for SELECT ... INTO newtable FROM ... (CREATE TABLE AS SELECT) */
        {
            std::string su_s = str_upper(s);
            /* Find "INTO" that appears before "FROM" and is not inside parens */
            size_t into_pos = std::string::npos;
            size_t from_pos = std::string::npos;
            int depth = 0; bool in_sq = false;
            for (size_t i = 0; i < s.size(); ++i) {
                char c = s[i];
                if (c == '\'' && !in_sq) { in_sq = true; continue; }
                if (c == '\'' && in_sq) { if (i+1<s.size()&&s[i+1]=='\''){++i;} else in_sq=false; continue; }
                if (in_sq) continue;
                if (c == '(') ++depth; else if (c == ')') --depth;
                if (depth == 0) {
                    if (i+4 <= s.size() && str_upper(s.substr(i,4)) == "INTO" &&
                        (i == 0 || !isalnum((unsigned char)s[i-1])) &&
                        (i+4 >= s.size() || !isalnum((unsigned char)s[i+4]))) {
                        into_pos = i;
                    }
                    if (i+4 <= s.size() && str_upper(s.substr(i,4)) == "FROM" &&
                        (i == 0 || !isalnum((unsigned char)s[i-1])) &&
                        (i+4 >= s.size() || !isalnum((unsigned char)s[i+4])) &&
                        into_pos != std::string::npos) {
                        from_pos = i;
                        break;
                    }
                }
            }
            if (into_pos != std::string::npos && from_pos != std::string::npos) {
                /* Extract newtable name */
                size_t np = into_pos + 4;
                while (np < s.size() && isspace((unsigned char)s[np])) ++np;
                size_t ns = np;
                while (np < s.size() && (isalnum((unsigned char)s[np]) || s[np] == '_')) ++np;
                std::string new_table = s.substr(ns, np - ns);
                /* Rewrite: SELECT <cols> FROM ... (without INTO part) */
                std::string rewritten = s.substr(0, into_pos) + s.substr(from_pos);
                /* Run the select */
                svdb_rows_t *rows = nullptr;
                rc = svdb_query_internal(db, rewritten, &rows);
                if (rc == SVDB_OK && rows && !new_table.empty()) {
                    /* Build CREATE TABLE statement from result columns */
                    std::string create_sql2 = "CREATE TABLE " + new_table + " (";
                    for (size_t ci = 0; ci < rows->col_names.size(); ++ci) {
                        if (ci) create_sql2 += ", ";
                        create_sql2 += rows->col_names[ci] + " TEXT";
                    }
                    create_sql2 += ")";
                    do_create_table(db, create_sql2);
                    /* Insert all rows */
                    for (auto &row : rows->rows) {
                        std::string ins = "INSERT INTO " + new_table + " VALUES (";
                        for (size_t ci = 0; ci < row.size(); ++ci) {
                            if (ci) ins += ", ";
                            if (row[ci].type == SVDB_TYPE_NULL) ins += "NULL";
                            else if (row[ci].type == SVDB_TYPE_INT) ins += std::to_string(row[ci].ival);
                            else if (row[ci].type == SVDB_TYPE_REAL) { char buf[64]; snprintf(buf,sizeof(buf),"%.17g",row[ci].rval); ins+=buf; }
                            else { ins += "'"; for (char ch:row[ci].sval){if(ch=='\'')ins+="''";else ins+=ch;} ins += "'"; }
                        }
                        ins += ")";
                        do_insert(db, ins, nullptr);
                    }
                }
                if (rows) svdb_rows_close(rows);
                if (rc != SVDB_OK && res) { res->code = rc; res->errmsg = db->last_error.c_str(); }
                return rc;
            }
        }
        /* svdb_exec on SELECT: run and discard result */
        svdb_rows_t *rows = nullptr;
        rc = svdb_query_internal(db, s, &rows);
        if (rows) svdb_rows_close(rows);
    } else {
        /* Unknown: accept silently */
        rc = SVDB_OK;
        /* Handle ANALYZE: populate sqlite_stat1 */
        if (kw == "ANALYZE") {
            db->stat1.clear();
            for (auto &kv : db->data) {
                size_t nrows = kv.second.size();
                size_t ncols = db->col_order.count(kv.first) ? db->col_order.at(kv.first).size() : 0;
                std::string stat = std::to_string(nrows);
                if (ncols > 0) for (size_t c = 0; c < ncols; ++c) stat += " " + std::to_string(nrows);
                db->stat1.emplace_back(kv.first, "", stat);
            }
            rc = SVDB_OK;
        }
    }

    if (rc != SVDB_OK && res) {
        res->code   = rc;
        res->errmsg = db->last_error.c_str();
    }
    return rc;
}

/* Prepared statement stubs */

svdb_code_t svdb_prepare(svdb_db_t *db, const char *sql, svdb_stmt_t **stmt) {
    BUG_ON(db == nullptr);
    BUG_ON(sql == nullptr);
    BUG_ON(stmt == nullptr);
    /* BUG_ON fires in debug builds only; the if-guard is the release-build safety net */
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
    BUG_ON(stmt == nullptr);
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_INT; sv.ival = val;
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_real(svdb_stmt_t *stmt, int idx, double val) {
    BUG_ON(stmt == nullptr);
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_REAL; sv.rval = val;
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_text(svdb_stmt_t *stmt, int idx,
                                  const char *val, size_t len) {
    BUG_ON(stmt == nullptr);
    if (!stmt) return SVDB_ERR;
    SvdbVal sv; sv.type = SVDB_TYPE_TEXT;
    sv.sval = val ? std::string(val, len) : std::string();
    stmt->bindings[idx] = sv;
    return SVDB_OK;
}

svdb_code_t svdb_stmt_bind_null(svdb_stmt_t *stmt, int idx) {
    BUG_ON(stmt == nullptr);
    if (!stmt) return SVDB_ERR;
    stmt->bindings[idx] = SvdbVal{};
    return SVDB_OK;
}

svdb_code_t svdb_stmt_exec(svdb_stmt_t *stmt, svdb_result_t *res) {
    BUG_ON(stmt == nullptr);
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
    BUG_ON(db == nullptr);
    BUG_ON(tx == nullptr);
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
    BUG_ON(tx == nullptr);
    if (!tx) return SVDB_ERR;
    if (tx->db) tx->db->in_transaction = false;
    delete tx;
    return SVDB_OK;
}

svdb_code_t svdb_rollback(svdb_tx_t *tx) {
    BUG_ON(tx == nullptr);
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
    BUG_ON(tx == nullptr);
    BUG_ON(name == nullptr);
    if (!tx || !name) return SVDB_ERR;
    tx->savepoints.push_back(name);
    /* Save current data as savepoint snapshot */
    tx->sp_data.push_back(tx->db->data);
    tx->sp_rowid.push_back(tx->db->rowid_counter);
    return SVDB_OK;
}

svdb_code_t svdb_release(svdb_tx_t *tx, const char *name) {
    BUG_ON(tx == nullptr);
    BUG_ON(name == nullptr);
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
    BUG_ON(tx == nullptr);
    BUG_ON(name == nullptr);
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
    BUG_ON(db == nullptr);
    BUG_ON(rows == nullptr);
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
    BUG_ON(db == nullptr);
    BUG_ON(table == nullptr);
    BUG_ON(rows == nullptr);
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
    BUG_ON(db == nullptr);
    BUG_ON(table == nullptr);
    BUG_ON(rows == nullptr);
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
    BUG_ON(src == nullptr);
    BUG_ON(dest_path == nullptr);
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
