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
#include "QP/parser.h"

#include <cctype>
#include <algorithm>
#include <string>
#include <vector>
#include <cstring>

/* Implemented in query.cpp */
extern svdb_code_t svdb_query_internal(svdb_db_t *db, const std::string &sql,
                                        svdb_rows_t **rows_out);

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
                               std::vector<std::string> &out_order) {
    /* Find the opening '(' */
    while (pos < sql.size() && sql[pos] != '(') ++pos;
    if (pos >= sql.size()) return;
    ++pos; /* skip '(' */

    int depth = 1;
    /* Walk the column list */
    while (pos < sql.size() && depth > 0) {
        /* Skip whitespace */
        while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
        if (pos >= sql.size() || sql[pos] == ')') break;

        /* Ignore inline PRIMARY KEY / UNIQUE / CHECK / FOREIGN KEY constraints */
        size_t tmp = pos;
        std::string kw;
        while (tmp < sql.size() && isalpha((unsigned char)sql[tmp])) ++tmp;
        kw = str_upper(sql.substr(pos, tmp - pos));
        if (kw == "PRIMARY" || kw == "UNIQUE" || kw == "CHECK" || kw == "FOREIGN") {
            /* skip to next ',' or ')' at depth 1 */
            while (pos < sql.size()) {
                if (sql[pos] == '(') depth++;
                else if (sql[pos] == ')') { depth--; if (depth < 1) break; }
                else if (sql[pos] == ',' && depth == 1) { ++pos; break; }
                ++pos;
            }
            continue;
        }

        /* Read column name */
        std::string col_name;
        if (sql[pos] == '"' || sql[pos] == '`') {
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
            while (pos < sql.size() && sql[pos] != ')') ++pos;
            if (pos < sql.size()) ++pos;
        }
        if (col_type.empty()) col_type = "TEXT";

        ColDef cd;
        cd.type = col_type;
        cd.not_null = false;

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
                /* skip "NULL" keyword — verify it is actually NULL */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t s3 = pos;
                while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
                std::string next_kw = str_upper(sql.substr(s3, pos - s3));
                if (next_kw == "NULL") cd.not_null = true;
            } else if (ckw == "DEFAULT") {
                /* read default value */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                size_t ds = pos;
                if (pos < sql.size() && sql[pos] == '\'') {
                    ++pos;
                    while (pos < sql.size() && sql[pos] != '\'') ++pos;
                    if (pos < sql.size()) ++pos;
                } else {
                    while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')' &&
                           !isspace((unsigned char)sql[pos])) ++pos;
                }
                cd.default_val = str_trim(sql.substr(ds, pos - ds));
            } else if (ckw == "PRIMARY") {
                /* PRIMARY KEY — skip KEY */
                while (pos < sql.size() && isspace((unsigned char)sql[pos])) ++pos;
                while (pos < sql.size() && isalpha((unsigned char)sql[pos])) ++pos;
            } else if (ckw == "UNIQUE" || ckw == "REFERENCES" || ckw == "CHECK") {
                /* skip until ',' or ')' */
                while (pos < sql.size() && sql[pos] != ',' && sql[pos] != ')') ++pos;
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
    if (v.empty() || str_upper(v) == "NULL") {
        sv.type = SVDB_TYPE_NULL;
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
    parse_column_defs(sql, 0, td, order);

    db->schema[tname]    = td;
    db->col_order[tname] = order;
    db->data[tname]      = {};
    db->rowid_counter[tname] = 0;
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
    /* Very basic: ALTER TABLE t ADD COLUMN col type
     *             ALTER TABLE t RENAME TO new_name */
    std::string su = str_upper(sql);
    size_t p = 0;
    auto skip_kw = [&](const char *kw) {
        size_t len = strlen(kw);
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        if (su.substr(p, len) == kw) p += len;
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

    /* sync p to su */
    skip_kw("ADD");
    bool have_column = false;
    {
        size_t tmp = p;
        while (tmp < su.size() && isspace((unsigned char)su[tmp])) ++tmp;
        if (su.substr(tmp, 6) == "COLUMN") { p = tmp + 6; have_column = true; }
    }
    if (!have_column && su.find("RENAME") != std::string::npos) {
        /* RENAME TO new_name */
        size_t to_pos = su.find("TO", p);
        if (to_pos == std::string::npos) return SVDB_ERR;
        size_t np = to_pos + 2;
        while (np < sql.size() && isspace((unsigned char)sql[np])) ++np;
        std::string new_name = sql.substr(np, sql.size() - np);
        new_name = str_trim(new_name);
        /* remove trailing ';' */
        while (!new_name.empty() && (new_name.back() == ';' || isspace((unsigned char)new_name.back())))
            new_name.pop_back();
        db->schema[new_name]      = db->schema[tname];
        db->col_order[new_name]   = db->col_order[tname];
        db->data[new_name]        = db->data[tname];
        db->rowid_counter[new_name] = db->rowid_counter[tname];
        db->schema.erase(tname);
        db->col_order.erase(tname);
        db->data.erase(tname);
        db->rowid_counter.erase(tname);
        return SVDB_OK;
    }

    /* ADD COLUMN col type [constraints] */
    while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
    std::string col_name;
    if (p < sql.size() && (sql[p] == '"' || sql[p] == '`')) {
        char q = sql[p++]; size_t s = p;
        while (p < sql.size() && sql[p] != q) ++p;
        col_name = sql.substr(s, p - s);
        if (p < sql.size()) ++p;
    } else {
        size_t s = p;
        while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) ++p;
        col_name = sql.substr(s, p - s);
    }
    if (col_name.empty()) { db->last_error = "ALTER TABLE ADD COLUMN: missing column name"; return SVDB_ERR; }
    while (p < sql.size() && isspace((unsigned char)sql[p])) ++p;
    std::string col_type;
    while (p < sql.size() && (isalnum((unsigned char)sql[p]) || sql[p] == '_')) {
        col_type += (char)toupper((unsigned char)sql[p]);
        ++p;
    }
    if (col_type.empty()) col_type = "TEXT";

    ColDef cd; cd.type = col_type;
    db->schema[tname][col_name] = cd;
    db->col_order[tname].push_back(col_name);
    /* Add NULL to existing rows */
    for (auto &row : db->data[tname]) {
        row[col_name] = SvdbVal{};
    }
    return SVDB_OK;
}

/* ── DML handlers ───────────────────────────────────────────────── */

static svdb_code_t do_insert(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
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

    /* Determine insertion columns */
    std::vector<std::string> ins_cols;
    if (ncols > 0) {
        for (int i = 0; i < ncols; ++i)
            ins_cols.push_back(svdb_ast_get_column(ast, i));
    } else {
        ins_cols = col_order;
    }

    int64_t inserted = 0;
    for (int ri = 0; ri < nrows; ++ri) {
        Row row;
        /* Set defaults first */
        for (const auto &cn : col_order) {
            auto dit = db->schema[tname].find(cn);
            if (dit != db->schema[tname].end() && !dit->second.default_val.empty())
                row[cn] = parse_literal(dit->second.default_val);
            else
                row[cn] = SvdbVal{};
        }

        int nv = svdb_ast_get_value_count(ast, ri);
        for (int ci = 0; ci < nv && ci < (int)ins_cols.size(); ++ci) {
            std::string vstr = svdb_ast_get_value(ast, ri, ci);
            row[ins_cols[ci]] = parse_literal(vstr);
        }

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
                size_t s = ap;
                while (ap < set_clause.size() && set_clause[ap] != ',' && !isspace((unsigned char)set_clause[ap])) ++ap;
                vstr = set_clause.substr(s, ap - s);
            }
            assignments.push_back({cn, vstr});
            while (ap < set_clause.size() && (isspace((unsigned char)set_clause[ap]) || set_clause[ap] == ',')) ++ap;
        }
    }

    const auto &col_order = db->col_order[tname];
    int64_t updated = 0;
    for (auto &row : db->data[tname]) {
        if (!eval_where(row, col_order, where_txt)) continue;
        for (const auto &asgn : assignments)
            row[asgn.first] = parse_literal(asgn.second);
        ++updated;
    }

    db->rows_affected = updated;
    if (res) { res->code = SVDB_OK; res->rows_affected = updated; }

    svdb_ast_node_free(ast);
    svdb_parser_destroy(p);
    return SVDB_OK;
}

static svdb_code_t do_delete(svdb_db_t *db, const std::string &sql,
                              svdb_result_t *res) {
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
    if (!db || !sql) return SVDB_ERR;
    if (res) { res->code = SVDB_OK; res->errmsg = ""; res->rows_affected = 0; res->last_insert_rowid = 0; }

    std::lock_guard<std::mutex> lk(db->mu);
    db->last_error.clear();
    db->rows_affected = 0;

    std::string s(sql);
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
        else if (what == "INDEX" || what == "UNIQUE") rc = SVDB_OK; /* ignore */
        else if (what == "VIEW")  rc = SVDB_OK; /* ignore */
        else                      rc = SVDB_OK;
    } else if (kw == "DROP") {
        std::string su = str_upper(s);
        size_t p = su.find("DROP") + 4;
        while (p < su.size() && isspace((unsigned char)su[p])) ++p;
        size_t s2 = p;
        while (p < su.size() && isalpha((unsigned char)su[p])) ++p;
        std::string what = su.substr(s2, p - s2);
        if (what == "TABLE") rc = do_drop_table(db, s);
        else                  rc = SVDB_OK; /* DROP INDEX/VIEW: ignore */
    } else if (kw == "ALTER") {
        rc = do_alter_table(db, s);
    } else if (kw == "INSERT") {
        rc = do_insert(db, s, res);
    } else if (kw == "UPDATE") {
        rc = do_update(db, s, res);
    } else if (kw == "DELETE") {
        rc = do_delete(db, s, res);
    } else if (kw == "BEGIN" || kw == "COMMIT" || kw == "ROLLBACK" ||
               kw == "SAVEPOINT" || kw == "RELEASE") {
        /* Transaction stubs — accepted, no-op */
        rc = SVDB_OK;
    } else if (kw == "PRAGMA" || kw == "VACUUM" || kw == "ANALYZE") {
        rc = SVDB_OK;
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
    svdb_tx_t *t = new (std::nothrow) svdb_tx_t();
    if (!t) return SVDB_NOMEM;
    t->db = db;
    *tx = t;
    return SVDB_OK;
}

svdb_code_t svdb_commit(svdb_tx_t *tx) {
    delete tx;
    return SVDB_OK;
}

svdb_code_t svdb_rollback(svdb_tx_t *tx) {
    delete tx;
    return SVDB_OK;
}

svdb_code_t svdb_savepoint(svdb_tx_t *tx, const char *name) {
    (void)tx; (void)name;
    return SVDB_OK;
}

svdb_code_t svdb_release(svdb_tx_t *tx, const char *name) {
    (void)tx; (void)name;
    return SVDB_OK;
}

svdb_code_t svdb_rollback_to(svdb_tx_t *tx, const char *name) {
    (void)tx; (void)name;
    return SVDB_OK;
}

/* Schema introspection */

svdb_code_t svdb_tables(svdb_db_t *db, svdb_rows_t **rows) {
    if (!db || !rows) return SVDB_ERR;
    svdb_rows_t *r = new (std::nothrow) svdb_rows_t();
    if (!r) return SVDB_NOMEM;
    r->col_names = {"name"};
    for (const auto &kv : db->schema) {
        SvdbVal sv; sv.type = SVDB_TYPE_TEXT; sv.sval = kv.first;
        r->rows.push_back({sv});
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
    (void)db; (void)table;
    if (!rows) return SVDB_ERR;
    *rows = new (std::nothrow) svdb_rows_t();
    if (!*rows) return SVDB_NOMEM;
    (*rows)->col_names = {"name"};
    return SVDB_OK;
}

svdb_code_t svdb_backup(svdb_db_t *src, const char *dest_path) {
    (void)src; (void)dest_path;
    return SVDB_OK;
}

} /* extern "C" */
