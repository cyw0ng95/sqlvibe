#include "binder.h"
#include <cstring>
#include <cctype>
#include <cstdlib>
#include <vector>
#include <string>

/* ------------------------------------------------------------------ helpers */

/* Returns true if the character at position p inside sql is inside a string */
static bool in_string_literal(const char* sql, size_t pos)
{
    bool in_str = false;
    for (size_t i = 0; i < pos; ++i) {
        if (sql[i] == '\'' && !in_str) {
            in_str = true;
        } else if (sql[i] == '\'' && in_str) {
            if (i + 1 < pos && sql[i + 1] == '\'')
                ++i; /* escaped '' */
            else
                in_str = false;
        }
    }
    return in_str;
}

/* Check whether position i starts a named placeholder (:name or @name) */
static bool is_named_start(char c)
{
    return c == ':' || c == '@';
}

static bool is_ident_char(char c)
{
    return isalnum((unsigned char)c) || c == '_';
}

/* ------------------------------------------------------------------ API */

extern "C" {

int svdb_binder_count_positional(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int count = 0;
    bool in_str = false;
    for (size_t i = 0; i < sql_len; ++i) {
        char c = sql[i];
        if (c == '\'' && !in_str) { in_str = true; continue; }
        if (c == '\'' && in_str) {
            if (i + 1 < sql_len && sql[i + 1] == '\'') { ++i; continue; }
            in_str = false; continue;
        }
        if (!in_str && c == '?') ++count;
    }
    return count;
}

int svdb_binder_count_named(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int count = 0;
    bool in_str = false;
    for (size_t i = 0; i < sql_len; ++i) {
        char c = sql[i];
        if (c == '\'' && !in_str) { in_str = true; continue; }
        if (c == '\'' && in_str) {
            if (i + 1 < sql_len && sql[i + 1] == '\'') { ++i; continue; }
            in_str = false; continue;
        }
        if (!in_str && is_named_start(c) &&
            i + 1 < sql_len && is_ident_char(sql[i + 1])) {
            ++count;
            /* skip identifier */
            while (i + 1 < sql_len && is_ident_char(sql[i + 1])) ++i;
        }
    }
    return count;
}

int svdb_binder_count_placeholders(const char* sql, size_t sql_len)
{
    return svdb_binder_count_positional(sql, sql_len) +
           svdb_binder_count_named(sql, sql_len);
}

int svdb_binder_get_named_param(const char* sql,
                                 size_t      sql_len,
                                 int         idx,
                                 char*       out_buf,
                                 int         out_buf_size)
{
    if (!sql || !out_buf || out_buf_size <= 0 || idx < 0) return -1;
    int found = 0;
    bool in_str = false;
    for (size_t i = 0; i < sql_len; ++i) {
        char c = sql[i];
        if (c == '\'' && !in_str) { in_str = true; continue; }
        if (c == '\'' && in_str) {
            if (i + 1 < sql_len && sql[i + 1] == '\'') { ++i; continue; }
            in_str = false; continue;
        }
        if (!in_str && is_named_start(c) &&
            i + 1 < sql_len && is_ident_char(sql[i + 1])) {
            size_t start = i + 1;
            size_t end   = start;
            while (end < sql_len && is_ident_char(sql[end])) ++end;
            if (found == idx) {
                int len = (int)(end - start);
                if (out_buf_size < len + 1) return -1;
                memcpy(out_buf, sql + start, (size_t)len);
                out_buf[len] = '\0';
                return len;
            }
            ++found;
            i = end - 1;
        }
    }
    return -1;
}

int svdb_binder_substitute_positional(const char* sql,
                                       size_t      sql_len,
                                       const char* values_json,
                                       char*       out_buf,
                                       int         out_buf_size)
{
    if (!sql || !values_json || !out_buf || out_buf_size <= 0) return -1;

    /* Parse JSON array: ["v1","v2",...] */
    std::vector<std::string> values;
    {
        const char* p = values_json;
        while (*p && *p != '[') ++p;
        if (*p == '[') ++p;
        while (*p && *p != ']') {
            while (*p == ' ' || *p == ',' || *p == '\t') ++p;
            if (*p == '"') {
                ++p;
                std::string v;
                while (*p && *p != '"') {
                    if (*p == '\\' && *(p+1)) { ++p; }
                    v += *p++;
                }
                if (*p == '"') ++p;
                values.push_back(v);
            } else if (*p != ']' && *p != '\0') {
                /* numeric or unquoted value */
                const char* start = p;
                while (*p && *p != ',' && *p != ']') ++p;
                values.push_back(std::string(start, p));
            }
        }
    }

    char* w   = out_buf;
    char* wend = out_buf + out_buf_size - 1;
    int   vi  = 0;
    bool  in_str = false;

    for (size_t i = 0; i < sql_len; ++i) {
        char c = sql[i];
        if (c == '\'' && !in_str) { in_str = true; }
        else if (c == '\'' && in_str) {
            if (i + 1 < sql_len && sql[i + 1] == '\'') {
                /* escaped quote - copy both */
                if (w + 1 >= wend) return -1;
                *w++ = c;
                *w++ = sql[++i];
                continue;
            }
            in_str = false;
        }

        if (!in_str && c == '?') {
            /* substitute value */
            const char* val = (vi < (int)values.size()) ? values[vi].c_str() : "NULL";
            ++vi;
            /* Check if val looks numeric: optional leading '-', digits, at most one '.' */
            bool numeric = (val && *val != '\0');
            if (numeric) {
                const char* q = val;
                if (*q == '-') ++q;  /* allow leading minus */
                if (!*q) numeric = false;  /* just '-' is not numeric */
                bool seen_dot = false;
                for (; *q; ++q) {
                    if (*q == '.') {
                        if (seen_dot) { numeric = false; break; }
                        seen_dot = true;
                    } else if (!isdigit((unsigned char)*q)) {
                        numeric = false; break;
                    }
                }
            }
            if (!numeric) {
                if (w >= wend) return -1;
                *w++ = '\'';
                for (const char* q = val; *q; ++q) {
                    if (w >= wend) return -1;
                    if (*q == '\'') { *w++ = '\''; if (w >= wend) return -1; }
                    *w++ = *q;
                }
                if (w >= wend) return -1;
                *w++ = '\'';
            } else {
                for (const char* q = val; *q; ++q) {
                    if (w >= wend) return -1;
                    *w++ = *q;
                }
            }
        } else {
            if (w >= wend) return -1;
            *w++ = c;
        }
    }

    *w = '\0';
    return (int)(w - out_buf);
}

} /* extern "C" */
