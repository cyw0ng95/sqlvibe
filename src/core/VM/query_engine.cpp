#include "query_engine.h"
#include <cstring>
#include <cctype>

/* ------------------------------------------------------------------ helpers */

static int icase_starts(const char* sql, size_t sql_len,
                         const char* keyword)
{
    /* Skip leading whitespace */
    size_t i = 0;
    while (i < sql_len && isspace((unsigned char)sql[i])) ++i;
    size_t kl = strlen(keyword);
    if (sql_len - i < kl) return 0;
    for (size_t j = 0; j < kl; ++j)
        if (tolower((unsigned char)sql[i + j]) !=
            tolower((unsigned char)keyword[j])) return 0;
    /* Ensure it is a full word */
    size_t after = i + kl;
    if (after < sql_len && (isalnum((unsigned char)sql[after]) || sql[after] == '_'))
        return 0;
    return 1;
}

static int icase_find_word(const char* sql, size_t sql_len,
                            const char* keyword)
{
    size_t kl = strlen(keyword);
    if (!kl || kl > sql_len) return 0;
    for (size_t i = 0; i <= sql_len - kl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < kl; ++j)
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)keyword[j])) { ok = false; break; }
        if (!ok) continue;
        /* word boundary check */
        if (i > 0 && (isalnum((unsigned char)sql[i-1]) || sql[i-1] == '_'))
            continue;
        size_t after = i + kl;
        if (after < sql_len && (isalnum((unsigned char)sql[after]) || sql[after] == '_'))
            continue;
        return 1;
    }
    return 0;
}

static const char* skip_ws_n(const char* p, const char* end)
{
    while (p < end && isspace((unsigned char)*p)) ++p;
    return p;
}

static int read_ident_n(const char* p, const char* end,
                         char* out_buf, int out_buf_size)
{
    const char* start = p;
    while (p < end && (isalnum((unsigned char)*p) || *p == '_')) ++p;
    int len = (int)(p - start);
    if (!out_buf || out_buf_size < len + 1) return -1;
    memcpy(out_buf, start, (size_t)len);
    out_buf[len] = '\0';
    return len;
}

/* Find position of keyword (case-insensitive); -1 if not found */
static int icase_pos_n(const char* sql, size_t sql_len, const char* kw)
{
    size_t kl = strlen(kw);
    if (!kl || kl > sql_len) return -1;
    for (size_t i = 0; i <= sql_len - kl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < kl; ++j)
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)kw[j])) { ok = false; break; }
        if (ok) return (int)i;
    }
    return -1;
}

/* ------------------------------------------------------------------ API */

extern "C" {

int svdb_qe_classify_query(const char* sql, size_t sql_len)
{
    if (!sql || sql_len == 0) return 0;
    if (icase_starts(sql, sql_len, "SELECT"))    return 1;
    if (icase_starts(sql, sql_len, "INSERT"))    return 2;
    if (icase_starts(sql, sql_len, "UPDATE"))    return 3;
    if (icase_starts(sql, sql_len, "DELETE"))    return 4;
    if (icase_starts(sql, sql_len, "CREATE"))    return 5;
    if (icase_starts(sql, sql_len, "DROP"))      return 6;
    if (icase_starts(sql, sql_len, "ALTER"))     return 7;
    if (icase_starts(sql, sql_len, "BEGIN"))     return 8;
    if (icase_starts(sql, sql_len, "COMMIT"))    return 9;
    if (icase_starts(sql, sql_len, "ROLLBACK"))  return 10;
    if (icase_starts(sql, sql_len, "PRAGMA"))    return 11;
    return 0;
}

int svdb_qe_extract_table_name(const char* sql, size_t sql_len,
                                char* out_buf, int out_buf_size)
{
    if (!sql || !out_buf || out_buf_size <= 0) return -1;

    static const char* kws[] = { "FROM", "INTO", "UPDATE", "TABLE", nullptr };
    for (int i = 0; kws[i]; ++i) {
        int pos = icase_pos_n(sql, sql_len, kws[i]);
        if (pos < 0) continue;
        const char* p   = sql + pos + strlen(kws[i]);
        const char* end = sql + sql_len;
        p = skip_ws_n(p, end);
        if (p >= end) continue;
        int len = read_ident_n(p, end, out_buf, out_buf_size);
        if (len > 0) return len;
    }
    return -1;
}

int svdb_qe_is_read_only(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int t = svdb_qe_classify_query(sql, sql_len);
    return (t == 1 || t == 11) ? 1 : 0;
}

int svdb_qe_is_transaction(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int t = svdb_qe_classify_query(sql, sql_len);
    if (t == 8 || t == 9 || t == 10) return 1;
    if (icase_starts(sql, sql_len, "SAVEPOINT")) return 1;
    return 0;
}

int svdb_qe_needs_schema(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int t = svdb_qe_classify_query(sql, sql_len);
    return (t >= 1 && t <= 6) ? 1 : 0;
}

int svdb_qe_strip_comments(const char* sql, size_t sql_len,
                             char* out_buf, int out_buf_size)
{
    if (!sql || !out_buf || out_buf_size <= 0) return -1;

    char* w    = out_buf;
    char* wend = out_buf + out_buf_size - 1;
    size_t i   = 0;
    bool in_str = false;

    while (i < sql_len) {
        /* String literal */
        if (!in_str && sql[i] == '\'') {
            in_str = true;
            if (w >= wend) return -1;
            *w++ = sql[i++];
            continue;
        }
        if (in_str && sql[i] == '\'') {
            if (i + 1 < sql_len && sql[i + 1] == '\'') {
                if (w + 1 >= wend) return -1;
                *w++ = '\''; *w++ = '\'';
                i += 2; continue;
            }
            in_str = false;
            if (w >= wend) return -1;
            *w++ = sql[i++];
            continue;
        }
        if (in_str) {
            if (w >= wend) return -1;
            *w++ = sql[i++];
            continue;
        }

        /* Line comment -- */
        if (i + 1 < sql_len && sql[i] == '-' && sql[i + 1] == '-') {
            while (i < sql_len && sql[i] != '\n') ++i;
            continue;
        }

        /* Block comment */
        if (i + 1 < sql_len && sql[i] == '/' && sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql_len) {
                if (sql[i] == '*' && sql[i + 1] == '/') { i += 2; break; }
                ++i;
            }
            continue;
        }

        if (w >= wend) return -1;
        *w++ = sql[i++];
    }

    *w = '\0';
    return (int)(w - out_buf);
}

} /* extern "C" */
