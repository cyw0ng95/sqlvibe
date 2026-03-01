#include "direct_compiler.h"
#include <cstring>
#include <cctype>
#include <cstdlib>

/* ------------------------------------------------------------------ helpers */

static int icase_find_n(const char* sql, size_t sql_len, const char* needle)
{
    if (!sql || !needle) return 0;
    size_t nl = strlen(needle);
    if (nl == 0 || nl > sql_len) return 0;
    for (size_t i = 0; i <= sql_len - nl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < nl; ++j) {
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)needle[j])) {
                ok = false; break;
            }
        }
        if (ok) return 1;
    }
    return 0;
}

/* Find position of keyword (case-insensitive); returns -1 if not found */
static int icase_pos(const char* sql, size_t sql_len, const char* keyword)
{
    size_t kl = strlen(keyword);
    if (kl == 0 || kl > sql_len) return -1;
    for (size_t i = 0; i <= sql_len - kl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < kl; ++j) {
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)keyword[j])) {
                ok = false; break;
            }
        }
        if (ok) return (int)i;
    }
    return -1;
}

/* Skip whitespace starting at p, return new pointer */
static const char* skip_ws(const char* p, const char* end)
{
    while (p < end && isspace((unsigned char)*p)) ++p;
    return p;
}

/* Read an identifier (alnum + _) starting at p into out_buf */
static int read_ident(const char* p, const char* end,
                       char* out_buf, int out_buf_size)
{
    const char* start = p;
    while (p < end && (isalnum((unsigned char)*p) || *p == '_' || *p == '.'))
        ++p;
    int len = (int)(p - start);
    /* Strip trailing dot if present */
    while (len > 0 && start[len - 1] == '.') --len;
    if (out_buf_size < len + 1) return -1;
    memcpy(out_buf, start, (size_t)len);
    out_buf[len] = '\0';
    return len;
}

/* Parse an integer starting at p, stopping at non-digit */
static int64_t parse_int64(const char* p, const char* end)
{
    int64_t v = 0;
    bool neg  = false;
    if (p < end && *p == '-') { neg = true; ++p; }
    while (p < end && isdigit((unsigned char)*p))
        v = v * 10 + (*p++ - '0');
    return neg ? -v : v;
}

/* ------------------------------------------------------------------ API */

extern "C" {

int svdb_direct_is_simple_select(const char* sql, size_t sql_len)
{
    if (!sql || sql_len == 0) return 0;
    static const char* blockers[] = {
        "WITH", "WINDOW", "OVER", "JOIN", "UNION", "INTERSECT", "EXCEPT",
        nullptr
    };
    for (int i = 0; blockers[i]; ++i)
        if (icase_find_n(sql, sql_len, blockers[i])) return 0;

    size_t pos = 0;
    while (pos < sql_len && isspace((unsigned char)sql[pos])) ++pos;
    if (sql_len - pos < 6) return 0;
    char sel[7] = {};
    for (int k = 0; k < 6; ++k)
        sel[k] = (char)tolower((unsigned char)sql[pos + k]);
    return strncmp(sel, "select", 6) == 0 ? 1 : 0;
}

int svdb_direct_extract_table_name(const char* sql,
                                    size_t      sql_len,
                                    char*       out_buf,
                                    int         out_buf_size)
{
    if (!sql || !out_buf || out_buf_size <= 0) return -1;

    int from_pos = icase_pos(sql, sql_len, "FROM");
    if (from_pos < 0) return -1;

    const char* p   = sql + from_pos + 4; /* skip "FROM" */
    const char* end = sql + sql_len;
    p = skip_ws(p, end);
    if (p >= end) return -1;

    return read_ident(p, end, out_buf, out_buf_size);
}

int64_t svdb_direct_extract_limit(const char* sql, size_t sql_len)
{
    if (!sql) return -1;
    int pos = icase_pos(sql, sql_len, "LIMIT");
    if (pos < 0) return -1;

    const char* p   = sql + pos + 5;
    const char* end = sql + sql_len;
    p = skip_ws(p, end);
    if (p >= end || !isdigit((unsigned char)*p)) return -1;
    return parse_int64(p, end);
}

int64_t svdb_direct_extract_offset(const char* sql, size_t sql_len)
{
    if (!sql) return 0;
    int pos = icase_pos(sql, sql_len, "OFFSET");
    if (pos < 0) return 0;

    const char* p   = sql + pos + 6;
    const char* end = sql + sql_len;
    p = skip_ws(p, end);
    if (p >= end || !isdigit((unsigned char)*p)) return 0;
    return parse_int64(p, end);
}

int svdb_direct_has_where(const char* sql, size_t sql_len)
{
    return icase_find_n(sql, sql_len, "WHERE");
}

int svdb_direct_has_order_by(const char* sql, size_t sql_len)
{
    return icase_find_n(sql, sql_len, "ORDER BY");
}

} /* extern "C" */
