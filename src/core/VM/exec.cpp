#include "exec.h"
#include "../SF/svdb_assert.h"
#include <cstring>
#include <cctype>

/* ------------------------------------------------------------------ helpers */

static int icase_find_e(const char* sql, size_t sql_len, const char* needle)
{
    svdb_assert_msg(sql != nullptr, "sql cannot be null");
    svdb_assert_msg(needle != nullptr, "needle cannot be null");
    
    if (!sql || !needle) return 0;
    size_t nl = strlen(needle);
    if (!nl || nl > sql_len) return 0;
    for (size_t i = 0; i <= sql_len - nl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < nl; ++j)
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)needle[j])) { ok = false; break; }
        if (ok) return 1;
    }
    return 0;
}

/* ------------------------------------------------------------------ API */

extern "C" {

int svdb_exec_is_result_cache_eligible(const char* sql, size_t sql_len)
{
    svdb_assert_msg(sql != nullptr, "sql cannot be null");
    
    if (!sql || sql_len == 0) return 0;

    /* Must be SELECT */
    size_t i = 0;
    while (i < sql_len && isspace((unsigned char)sql[i])) ++i;
    if (sql_len - i < 6) return 0;
    char sel[7] = {};
    for (int k = 0; k < 6; ++k)
        sel[k] = (char)tolower((unsigned char)sql[i + k]);
    if (strncmp(sel, "select", 6) != 0) return 0;

    /* Non-deterministic functions that disqualify caching */
    static const char* volatile_funcs[] = {
        "RANDOM(", "RANDOMBLOB(", "NOW(", "CURRENT_TIMESTAMP",
        "CURRENT_TIME", "CURRENT_DATE", "STRFTIME(", nullptr
    };
    for (int j = 0; volatile_funcs[j]; ++j)
        if (icase_find_e(sql, sql_len, volatile_funcs[j])) return 0;

    return 1;
}

size_t svdb_exec_estimate_result_size(int num_cols, int num_rows)
{
    svdb_assert_msg(num_cols > 0, "num_cols must be positive: %d", num_cols);
    svdb_assert_msg(num_rows > 0, "num_rows must be positive: %d", num_rows);
    
    if (num_cols <= 0 || num_rows <= 0) return 0;
    return (size_t)num_cols * (size_t)num_rows * 8u;
}

int svdb_exec_should_use_columnar(int num_cols, int num_rows)
{
    svdb_assert_msg(num_cols >= 0, "num_cols cannot be negative: %d", num_cols);
    svdb_assert_msg(num_rows >= 0, "num_rows cannot be negative: %d", num_rows);
    
    return (num_cols >= 4 && num_rows >= 1000) ? 1 : 0;
}

int svdb_exec_max_inline_rows(void)
{
    return 128;
}

uint64_t svdb_exec_compute_hash(const char* sql, size_t sql_len)
{
    svdb_assert_msg(sql != nullptr, "sql cannot be null");
    
    /* FNV-1a 64-bit */
    uint64_t hash = 14695981039346656037ULL;
    const unsigned char* p = (const unsigned char*)sql;
    for (size_t i = 0; i < sql_len; ++i) {
        hash ^= (uint64_t)p[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

int svdb_exec_normalize_whitespace(const char* sql, size_t sql_len,
                                    char* out_buf, int out_buf_size)
{
    svdb_assert_msg(sql != nullptr, "sql cannot be null");
    svdb_assert_msg(out_buf != nullptr, "out_buf cannot be null");
    svdb_assert_msg(out_buf_size > 0, "out_buf_size must be positive: %d", out_buf_size);
    
    if (!sql || !out_buf || out_buf_size <= 0) return -1;

    char* w    = out_buf;
    char* wend = out_buf + out_buf_size - 1;
    bool  last_space = true; /* suppress leading whitespace */

    for (size_t i = 0; i < sql_len; ++i) {
        unsigned char c = (unsigned char)sql[i];
        if (isspace(c)) {
            if (!last_space) {
                if (w >= wend) return -1;
                *w++ = ' ';
                last_space = true;
            }
        } else {
            if (w >= wend) return -1;
            *w++ = (char)c;
            last_space = false;
        }
    }

    /* Trim trailing space */
    if (w > out_buf && *(w - 1) == ' ') --w;

    *w = '\0';
    return (int)(w - out_buf);
}

} /* extern "C" */
