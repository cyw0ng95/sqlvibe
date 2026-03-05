#include "normalize.h"
#include <cctype>
#include <cstring>

/* ------------------------------------------------------------------ impl */

/*
 * State machine states:
 *   NORMAL      - default SQL text
 *   IN_STRING   - inside 'single-quoted literal'
 *   IN_NUMBER   - inside a numeric literal
 */
enum NormState { NORMAL, IN_STRING, IN_NUMBER, IN_NUMBER_FRAC };

/*
 * Core normalization engine.  Writes into out/out_end; returns final
 * write pointer.  Caller ensures out has sufficient space.
 */
static char* normalize_core(const char* p,
                              const char* end,
                              char*       out,
                              const char* out_end)
{
#define EMIT(c) do { if (out < out_end) *out++ = (c); } while(0)

    NormState state = NORMAL;
    bool last_was_space = true; /* suppress leading whitespace */

    while (p < end) {
        unsigned char ch = (unsigned char)*p;

        if (state == IN_STRING) {
            /* consume until closing quote, handle '' escape */
            if (ch == '\'') {
                if (p + 1 < end && *(p + 1) == '\'') {
                    p += 2; /* escaped quote inside string */
                } else {
                    ++p; /* closing quote */
                    state = NORMAL;
                    /* emit replacement placeholder */
                    EMIT('?');
                    last_was_space = false;
                }
            } else {
                ++p; /* skip string content */
            }
            continue;
        }

        if (state == IN_NUMBER) {
            if (isdigit(ch)) {
                ++p;
            } else if (ch == '.') {
                state = IN_NUMBER_FRAC;
                ++p;
            } else {
                state = NORMAL;
                EMIT('?');
                last_was_space = false;
                /* don't advance p - reprocess current char */
            }
            continue;
        }

        if (state == IN_NUMBER_FRAC) {
            if (isdigit(ch)) {
                ++p;
            } else {
                state = NORMAL;
                EMIT('?');
                last_was_space = false;
                /* don't advance p - reprocess current char */
            }
            continue;
        }

        /* NORMAL state */
        if (ch == '\'') {
            state = IN_STRING;
            ++p;
            continue;
        }

        if (isdigit(ch)) {
            state = IN_NUMBER;
            ++p;
            continue;
        }

        if (isspace(ch)) {
            if (!last_was_space) {
                EMIT(' ');
                last_was_space = true;
            }
            ++p;
            continue;
        }

        EMIT((char)tolower(ch));
        last_was_space = false;
        ++p;
    }

    /* flush pending number */
    if (state == IN_NUMBER || state == IN_NUMBER_FRAC) {
        EMIT('?');
    }

#undef EMIT
    return out;
}

/* Trim a single trailing space if present */
static inline char* trim_trailing(char* start, char* end)
{
    if (end > start && *(end - 1) == ' ') --end;
    return end;
}

extern "C" {

int svdb_normalize_query(const char* sql,
                          size_t      sql_len,
                          char*       out_buf,
                          size_t      out_buf_size)
{
    if (!sql || !out_buf || out_buf_size == 0) return -1;

    /* Trim leading whitespace in input */
    const char* p   = sql;
    const char* end = sql + sql_len;
    while (p < end && isspace((unsigned char)*p)) ++p;

    char* w_start = out_buf;
    char* w_end   = out_buf + out_buf_size - 1; /* reserve for NUL */

    char* w = normalize_core(p, end, w_start, w_end);
    w = trim_trailing(w_start, w);

    /* Check overflow: if w reached w_end the buffer was too small */
    size_t written = (size_t)(w - w_start);
    if (written >= out_buf_size) {
        out_buf[0] = '\0';
        return -1;
    }

    *w = '\0';
    return (int)written;
}

size_t svdb_normalize_get_required_size(const char* sql, size_t sql_len)
{
    if (!sql) return 1;
    /* Worst case: every char becomes a char + NUL */
    return sql_len + 2;
}

} /* extern "C" */
