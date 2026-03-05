#include "tokenizer.h"
#include <cctype>
#include <cstring>

/* Simple but fast SQL tokenizer.
 * - Identifiers and keywords are distinguished only at the type level here;
 *   the Go layer converts SVDB_TOK_IDENTIFIER to SVDB_TOK_KEYWORD when the
 *   word is in the keyword table.
 * - Single-quoted strings and double-quoted identifiers are both TOKEN_STRING
 *   here; Go layer re-maps as needed.
 * - Block comments (/ * ... * /) and line comments (--) are emitted as
 *   SVDB_TOK_COMMENT so the Go layer can skip them cheaply.
 */

extern "C" {

static inline int is_ident_start(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}
static inline int is_ident_cont(unsigned char c) {
    return is_ident_start(c) || (c >= '0' && c <= '9');
}

static size_t do_tokenize(
    const char*   sql,
    size_t        sql_len,
    svdb_token_t* out,
    size_t        max_out
) {
    size_t n = 0;
    size_t i = 0;

#define EMIT(t, s, e) do { \
    if (out && n < max_out) { out[n].type = (t); out[n].start = (int32_t)(s); out[n].end = (int32_t)(e); } \
    ++n; \
} while(0)

    while (i < sql_len) {
        unsigned char c = (unsigned char)sql[i];

        /* Whitespace */
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            size_t start = i;
            while (i < sql_len && ((unsigned char)sql[i] == ' ' || (unsigned char)sql[i] == '\t'
                                   || (unsigned char)sql[i] == '\n' || (unsigned char)sql[i] == '\r')) ++i;
            /* skip whitespace - don't emit */
            (void)start;
            continue;
        }

        /* Line comment -- */
        if (c == '-' && i + 1 < sql_len && sql[i + 1] == '-') {
            size_t start = i;
            while (i < sql_len && sql[i] != '\n') ++i;
            EMIT(SVDB_TOK_COMMENT, start, i);
            continue;
        }

        /* Block comment */
        if (c == '/' && i + 1 < sql_len && sql[i + 1] == '*') {
            size_t start = i;
            i += 2;
            while (i + 1 < sql_len && !(sql[i] == '*' && sql[i + 1] == '/')) ++i;
            if (i + 1 < sql_len) i += 2;
            EMIT(SVDB_TOK_COMMENT, start, i);
            continue;
        }

        /* String literal (single-quoted) */
        if (c == '\'') {
            size_t start = i++;
            while (i < sql_len) {
                if (sql[i] == '\'' && i + 1 < sql_len && sql[i + 1] == '\'') {
                    i += 2; /* escaped quote */
                } else if (sql[i] == '\'') {
                    ++i; break;
                } else {
                    ++i;
                }
            }
            EMIT(SVDB_TOK_STRING, start, i);
            continue;
        }

        /* Double-quoted identifier or string */
        if (c == '"') {
            size_t start = i++;
            while (i < sql_len && sql[i] != '"') ++i;
            if (i < sql_len) ++i;
            EMIT(SVDB_TOK_STRING, start, i);
            continue;
        }

        /* Back-tick quoted identifier */
        if (c == '`') {
            size_t start = i++;
            while (i < sql_len && sql[i] != '`') ++i;
            if (i < sql_len) ++i;
            EMIT(SVDB_TOK_IDENTIFIER, start, i);
            continue;
        }

        /* Named parameter :name or positional ? */
        if (c == '?') {
            EMIT(SVDB_TOK_PARAM, i, i + 1);
            ++i;
            continue;
        }
        if (c == ':' && i + 1 < sql_len && is_ident_start((unsigned char)sql[i + 1])) {
            size_t start = i++;
            while (i < sql_len && is_ident_cont((unsigned char)sql[i])) ++i;
            EMIT(SVDB_TOK_NAMED_PARAM, start, i);
            continue;
        }

        /* Numeric literal */
        if ((c >= '0' && c <= '9') || (c == '.' && i + 1 < sql_len && sql[i + 1] >= '0' && sql[i + 1] <= '9')) {
            size_t start = i;
            int is_float = 0;
            while (i < sql_len && (sql[i] >= '0' && sql[i] <= '9')) ++i;
            if (i < sql_len && sql[i] == '.') { is_float = 1; ++i; while (i < sql_len && sql[i] >= '0' && sql[i] <= '9') ++i; }
            if (i < sql_len && (sql[i] == 'e' || sql[i] == 'E')) {
                is_float = 1; ++i;
                if (i < sql_len && (sql[i] == '+' || sql[i] == '-')) ++i;
                while (i < sql_len && sql[i] >= '0' && sql[i] <= '9') ++i;
            }
            /* Hex literal */
            if (!is_float && i - start == 1 && sql[start] == '0' && i < sql_len && (sql[i] == 'x' || sql[i] == 'X')) {
                ++i;
                while (i < sql_len && ((sql[i] >= '0' && sql[i] <= '9') || (sql[i] >= 'a' && sql[i] <= 'f') || (sql[i] >= 'A' && sql[i] <= 'F'))) ++i;
            }
            EMIT(is_float ? SVDB_TOK_FLOAT : SVDB_TOK_INTEGER, start, i);
            continue;
        }

        /* Identifier or keyword */
        if (is_ident_start(c)) {
            size_t start = i;
            while (i < sql_len && is_ident_cont((unsigned char)sql[i])) ++i;
            EMIT(SVDB_TOK_IDENTIFIER, start, i);
            continue;
        }

        /* Two-char operators */
        if (i + 1 < sql_len) {
            char c2 = sql[i + 1];
            if ((c == '<' && c2 == '=') || (c == '>' && c2 == '=') ||
                (c == '<' && c2 == '>') || (c == '!' && c2 == '=') ||
                (c == '|' && c2 == '|') || (c == '<' && c2 == '<') ||
                (c == '>' && c2 == '>')) {
                EMIT(SVDB_TOK_OPERATOR, i, i + 2);
                i += 2;
                continue;
            }
        }

        /* Single-char operator or punctuation */
        int typ = SVDB_TOK_PUNCT;
        if (c == '+' || c == '-' || c == '*' || c == '/' || c == '%' ||
            c == '<' || c == '>' || c == '=' || c == '&' || c == '|' || c == '~') {
            typ = SVDB_TOK_OPERATOR;
        }
        EMIT(typ, i, i + 1);
        ++i;
    }

    /* EOF */
    EMIT(SVDB_TOK_EOF, i, i);
    return n;
#undef EMIT
}

size_t svdb_token_count(const char* sql, size_t sql_len) {
    return do_tokenize(sql, sql_len, NULL, 0);
}

size_t svdb_tokenize(
    const char*   sql,
    size_t        sql_len,
    svdb_token_t* tokens,
    size_t        max_tokens
) {
    return do_tokenize(sql, sql_len, tokens, max_tokens);
}

} // extern "C"
