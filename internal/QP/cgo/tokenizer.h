#ifndef SVDB_QP_TOKENIZER_H
#define SVDB_QP_TOKENIZER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Token types — must match QP.TokenType constants in tokenizer.go */
#define SVDB_TOK_EOF         0
#define SVDB_TOK_IDENTIFIER  1
#define SVDB_TOK_INTEGER     2
#define SVDB_TOK_FLOAT       3
#define SVDB_TOK_STRING      4
#define SVDB_TOK_KEYWORD     5
#define SVDB_TOK_PUNCT       6
#define SVDB_TOK_OPERATOR    7
#define SVDB_TOK_PARAM       8
#define SVDB_TOK_NAMED_PARAM 9
#define SVDB_TOK_COMMENT     10
#define SVDB_TOK_WHITESPACE  11

typedef struct {
    int     type;   /* one of SVDB_TOK_* */
    int32_t start;  /* byte offset in original SQL */
    int32_t end;    /* exclusive end offset */
} svdb_token_t;

/*
 * Count SQL tokens without allocating.
 * Returns number of tokens (including EOF).
 */
size_t svdb_token_count(const char* sql, size_t sql_len);

/*
 * Tokenise sql into tokens[0..max_tokens-1].
 * Returns number of tokens written (including EOF).
 * Caller must ensure tokens has at least svdb_token_count(sql,len) entries.
 */
size_t svdb_tokenize(
    const char*   sql,
    size_t        sql_len,
    svdb_token_t* tokens,
    size_t        max_tokens
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_TOKENIZER_H */
