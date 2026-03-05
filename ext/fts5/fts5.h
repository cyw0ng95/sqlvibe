#ifndef SVDB_EXT_FTS5_H
#define SVDB_EXT_FTS5_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Token types
typedef enum {
    SVDB_FTS5_TOKEN_ASCII,
    SVDB_FTS5_TOKEN_PORTER,
    SVDB_FTS5_TOKEN_UNICODE61
} svdb_fts5_tokenizer_type_t;

// Token structure
typedef struct {
    char* term;
    int start;
    int end;
    int position;
} svdb_fts5_token_t;

// Tokenizer handle
typedef struct svdb_fts5_tokenizer svdb_fts5_tokenizer_t;

// Index handle
typedef struct svdb_fts5_index svdb_fts5_index_t;

// Ranker handle
typedef struct svdb_fts5_ranker svdb_fts5_ranker_t;

// Tokenizer functions
svdb_fts5_tokenizer_t* svdb_fts5_tokenizer_create(svdb_fts5_tokenizer_type_t type);
void svdb_fts5_tokenizer_destroy(svdb_fts5_tokenizer_t* tokenizer);

svdb_fts5_token_t* svdb_fts5_tokenize(svdb_fts5_tokenizer_t* tokenizer, const char* text, int* token_count);
void svdb_fts5_token_free(svdb_fts5_token_t* token);

// Index functions
svdb_fts5_index_t* svdb_fts5_index_create(int column_count);
void svdb_fts5_index_destroy(svdb_fts5_index_t* index);

int svdb_fts5_index_add_document(svdb_fts5_index_t* index, int64_t doc_id, const char* const* column_values, int column_count);
int64_t* svdb_fts5_index_query(svdb_fts5_index_t* index, const char* query, int* doc_count);

// Index metadata
int svdb_fts5_index_get_doc_count(svdb_fts5_index_t* index);
int svdb_fts5_index_get_term_count(svdb_fts5_index_t* index, const char* term);
int svdb_fts5_index_get_doc_length(svdb_fts5_index_t* index, int64_t doc_id);
double svdb_fts5_index_get_avg_doc_length(svdb_fts5_index_t* index);

// Ranker functions
svdb_fts5_ranker_t* svdb_fts5_ranker_create(svdb_fts5_index_t* index, double k1, double b);
void svdb_fts5_ranker_destroy(svdb_fts5_ranker_t* ranker);

double svdb_fts5_ranker_score(svdb_fts5_ranker_t* ranker, int64_t doc_id, const char* const* terms, int term_count);
double svdb_fts5_bm25(int doc_len, double avg_dl, int tf, int df, int n, double k1, double b);

// Helper functions
void svdb_fts5_free_string(char* str);
void svdb_fts5_free_int64_array(int64_t* arr);

#ifdef __cplusplus
}
#endif

#endif // SVDB_EXT_FTS5_H
