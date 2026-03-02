#ifndef SVDB_DS_COLUMN_VECTOR_H
#define SVDB_DS_COLUMN_VECTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Column type constants */
#define SVDB_TYPE_NULL  0
#define SVDB_TYPE_INT   1
#define SVDB_TYPE_REAL  2
#define SVDB_TYPE_TEXT  3
#define SVDB_TYPE_BLOB  4

typedef struct svdb_column_vector svdb_column_vector_t;

/* Create a new column vector with the given name and type */
svdb_column_vector_t* svdb_column_vector_create(const char* name, int type);

/* Destroy a column vector */
void svdb_column_vector_destroy(svdb_column_vector_t* cv);

/* Get the number of elements */
int svdb_column_vector_len(const svdb_column_vector_t* cv);

/* Check if element at index is null */
int svdb_column_vector_is_null(const svdb_column_vector_t* cv, int idx);

/* Set null flag for element at index */
void svdb_column_vector_set_null(svdb_column_vector_t* cv, int idx, int is_null);

/* Append a null value */
void svdb_column_vector_append_null(svdb_column_vector_t* cv);

/* Append an int64 value */
void svdb_column_vector_append_int(svdb_column_vector_t* cv, int64_t val);

/* Append a double value */
void svdb_column_vector_append_float(svdb_column_vector_t* cv, double val);

/* Append a string value */
void svdb_column_vector_append_text(svdb_column_vector_t* cv, const char* val, size_t len);

/* Append a blob value */
void svdb_column_vector_append_blob(svdb_column_vector_t* cv, const uint8_t* val, size_t len);

/* Get int64 value at index */
int64_t svdb_column_vector_get_int(const svdb_column_vector_t* cv, int idx);

/* Get double value at index */
double svdb_column_vector_get_float(const svdb_column_vector_t* cv, int idx);

/* Get string value at index - returns pointer and sets out_len */
const char* svdb_column_vector_get_text(const svdb_column_vector_t* cv, int idx, size_t* out_len);

/* Get blob value at index - returns pointer and sets out_len */
const uint8_t* svdb_column_vector_get_blob(const svdb_column_vector_t* cv, int idx, size_t* out_len);

/* Clear all elements */
void svdb_column_vector_clear(svdb_column_vector_t* cv);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_DS_COLUMN_VECTOR_H */
