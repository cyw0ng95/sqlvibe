#ifndef SVDB_TYPES_H
#define SVDB_TYPES_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Value types */
#define SVDB_VAL_NULL   0
#define SVDB_VAL_INT    1
#define SVDB_VAL_FLOAT  2
#define SVDB_VAL_TEXT   3
#define SVDB_VAL_BLOB   4
#define SVDB_VAL_BOOL   5

typedef struct {
    int32_t     val_type;
    int64_t     int_val;
    double      float_val;
    char*       str_data;  /* writable for internal use */
    size_t      str_len;
    char*       bytes_data;  /* writable for internal use */
    size_t      bytes_len;
} svdb_value_t;

#ifdef __cplusplus
}
#endif
#endif /* SVDB_TYPES_H */
