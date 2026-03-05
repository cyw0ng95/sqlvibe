#ifndef SVDB_QP_TYPE_INFER_H
#define SVDB_QP_TYPE_INFER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* SQL type constants */
#define SVDB_TYPE_ANY   0
#define SVDB_TYPE_INT   1
#define SVDB_TYPE_FLOAT 2
#define SVDB_TYPE_TEXT  3
#define SVDB_TYPE_BLOB  4
#define SVDB_TYPE_BOOL  5
#define SVDB_TYPE_NULL  6

/*
 * Infer the type of a literal value string.
 * integer literal  → SVDB_TYPE_INT
 * float (contains '.') → SVDB_TYPE_FLOAT
 * "NULL" (case-insensitive) → SVDB_TYPE_NULL
 * otherwise        → SVDB_TYPE_TEXT
 */
int svdb_type_infer_literal(const char* value_str, size_t value_len);

/*
 * Promote two types per SQL rules:
 *   NULL + anything  → other type
 *   INT  + FLOAT     → FLOAT
 *   same types       → that type
 *   otherwise        → ANY
 */
int svdb_type_promote(int a, int b);

/*
 * Parse a SQL type name string into an SVDB_TYPE_* constant.
 * Recognises: INTEGER/INT → INT, TEXT/VARCHAR → TEXT, REAL/DOUBLE/FLOAT → FLOAT,
 *             BLOB → BLOB, BOOLEAN/BOOL → BOOL, NULL → NULL.
 * Unknown names → ANY.
 */
int svdb_type_from_name(const char* type_str, size_t type_len);

/*
 * Return the return type of a built-in SQL function by name.
 *   COUNT/LENGTH/INSTR                            → INT
 *   AVG/ROUND/ABS/CEIL/FLOOR                      → FLOAT
 *   UPPER/LOWER/TRIM/SUBSTR/REPLACE/PRINTF/FORMAT → TEXT
 *   others                                         → ANY
 */
int svdb_type_get_func_return_type(const char* func_name, size_t func_name_len);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_QP_TYPE_INFER_H */
