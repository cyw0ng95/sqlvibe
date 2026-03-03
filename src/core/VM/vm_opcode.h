#ifndef SVDB_VM_OPCODE_H
#define SVDB_VM_OPCODE_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Return codes from svdb_vm_dispatch_pure */
#define SVDB_PURE_OK        0   /* Handled; result written to *out_val */
#define SVDB_PURE_NOT_PURE  1   /* Opcode is not a pure register op; caller handles */
#define SVDB_PURE_ERROR    -1   /* Execution error (e.g., invalid register) */

/*
 * Go OpCode iota constants (must stay in sync with internal/VM/opcodes.go).
 * Only the opcodes routed through svdb_vm_dispatch_pure are listed.
 */
#define SVDB_OP_NULL           0
#define SVDB_OP_CONST_NULL     25
#define SVDB_OP_MOVE           26
#define SVDB_OP_COPY           27
#define SVDB_OP_SCOPY          28
#define SVDB_OP_INT_COPY       29
#define SVDB_OP_EQ             48
#define SVDB_OP_NE             49
#define SVDB_OP_LT             50
#define SVDB_OP_LE             51
#define SVDB_OP_GT             52
#define SVDB_OP_GE             53
#define SVDB_OP_IS             54
#define SVDB_OP_IS_NOT         55
#define SVDB_OP_IS_NULL        56
#define SVDB_OP_NOT_NULL       57
#define SVDB_OP_IF_NULL2       59
#define SVDB_OP_ADD            60
#define SVDB_OP_SUBTRACT       61
#define SVDB_OP_MULTIPLY       62
#define SVDB_OP_DIVIDE         63
#define SVDB_OP_REMAINDER      64
#define SVDB_OP_ADD_IMM        65
#define SVDB_OP_BIT_AND        66
#define SVDB_OP_BIT_OR         67
#define SVDB_OP_SHIFT_LEFT     68
#define SVDB_OP_SHIFT_RIGHT    69
#define SVDB_OP_CONCAT         70
#define SVDB_OP_LENGTH         72
#define SVDB_OP_UPPER          73
#define SVDB_OP_LOWER          74
#define SVDB_OP_TRIM           75
#define SVDB_OP_LTRIM          76
#define SVDB_OP_RTRIM          77
#define SVDB_OP_INSTR          79
#define SVDB_OP_LIKE           80
#define SVDB_OP_NOT_LIKE       81
#define SVDB_OP_GLOB           82
#define SVDB_OP_MATCH          83
#define SVDB_OP_ABS            89
#define SVDB_OP_ROUND          90
#define SVDB_OP_CEIL           91
#define SVDB_OP_CEILING        92
#define SVDB_OP_FLOOR          93
#define SVDB_OP_POW            94
#define SVDB_OP_SQRT           95
#define SVDB_OP_MOD            96
#define SVDB_OP_EXP            97
#define SVDB_OP_LOG            98
#define SVDB_OP_LOG10          99
#define SVDB_OP_LN             100
#define SVDB_OP_SIN            101
#define SVDB_OP_COS            102
#define SVDB_OP_TAN            103
#define SVDB_OP_ASIN           104
#define SVDB_OP_ACOS           105
#define SVDB_OP_ATAN           106
#define SVDB_OP_ATAN2          107
#define SVDB_OP_SINH           108
#define SVDB_OP_COSH           109
#define SVDB_OP_TANH           110
#define SVDB_OP_DEG_TO_RAD     111
#define SVDB_OP_RAD_TO_DEG     112
#define SVDB_OP_TO_TEXT        113
#define SVDB_OP_TO_NUMERIC     114
#define SVDB_OP_TO_INT         115
#define SVDB_OP_TO_REAL        116
#define SVDB_OP_REAL_TO_INT    149
#define SVDB_OP_TYPEOF         151
#define SVDB_OP_RANDOM         152

/*
 * Execute a pure (register-to-register) opcode.
 *
 * Parameters:
 *   opcode   - one of the SVDB_OP_* constants above
 *   v1       - first operand (registers[P1]); may be NULL pointer when unused
 *   v2       - second operand (registers[P2]); may be NULL pointer when unused
 *   v3       - third operand (additional register); may be NULL pointer
 *   aux_i    - integer auxiliary (e.g., immediate value for SVDB_OP_ADD_IMM,
 *               decimal count for SVDB_OP_ROUND)
 *   aux_s    - string auxiliary (pattern for LIKE/GLOB, chars for TRIM;
 *               may be NULL)
 *   aux_len  - byte length of aux_s
 *   out_val  - receives the result value; must not be NULL
 *
 * Returns:
 *   SVDB_PURE_OK       (0) — opcode handled; *out_val contains result
 *   SVDB_PURE_NOT_PURE (1) — opcode not handled by this function
 *   SVDB_PURE_ERROR   (-1) — error (bad input, divide-by-zero produces NULL
 *                            rather than an error)
 *
 * Memory:
 *   If *out_val has val_type == SVDB_VAL_TEXT or SVDB_VAL_BLOB after a
 *   successful call, str_data points to a malloc'd buffer that the caller
 *   must free with svdb_pure_value_free().
 */
int32_t svdb_vm_dispatch_pure(
    int32_t             opcode,
    const svdb_value_t* v1,
    const svdb_value_t* v2,
    const svdb_value_t* v3,
    int64_t             aux_i,
    const char*         aux_s,
    size_t              aux_len,
    svdb_value_t*       out_val
);

/*
 * Free heap-allocated string/blob data inside a value produced by
 * svdb_vm_dispatch_pure.  Safe to call on any value; a no-op for
 * numeric / NULL values.
 */
void svdb_pure_value_free(svdb_value_t* v);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_OPCODE_H */
