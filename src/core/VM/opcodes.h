#ifndef SVDB_VM_OPCODES_H
#define SVDB_VM_OPCODES_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bytecode opcode values — must stay in sync with Go VM/bc_opcodes.go */
typedef enum {
    SVDB_BC_NOOP           =  0,
    SVDB_BC_HALT           =  1,
    SVDB_BC_LOAD_CONST     =  2,
    SVDB_BC_LOAD_COL       =  3,
    SVDB_BC_STORE          =  4,
    SVDB_BC_ADD            =  5,
    SVDB_BC_SUB            =  6,
    SVDB_BC_MUL            =  7,
    SVDB_BC_DIV            =  8,
    SVDB_BC_MOD            =  9,
    SVDB_BC_NEG            = 10,
    SVDB_BC_EQ             = 11,
    SVDB_BC_NEQ            = 12,
    SVDB_BC_LT             = 13,
    SVDB_BC_LE             = 14,
    SVDB_BC_GT             = 15,
    SVDB_BC_GE             = 16,
    SVDB_BC_AND            = 17,
    SVDB_BC_OR             = 18,
    SVDB_BC_NOT            = 19,
    SVDB_BC_JUMP           = 20,
    SVDB_BC_JUMP_IF_FALSE  = 21,
    SVDB_BC_JUMP_IF_TRUE   = 22,
    SVDB_BC_CALL           = 23,
    SVDB_BC_RESULT_ROW     = 24,
    SVDB_BC_OPEN_READ      = 25,
    SVDB_BC_OPEN_WRITE     = 26,
    SVDB_BC_REWIND         = 27,
    SVDB_BC_NEXT           = 28,
    SVDB_BC_EOF            = 29,
    SVDB_BC_COLUMN         = 30,
    SVDB_BC_ROWID          = 31,
    SVDB_BC_SEEK_ROWID     = 32,
    SVDB_BC_AGG_STEP       = 33,
    SVDB_BC_AGG_FINAL      = 34,
    SVDB_BC_INIT_COROUTINE = 35,
    SVDB_BC_YIELD          = 36,
    SVDB_BC_CLOSE          = 37,
    SVDB_BC_CONCAT         = 38,
    SVDB_BC_IS_NULL        = 39,
    SVDB_BC_NOT_NULL       = 40,
    SVDB_BC_CAST           = 41,
    SVDB_BC_LIKE           = 42,
    SVDB_BC_COPY           = 43,
    SVDB_BC_MOVE           = 44,
    SVDB_BC_SWAP           = 45,
    SVDB_BC_MAX_OPCODE     = 46  /* sentinel */
} svdb_opcode_t;

/* Return the human-readable name of an opcode (e.g. "HALT", "ADD") */
const char* svdb_opcode_name(int op);

/* Number of explicit operands the opcode uses (0-3) */
int svdb_opcode_num_operands(int op);

/* 1 if the opcode is a control-flow jump */
int svdb_opcode_is_jump(int op);

/* 1 if the opcode loads a value into a register */
int svdb_opcode_is_load(int op);

/* 1 if the opcode performs arithmetic */
int svdb_opcode_is_arith(int op);

/* 1 if the opcode performs a comparison */
int svdb_opcode_is_compare(int op);

/* 1 if HALT or RESULT_ROW */
int svdb_opcode_is_terminal(int op);

/* Reverse lookup: return opcode value, or -1 if unknown */
int svdb_opcode_from_name(const char* name);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_OPCODES_H */
