#include "opcodes.h"
#include <cstring>
#include <cctype>

/* ------------------------------------------------------------------ tables */

struct OpcodeInfo {
    const char* name;
    int         num_operands; /* 0-3 */
};

static const OpcodeInfo kOpcodeTable[SVDB_BC_MAX_OPCODE] = {
    /* 0  */ { "NOOP",           0 },
    /* 1  */ { "HALT",           0 },
    /* 2  */ { "LOAD_CONST",     2 },
    /* 3  */ { "LOAD_COL",       2 },
    /* 4  */ { "STORE",          2 },
    /* 5  */ { "ADD",            3 },
    /* 6  */ { "SUB",            3 },
    /* 7  */ { "MUL",            3 },
    /* 8  */ { "DIV",            3 },
    /* 9  */ { "MOD",            3 },
    /* 10 */ { "NEG",            2 },
    /* 11 */ { "EQ",             3 },
    /* 12 */ { "NEQ",            3 },
    /* 13 */ { "LT",             3 },
    /* 14 */ { "LE",             3 },
    /* 15 */ { "GT",             3 },
    /* 16 */ { "GE",             3 },
    /* 17 */ { "AND",            3 },
    /* 18 */ { "OR",             3 },
    /* 19 */ { "NOT",            2 },
    /* 20 */ { "JUMP",           1 },
    /* 21 */ { "JUMP_IF_FALSE",  2 },
    /* 22 */ { "JUMP_IF_TRUE",   2 },
    /* 23 */ { "CALL",           3 },
    /* 24 */ { "RESULT_ROW",     2 },
    /* 25 */ { "OPEN_READ",      2 },
    /* 26 */ { "OPEN_WRITE",     2 },
    /* 27 */ { "REWIND",         2 },
    /* 28 */ { "NEXT",           2 },
    /* 29 */ { "EOF",            2 },
    /* 30 */ { "COLUMN",         3 },
    /* 31 */ { "ROWID",          2 },
    /* 32 */ { "SEEK_ROWID",     2 },
    /* 33 */ { "AGG_STEP",       3 },
    /* 34 */ { "AGG_FINAL",      2 },
    /* 35 */ { "INIT_COROUTINE", 2 },
    /* 36 */ { "YIELD",          1 },
    /* 37 */ { "CLOSE",          1 },
    /* 38 */ { "CONCAT",         3 },
    /* 39 */ { "IS_NULL",        2 },
    /* 40 */ { "NOT_NULL",       2 },
    /* 41 */ { "CAST",           3 },
    /* 42 */ { "LIKE",           3 },
    /* 43 */ { "COPY",           2 },
    /* 44 */ { "MOVE",           2 },
    /* 45 */ { "SWAP",           2 },
};

/* ------------------------------------------------------------------ API */

extern "C" {

const char* svdb_opcode_name(int op)
{
    if (op < 0 || op >= SVDB_BC_MAX_OPCODE) return "UNKNOWN";
    return kOpcodeTable[op].name;
}

int svdb_opcode_num_operands(int op)
{
    if (op < 0 || op >= SVDB_BC_MAX_OPCODE) return 0;
    return kOpcodeTable[op].num_operands;
}

int svdb_opcode_is_jump(int op)
{
    return op == SVDB_BC_JUMP ||
           op == SVDB_BC_JUMP_IF_FALSE ||
           op == SVDB_BC_JUMP_IF_TRUE;
}

int svdb_opcode_is_load(int op)
{
    return op == SVDB_BC_LOAD_CONST ||
           op == SVDB_BC_LOAD_COL   ||
           op == SVDB_BC_COLUMN     ||
           op == SVDB_BC_ROWID;
}

int svdb_opcode_is_arith(int op)
{
    return op == SVDB_BC_ADD ||
           op == SVDB_BC_SUB ||
           op == SVDB_BC_MUL ||
           op == SVDB_BC_DIV ||
           op == SVDB_BC_MOD ||
           op == SVDB_BC_NEG;
}

int svdb_opcode_is_compare(int op)
{
    return op == SVDB_BC_EQ  ||
           op == SVDB_BC_NEQ ||
           op == SVDB_BC_LT  ||
           op == SVDB_BC_LE  ||
           op == SVDB_BC_GT  ||
           op == SVDB_BC_GE;
}

int svdb_opcode_is_terminal(int op)
{
    return op == SVDB_BC_HALT || op == SVDB_BC_RESULT_ROW;
}

int svdb_opcode_from_name(const char* name)
{
    if (!name) return -1;
    for (int i = 0; i < SVDB_BC_MAX_OPCODE; ++i)
        if (strcasecmp(kOpcodeTable[i].name, name) == 0) return i;
    return -1;
}

} /* extern "C" */
