#include "instruction.h"
#include "opcodes.h"
#include <cstring>

extern "C" {

svdb_instr_t svdb_instr_make(uint16_t op)
{
    svdb_instr_t i;
    memset(&i, 0, sizeof(i));
    i.op = op;
    return i;
}

svdb_instr_t svdb_instr_make_a(uint16_t op, int32_t a)
{
    svdb_instr_t i = svdb_instr_make(op);
    i.a = a;
    return i;
}

svdb_instr_t svdb_instr_make_ab(uint16_t op, int32_t a, int32_t b)
{
    svdb_instr_t i = svdb_instr_make(op);
    i.a = a;
    i.b = b;
    return i;
}

svdb_instr_t svdb_instr_make_abc(uint16_t op, int32_t a, int32_t b, int32_t c)
{
    svdb_instr_t i = svdb_instr_make(op);
    i.a = a;
    i.b = b;
    i.c = c;
    return i;
}

uint16_t svdb_instr_get_op(svdb_instr_t instr)
{
    return instr.op;
}

int svdb_instr_has_flag(svdb_instr_t instr, uint16_t flag)
{
    return (instr.fl & flag) != 0;
}

svdb_instr_t svdb_instr_set_flag(svdb_instr_t instr, uint16_t flag)
{
    instr.fl = (uint16_t)(instr.fl | flag);
    return instr;
}

int svdb_instr_is_jump(svdb_instr_t instr)
{
    return svdb_opcode_is_jump((int)instr.op);
}

int svdb_instr_is_terminal(svdb_instr_t instr)
{
    return svdb_opcode_is_terminal((int)instr.op);
}

} /* extern "C" */
