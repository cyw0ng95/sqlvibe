#include "program.h"
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

/* ------------------------------------------------------------------ types */

struct Program {
    int                       num_regs;
    std::vector<svdb_instr_t> instrs;
    std::vector<std::string>  col_names;
    std::vector<std::string>  consts; /* stored as JSON strings */

    Program(int nr, int ni_hint)
        : num_regs(nr < 0 ? 0 : nr)
    {
        if (ni_hint > 0) instrs.reserve((size_t)ni_hint);
    }
};

static Program* cast(svdb_program_t p) { return static_cast<Program*>(p); }

/* ------------------------------------------------------------------ API */

extern "C" {

svdb_program_t svdb_program_create(int num_regs, int num_instrs)
{
    return new Program(num_regs, num_instrs);
}

void svdb_program_destroy(svdb_program_t prog)
{
    delete cast(prog);
}

int svdb_program_get_num_regs(svdb_program_t prog)
{
    Program* p = cast(prog);
    return p ? p->num_regs : 0;
}

int svdb_program_get_num_instrs(svdb_program_t prog)
{
    Program* p = cast(prog);
    return p ? (int)p->instrs.size() : 0;
}

int svdb_program_get_instr(svdb_program_t prog, int idx, svdb_instr_t* out_instr)
{
    Program* p = cast(prog);
    if (!p || !out_instr || idx < 0 || idx >= (int)p->instrs.size()) return 0;
    *out_instr = p->instrs[(size_t)idx];
    return 1;
}

void svdb_program_set_instr(svdb_program_t prog, int idx, svdb_instr_t instr)
{
    Program* p = cast(prog);
    if (!p || idx < 0 || idx >= (int)p->instrs.size()) return;
    p->instrs[(size_t)idx] = instr;
}

int svdb_program_add_instr(svdb_program_t prog, svdb_instr_t instr)
{
    Program* p = cast(prog);
    if (!p) return -1;
    int idx = (int)p->instrs.size();
    p->instrs.push_back(instr);
    return idx;
}

int svdb_program_get_col_name(svdb_program_t prog, int col_idx,
                               char* out_buf, int out_buf_size)
{
    Program* p = cast(prog);
    if (!p || !out_buf || out_buf_size <= 0 ||
        col_idx < 0 || col_idx >= (int)p->col_names.size())
        return -1;
    const std::string& n = p->col_names[(size_t)col_idx];
    int len = (int)n.size();
    if (out_buf_size < len + 1) return -1;
    memcpy(out_buf, n.c_str(), (size_t)(len + 1));
    return len;
}

void svdb_program_set_col_name(svdb_program_t prog, int col_idx,
                                const char* name, int name_len)
{
    Program* p = cast(prog);
    if (!p || !name || col_idx < 0 || col_idx >= (int)p->col_names.size()) return;
    p->col_names[(size_t)col_idx] = std::string(name, (size_t)(name_len < 0 ? (int)strlen(name) : name_len));
}

int svdb_program_get_num_cols(svdb_program_t prog)
{
    Program* p = cast(prog);
    return p ? (int)p->col_names.size() : 0;
}

int svdb_program_add_col_name(svdb_program_t prog, const char* name, int name_len)
{
    Program* p = cast(prog);
    if (!p || !name) return -1;
    int idx = (int)p->col_names.size();
    p->col_names.emplace_back(name, (size_t)(name_len < 0 ? (int)strlen(name) : name_len));
    return idx;
}

int svdb_program_get_num_consts(svdb_program_t prog)
{
    Program* p = cast(prog);
    return p ? (int)p->consts.size() : 0;
}

int svdb_program_get_const_json(svdb_program_t prog, int idx,
                                 char* out_buf, int out_buf_size)
{
    Program* p = cast(prog);
    if (!p || !out_buf || out_buf_size <= 0 ||
        idx < 0 || idx >= (int)p->consts.size())
        return -1;
    const std::string& c = p->consts[(size_t)idx];
    int len = (int)c.size();
    if (out_buf_size < len + 1) return -1;
    memcpy(out_buf, c.c_str(), (size_t)(len + 1));
    return len;
}

} /* extern "C" */
