#ifndef SVDB_VM_PROGRAM_H
#define SVDB_VM_PROGRAM_H

#include "instruction.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* svdb_program_t;

/* Create a program with initial capacity hints */
svdb_program_t svdb_program_create(int num_regs, int num_instrs);

/* Destroy and free all resources */
void svdb_program_destroy(svdb_program_t prog);

/* Register/instruction counts */
int svdb_program_get_num_regs(svdb_program_t prog);
int svdb_program_get_num_instrs(svdb_program_t prog);

/* Instruction access */
int  svdb_program_get_instr(svdb_program_t prog, int idx, svdb_instr_t* out_instr);
void svdb_program_set_instr(svdb_program_t prog, int idx, svdb_instr_t instr);
int  svdb_program_add_instr(svdb_program_t prog, svdb_instr_t instr);

/* Column names */
int  svdb_program_get_col_name(svdb_program_t prog, int col_idx,
                                char* out_buf, int out_buf_size);
void svdb_program_set_col_name(svdb_program_t prog, int col_idx,
                                const char* name, int name_len);
int  svdb_program_get_num_cols(svdb_program_t prog);
int  svdb_program_add_col_name(svdb_program_t prog,
                                const char* name, int name_len);

/* Constant pool */
int svdb_program_get_num_consts(svdb_program_t prog);
int svdb_program_get_const_json(svdb_program_t prog, int idx,
                                 char* out_buf, int out_buf_size);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_PROGRAM_H */
