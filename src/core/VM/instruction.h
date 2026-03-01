#ifndef SVDB_VM_INSTRUCTION_H
#define SVDB_VM_INSTRUCTION_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 16-byte instruction layout — must match Go VM.Instr */
typedef struct {
    uint16_t op;
    uint16_t fl;
    int32_t  a;
    int32_t  b;
    int32_t  c;
} svdb_instr_t;

/* Instruction flag bits — must match Go InstrFlag constants */
#define SVDB_INSTR_FLAG_IMM_A       0x0001u  /* A is immediate, not register    */
#define SVDB_INSTR_FLAG_CONST_B     0x0002u  /* B is constant pool index        */
#define SVDB_INSTR_FLAG_JUMP_C      0x0004u  /* C is jump target PC             */
#define SVDB_INSTR_FLAG_TYPED_INT   0x0008u  /* A and B are int                 */
#define SVDB_INSTR_FLAG_TYPED_FLOAT 0x0010u  /* A and B are float               */
#define SVDB_INSTR_FLAG_NULLABLE    0x0020u  /* result may be NULL              */

/* Constructors */
svdb_instr_t svdb_instr_make(uint16_t op);
svdb_instr_t svdb_instr_make_a(uint16_t op, int32_t a);
svdb_instr_t svdb_instr_make_ab(uint16_t op, int32_t a, int32_t b);
svdb_instr_t svdb_instr_make_abc(uint16_t op, int32_t a, int32_t b, int32_t c);

/* Accessors */
uint16_t     svdb_instr_get_op(svdb_instr_t instr);
int          svdb_instr_has_flag(svdb_instr_t instr, uint16_t flag);
svdb_instr_t svdb_instr_set_flag(svdb_instr_t instr, uint16_t flag);
int          svdb_instr_is_jump(svdb_instr_t instr);
int          svdb_instr_is_terminal(svdb_instr_t instr);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_INSTRUCTION_H */
