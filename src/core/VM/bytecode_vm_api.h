#ifndef SVDB_VM_BYTECODE_VM_API_H
#define SVDB_VM_BYTECODE_VM_API_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opcode constants (match internal/VM/opcodes.go) */
#define VM_OP_NOP     0
#define VM_OP_HALT    1
#define VM_OP_GOTO    2
#define VM_OP_MOVE    10
#define VM_OP_COPY    11
#define VM_OP_INT_COPY 12
#define VM_OP_ADD     20
#define VM_OP_SUB     21
#define VM_OP_MUL     22
#define VM_OP_DIV     23
#define VM_OP_EQ      30
#define VM_OP_NE      31
#define VM_OP_LT      32
#define VM_OP_LE      33
#define VM_OP_GT      34
#define VM_OP_GE      35
#define VM_OP_AND     40
#define VM_OP_OR      41
#define VM_OP_NOT     42

/* VM state structure */
typedef struct {
    svdb_value_t* registers;
    int32_t       num_regs;
    int32_t       pc;
    int32_t       halted;
    int32_t       error_code;
} svdb_vm_state_t;

/* Instruction structure */
typedef struct {
    int32_t opcode;
    int32_t p1;
    int32_t p2;
    int32_t p3;
    svdb_value_t p4;
} svdb_vm_instruction_t;

/* Program structure */
typedef struct {
    svdb_vm_instruction_t* instructions;
    int32_t                num_instructions;
    int32_t                num_registers;
} svdb_vm_program_t;

/* Allocation helpers */
svdb_vm_state_t*    svdb_vm_state_create(int32_t num_regs);
void                svdb_vm_state_destroy(svdb_vm_state_t* state);
svdb_vm_program_t*  svdb_vm_program_create(int32_t num_instructions, int32_t num_regs);
void                svdb_vm_program_destroy(svdb_vm_program_t* prog);

/* Execution - batch mode for hot paths */
int32_t svdb_vm_execute_batch(
    svdb_vm_state_t* state,
    const svdb_vm_program_t* prog,
    int32_t max_iterations
);

/* Individual opcode handlers for selective acceleration */
int32_t svdb_vm_op_move(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_copy(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_int_copy(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);

int32_t svdb_vm_op_add(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_sub(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_mul(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_div(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);

int32_t svdb_vm_op_eq(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_ne(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_lt(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_le(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_gt(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_ge(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);

int32_t svdb_vm_op_and(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_or(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);
int32_t svdb_vm_op_not(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst);

/* Value comparison utility */
int32_t svdb_value_compare(const svdb_value_t* a, const svdb_value_t* b);
int32_t svdb_value_to_bool(const svdb_value_t* v);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_VM_BYTECODE_VM_API_H */
