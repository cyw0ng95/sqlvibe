#ifndef SVDB_VM_BYTECODE_VM_H
#define SVDB_VM_BYTECODE_VM_H

#include <stdint.h>
#include <stddef.h>
#include "value.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque bytecode VM handle. */
typedef struct svdb_bytecode_vm_t svdb_bytecode_vm_t;

/* Create a new bytecode VM instance. */
svdb_bytecode_vm_t* svdb_bytecode_vm_create(void);

/* Destroy a bytecode VM instance. */
void svdb_bytecode_vm_destroy(svdb_bytecode_vm_t* vm);

/*
 * Set a register value.
 * value_type: one of the SVDB_TYPE_* constants.
 * value_text / text_len: used when value_type == SVDB_TYPE_TEXT or SVDB_TYPE_BLOB.
 * value_int:  used when value_type == SVDB_TYPE_INT.
 * value_real: used when value_type == SVDB_TYPE_REAL.
 */
void svdb_bytecode_vm_set_register(svdb_bytecode_vm_t* vm,
                                    int reg_idx,
                                    int value_type,
                                    int64_t value_int,
                                    double  value_real,
                                    const char* value_text,
                                    size_t  text_len);

/*
 * Read a register value.
 * out_text is written with up to (text_cap-1) bytes and null-terminated.
 * Returns 1 on success, 0 if reg_idx is out of range.
 */
int svdb_bytecode_vm_get_register(svdb_bytecode_vm_t* vm,
                                   int reg_idx,
                                   int* out_type,
                                   int64_t* out_int,
                                   double*  out_real,
                                   char*    out_text,
                                   size_t   text_cap);

/*
 * Execute one instruction step.
 * opcode: one of the SVDB_BC_* values (see opcodes.h).
 * p1, p2, p3: integer operands.
 * p4_str: optional string operand (may be NULL).
 * out_jump_pc: if non-NULL, set to the jump target PC when a jump is taken,
 *              or to -1 if no jump (caller should use pc+1).
 *
 * Returns:
 *   0   – step completed normally, continue execution
 *  -1   – HALT reached (normal termination)
 *  -2   – RESULT_ROW available (call svdb_bytecode_vm_get_result_row)
 *  < -3 – error
 */
int svdb_bytecode_vm_step(svdb_bytecode_vm_t* vm,
                           int opcode,
                           int p1, int p2, int p3,
                           const char* p4_str,
                           int* out_jump_pc);

/* Return 1 if a result row is ready (RESULT_ROW was the last step). */
int svdb_bytecode_vm_has_result(svdb_bytecode_vm_t* vm);

/*
 * Reset the VM: clear all registers and internal state.
 * The VM can be reused after reset.
 */
void svdb_bytecode_vm_reset(svdb_bytecode_vm_t* vm);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_BYTECODE_VM_H */
