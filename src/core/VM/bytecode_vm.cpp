#include "bytecode_vm_api.h"
#include <stdlib.h>
#include <string.h>
#include <cmath>

/* Forward declaration - svdb_value_compare is defined in value.cpp */
extern "C" int32_t svdb_value_compare(const svdb_value_t* a, const svdb_value_t* b);

/* ── Allocation ─────────────────────────────────────────────────────────── */

svdb_vm_state_t* svdb_vm_state_create(int32_t num_regs) {
    if (num_regs <= 0) return nullptr;
    
    svdb_vm_state_t* state = (svdb_vm_state_t*)calloc(1, sizeof(svdb_vm_state_t));
    if (!state) return nullptr;
    
    state->registers = (svdb_value_t*)calloc((size_t)num_regs, sizeof(svdb_value_t));
    if (!state->registers) {
        free(state);
        return nullptr;
    }
    state->num_regs = num_regs;
    state->pc = 0;
    state->halted = 0;
    state->error_code = 0;
    return state;
}

void svdb_vm_state_destroy(svdb_vm_state_t* state) {
    if (!state) return;
    
    if (state->registers) {
        /* Free any string/blob data */
        for (int32_t i = 0; i < state->num_regs; i++) {
            if ((state->registers[i].val_type == SVDB_VAL_TEXT || 
                 state->registers[i].val_type == SVDB_VAL_BLOB) &&
                state->registers[i].str_data) {
                free((void*)state->registers[i].str_data);
            }
        }
        free(state->registers);
    }
    free(state);
}

svdb_vm_program_t* svdb_vm_program_create(int32_t num_instructions, int32_t num_regs) {
    if (num_instructions <= 0 || num_regs <= 0) return nullptr;
    
    svdb_vm_program_t* prog = (svdb_vm_program_t*)calloc(1, sizeof(svdb_vm_program_t));
    if (!prog) return nullptr;
    
    prog->instructions = (svdb_vm_instruction_t*)calloc(
        (size_t)num_instructions, sizeof(svdb_vm_instruction_t));
    if (!prog->instructions) {
        free(prog);
        return nullptr;
    }
    prog->num_instructions = num_instructions;
    prog->num_registers = num_regs;
    return prog;
}

void svdb_vm_program_destroy(svdb_vm_program_t* prog) {
    if (!prog) return;
    free(prog->instructions);
    free(prog);
}

/* ── Value utilities ────────────────────────────────────────────────────── */

/* svdb_value_compare is defined in value.cpp */

int32_t svdb_value_to_bool(const svdb_value_t* v) {
    if (!v || v->val_type == SVDB_VAL_NULL) return 0;
    
    switch (v->val_type) {
        case SVDB_VAL_INT:
            return v->int_val != 0;
        case SVDB_VAL_FLOAT:
            return v->float_val != 0.0;
        case SVDB_VAL_BOOL:
            return v->int_val != 0;
        case SVDB_VAL_TEXT:
            return v->str_len > 0;
        case SVDB_VAL_BLOB:
            return v->bytes_len > 0;
        default:
            return 0;
    }
}

static void copy_value(svdb_value_t* dst, const svdb_value_t* src) {
    if (!dst || !src) return;
    
    /* Free existing string/blob in dst */
    if ((dst->val_type == SVDB_VAL_TEXT || dst->val_type == SVDB_VAL_BLOB) && dst->str_data) {
        free((void*)dst->str_data);
    }
    
    *dst = *src;
    
    /* Deep copy string/blob */
    if ((src->val_type == SVDB_VAL_TEXT || src->val_type == SVDB_VAL_BLOB) && 
        src->str_data && src->str_len > 0) {
        char* p = (char*)malloc(src->str_len + 1);
        if (p) {
            memcpy(p, src->str_data, src->str_len);
            p[src->str_len] = '\0';
            dst->str_data = p;
        } else {
            dst->val_type = SVDB_VAL_NULL;
        }
    }
}

/* ── Data movement opcodes ─────────────────────────────────────────────── */

int32_t svdb_vm_op_move(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs) {
        return -1;
    }
    copy_value(&state->registers[inst->p2], &state->registers[inst->p1]);
    return 0;
}

int32_t svdb_vm_op_copy(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs) {
        return -1;
    }
    if (inst->p1 != inst->p2) {
        copy_value(&state->registers[inst->p2], &state->registers[inst->p1]);
    }
    return 0;
}

int32_t svdb_vm_op_int_copy(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* src = &state->registers[inst->p1];
    svdb_value_t* dst = &state->registers[inst->p2];
    
    if (src->val_type == SVDB_VAL_INT) {
        dst->val_type = SVDB_VAL_INT;
        dst->int_val = src->int_val;
    } else if (src->val_type == SVDB_VAL_FLOAT) {
        dst->val_type = SVDB_VAL_INT;
        dst->int_val = (int64_t)src->float_val;
    } else {
        dst->val_type = SVDB_VAL_NULL;
    }
    return 0;
}

/* ── Arithmetic opcodes ────────────────────────────────────────────────── */

static int32_t exec_arith(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst,
                          double (*op)(double, double)) {
    if (!state || !inst || !op) return -1;
    if (inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs ||
        inst->p3 < 0 || inst->p3 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* a = &state->registers[inst->p2];
    const svdb_value_t* b = &state->registers[inst->p3];
    svdb_value_t* result = &state->registers[inst->p1];
    
    /* NULL propagation */
    if (a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL) {
        result->val_type = SVDB_VAL_NULL;
        return 0;
    }
    
    /* Convert to double */
    double da = (a->val_type == SVDB_VAL_FLOAT) ? a->float_val : 
                (a->val_type == SVDB_VAL_INT) ? (double)a->int_val : 0.0;
    double db = (b->val_type == SVDB_VAL_FLOAT) ? b->float_val : 
                (b->val_type == SVDB_VAL_INT) ? (double)b->int_val : 0.0;
    
    result->val_type = SVDB_VAL_FLOAT;
    result->float_val = op(da, db);
    return 0;
}

static double add_op(double a, double b) { return a + b; }
static double sub_op(double a, double b) { return a - b; }
static double mul_op(double a, double b) { return a * b; }
static double div_op(double a, double b) { return b != 0.0 ? a / b : 0.0; }

int32_t svdb_vm_op_add(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_arith(state, inst, add_op);
}

int32_t svdb_vm_op_sub(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_arith(state, inst, sub_op);
}

int32_t svdb_vm_op_mul(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_arith(state, inst, mul_op);
}

int32_t svdb_vm_op_div(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_arith(state, inst, div_op);
}

/* ── Comparison opcodes ────────────────────────────────────────────────── */

static int32_t exec_compare(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst,
                            int (*cmp)(int)) {
    if (!state || !inst || !cmp) return -1;
    if (inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs ||
        inst->p3 < 0 || inst->p3 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* a = &state->registers[inst->p2];
    const svdb_value_t* b = &state->registers[inst->p3];
    svdb_value_t* result = &state->registers[inst->p1];
    
    int c = svdb_value_compare(a, b);
    result->val_type = SVDB_VAL_BOOL;
    result->int_val = cmp(c) ? 1 : 0;
    return 0;
}

int32_t svdb_vm_op_eq(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c == 0; });
}

int32_t svdb_vm_op_ne(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c != 0; });
}

int32_t svdb_vm_op_lt(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c < 0; });
}

int32_t svdb_vm_op_le(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c <= 0; });
}

int32_t svdb_vm_op_gt(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c > 0; });
}

int32_t svdb_vm_op_ge(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    return exec_compare(state, inst, [](int c) -> int { return c >= 0; });
}

/* ── Logic opcodes ─────────────────────────────────────────────────────── */

int32_t svdb_vm_op_and(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs ||
        inst->p3 < 0 || inst->p3 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* a = &state->registers[inst->p2];
    const svdb_value_t* b = &state->registers[inst->p3];
    svdb_value_t* result = &state->registers[inst->p1];
    
    /* SQL three-valued logic */
    if (a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL) {
        result->val_type = SVDB_VAL_NULL;
        return 0;
    }
    
    result->val_type = SVDB_VAL_BOOL;
    result->int_val = (svdb_value_to_bool(a) && svdb_value_to_bool(b)) ? 1 : 0;
    return 0;
}

int32_t svdb_vm_op_or(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs ||
        inst->p3 < 0 || inst->p3 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* a = &state->registers[inst->p2];
    const svdb_value_t* b = &state->registers[inst->p3];
    svdb_value_t* result = &state->registers[inst->p1];
    
    /* SQL three-valued logic */
    if (a->val_type == SVDB_VAL_NULL && b->val_type == SVDB_VAL_NULL) {
        result->val_type = SVDB_VAL_NULL;
        return 0;
    }
    if (a->val_type == SVDB_VAL_NULL) {
        result->val_type = svdb_value_to_bool(b) ? SVDB_VAL_BOOL : SVDB_VAL_NULL;
        result->int_val = svdb_value_to_bool(b) ? 1 : 0;
        return 0;
    }
    if (b->val_type == SVDB_VAL_NULL) {
        result->val_type = svdb_value_to_bool(a) ? SVDB_VAL_BOOL : SVDB_VAL_NULL;
        result->int_val = svdb_value_to_bool(a) ? 1 : 0;
        return 0;
    }
    
    result->val_type = SVDB_VAL_BOOL;
    result->int_val = (svdb_value_to_bool(a) || svdb_value_to_bool(b)) ? 1 : 0;
    return 0;
}

int32_t svdb_vm_op_not(svdb_vm_state_t* state, const svdb_vm_instruction_t* inst) {
    if (!state || !inst || inst->p1 < 0 || inst->p1 >= state->num_regs ||
        inst->p2 < 0 || inst->p2 >= state->num_regs) {
        return -1;
    }
    
    const svdb_value_t* a = &state->registers[inst->p2];
    svdb_value_t* result = &state->registers[inst->p1];
    
    if (a->val_type == SVDB_VAL_NULL) {
        result->val_type = SVDB_VAL_NULL;
        return 0;
    }
    
    result->val_type = SVDB_VAL_BOOL;
    result->int_val = svdb_value_to_bool(a) ? 0 : 1;
    return 0;
}

/* ── Batch execution ───────────────────────────────────────────────────── */

int32_t svdb_vm_execute_batch(
    svdb_vm_state_t* state,
    const svdb_vm_program_t* prog,
    int32_t max_iterations) {
    
    if (!state || !prog || !state->registers || !prog->instructions) {
        return -1;
    }
    
    state->halted = 0;
    state->error_code = 0;
    state->pc = 0;
    
    int32_t iterations = 0;
    
    while (!state->halted && state->pc >= 0 && state->pc < prog->num_instructions) {
        if (++iterations > max_iterations) {
            state->error_code = 1;  /* Max iterations exceeded */
            return -1;
        }
        
        const svdb_vm_instruction_t* inst = &prog->instructions[state->pc];
        int32_t result = 0;
        int32_t advance_pc = 1;
        
        switch (inst->opcode) {
            /* Data movement */
            case VM_OP_MOVE:
                result = svdb_vm_op_move(state, inst);
                break;
            case VM_OP_COPY:
                result = svdb_vm_op_copy(state, inst);
                break;
            case VM_OP_INT_COPY:
                result = svdb_vm_op_int_copy(state, inst);
                break;
            
            /* Arithmetic */
            case VM_OP_ADD:
                result = svdb_vm_op_add(state, inst);
                break;
            case VM_OP_SUB:
                result = svdb_vm_op_sub(state, inst);
                break;
            case VM_OP_MUL:
                result = svdb_vm_op_mul(state, inst);
                break;
            case VM_OP_DIV:
                result = svdb_vm_op_div(state, inst);
                break;
            
            /* Comparison */
            case VM_OP_EQ:
                result = svdb_vm_op_eq(state, inst);
                break;
            case VM_OP_NE:
                result = svdb_vm_op_ne(state, inst);
                break;
            case VM_OP_LT:
                result = svdb_vm_op_lt(state, inst);
                break;
            case VM_OP_LE:
                result = svdb_vm_op_le(state, inst);
                break;
            case VM_OP_GT:
                result = svdb_vm_op_gt(state, inst);
                break;
            case VM_OP_GE:
                result = svdb_vm_op_ge(state, inst);
                break;
            
            /* Logic */
            case VM_OP_AND:
                result = svdb_vm_op_and(state, inst);
                break;
            case VM_OP_OR:
                result = svdb_vm_op_or(state, inst);
                break;
            case VM_OP_NOT:
                result = svdb_vm_op_not(state, inst);
                break;
            
            /* Control flow */
            case VM_OP_GOTO:
                state->pc = inst->p2;
                advance_pc = 0;
                break;
            
            case VM_OP_HALT:
                state->halted = 1;
                advance_pc = 0;
                break;
            
            default:
                /* Unknown opcode - let Go handle it */
                state->error_code = 2;  /* Unknown opcode */
                return state->pc;  /* Return current PC for Go to continue */
        }
        
        if (result < 0) {
            state->error_code = 3;  /* Execution error */
            return -1;
        }
        
        if (advance_pc) {
            state->pc++;
        }
    }
    
    return state->pc;
}
