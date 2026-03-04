/* vm_execute.h — VM Execution Engine C API */
#pragma once
#ifndef SVDB_VM_EXECUTE_H
#define SVDB_VM_EXECUTE_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque VM state handle ─────────────────────────────────── */
typedef struct svdb_vm_s svdb_vm_t;

/* ── Instruction format (matches Go Instruction type) ────────── */
typedef struct {
    uint16_t opcode;      /* Opcode */
    int32_t  p1;          /* P1 operand */
    int32_t  p2;          /* P2 operand */
    int32_t  p3_type;     /* P3 type: 0=null, 1=int, 2=string */
    int32_t  p3_int;      /* P3 int value */
    const char* p3_str;   /* P3 string value */
    int32_t  p4_type;     /* P4 type: 0=null, 1=int, 2=string, 3=float */
    int32_t  p4_int;      /* P4 int value */
    double   p4_float;    /* P4 float value */
    const char* p4_str;   /* P4 string value */
    int32_t  has_dst;     /* HasDst flag */
    int32_t  dst_reg;     /* DstReg value */
} svdb_vm_instr_t;

/* ── Program structure ──────────────────────────────────────── */
typedef struct {
    const svdb_vm_instr_t* instructions;
    int32_t                num_instructions;
    int32_t                num_regs;
    int32_t                num_cursors;
    int32_t                num_agg;
} svdb_vm_program_t;

/* ── Execution context (callbacks to Go for storage) ────────── */
typedef struct {
    void* user_data;
    
    /* Get table rows: returns (rows, num_rows, num_cols, col_names, error) */
    int32_t (*get_table_rows)(
        void* user_data,
        const char* table_name,
        svdb_value_t** out_rows,
        int32_t* out_num_rows,
        int32_t* out_num_cols,
        const char*** out_col_names);
    
    /* Get table columns: returns (col_names, num_cols) */
    int32_t (*get_table_columns)(
        void* user_data,
        const char* table_name,
        const char*** out_col_names,
        int32_t* out_num_cols);
    
    /* Insert row: returns error code */
    int32_t (*insert_row)(
        void* user_data,
        const char* table_name,
        const svdb_value_t* row_values,
        int32_t num_cols);
    
    /* Update row: returns error code */
    int32_t (*update_row)(
        void* user_data,
        const char* table_name,
        int32_t row_index,
        const svdb_value_t* row_values,
        int32_t num_cols);
    
    /* Delete row: returns error code */
    int32_t (*delete_row)(
        void* user_data,
        const char* table_name,
        int32_t row_index);
} svdb_vm_context_t;

/* ── Result structure ───────────────────────────────────────── */
typedef struct {
    svdb_value_t* rows;       /* Flattened row data */
    int32_t*      row_indices; /* Start index of each row */
    const char**  col_names;  /* Column names */
    int32_t       num_rows;
    int32_t       num_cols;
    int64_t       rows_affected;
    int64_t       last_insert_rowid;
    char*         error_msg;  /* NULL if no error */
} svdb_vm_result_t;

/* ── VM lifecycle ───────────────────────────────────────────── */
svdb_vm_t* svdb_vm_create(void);
void       svdb_vm_destroy(svdb_vm_t* vm);

/* ── VM execution ───────────────────────────────────────────── */
int32_t svdb_vm_execute(
    svdb_vm_t* vm,
    const svdb_vm_program_t* program,
    const svdb_vm_context_t* ctx,
    svdb_vm_result_t* result);

/* ── VM state access ────────────────────────────────────────── */
int32_t  svdb_vm_get_pc(const svdb_vm_t* vm);
void     svdb_vm_set_pc(svdb_vm_t* vm, int32_t pc);
int32_t  svdb_vm_get_register_int(const svdb_vm_t* vm, int32_t reg);
double   svdb_vm_get_register_float(const svdb_vm_t* vm, int32_t reg);
const char* svdb_vm_get_register_text(const svdb_vm_t* vm, int32_t reg);
void     svdb_vm_set_register_int(svdb_vm_t* vm, int32_t reg, int64_t val);
void     svdb_vm_set_register_float(svdb_vm_t* vm, int32_t reg, double val);
void     svdb_vm_set_register_text(svdb_vm_t* vm, int32_t reg, const char* val);

/* ── Result cleanup ─────────────────────────────────────────── */
void svdb_vm_result_destroy(svdb_vm_result_t* result);

#ifdef __cplusplus
}
#endif
#endif /* SVDB_VM_EXECUTE_H */
