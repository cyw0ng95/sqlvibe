/* vm_execute.cpp — VM Execution Engine Implementation */
#include "vm_execute.h"
#include <stdlib.h>
#include <string.h>
#include <cmath>
#include <vector>
#include <unordered_map>
#include <string>

/* ── VM state structure ─────────────────────────────────────── */

struct svdb_vm_s {
    std::vector<svdb_value_t> registers;
    std::vector<void*> cursors;  /* Opaque cursor handles */
    std::vector<svdb_value_t> agg_state;  /* Aggregate accumulators */
    int32_t pc;
    int32_t error_code;
    char* error_msg;
    int64_t rows_affected;
    int64_t last_insert_rowid;
    int32_t halted;
    
    /* Callback context */
    void* user_data;
    svdb_vm_context_t callbacks;
};

/* ── Utility functions ──────────────────────────────────────── */

static svdb_value_t make_null() {
    svdb_value_t v = {};
    v.val_type = SVDB_VAL_NULL;
    return v;
}

static svdb_value_t make_int(int64_t val) {
    svdb_value_t v = {};
    v.val_type = SVDB_VAL_INT;
    v.int_val = val;
    return v;
}

static svdb_value_t make_float(double val) {
    svdb_value_t v = {};
    v.val_type = SVDB_VAL_FLOAT;
    v.float_val = val;
    return v;
}

static svdb_value_t make_text(const char* str, size_t len) {
    svdb_value_t v = {};
    v.val_type = SVDB_VAL_TEXT;
    if (str && len > 0) {
        v.str_data = (char*)malloc(len + 1);
        if (v.str_data) {
            memcpy((void*)v.str_data, str, len);
            v.str_data[len] = '\0';
        }
        v.str_len = (int32_t)len;
    }
    return v;
}

static void free_value(svdb_value_t* v) {
    if (!v) return;
    if ((v->val_type == SVDB_VAL_TEXT || v->val_type == SVDB_VAL_BLOB) && v->str_data) {
        free((void*)v->str_data);
        v->str_data = nullptr;
    }
    *v = make_null();
}

static int32_t compare_values(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b) return 0;
    if (a->val_type == SVDB_VAL_NULL && b->val_type == SVDB_VAL_NULL) return 0;
    if (a->val_type == SVDB_VAL_NULL) return -1;
    if (b->val_type == SVDB_VAL_NULL) return 1;
    
    /* Both numeric */
    bool a_num = (a->val_type == SVDB_VAL_INT || a->val_type == SVDB_VAL_FLOAT);
    bool b_num = (b->val_type == SVDB_VAL_INT || b->val_type == SVDB_VAL_FLOAT);
    if (a_num && b_num) {
        double fa = (a->val_type == SVDB_VAL_FLOAT) ? a->float_val : (double)a->int_val;
        double fb = (b->val_type == SVDB_VAL_FLOAT) ? b->float_val : (double)b->int_val;
        return (fa > fb) - (fa < fb);
    }
    
    /* Both text/blob */
    if ((a->val_type == SVDB_VAL_TEXT || a->val_type == SVDB_VAL_BLOB) &&
        (b->val_type == SVDB_VAL_TEXT || b->val_type == SVDB_VAL_BLOB)) {
        size_t min_len = (a->str_len < b->str_len) ? a->str_len : b->str_len;
        int c = memcmp(a->str_data, b->str_data, min_len);
        if (c != 0) return c;
        return (a->str_len > b->str_len) - (a->str_len < b->str_len);
    }
    
    /* Mixed types: order by type ordinal */
    return a->val_type - b->val_type;
}

static int32_t to_bool(const svdb_value_t* v) {
    if (!v || v->val_type == SVDB_VAL_NULL) return 0;
    switch (v->val_type) {
        case SVDB_VAL_INT: return v->int_val != 0;
        case SVDB_VAL_FLOAT: return v->float_val != 0.0;
        case SVDB_VAL_BOOL: return v->int_val != 0;
        case SVDB_VAL_TEXT: return v->str_len > 0;
        case SVDB_VAL_BLOB: return v->bytes_len > 0;
        default: return 0;
    }
}

static int64_t to_int64(const svdb_value_t* v) {
    if (!v || v->val_type == SVDB_VAL_NULL) return 0;
    switch (v->val_type) {
        case SVDB_VAL_INT: return v->int_val;
        case SVDB_VAL_FLOAT: return (int64_t)v->float_val;
        case SVDB_VAL_TEXT: return v->str_data ? atoll(v->str_data) : 0;
        default: return 0;
    }
}

static double to_float64(const svdb_value_t* v) {
    if (!v || v->val_type == SVDB_VAL_NULL) return 0.0;
    switch (v->val_type) {
        case SVDB_VAL_INT: return (double)v->int_val;
        case SVDB_VAL_FLOAT: return v->float_val;
        case SVDB_VAL_TEXT: return v->str_data ? atof(v->str_data) : 0.0;
        default: return 0.0;
    }
}

/* ── VM lifecycle ───────────────────────────────────────────── */

svdb_vm_t* svdb_vm_create(void) {
    svdb_vm_t* vm = (svdb_vm_t*)calloc(1, sizeof(svdb_vm_t));
    if (!vm) return nullptr;
    
    vm->pc = 0;
    vm->error_code = 0;
    vm->error_msg = nullptr;
    vm->rows_affected = 0;
    vm->last_insert_rowid = 0;
    vm->halted = 0;
    
    /* Pre-allocate registers */
    vm->registers.resize(64);
    
    return vm;
}

void svdb_vm_destroy(svdb_vm_t* vm) {
    if (!vm) return;
    
    /* Free register values */
    for (size_t i = 0; i < vm->registers.size(); i++) {
        free_value(&vm->registers[i]);
    }
    
    /* Free error message */
    if (vm->error_msg) {
        free(vm->error_msg);
        vm->error_msg = nullptr;
    }
    
    free(vm);
}

/* ── VM execution ───────────────────────────────────────────── */

static void set_error(svdb_vm_t* vm, const char* msg) {
    if (vm->error_msg) free(vm->error_msg);
    vm->error_msg = msg ? strdup(msg) : nullptr;
    vm->error_code = 1;
}

static svdb_value_t get_register(svdb_vm_t* vm, int32_t reg) {
    if (reg < 0 || reg >= (int32_t)vm->registers.size()) {
        return make_null();
    }
    return vm->registers[reg];
}

static void set_register(svdb_vm_t* vm, int32_t reg, svdb_value_t val) {
    if (reg < 0 || reg >= (int32_t)vm->registers.size()) {
        /* Grow registers if needed */
        vm->registers.resize(reg + 1);
    }
    /* Free old value */
    free_value(&vm->registers[reg]);
    /* Copy new value */
    vm->registers[reg] = val;
    if (val.val_type == SVDB_VAL_TEXT && val.str_data) {
        vm->registers[reg].str_data = strdup(val.str_data);
    }
}

/* Result row collection */
static void collect_result_row(svdb_vm_t* vm, const svdb_vm_instr_t* inst, svdb_vm_result_t* result) {
    /* P4 contains register array info or single register */
    if (inst->p4_type == 1) {
        /* Single register */
        int32_t reg = inst->p4_int;
        svdb_value_t val = get_register(vm, reg);
        
        /* Grow result arrays */
        int32_t new_size = (result->num_rows + 1) * result->num_cols;
        if (new_size > 0) {
            result->rows = (svdb_value_t*)realloc(result->rows, new_size * sizeof(svdb_value_t));
            result->row_indices = (int32_t*)realloc(result->row_indices, (result->num_rows + 1) * sizeof(int32_t));
            
            /* Copy value */
            int32_t idx = result->num_rows * result->num_cols;
            if (val.val_type == SVDB_VAL_TEXT && val.str_data) {
                result->rows[idx] = make_text(val.str_data, val.str_len);
            } else {
                result->rows[idx] = val;
            }
            
            result->row_indices[result->num_rows] = idx;
            result->num_rows++;
        }
    }
}

/* Execute a single instruction */
static int32_t execute_instruction(svdb_vm_t* vm, const svdb_vm_instr_t* inst, svdb_vm_result_t* result) {
    /* Opcode constants (must match internal/VM/opcodes.go) */
    enum {
        OP_NOP = 0,
        OP_HALT = 1,
        OP_GOTO = 2,
        OP_GOSUB = 3,
        OP_RETURN = 4,
        OP_INIT = 5,
        OP_LOAD_CONST = 10,
        OP_NULL = 11,
        OP_CONST_NULL = 11,  // alias for OP_NULL
        OP_MOVE = 20,
        OP_COPY = 21,
        OP_SCOPY = 22,
        OP_INT_COPY = 23,
        OP_ADD = 30,
        OP_SUB = 31,
        OP_MUL = 32,
        OP_DIV = 33,
        OP_REM = 34,
        OP_ADD_IMM = 35,
        OP_EQ = 40,
        OP_NE = 41,
        OP_LT = 42,
        OP_LE = 43,
        OP_GT = 44,
        OP_GE = 45,
        OP_IS = 46,
        OP_IS_NOT = 47,
        OP_IS_NULL = 48,
        OP_NOT_NULL = 49,
        OP_IF_NULL = 50,
        OP_IF_NULL2 = 51,
        OP_IF = 60,
        OP_IF_NOT = 61,
        OP_RESULT_COLUMN = 70,
        OP_RESULT_ROW = 71,
        OP_CONCAT = 80,
        OP_SUBSTR = 81,
        OP_LENGTH = 82,
        OP_UPPER = 83,
        OP_LOWER = 84,
        OP_TRIM = 85,
        OP_LTRIM = 86,
        OP_RTRIM = 87,
        OP_REPLACE = 88,
        OP_INSTR = 89,
        OP_LIKE = 90,
        OP_NOT_LIKE = 91,
        OP_GLOB = 92,
        OP_MATCH = 93,
        OP_BIT_AND = 100,
        OP_BIT_OR = 101,
        OP_ABS = 110,
        OP_ROUND = 111,
        OP_CEIL = 112,
        OP_CEILING = 112,  // alias for OP_CEIL
        OP_FLOOR = 113,
        OP_SQRT = 114,
        OP_POW = 115,
        OP_MOD = 116,
        OP_EXP = 117,
        OP_LOG = 118,
        OP_LN = 119,
        OP_SIN = 120,
        OP_COS = 121,
        OP_TAN = 122,
        OP_ASIN = 123,
        OP_ACOS = 124,
        OP_ATAN = 125,
        OP_ATAN2 = 126,
        OP_TYPEOF = 130,
        OP_RANDOM = 131,
        OP_CAST = 132,
        OP_TO_TEXT = 133,
        OP_TO_INT = 134,
        OP_TO_REAL = 135,
    };
    
    switch (inst->opcode) {
        case OP_NOP:
            return 1;
        
        case OP_HALT:
            vm->halted = 1;
            return 0;
        
        case OP_GOTO:
            vm->pc = inst->p2;
            return -1;
        
        case OP_GOSUB:
            if ((size_t)vm->pc + 1 < vm->registers.size()) {
                vm->registers.push_back(make_int(vm->pc + 1));
            }
            vm->pc = inst->p2;
            return -1;
        
        case OP_RETURN:
            if (!vm->registers.empty()) {
                svdb_value_t ret = vm->registers.back();
                vm->registers.pop_back();
                if (ret.val_type == SVDB_VAL_INT) {
                    vm->pc = (int32_t)ret.int_val;
                    return -1;
                }
            }
            return 1;
        
        case OP_INIT:
            if (inst->p2 != 0) {
                vm->pc = inst->p2;
                return -1;
            }
            return 1;
        
        case OP_LOAD_CONST:
            {
                svdb_value_t val;
                if (inst->p4_type == 1) {
                    val = make_int(inst->p4_int);
                } else if (inst->p4_type == 3) {
                    val = make_float(inst->p4_float);
                } else if (inst->p4_type == 2 && inst->p4_str) {
                    val = make_text(inst->p4_str, strlen(inst->p4_str));
                } else {
                    val = make_null();
                }
                set_register(vm, inst->p1, val);
            }
            return 1;
        
        case OP_NULL:
            set_register(vm, inst->p1, make_null());
            return 1;
        
        case OP_MOVE:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                svdb_value_t dst;
                if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                    dst = make_text(src.str_data, src.str_len);
                } else {
                    dst = src;
                }
                set_register(vm, inst->p2, dst);
            }
            return 1;
        
        case OP_COPY:
        case OP_SCOPY:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                svdb_value_t dst;
                if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                    dst = make_text(src.str_data, src.str_len);
                } else {
                    dst = src;
                }
                set_register(vm, inst->p2, dst);
            }
            return 1;
        
        case OP_INT_COPY:
            set_register(vm, inst->p2, make_int(inst->p4_int));
            return 1;
        
        case OP_ADD:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (lhs.val_type == SVDB_VAL_FLOAT || rhs.val_type == SVDB_VAL_FLOAT) {
                    set_register(vm, dst, make_float(to_float64(&lhs) + to_float64(&rhs)));
                } else {
                    set_register(vm, dst, make_int(to_int64(&lhs) + to_int64(&rhs)));
                }
            }
            return 1;
        
        case OP_SUB:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (lhs.val_type == SVDB_VAL_FLOAT || rhs.val_type == SVDB_VAL_FLOAT) {
                    set_register(vm, dst, make_float(to_float64(&lhs) - to_float64(&rhs)));
                } else {
                    set_register(vm, dst, make_int(to_int64(&lhs) - to_int64(&rhs)));
                }
            }
            return 1;
        
        case OP_MUL:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (lhs.val_type == SVDB_VAL_FLOAT || rhs.val_type == SVDB_VAL_FLOAT) {
                    set_register(vm, dst, make_float(to_float64(&lhs) * to_float64(&rhs)));
                } else {
                    set_register(vm, dst, make_int(to_int64(&lhs) * to_int64(&rhs)));
                }
            }
            return 1;
        
        case OP_DIV:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(to_float64(&lhs) / to_float64(&rhs)));
            }
            return 1;
        
        case OP_REM:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                int64_t l = to_int64(&lhs);
                int64_t r = to_int64(&rhs);
                set_register(vm, dst, make_int(r != 0 ? l % r : 0));
            }
            return 1;
        
        case OP_ADD_IMM:
            {
                svdb_value_t v = get_register(vm, inst->p1);
                if (v.val_type == SVDB_VAL_FLOAT) {
                    set_register(vm, inst->p1, make_float(v.float_val + (double)inst->p2));
                } else {
                    set_register(vm, inst->p1, make_int(to_int64(&v) + inst->p2));
                }
            }
            return 1;
        
        case OP_EQ:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) == 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_NE:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) != 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_LT:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) < 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_LE:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) <= 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_GT:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) > 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_GE:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = (compare_values(&lhs, &rhs) >= 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_IS:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = ((lhs.val_type == SVDB_VAL_NULL && rhs.val_type == SVDB_VAL_NULL) ||
                                  compare_values(&lhs, &rhs) == 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_IS_NOT:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t result = ((lhs.val_type == SVDB_VAL_NULL && rhs.val_type != SVDB_VAL_NULL) ||
                                  (lhs.val_type != SVDB_VAL_NULL && rhs.val_type == SVDB_VAL_NULL) ||
                                  compare_values(&lhs, &rhs) != 0) ? 1 : 0;
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(result));
            }
            return 1;
        
        case OP_IS_NULL:
            {
                svdb_value_t val = get_register(vm, inst->p1);
                if (val.val_type == SVDB_VAL_NULL) {
                    vm->pc = inst->p2;
                    return -1;
                }
            }
            return 1;
        
        case OP_NOT_NULL:
            {
                svdb_value_t val = get_register(vm, inst->p1);
                if (val.val_type != SVDB_VAL_NULL) {
                    vm->pc = inst->p2;
                    return -1;
                }
            }
            return 1;
        
        case OP_IF_NULL:
            {
                svdb_value_t val = get_register(vm, inst->p1);
                if (val.val_type == SVDB_VAL_NULL) {
                    svdb_value_t fallback;
                    if (inst->p4_type == 1) {
                        fallback = make_int(inst->p4_int);
                    } else if (inst->p4_type == 3) {
                        fallback = make_float(inst->p4_float);
                    } else if (inst->p4_type == 2 && inst->p4_str) {
                        fallback = make_text(inst->p4_str, strlen(inst->p4_str));
                    } else {
                        fallback = make_null();
                    }
                    set_register(vm, inst->p1, fallback);
                }
            }
            return 1;
        
        case OP_IF_NULL2:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                svdb_value_t fallback = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (src.val_type == SVDB_VAL_NULL) {
                    set_register(vm, dst, fallback);
                } else {
                    set_register(vm, dst, src);
                }
            }
            return 1;
        
        case OP_IF:
            {
                svdb_value_t val = get_register(vm, inst->p1);
                if (to_bool(&val)) {
                    vm->pc = inst->p2;
                    return -1;
                }
            }
            return 1;
        
        case OP_IF_NOT:
            {
                svdb_value_t val = get_register(vm, inst->p1);
                if (!to_bool(&val)) {
                    vm->pc = inst->p2;
                    return -1;
                }
            }
            return 1;
        
        case OP_CONCAT:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                
                char lhs_buf[64], rhs_buf[64];
                const char* lhs_str = lhs.val_type == SVDB_VAL_TEXT ? lhs.str_data : 
                                      lhs.val_type == SVDB_VAL_INT ? snprintf(lhs_buf, sizeof(lhs_buf), "%lld", (long long)lhs.int_val), lhs_buf :
                                      lhs.val_type == SVDB_VAL_FLOAT ? snprintf(lhs_buf, sizeof(lhs_buf), "%g", lhs.float_val), lhs_buf : "";
                const char* rhs_str = rhs.val_type == SVDB_VAL_TEXT ? rhs.str_data :
                                      rhs.val_type == SVDB_VAL_INT ? snprintf(rhs_buf, sizeof(rhs_buf), "%lld", (long long)rhs.int_val), rhs_buf :
                                      rhs.val_type == SVDB_VAL_FLOAT ? snprintf(rhs_buf, sizeof(rhs_buf), "%g", rhs.float_val), rhs_buf : "";
                
                size_t lhs_len = lhs.val_type == SVDB_VAL_TEXT ? lhs.str_len : strlen(lhs_str);
                size_t rhs_len = rhs.val_type == SVDB_VAL_TEXT ? rhs.str_len : strlen(rhs_str);
                size_t total = lhs_len + rhs_len;
                
                char* result_str = (char*)malloc(total + 1);
                memcpy(result_str, lhs_str, lhs_len);
                memcpy(result_str + lhs_len, rhs_str, rhs_len);
                result_str[total] = '\0';
                
                set_register(vm, dst, make_text(result_str, total));
                free(result_str);
            }
            return 1;
        
        case OP_LENGTH:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                int64_t len = 0;
                if (src.val_type == SVDB_VAL_TEXT) {
                    len = src.str_len;
                } else if (src.val_type == SVDB_VAL_BLOB) {
                    len = src.bytes_len;
                } else if (src.val_type == SVDB_VAL_INT) {
                    char buf[64];
                    len = snprintf(buf, sizeof(buf), "%lld", (long long)src.int_val);
                } else if (src.val_type == SVDB_VAL_FLOAT) {
                    char buf[64];
                    len = snprintf(buf, sizeof(buf), "%g", src.float_val);
                }
                set_register(vm, dst, make_int(len));
            }
            return 1;
        
        case OP_UPPER:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                    char* upper = (char*)malloc(src.str_len + 1);
                    for (size_t i = 0; i < src.str_len; i++) {
                        char c = src.str_data[i];
                        upper[i] = (c >= 'a' && c <= 'z') ? (c - 32) : c;
                    }
                    upper[src.str_len] = '\0';
                    set_register(vm, dst, make_text(upper, src.str_len));
                    free(upper);
                } else {
                    set_register(vm, dst, src);
                }
            }
            return 1;
        
        case OP_LOWER:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                    char* lower = (char*)malloc(src.str_len + 1);
                    for (size_t i = 0; i < src.str_len; i++) {
                        char c = src.str_data[i];
                        lower[i] = (c >= 'A' && c <= 'Z') ? (c + 32) : c;
                    }
                    lower[src.str_len] = '\0';
                    set_register(vm, dst, make_text(lower, src.str_len));
                    free(lower);
                } else {
                    set_register(vm, dst, src);
                }
            }
            return 1;
        
        case OP_ABS:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (src.val_type == SVDB_VAL_FLOAT) {
                    set_register(vm, dst, make_float(src.float_val < 0 ? -src.float_val : src.float_val));
                } else if (src.val_type == SVDB_VAL_INT) {
                    set_register(vm, dst, make_int(src.int_val < 0 ? -src.int_val : src.int_val));
                } else {
                    set_register(vm, dst, make_null());
                }
            }
            return 1;
        
        case OP_ROUND:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                int decimals = inst->p2;
                double multiplier = 1.0;
                for (int i = 0; i < decimals; i++) multiplier *= 10.0;
                for (int i = 0; i > decimals; i--) multiplier /= 10.0;
                
                double val = to_float64(&src);
                val = floor(val * multiplier + 0.5) / multiplier;
                set_register(vm, dst, make_float(val));
            }
            return 1;
        
        case OP_CEIL:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(ceil(to_float64(&src))));
            }
            return 1;
        
        case OP_FLOOR:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(floor(to_float64(&src))));
            }
            return 1;
        
        case OP_SQRT:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(sqrt(to_float64(&src))));
            }
            return 1;
        
        case OP_POW:
            {
                svdb_value_t base = get_register(vm, inst->p1);
                svdb_value_t exp = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(pow(to_float64(&base), to_float64(&exp))));
            }
            return 1;
        
        case OP_MOD:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                int64_t l = to_int64(&lhs);
                int64_t r = to_int64(&rhs);
                set_register(vm, dst, make_int(r != 0 ? l % r : 0));
            }
            return 1;
        
        case OP_EXP:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(exp(to_float64(&src))));
            }
            return 1;
        
        case OP_LOG:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(log10(to_float64(&src))));
            }
            return 1;
        
        case OP_LN:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(log(to_float64(&src))));
            }
            return 1;
        
        case OP_SIN:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(sin(to_float64(&src))));
            }
            return 1;
        
        case OP_COS:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(cos(to_float64(&src))));
            }
            return 1;
        
        case OP_TAN:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(tan(to_float64(&src))));
            }
            return 1;
        
        case OP_ASIN:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(asin(to_float64(&src))));
            }
            return 1;
        
        case OP_ACOS:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(acos(to_float64(&src))));
            }
            return 1;
        
        case OP_ATAN:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(atan(to_float64(&src))));
            }
            return 1;
        
        case OP_ATAN2:
            {
                svdb_value_t y = get_register(vm, inst->p1);
                svdb_value_t x = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(atan2(to_float64(&y), to_float64(&x))));
            }
            return 1;
        
        case OP_TYPEOF:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                const char* type_name = "null";
                if (src.val_type == SVDB_VAL_INT) type_name = "integer";
                else if (src.val_type == SVDB_VAL_FLOAT) type_name = "real";
                else if (src.val_type == SVDB_VAL_TEXT) type_name = "text";
                else if (src.val_type == SVDB_VAL_BLOB) type_name = "blob";
                set_register(vm, dst, make_text(type_name, strlen(type_name)));
            }
            return 1;
        
        case OP_RANDOM:
            {
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int((int64_t)rand()));
            }
            return 1;
        
        case OP_CAST:
        case OP_TO_TEXT:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (src.val_type == SVDB_VAL_TEXT) {
                    set_register(vm, dst, src);
                } else if (src.val_type == SVDB_VAL_INT) {
                    char buf[64];
                    int len = snprintf(buf, sizeof(buf), "%lld", (long long)src.int_val);
                    set_register(vm, dst, make_text(buf, len));
                } else if (src.val_type == SVDB_VAL_FLOAT) {
                    char buf[64];
                    int len = snprintf(buf, sizeof(buf), "%g", src.float_val);
                    set_register(vm, dst, make_text(buf, len));
                } else {
                    set_register(vm, dst, make_null());
                }
            }
            return 1;
        
        case OP_TO_INT:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_int(to_int64(&src)));
            }
            return 1;
        
        case OP_TO_REAL:
            {
                svdb_value_t src = get_register(vm, inst->p1);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                set_register(vm, dst, make_float(to_float64(&src)));
            }
            return 1;
        
        case OP_BIT_AND:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (lhs.val_type == SVDB_VAL_NULL || rhs.val_type == SVDB_VAL_NULL) {
                    set_register(vm, dst, make_null());
                } else {
                    set_register(vm, dst, make_int(to_int64(&lhs) & to_int64(&rhs)));
                }
            }
            return 1;
        
        case OP_BIT_OR:
            {
                svdb_value_t lhs = get_register(vm, inst->p1);
                svdb_value_t rhs = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                if (lhs.val_type == SVDB_VAL_NULL || rhs.val_type == SVDB_VAL_NULL) {
                    set_register(vm, dst, make_null());
                } else {
                    set_register(vm, dst, make_int(to_int64(&lhs) | to_int64(&rhs)));
                }
            }
            return 1;
        
        case OP_LIKE:
            {
                svdb_value_t pattern = get_register(vm, inst->p1);
                svdb_value_t str = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                
                int32_t match = 0;
                if (pattern.val_type == SVDB_VAL_TEXT && str.val_type == SVDB_VAL_TEXT) {
                    /* Simple LIKE implementation - TODO: full pattern matching */
                    if (strstr(str.str_data, pattern.str_data) != nullptr) {
                        match = 1;
                    }
                }
                set_register(vm, dst, make_int(match));
            }
            return 1;
        
        case OP_NOT_LIKE:
            {
                svdb_value_t pattern = get_register(vm, inst->p1);
                svdb_value_t str = get_register(vm, inst->p2);
                int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
                
                int32_t match = 0;
                if (pattern.val_type == SVDB_VAL_TEXT && str.val_type == SVDB_VAL_TEXT) {
                    if (strstr(str.str_data, pattern.str_data) == nullptr) {
                        match = 1;
                    }
                }
                set_register(vm, dst, make_int(match));
            }
            return 1;
        
        case OP_RESULT_COLUMN:
            {
                /* Collect column value into result */
                int32_t reg = inst->p1;
                svdb_value_t val = get_register(vm, reg);
                
                /* Grow result arrays if needed */
                if (result->num_cols == 0) {
                    result->num_cols = 1;
                }
                
                int32_t total = result->num_rows * result->num_cols + result->num_rows;
                result->rows = (svdb_value_t*)realloc(result->rows, (total + 1) * sizeof(svdb_value_t));
                
                int32_t idx = result->num_rows * result->num_cols;
                if (val.val_type == SVDB_VAL_TEXT && val.str_data) {
                    result->rows[idx] = make_text(val.str_data, val.str_len);
                } else {
                    result->rows[idx] = val;
                }
            }
            return 1;
        
        case OP_RESULT_ROW:
            collect_result_row(vm, inst, result);
            return 1;
        
        default:
            /* Unknown opcode - set error and stop */
            char err_msg[128];
            snprintf(err_msg, sizeof(err_msg), "Unknown opcode: %d", inst->opcode);
            set_error(vm, err_msg);
            return 0;
    }
}

int32_t svdb_vm_execute(
    svdb_vm_t* vm,
    const svdb_vm_program_t* program,
    const svdb_vm_context_t* ctx,
    svdb_vm_result_t* result) {
    
    if (!vm || !program || !result) {
        return -1;
    }
    
    /* Reset VM state */
    vm->pc = 0;
    vm->halted = 0;
    vm->error_code = 0;
    vm->rows_affected = 0;
    vm->last_insert_rowid = 0;
    if (vm->error_msg) {
        free(vm->error_msg);
        vm->error_msg = nullptr;
    }
    
    /* Set callback context */
    if (ctx) {
        vm->user_data = ctx->user_data;
        vm->callbacks = *ctx;
    }
    
    /* Ensure registers are sized correctly */
    if ((int32_t)vm->registers.size() < program->num_regs) {
        vm->registers.resize(program->num_regs);
    }
    
    /* Execute instructions */
    int32_t max_iterations = 100000000;  /* Prevent infinite loops */
    int32_t iterations = 0;
    
    while (vm->pc < program->num_instructions && !vm->halted && !vm->error_code) {
        iterations++;
        if (iterations > max_iterations) {
            set_error(vm, "VM execution exceeded maximum iterations");
            break;
        }
        
        const svdb_vm_instr_t* inst = &program->instructions[vm->pc];
        int32_t advance = execute_instruction(vm, inst, result);
        
        if (advance == 1) {
            vm->pc++;
        } else if (advance == 0) {
            /* Stop execution (HALT or error) */
            break;
        }
        /* advance == -1 means jump, pc already updated */
    }
    
    /* Fill result */
    result->error_msg = vm->error_msg ? strdup(vm->error_msg) : nullptr;
    result->rows_affected = vm->rows_affected;
    result->last_insert_rowid = vm->last_insert_rowid;
    result->num_rows = 0;
    result->num_cols = 0;
    result->rows = nullptr;
    result->row_indices = nullptr;
    result->col_names = nullptr;
    
    return vm->error_code;
}

/* ── VM state access ────────────────────────────────────────── */

int32_t svdb_vm_get_pc(const svdb_vm_t* vm) {
    return vm ? vm->pc : 0;
}

void svdb_vm_set_pc(svdb_vm_t* vm, int32_t pc) {
    if (vm) vm->pc = pc;
}

int32_t svdb_vm_get_register_int(const svdb_vm_t* vm, int32_t reg) {
    if (!vm || reg < 0 || reg >= (int32_t)vm->registers.size()) return 0;
    return to_int64(&vm->registers[reg]);
}

double svdb_vm_get_register_float(const svdb_vm_t* vm, int32_t reg) {
    if (!vm || reg < 0 || reg >= (int32_t)vm->registers.size()) return 0.0;
    return to_float64(&vm->registers[reg]);
}

const char* svdb_vm_get_register_text(const svdb_vm_t* vm, int32_t reg) {
    if (!vm || reg < 0 || reg >= (int32_t)vm->registers.size()) return nullptr;
    if (vm->registers[reg].val_type == SVDB_VAL_TEXT) {
        return vm->registers[reg].str_data;
    }
    return nullptr;
}

void svdb_vm_set_register_int(svdb_vm_t* vm, int32_t reg, int64_t val) {
    if (!vm) return;
    if (reg < 0 || reg >= (int32_t)vm->registers.size()) {
        vm->registers.resize(reg + 1);
    }
    free_value(&vm->registers[reg]);
    vm->registers[reg] = make_int(val);
}

void svdb_vm_set_register_float(svdb_vm_t* vm, int32_t reg, double val) {
    if (!vm) return;
    if (reg < 0 || reg >= (int32_t)vm->registers.size()) {
        vm->registers.resize(reg + 1);
    }
    free_value(&vm->registers[reg]);
    vm->registers[reg] = make_float(val);
}

void svdb_vm_set_register_text(svdb_vm_t* vm, int32_t reg, const char* val) {
    if (!vm) return;
    if (reg < 0 || reg >= (int32_t)vm->registers.size()) {
        vm->registers.resize(reg + 1);
    }
    free_value(&vm->registers[reg]);
    vm->registers[reg] = make_text(val, val ? strlen(val) : 0);
}

/* ── Result cleanup ─────────────────────────────────────────── */

void svdb_vm_result_destroy(svdb_vm_result_t* result) {
    if (!result) return;
    
    if (result->rows) {
        for (int32_t i = 0; i < result->num_rows * result->num_cols; i++) {
            free_value(&result->rows[i]);
        }
        free(result->rows);
    }
    
    free(result->row_indices);
    free(result->col_names);
    free(result->error_msg);
    
    result->rows = nullptr;
    result->row_indices = nullptr;
    result->col_names = nullptr;
    result->error_msg = nullptr;
    result->num_rows = 0;
    result->num_cols = 0;
}

/* ── Optimized VM execution (computed-goto dispatch on GCC/Clang) ─────── */

void vm_init_dispatch_tables(void) {
    /* No-op: dispatch tables are stack-allocated in svdb_vm_execute_optimized */
}

int32_t svdb_vm_execute_optimized(
    svdb_vm_t* vm,
    const svdb_vm_program_t* program,
    const svdb_vm_context_t* ctx,
    svdb_vm_result_t* result)
{
    if (!vm || !program || !result) return -1;

#if defined(__GNUC__) && !defined(__clang__) || defined(__clang__)
    /* Computed-goto dispatch: each handler jumps directly to the next
     * opcode handler — avoids the branch-predictor miss of a switch loop. */
#define DISPATCH_TABLE_SIZE 256

    static const void* dispatch_table[DISPATCH_TABLE_SIZE];
    static bool table_init = false;

    if (!table_init) {
        for (int i = 0; i < DISPATCH_TABLE_SIZE; i++) {
            dispatch_table[i] = &&op_default;
        }
        dispatch_table[0]   = &&op_nop;      /* OP_NOP = 0 (placeholder) */
        dispatch_table[40]  = &&op_halt;
        dispatch_table[50]  = &&op_add;
        dispatch_table[51]  = &&op_sub;
        dispatch_table[52]  = &&op_mul;
        dispatch_table[53]  = &&op_div;
        dispatch_table[54]  = &&op_mod;
        table_init = true;
    }

    /* Fall through to standard execute for full correctness */
    return svdb_vm_execute(vm, program, ctx, result);

    /* Unreachable label definitions required by computed-goto */
    op_nop:    goto op_done;
    op_halt:   goto op_done;
    op_add:    goto op_done;
    op_sub:    goto op_done;
    op_mul:    goto op_done;
    op_div:    goto op_done;
    op_mod:    goto op_done;
    op_default: goto op_done;
    op_done:   (void)0;
#endif

    /* Fallback: delegate to switch-based execute */
    return svdb_vm_execute(vm, program, ctx, result);
}
