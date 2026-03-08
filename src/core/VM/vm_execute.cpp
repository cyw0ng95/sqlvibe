/* vm_execute.cpp — VM Execution Engine Implementation */
#include "vm_execute.h"
#include "../DS/query_arena.h"
#include <stdlib.h>
#include <string.h>
#include <cmath>
#include <cerrno>
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

    /* Query arena for TEXT value allocations */
    svdb::ds::QueryArena* text_arena;

    /* Callback context */
    void* user_data;
    svdb_vm_context_t callbacks;
};

/* ── Per-VM arena accessor for text allocation ─────────────────────── */
static svdb::ds::QueryArena* g_default_arena = nullptr;

static svdb::ds::QueryArena* get_text_arena(svdb_vm_t* vm) {
    if (vm && vm->text_arena) return vm->text_arena;
    /* Fallback to global arena for edge cases */
    if (!g_default_arena) {
        g_default_arena = new svdb::ds::QueryArena(64 * 1024, 16 * 1024 * 1024);
    }
    return g_default_arena;
}

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

/* make_text_arena: Allocate TEXT value from arena (preferred for query execution) */
static svdb_value_t make_text_arena(svdb::ds::QueryArena* arena, const char* str, size_t len) {
    svdb_value_t v = {};
    v.val_type = SVDB_VAL_TEXT;
    if (str && len > 0) {
        v.str_data = arena->AllocText(str, len);
        if (v.str_data) {
            v.str_len = (int32_t)len;
        }
    }
    return v;
}

/* make_text: Fallback TEXT allocation using arena if available, else malloc */
static svdb_value_t make_text(const char* str, size_t len) {
    /* Use global default arena as fallback */
    svdb::ds::QueryArena* arena = g_default_arena;
    if (arena) {
        return make_text_arena(arena, str, len);
    }
    /* Fallback to malloc (for cases where arena is not initialized) */
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

/* make_text_vm: Allocate TEXT value using VM's arena (for use during execution) */
static svdb_value_t make_text_vm(svdb_vm_t* vm, const char* str, size_t len) {
    return make_text_arena(get_text_arena(vm), str, len);
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
        case SVDB_VAL_TEXT: {
            if (!v->str_data) return 0;
            /* Use strtoll for faster parsing with error checking */
            char* end;
            errno = 0;
            long long val = strtoll(v->str_data, &end, 10);
            /* Only accept if entire string was valid */
            if (errno != 0 || end == v->str_data || *end != '\0') {
                return 0;
            }
            return (int64_t)val;
        }
        default: return 0;
    }
}

static double to_float64(const svdb_value_t* v) {
    if (!v || v->val_type == SVDB_VAL_NULL) return 0.0;
    switch (v->val_type) {
        case SVDB_VAL_INT: return (double)v->int_val;
        case SVDB_VAL_FLOAT: return v->float_val;
        case SVDB_VAL_TEXT: {
            if (!v->str_data) return 0.0;
            /* Use strtod for faster parsing with error checking */
            char* end;
            errno = 0;
            double val = strtod(v->str_data, &end);
            /* Only accept if entire string was valid */
            if (errno != 0 || end == v->str_data || *end != '\0') {
                return 0.0;
            }
            return val;
        }
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

    /* Create query arena for TEXT allocations */
    vm->text_arena = new svdb::ds::QueryArena(64 * 1024, 16 * 1024 * 1024);

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

    /* Destroy query arena */
    if (vm->text_arena) {
        delete vm->text_arena;
        vm->text_arena = nullptr;
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
        /* Use arena for TEXT copy */
        vm->registers[reg].str_data = vm->text_arena->AllocText(val.str_data, val.str_len);
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
                result->rows[idx] = make_text_vm(vm, val.str_data, val.str_len);
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
                    val = make_text_vm(vm, inst->p4_str, strlen(inst->p4_str));
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
                    dst = make_text_vm(vm, src.str_data, src.str_len);
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
                    dst = make_text_vm(vm, src.str_data, src.str_len);
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
                        fallback = make_text_vm(vm, inst->p4_str, strlen(inst->p4_str));
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

                /* Allocate from arena instead of malloc */
                char* result_str = vm->text_arena->AllocText(lhs_str, lhs_len);
                if (result_str) {
                    /* Append rhs to the result */
                    char* combined = (char*)vm->text_arena->Alloc(total + 1);
                    if (combined) {
                        memcpy(combined, lhs_str, lhs_len);
                        memcpy(combined + lhs_len, rhs_str, rhs_len);
                        combined[total] = '\0';
                        svdb_value_t text_val = {};
                        text_val.val_type = SVDB_VAL_TEXT;
                        text_val.str_data = combined;
                        text_val.str_len = (int32_t)total;
                        set_register(vm, dst, text_val);
                    }
                }
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
                    /* Allocate from arena instead of malloc */
                    char* upper = (char*)vm->text_arena->Alloc(src.str_len + 1);
                    if (upper) {
                        for (size_t i = 0; i < src.str_len; i++) {
                            char c = src.str_data[i];
                            upper[i] = (c >= 'a' && c <= 'z') ? (c - 32) : c;
                        }
                        upper[src.str_len] = '\0';
                        svdb_value_t text_val = {};
                        text_val.val_type = SVDB_VAL_TEXT;
                        text_val.str_data = upper;
                        text_val.str_len = src.str_len;
                        set_register(vm, dst, text_val);
                    }
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
                    /* Allocate from arena instead of malloc */
                    char* lower = (char*)vm->text_arena->Alloc(src.str_len + 1);
                    if (lower) {
                        for (size_t i = 0; i < src.str_len; i++) {
                            char c = src.str_data[i];
                            lower[i] = (c >= 'A' && c <= 'Z') ? (c + 32) : c;
                        }
                        lower[src.str_len] = '\0';
                        svdb_value_t text_val = {};
                        text_val.val_type = SVDB_VAL_TEXT;
                        text_val.str_data = lower;
                        text_val.str_len = src.str_len;
                        set_register(vm, dst, text_val);
                    }
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
                set_register(vm, dst, make_text_vm(vm, type_name, strlen(type_name)));
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
                    set_register(vm, dst, make_text_vm(vm, buf, len));
                } else if (src.val_type == SVDB_VAL_FLOAT) {
                    char buf[64];
                    int len = snprintf(buf, sizeof(buf), "%g", src.float_val);
                    set_register(vm, dst, make_text_vm(vm, buf, len));
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
                    result->rows[idx] = make_text_vm(vm, val.str_data, val.str_len);
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

    /* Reset query arena for new query execution */
    if (vm->text_arena) {
        vm->text_arena->Reset();
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
    vm->registers[reg] = make_text_vm(vm, val, val ? strlen(val) : 0);
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
    /* No-op: dispatch tables are initialized on first use in svdb_vm_execute_optimized */
}

/* Internal opcodes enum for dispatch (must match execute_instruction) */
enum {
    OP_NOP = 0,
    OP_HALT = 1,
    OP_GOTO = 2,
    OP_GOSUB = 3,
    OP_RETURN = 4,
    OP_INIT = 5,
    OP_LOAD_CONST = 10,
    OP_NULL = 11,
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
    OP_MAX_OPCODE = 136
};

#if defined(__GNUC__) || defined(__clang__)
#define USE_COMPUTED_GOTO 1
#else
#define USE_COMPUTED_GOTO 0
#endif

int32_t svdb_vm_execute_optimized(
    svdb_vm_t* vm,
    const svdb_vm_program_t* program,
    const svdb_vm_context_t* ctx,
    svdb_vm_result_t* result)
{
    if (!vm || !program || !result) return -1;

#if USE_COMPUTED_GOTO
    /* Computed-goto dispatch table initialized once */
    #define DISPATCH_TABLE_SIZE 256
    static const void* dispatch_table[DISPATCH_TABLE_SIZE];
    static bool table_init = false;

    if (!table_init) {
        /* Initialize all entries to default handler */
        for (int i = 0; i < DISPATCH_TABLE_SIZE; i++) {
            dispatch_table[i] = &&op_default;
        }
        /* Map opcodes to labels */
        dispatch_table[OP_NOP]         = &&op_nop;
        dispatch_table[OP_HALT]        = &&op_halt;
        dispatch_table[OP_GOTO]        = &&op_goto;
        dispatch_table[OP_GOSUB]       = &&op_gosub;
        dispatch_table[OP_RETURN]      = &&op_return;
        dispatch_table[OP_INIT]        = &&op_init;
        dispatch_table[OP_LOAD_CONST]  = &&op_load_const;
        dispatch_table[OP_NULL]        = &&op_null;
        dispatch_table[OP_MOVE]        = &&op_move;
        dispatch_table[OP_COPY]        = &&op_copy;
        dispatch_table[OP_SCOPY]       = &&op_scopy;
        dispatch_table[OP_INT_COPY]    = &&op_int_copy;
        dispatch_table[OP_ADD]         = &&op_add;
        dispatch_table[OP_SUB]         = &&op_sub;
        dispatch_table[OP_MUL]         = &&op_mul;
        dispatch_table[OP_DIV]         = &&op_div;
        dispatch_table[OP_REM]         = &&op_rem;
        dispatch_table[OP_ADD_IMM]     = &&op_add_imm;
        dispatch_table[OP_EQ]          = &&op_eq;
        dispatch_table[OP_NE]          = &&op_ne;
        dispatch_table[OP_LT]          = &&op_lt;
        dispatch_table[OP_LE]          = &&op_le;
        dispatch_table[OP_GT]          = &&op_gt;
        dispatch_table[OP_GE]          = &&op_ge;
        dispatch_table[OP_IS]          = &&op_is;
        dispatch_table[OP_IS_NOT]      = &&op_is_not;
        dispatch_table[OP_IS_NULL]     = &&op_is_null;
        dispatch_table[OP_NOT_NULL]    = &&op_not_null;
        dispatch_table[OP_IF_NULL]     = &&op_if_null;
        dispatch_table[OP_IF_NULL2]    = &&op_if_null2;
        dispatch_table[OP_IF]          = &&op_if;
        dispatch_table[OP_IF_NOT]      = &&op_if_not;
        dispatch_table[OP_RESULT_COLUMN] = &&op_result_column;
        dispatch_table[OP_RESULT_ROW]  = &&op_result_row;
        dispatch_table[OP_CONCAT]      = &&op_concat;
        dispatch_table[OP_SUBSTR]      = &&op_substr;
        dispatch_table[OP_LENGTH]      = &&op_length;
        dispatch_table[OP_UPPER]       = &&op_upper;
        dispatch_table[OP_LOWER]       = &&op_lower;
        dispatch_table[OP_TRIM]        = &&op_trim;
        dispatch_table[OP_LTRIM]       = &&op_ltrim;
        dispatch_table[OP_RTRIM]       = &&op_rtrim;
        dispatch_table[OP_REPLACE]     = &&op_replace;
        dispatch_table[OP_INSTR]       = &&op_instr;
        dispatch_table[OP_LIKE]        = &&op_like;
        dispatch_table[OP_NOT_LIKE]    = &&op_not_like;
        dispatch_table[OP_GLOB]        = &&op_glob;
        dispatch_table[OP_MATCH]       = &&op_match;
        dispatch_table[OP_BIT_AND]     = &&op_bit_and;
        dispatch_table[OP_BIT_OR]      = &&op_bit_or;
        dispatch_table[OP_ABS]         = &&op_abs;
        dispatch_table[OP_ROUND]       = &&op_round;
        dispatch_table[OP_CEIL]        = &&op_ceil;
        dispatch_table[OP_FLOOR]       = &&op_floor;
        dispatch_table[OP_SQRT]        = &&op_sqrt;
        dispatch_table[OP_POW]         = &&op_pow;
        dispatch_table[OP_MOD]         = &&op_mod;
        dispatch_table[OP_EXP]         = &&op_exp;
        dispatch_table[OP_LOG]         = &&op_log;
        dispatch_table[OP_LN]          = &&op_ln;
        dispatch_table[OP_SIN]         = &&op_sin;
        dispatch_table[OP_COS]         = &&op_cos;
        dispatch_table[OP_TAN]         = &&op_tan;
        dispatch_table[OP_ASIN]        = &&op_asin;
        dispatch_table[OP_ACOS]        = &&op_acos;
        dispatch_table[OP_ATAN]        = &&op_atan;
        dispatch_table[OP_ATAN2]       = &&op_atan2;
        dispatch_table[OP_TYPEOF]      = &&op_typeof;
        dispatch_table[OP_RANDOM]      = &&op_random;
        dispatch_table[OP_CAST]        = &&op_cast;
        dispatch_table[OP_TO_TEXT]     = &&op_to_text;
        dispatch_table[OP_TO_INT]      = &&op_to_int;
        dispatch_table[OP_TO_REAL]     = &&op_to_real;
        table_init = true;
    }
    #undef DISPATCH_TABLE_SIZE

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

    /* Initialize result */
    result->error_msg = nullptr;
    result->rows_affected = 0;
    result->last_insert_rowid = 0;
    result->num_rows = 0;
    result->num_cols = 0;
    result->rows = nullptr;
    result->row_indices = nullptr;
    result->col_names = nullptr;

    /* Execution loop with computed goto */
    const svdb_vm_instr_t* instructions = program->instructions;
    int32_t num_instructions = program->num_instructions;
    const svdb_vm_instr_t* inst = nullptr;  /* Declared before any goto */

    /* Load first instruction and jump to handler */
    if (vm->pc >= num_instructions) goto execution_done;

    inst = &instructions[vm->pc];
    goto *dispatch_table[inst->opcode];

    /* ── Opcode handlers ───────────────────────────────────────── */

    #define DISPATCH_NEXT() \
        vm->pc++; \
        if (vm->pc >= num_instructions || vm->halted) goto execution_done; \
        inst = &instructions[vm->pc]; \
        goto *dispatch_table[inst->opcode]

    #define DISPATCH_JUMP(new_pc) \
        vm->pc = (new_pc); \
        if (vm->pc >= num_instructions || vm->halted) goto execution_done; \
        inst = &instructions[vm->pc]; \
        goto *dispatch_table[inst->opcode]

    op_nop:
        DISPATCH_NEXT();

    op_halt:
        vm->halted = 1;
        goto execution_done;

    op_goto:
        DISPATCH_JUMP(inst->p2);

    op_gosub:
        if ((size_t)vm->pc + 1 < vm->registers.size()) {
            vm->registers.push_back(make_int(vm->pc + 1));
        }
        DISPATCH_JUMP(inst->p2);

    op_return:
        if (!vm->registers.empty()) {
            svdb_value_t ret = vm->registers.back();
            vm->registers.pop_back();
            if (ret.val_type == SVDB_VAL_INT) {
                DISPATCH_JUMP((int32_t)ret.int_val);
            }
        }
        DISPATCH_NEXT();

    op_init:
        if (inst->p2 != 0) {
            DISPATCH_JUMP(inst->p2);
        }
        DISPATCH_NEXT();

    op_load_const:
        {
            svdb_value_t val;
            if (inst->p4_type == 1) {
                val = make_int(inst->p4_int);
            } else if (inst->p4_type == 3) {
                val = make_float(inst->p4_float);
            } else if (inst->p4_type == 2 && inst->p4_str) {
                val = make_text_vm(vm, inst->p4_str, strlen(inst->p4_str));
            } else {
                val = make_null();
            }
            set_register(vm, inst->p1, val);
        }
        DISPATCH_NEXT();

    op_null:
        set_register(vm, inst->p1, make_null());
        DISPATCH_NEXT();

    op_move:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            svdb_value_t dst;
            if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                dst = make_text_vm(vm, src.str_data, src.str_len);
            } else {
                dst = src;
            }
            set_register(vm, inst->p2, dst);
        }
        DISPATCH_NEXT();

    op_copy:
    op_scopy:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            svdb_value_t dst;
            if (src.val_type == SVDB_VAL_TEXT && src.str_data) {
                dst = make_text_vm(vm, src.str_data, src.str_len);
            } else {
                dst = src;
            }
            set_register(vm, inst->p2, dst);
        }
        DISPATCH_NEXT();

    op_int_copy:
        set_register(vm, inst->p2, make_int(inst->p4_int));
        DISPATCH_NEXT();

    op_add:
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
        DISPATCH_NEXT();

    op_sub:
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
        DISPATCH_NEXT();

    op_mul:
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
        DISPATCH_NEXT();

    op_div:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(to_float64(&lhs) / to_float64(&rhs)));
        }
        DISPATCH_NEXT();

    op_rem:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            int64_t l = to_int64(&lhs);
            int64_t r = to_int64(&rhs);
            set_register(vm, dst, make_int(r != 0 ? l % r : 0));
        }
        DISPATCH_NEXT();

    op_add_imm:
        {
            svdb_value_t v = get_register(vm, inst->p1);
            if (v.val_type == SVDB_VAL_FLOAT) {
                set_register(vm, inst->p1, make_float(v.float_val + (double)inst->p2));
            } else {
                set_register(vm, inst->p1, make_int(to_int64(&v) + inst->p2));
            }
        }
        DISPATCH_NEXT();

    op_eq:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) == 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_ne:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) != 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_lt:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) < 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_le:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) <= 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_gt:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) > 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_ge:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = (compare_values(&lhs, &rhs) >= 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_is:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = ((lhs.val_type == SVDB_VAL_NULL && rhs.val_type == SVDB_VAL_NULL) ||
                              compare_values(&lhs, &rhs) == 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_is_not:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t cmp_result = ((lhs.val_type == SVDB_VAL_NULL && rhs.val_type != SVDB_VAL_NULL) ||
                              (lhs.val_type != SVDB_VAL_NULL && rhs.val_type == SVDB_VAL_NULL) ||
                              compare_values(&lhs, &rhs) != 0) ? 1 : 0;
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(cmp_result));
        }
        DISPATCH_NEXT();

    op_is_null:
        {
            svdb_value_t val = get_register(vm, inst->p1);
            if (val.val_type == SVDB_VAL_NULL) {
                DISPATCH_JUMP(inst->p2);
            }
        }
        DISPATCH_NEXT();

    op_not_null:
        {
            svdb_value_t val = get_register(vm, inst->p1);
            if (val.val_type != SVDB_VAL_NULL) {
                DISPATCH_JUMP(inst->p2);
            }
        }
        DISPATCH_NEXT();

    op_if_null:
        {
            svdb_value_t val = get_register(vm, inst->p1);
            if (val.val_type == SVDB_VAL_NULL) {
                svdb_value_t fallback;
                if (inst->p4_type == 1) {
                    fallback = make_int(inst->p4_int);
                } else if (inst->p4_type == 3) {
                    fallback = make_float(inst->p4_float);
                } else if (inst->p4_type == 2 && inst->p4_str) {
                    fallback = make_text_vm(vm, inst->p4_str, strlen(inst->p4_str));
                } else {
                    fallback = make_null();
                }
                set_register(vm, inst->p1, fallback);
            }
        }
        DISPATCH_NEXT();

    op_if_null2:
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
        DISPATCH_NEXT();

    op_if:
        {
            svdb_value_t val = get_register(vm, inst->p1);
            if (to_bool(&val)) {
                DISPATCH_JUMP(inst->p2);
            }
        }
        DISPATCH_NEXT();

    op_if_not:
        {
            svdb_value_t val = get_register(vm, inst->p1);
            if (!to_bool(&val)) {
                DISPATCH_JUMP(inst->p2);
            }
        }
        DISPATCH_NEXT();

    op_result_column:
        {
            int32_t reg = inst->p1;
            svdb_value_t val = get_register(vm, reg);
            if (result->num_cols == 0) {
                result->num_cols = 1;
            }
            int32_t total = result->num_rows * result->num_cols + result->num_rows;
            result->rows = (svdb_value_t*)realloc(result->rows, (total + 1) * sizeof(svdb_value_t));
            int32_t idx = result->num_rows * result->num_cols;
            if (val.val_type == SVDB_VAL_TEXT && val.str_data) {
                result->rows[idx] = make_text_vm(vm, val.str_data, val.str_len);
            } else {
                result->rows[idx] = val;
            }
        }
        DISPATCH_NEXT();

    op_result_row:
        collect_result_row(vm, inst, result);
        DISPATCH_NEXT();

    op_concat:
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

            char* concat_result = (char*)malloc(total + 1);
            memcpy(concat_result, lhs_str, lhs_len);
            memcpy(concat_result + lhs_len, rhs_str, rhs_len);
            concat_result[total] = '\0';

            set_register(vm, dst, make_text_vm(vm, concat_result, total));
            free(concat_result);
        }
        DISPATCH_NEXT();

    op_substr:
        /* TODO: Implement SUBSTR */
        DISPATCH_NEXT();

    op_length:
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
        DISPATCH_NEXT();

    op_upper:
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
                set_register(vm, dst, make_text_vm(vm, upper, src.str_len));
                free(upper);
            } else {
                set_register(vm, dst, src);
            }
        }
        DISPATCH_NEXT();

    op_lower:
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
                set_register(vm, dst, make_text_vm(vm, lower, src.str_len));
                free(lower);
            } else {
                set_register(vm, dst, src);
            }
        }
        DISPATCH_NEXT();

    op_trim:
    op_ltrim:
    op_rtrim:
        /* TODO: Implement TRIM variants */
        DISPATCH_NEXT();

    op_replace:
    op_instr:
        /* TODO: Implement REPLACE and INSTR */
        DISPATCH_NEXT();

    op_like:
        {
            svdb_value_t pattern = get_register(vm, inst->p1);
            svdb_value_t str = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;

            int32_t match = 0;
            if (pattern.val_type == SVDB_VAL_TEXT && str.val_type == SVDB_VAL_TEXT) {
                if (strstr(str.str_data, pattern.str_data) != nullptr) {
                    match = 1;
                }
            }
            set_register(vm, dst, make_int(match));
        }
        DISPATCH_NEXT();

    op_not_like:
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
        DISPATCH_NEXT();

    op_glob:
    op_match:
        /* TODO: Implement GLOB and MATCH */
        DISPATCH_NEXT();

    op_bit_and:
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
        DISPATCH_NEXT();

    op_bit_or:
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
        DISPATCH_NEXT();

    op_abs:
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
        DISPATCH_NEXT();

    op_round:
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
        DISPATCH_NEXT();

    op_ceil:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(ceil(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_floor:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(floor(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_sqrt:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(sqrt(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_pow:
        {
            svdb_value_t base = get_register(vm, inst->p1);
            svdb_value_t exp = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(pow(to_float64(&base), to_float64(&exp))));
        }
        DISPATCH_NEXT();

    op_mod:
        {
            svdb_value_t lhs = get_register(vm, inst->p1);
            svdb_value_t rhs = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            int64_t l = to_int64(&lhs);
            int64_t r = to_int64(&rhs);
            set_register(vm, dst, make_int(r != 0 ? l % r : 0));
        }
        DISPATCH_NEXT();

    op_exp:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(exp(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_log:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(log10(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_ln:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(log(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_sin:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(sin(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_cos:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(cos(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_tan:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(tan(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_asin:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(asin(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_acos:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(acos(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_atan:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(atan(to_float64(&src))));
        }
        DISPATCH_NEXT();

    op_atan2:
        {
            svdb_value_t y = get_register(vm, inst->p1);
            svdb_value_t x = get_register(vm, inst->p2);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(atan2(to_float64(&y), to_float64(&x))));
        }
        DISPATCH_NEXT();

    op_typeof:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            const char* type_name = "null";
            if (src.val_type == SVDB_VAL_INT) type_name = "integer";
            else if (src.val_type == SVDB_VAL_FLOAT) type_name = "real";
            else if (src.val_type == SVDB_VAL_TEXT) type_name = "text";
            else if (src.val_type == SVDB_VAL_BLOB) type_name = "blob";
            set_register(vm, dst, make_text_vm(vm, type_name, strlen(type_name)));
        }
        DISPATCH_NEXT();

    op_random:
        {
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int((int64_t)rand()));
        }
        DISPATCH_NEXT();

    op_cast:
    op_to_text:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            if (src.val_type == SVDB_VAL_TEXT) {
                set_register(vm, dst, src);
            } else if (src.val_type == SVDB_VAL_INT) {
                char buf[64];
                int len = snprintf(buf, sizeof(buf), "%lld", (long long)src.int_val);
                set_register(vm, dst, make_text_vm(vm, buf, len));
            } else if (src.val_type == SVDB_VAL_FLOAT) {
                char buf[64];
                int len = snprintf(buf, sizeof(buf), "%g", src.float_val);
                set_register(vm, dst, make_text_vm(vm, buf, len));
            } else {
                set_register(vm, dst, make_null());
            }
        }
        DISPATCH_NEXT();

    op_to_int:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_int(to_int64(&src)));
        }
        DISPATCH_NEXT();

    op_to_real:
        {
            svdb_value_t src = get_register(vm, inst->p1);
            int32_t dst = inst->has_dst ? inst->dst_reg : inst->p4_int;
            set_register(vm, dst, make_float(to_float64(&src)));
        }
        DISPATCH_NEXT();

    op_default:
        {
            char err_msg[128];
            snprintf(err_msg, sizeof(err_msg), "Unknown opcode: %d", inst->opcode);
            set_error(vm, err_msg);
        }
        goto execution_done;

    #undef DISPATCH_NEXT
    #undef DISPATCH_JUMP

execution_done:
    result->error_msg = vm->error_msg ? strdup(vm->error_msg) : nullptr;
    result->rows_affected = vm->rows_affected;
    result->last_insert_rowid = vm->last_insert_rowid;
    return vm->error_code;

#else
    /* MSVC fallback: use switch-based dispatch */
    return svdb_vm_execute(vm, program, ctx, result);
#endif
}
