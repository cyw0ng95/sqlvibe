#include "bytecode_vm.h"
#include "opcodes.h"
#include "value.h"
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>

static const int SVDB_VM_MAX_REGS = 256;

struct Register {
    int         val_type;   /* SVDB_TYPE_* */
    int64_t     int_val;
    double      real_val;
    std::string str_val;    /* TEXT or BLOB */

    Register() : val_type(0 /* NULL */), int_val(0), real_val(0.0) {}
};

struct svdb_bytecode_vm_t {
    std::vector<Register> regs;
    bool                  halted;

    svdb_bytecode_vm_t() : regs(SVDB_VM_MAX_REGS), halted(false) {}

    void reset() {
        for (auto& r : regs) {
            r.val_type = 0;
            r.int_val  = 0;
            r.real_val = 0.0;
            r.str_val.clear();
        }
        halted = false;
    }
};

/* ── helpers ─────────────────────────────────────────────── */

static inline bool valid_reg(const svdb_bytecode_vm_t* vm, int idx) {
    return idx >= 0 && idx < (int)vm->regs.size();
}

static void copy_val_to_reg(Register& r, int vtype, int64_t vi, double vr,
                             const char* text, size_t text_len) {
    r.val_type = vtype;
    r.int_val  = vi;
    r.real_val = vr;
    if ((vtype == 3 /* TEXT */ || vtype == 4 /* BLOB */) && text) {
        r.str_val.assign(text, text_len);
    } else {
        r.str_val.clear();
    }
}

/* ── C API ───────────────────────────────────────────────── */

extern "C" {

svdb_bytecode_vm_t* svdb_bytecode_vm_create(void) {
    return new svdb_bytecode_vm_t();
}

void svdb_bytecode_vm_destroy(svdb_bytecode_vm_t* vm) {
    delete vm;
}

void svdb_bytecode_vm_set_register(svdb_bytecode_vm_t* vm,
                                    int reg_idx,
                                    int value_type,
                                    int64_t value_int,
                                    double  value_real,
                                    const char* value_text,
                                    size_t  text_len) {
    if (!vm || !valid_reg(vm, reg_idx)) return;
    copy_val_to_reg(vm->regs[(size_t)reg_idx], value_type,
                    value_int, value_real, value_text, text_len);
}

int svdb_bytecode_vm_get_register(svdb_bytecode_vm_t* vm,
                                   int reg_idx,
                                   int* out_type,
                                   int64_t* out_int,
                                   double*  out_real,
                                   char*    out_text,
                                   size_t   text_cap) {
    if (!vm || !valid_reg(vm, reg_idx)) return 0;
    const Register& r = vm->regs[(size_t)reg_idx];
    if (out_type) *out_type = r.val_type;
    if (out_int)  *out_int  = r.int_val;
    if (out_real) *out_real = r.real_val;
    if (out_text && text_cap > 0) {
        size_t copy_len = r.str_val.size() < (text_cap - 1)
                          ? r.str_val.size() : (text_cap - 1);
        memcpy(out_text, r.str_val.data(), copy_len);
        out_text[copy_len] = '\0';
    }
    return 1;
}

int svdb_bytecode_vm_step(svdb_bytecode_vm_t* vm,
                           int opcode,
                           int p1, int p2, int p3,
                           const char* p4_str) {
    if (!vm) return -2;
    if (vm->halted) return -1;

    switch ((svdb_opcode_t)opcode) {
    case SVDB_BC_NOOP:
        break;

    case SVDB_BC_HALT:
        vm->halted = true;
        return -1;

    case SVDB_BC_LOAD_CONST:
        /* p1 = dest reg; p4_str = constant string value (NULL-type if absent). */
        if (!valid_reg(vm, p1)) return -2;
        if (p4_str) {
            copy_val_to_reg(vm->regs[(size_t)p1], 3 /* TEXT */,
                            0, 0.0, p4_str, strlen(p4_str));
        } else {
            vm->regs[(size_t)p1].val_type = 0; /* NULL */
        }
        break;

    case SVDB_BC_COPY:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        vm->regs[(size_t)p2] = vm->regs[(size_t)p1];
        break;

    case SVDB_BC_MOVE:
        /* p1 = src reg, p2 = dest reg (src becomes NULL) */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        vm->regs[(size_t)p2] = vm->regs[(size_t)p1];
        vm->regs[(size_t)p1] = Register{};
        break;

    case SVDB_BC_STORE:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        vm->regs[(size_t)p2] = vm->regs[(size_t)p1];
        break;

    case SVDB_BC_ADD:
    case SVDB_BC_SUB:
    case SVDB_BC_MUL:
    case SVDB_BC_DIV:
    case SVDB_BC_MOD: {
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -2;
        Register& lhs  = vm->regs[(size_t)p1];
        Register& rhs  = vm->regs[(size_t)p2];
        Register& dest = vm->regs[(size_t)p3];
        /* Operate in float if either operand is float, else int. */
        bool use_float = (lhs.val_type == 2) || (rhs.val_type == 2);
        double lv = use_float ? (lhs.val_type == 2 ? lhs.real_val : (double)lhs.int_val) : 0.0;
        double rv = use_float ? (rhs.val_type == 2 ? rhs.real_val : (double)rhs.int_val) : 0.0;
        int64_t li = lhs.int_val, ri = rhs.int_val;
        if (use_float) {
            double result = 0.0;
            if (opcode == SVDB_BC_ADD) result = lv + rv;
            else if (opcode == SVDB_BC_SUB) result = lv - rv;
            else if (opcode == SVDB_BC_MUL) result = lv * rv;
            else if (opcode == SVDB_BC_DIV) result = (rv != 0.0) ? lv / rv : 0.0;
            else result = 0.0; /* MOD not meaningful for floats */
            copy_val_to_reg(dest, 2, 0, result, nullptr, 0);
        } else {
            int64_t result = 0;
            if (opcode == SVDB_BC_ADD) result = li + ri;
            else if (opcode == SVDB_BC_SUB) result = li - ri;
            else if (opcode == SVDB_BC_MUL) result = li * ri;
            else if (opcode == SVDB_BC_DIV) result = (ri != 0) ? li / ri : 0;
            else result = (ri != 0) ? li % ri : 0;
            copy_val_to_reg(dest, 1, result, 0.0, nullptr, 0);
        }
        break;
    }

    case SVDB_BC_NEG:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        {
            Register& src = vm->regs[(size_t)p1];
            if (src.val_type == 2)
                copy_val_to_reg(vm->regs[(size_t)p2], 2, 0, -src.real_val, nullptr, 0);
            else
                copy_val_to_reg(vm->regs[(size_t)p2], 1, -src.int_val, 0.0, nullptr, 0);
        }
        break;

    case SVDB_BC_IS_NULL:
        /* p1 = src reg, p2 = dest reg: dest = 1 if src is NULL else 0 */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        copy_val_to_reg(vm->regs[(size_t)p2], 1,
                        vm->regs[(size_t)p1].val_type == 0 ? 1 : 0, 0.0, nullptr, 0);
        break;

    case SVDB_BC_NOT_NULL:
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -2;
        copy_val_to_reg(vm->regs[(size_t)p2], 1,
                        vm->regs[(size_t)p1].val_type != 0 ? 1 : 0, 0.0, nullptr, 0);
        break;

    default:
        /* Unknown or unimplemented opcode — treat as NOP for extensibility. */
        break;
    }

    return 0;
}

void svdb_bytecode_vm_reset(svdb_bytecode_vm_t* vm) {
    if (vm) vm->reset();
}

} /* extern "C" */
