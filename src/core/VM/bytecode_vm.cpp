#include "bytecode_vm.h"
#include "opcodes.h"
#include "value.h"
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <vector>
#include <string>
#include <algorithm>

static const int SVDB_VM_MAX_REGS = 256;

/* SVDB_TYPE_* constants (must match Go vm.VmValType). */
static const int SVDB_TYPE_NULL  = 0;
static const int SVDB_TYPE_INT   = 1;
static const int SVDB_TYPE_REAL  = 2;
static const int SVDB_TYPE_TEXT  = 3;
static const int SVDB_TYPE_BLOB  = 4;

struct Register {
    int         val_type;   /* SVDB_TYPE_* */
    int64_t     int_val;
    double      real_val;
    std::string str_val;    /* TEXT or BLOB */

    Register() : val_type(SVDB_TYPE_NULL), int_val(0), real_val(0.0) {}
};

/* SQL LIKE pattern matching (% = any sequence, _ = single char, case-insensitive). */
static bool like_match(const char* pattern, size_t plen,
                        const char* text, size_t tlen) {
    /* Use dynamic programming to handle % wildcards. */
    const char* p = pattern;
    const char* t = text;
    const char* p_end = pattern + plen;
    const char* t_end = text + tlen;
    const char* star_p = nullptr;
    const char* star_t = text;

    while (t < t_end) {
        if (p < p_end && *p == '_') {
            /* '_' matches exactly one character (no case comparison). */
            ++p; ++t;
        } else if (p < p_end && tolower((unsigned char)*p) == tolower((unsigned char)*t)) {
            ++p; ++t;
        } else if (p < p_end && *p == '%') {
            star_p = p++;
            star_t = t;
        } else if (star_p) {
            p = star_p + 1;
            t = ++star_t;
        } else {
            return false;
        }
    }
    while (p < p_end && *p == '%') ++p;
    return p == p_end;
}

struct svdb_bytecode_vm_t {
    std::vector<Register> regs;
    bool                  halted;
    bool                  has_result;

    svdb_bytecode_vm_t() : regs(SVDB_VM_MAX_REGS), halted(false), has_result(false) {}

    void reset() {
        for (auto& r : regs) {
            r.val_type = SVDB_TYPE_NULL;
            r.int_val  = 0;
            r.real_val = 0.0;
            r.str_val.clear();
        }
        halted     = false;
        has_result = false;
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
    if ((vtype == SVDB_TYPE_TEXT || vtype == SVDB_TYPE_BLOB) && text) {
        r.str_val.assign(text, text_len);
    } else {
        r.str_val.clear();
    }
}

/* Compare two registers: returns -1/0/1 (NULL always compares less). */
static int compare_regs(const Register& a, const Register& b) {
    if (a.val_type == SVDB_TYPE_NULL && b.val_type == SVDB_TYPE_NULL) return 0;
    if (a.val_type == SVDB_TYPE_NULL) return -1;
    if (b.val_type == SVDB_TYPE_NULL) return  1;

    /* Both numeric: promote to double if either is float. */
    bool a_num = (a.val_type == SVDB_TYPE_INT || a.val_type == SVDB_TYPE_REAL);
    bool b_num = (b.val_type == SVDB_TYPE_INT || b.val_type == SVDB_TYPE_REAL);
    if (a_num && b_num) {
        double av = (a.val_type == SVDB_TYPE_REAL) ? a.real_val : (double)a.int_val;
        double bv = (b.val_type == SVDB_TYPE_REAL) ? b.real_val : (double)b.int_val;
        if (av < bv) return -1;
        if (av > bv) return  1;
        return 0;
    }
    /* Both text/blob: lexicographic. */
    bool a_str = (a.val_type == SVDB_TYPE_TEXT || a.val_type == SVDB_TYPE_BLOB);
    bool b_str = (b.val_type == SVDB_TYPE_TEXT || b.val_type == SVDB_TYPE_BLOB);
    if (a_str && b_str) {
        return a.str_val.compare(b.str_val) < 0 ? -1 : (a.str_val > b.str_val ? 1 : 0);
    }
    /* Mixed types: numeric < text. */
    if (a_num && b_str) return -1;
    if (a_str && b_num) return  1;
    return 0;
}

/* Coerce register to double (for arithmetic). */
static double reg_to_float(const Register& r) {
    if (r.val_type == SVDB_TYPE_REAL)  return r.real_val;
    if (r.val_type == SVDB_TYPE_INT)   return (double)r.int_val;
    if (r.val_type == SVDB_TYPE_TEXT || r.val_type == SVDB_TYPE_BLOB) {
        try { return std::stod(r.str_val); } catch (...) { return 0.0; }
    }
    return 0.0;
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
                           const char* p4_str,
                           int* out_jump_pc) {
    if (!vm) return -3;
    if (out_jump_pc) *out_jump_pc = -1;  /* no jump by default */
    if (vm->halted) return -1;
    vm->has_result = false;

    switch ((svdb_opcode_t)opcode) {
    case SVDB_BC_NOOP:
        break;

    case SVDB_BC_HALT:
        vm->halted = true;
        return -1;

    /* ── Load ─────────────────────────────────────────────── */
    case SVDB_BC_LOAD_CONST:
        /* p1 = dest reg; p4_str = constant string value (NULL-type if absent). */
        if (!valid_reg(vm, p1)) return -3;
        if (p4_str) {
            copy_val_to_reg(vm->regs[(size_t)p1], SVDB_TYPE_TEXT,
                            0, 0.0, p4_str, strlen(p4_str));
        } else {
            vm->regs[(size_t)p1].val_type = SVDB_TYPE_NULL;
        }
        break;

    /* ── Register move/copy ───────────────────────────────── */
    case SVDB_BC_COPY:
    case SVDB_BC_STORE:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        vm->regs[(size_t)p2] = vm->regs[(size_t)p1];
        break;

    case SVDB_BC_MOVE:
        /* p1 = src reg, p2 = dest reg (src becomes NULL) */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        vm->regs[(size_t)p2] = vm->regs[(size_t)p1];
        vm->regs[(size_t)p1] = Register{};
        break;

    case SVDB_BC_SWAP:
        /* p1 = reg_a, p2 = reg_b */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        std::swap(vm->regs[(size_t)p1], vm->regs[(size_t)p2]);
        break;

    /* ── Arithmetic ───────────────────────────────────────── */
    case SVDB_BC_ADD:
    case SVDB_BC_SUB:
    case SVDB_BC_MUL:
    case SVDB_BC_DIV:
    case SVDB_BC_MOD: {
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        Register& lhs  = vm->regs[(size_t)p1];
        Register& rhs  = vm->regs[(size_t)p2];
        Register& dest = vm->regs[(size_t)p3];
        if (lhs.val_type == SVDB_TYPE_NULL || rhs.val_type == SVDB_TYPE_NULL) {
            dest = Register{};
            break;
        }
        bool use_float = (lhs.val_type == SVDB_TYPE_REAL || rhs.val_type == SVDB_TYPE_REAL);
        if (use_float) {
            double lv = reg_to_float(lhs), rv = reg_to_float(rhs);
            double result = 0.0;
            if (opcode == SVDB_BC_ADD) result = lv + rv;
            else if (opcode == SVDB_BC_SUB) result = lv - rv;
            else if (opcode == SVDB_BC_MUL) result = lv * rv;
            else if (opcode == SVDB_BC_DIV) {
                if (rv == 0.0) { dest = Register{}; break; }
                result = lv / rv;
            } else { dest = Register{}; break; } /* MOD on float → NULL */
            copy_val_to_reg(dest, SVDB_TYPE_REAL, 0, result, nullptr, 0);
        } else {
            int64_t li = lhs.int_val, ri = rhs.int_val;
            int64_t result = 0;
            if (opcode == SVDB_BC_ADD) result = li + ri;
            else if (opcode == SVDB_BC_SUB) result = li - ri;
            else if (opcode == SVDB_BC_MUL) result = li * ri;
            else if (opcode == SVDB_BC_DIV) {
                if (ri == 0) { dest = Register{}; break; }
                result = li / ri;
            } else {
                if (ri == 0) { dest = Register{}; break; }
                result = li % ri;
            }
            copy_val_to_reg(dest, SVDB_TYPE_INT, result, 0.0, nullptr, 0);
        }
        break;
    }

    case SVDB_BC_NEG:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        {
            const Register& src = vm->regs[(size_t)p1];
            if (src.val_type == SVDB_TYPE_REAL)
                copy_val_to_reg(vm->regs[(size_t)p2], SVDB_TYPE_REAL, 0, -src.real_val, nullptr, 0);
            else if (src.val_type == SVDB_TYPE_INT)
                copy_val_to_reg(vm->regs[(size_t)p2], SVDB_TYPE_INT, -src.int_val, 0.0, nullptr, 0);
            else
                vm->regs[(size_t)p2] = Register{};
        }
        break;

    /* ── Comparison ───────────────────────────────────────── */
    case SVDB_BC_EQ:
    case SVDB_BC_NEQ:
    case SVDB_BC_LT:
    case SVDB_BC_LE:
    case SVDB_BC_GT:
    case SVDB_BC_GE: {
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        const Register& lhs = vm->regs[(size_t)p1];
        const Register& rhs = vm->regs[(size_t)p2];
        /* NULL comparisons → NULL result (SQL three-valued logic). */
        if (lhs.val_type == SVDB_TYPE_NULL || rhs.val_type == SVDB_TYPE_NULL) {
            vm->regs[(size_t)p3] = Register{};
            break;
        }
        int cmp = compare_regs(lhs, rhs);
        bool result = false;
        if (opcode == SVDB_BC_EQ)  result = (cmp == 0);
        else if (opcode == SVDB_BC_NEQ) result = (cmp != 0);
        else if (opcode == SVDB_BC_LT)  result = (cmp <  0);
        else if (opcode == SVDB_BC_LE)  result = (cmp <= 0);
        else if (opcode == SVDB_BC_GT)  result = (cmp >  0);
        else                             result = (cmp >= 0);
        copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT, result ? 1 : 0, 0.0, nullptr, 0);
        break;
    }

    /* ── Logical ──────────────────────────────────────────── */
    case SVDB_BC_AND: {
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        const Register& la = vm->regs[(size_t)p1];
        const Register& ra = vm->regs[(size_t)p2];
        /* FALSE AND anything → FALSE; NULL AND FALSE → FALSE; else NULL/TRUE */
        bool l_null = (la.val_type == SVDB_TYPE_NULL);
        bool r_null = (ra.val_type == SVDB_TYPE_NULL);
        bool l_true = !l_null && (la.val_type != SVDB_TYPE_INT || la.int_val != 0);
        bool r_true = !r_null && (ra.val_type != SVDB_TYPE_INT || ra.int_val != 0);
        if ((!l_true && !l_null) || (!r_true && !r_null)) {
            copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT, 0, 0.0, nullptr, 0);
        } else if (l_null || r_null) {
            vm->regs[(size_t)p3] = Register{};
        } else {
            copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT, 1, 0.0, nullptr, 0);
        }
        break;
    }

    case SVDB_BC_OR: {
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        const Register& lo = vm->regs[(size_t)p1];
        const Register& ro = vm->regs[(size_t)p2];
        bool l_null = (lo.val_type == SVDB_TYPE_NULL);
        bool r_null = (ro.val_type == SVDB_TYPE_NULL);
        bool l_true = !l_null && (lo.val_type != SVDB_TYPE_INT || lo.int_val != 0);
        bool r_true = !r_null && (ro.val_type != SVDB_TYPE_INT || ro.int_val != 0);
        if (l_true || r_true) {
            copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT, 1, 0.0, nullptr, 0);
        } else if (l_null || r_null) {
            vm->regs[(size_t)p3] = Register{};
        } else {
            copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT, 0, 0.0, nullptr, 0);
        }
        break;
    }

    case SVDB_BC_NOT:
        /* p1 = src reg, p2 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        {
            const Register& src = vm->regs[(size_t)p1];
            if (src.val_type == SVDB_TYPE_NULL) {
                vm->regs[(size_t)p2] = Register{};
            } else {
                int64_t v = (src.val_type == SVDB_TYPE_INT) ? src.int_val : (src.real_val != 0.0 ? 1 : 0);
                copy_val_to_reg(vm->regs[(size_t)p2], SVDB_TYPE_INT, v == 0 ? 1 : 0, 0.0, nullptr, 0);
            }
        }
        break;

    /* ── Jump ─────────────────────────────────────────────── */
    case SVDB_BC_JUMP:
        /* p1 = jump target PC (0-based) */
        if (out_jump_pc) *out_jump_pc = p1;
        break;

    case SVDB_BC_JUMP_IF_FALSE:
        /* p1 = condition reg, p2 = jump target PC */
        if (!valid_reg(vm, p1)) return -3;
        {
            const Register& cond = vm->regs[(size_t)p1];
            bool is_false = (cond.val_type == SVDB_TYPE_NULL) ||
                            (cond.val_type == SVDB_TYPE_INT  && cond.int_val  == 0) ||
                            (cond.val_type == SVDB_TYPE_REAL && cond.real_val == 0.0);
            if (is_false && out_jump_pc) *out_jump_pc = p2;
        }
        break;

    case SVDB_BC_JUMP_IF_TRUE:
        /* p1 = condition reg, p2 = jump target PC */
        if (!valid_reg(vm, p1)) return -3;
        {
            const Register& cond = vm->regs[(size_t)p1];
            bool is_true = (cond.val_type != SVDB_TYPE_NULL) &&
                           !((cond.val_type == SVDB_TYPE_INT  && cond.int_val  == 0) ||
                             (cond.val_type == SVDB_TYPE_REAL && cond.real_val == 0.0));
            if (is_true && out_jump_pc) *out_jump_pc = p2;
        }
        break;

    /* ── String operations ────────────────────────────────── */
    case SVDB_BC_CONCAT:
        /* p1 = lhs reg, p2 = rhs reg, p3 = dest reg */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        {
            const Register& lc = vm->regs[(size_t)p1];
            const Register& rc = vm->regs[(size_t)p2];
            if (lc.val_type == SVDB_TYPE_NULL || rc.val_type == SVDB_TYPE_NULL) {
                vm->regs[(size_t)p3] = Register{};
            } else {
                std::string ls = (lc.val_type == SVDB_TYPE_TEXT || lc.val_type == SVDB_TYPE_BLOB)
                    ? lc.str_val : (lc.val_type == SVDB_TYPE_INT ? std::to_string(lc.int_val) : std::to_string(lc.real_val));
                std::string rs = (rc.val_type == SVDB_TYPE_TEXT || rc.val_type == SVDB_TYPE_BLOB)
                    ? rc.str_val : (rc.val_type == SVDB_TYPE_INT ? std::to_string(rc.int_val) : std::to_string(rc.real_val));
                std::string result = ls + rs;
                copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_TEXT, 0, 0.0, result.c_str(), result.size());
            }
        }
        break;

    case SVDB_BC_LIKE:
        /* p1 = text reg, p2 = pattern reg, p3 = dest reg (0 or 1) */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2) || !valid_reg(vm, p3)) return -3;
        {
            const Register& txt = vm->regs[(size_t)p1];
            const Register& pat = vm->regs[(size_t)p2];
            if (txt.val_type == SVDB_TYPE_NULL || pat.val_type == SVDB_TYPE_NULL) {
                vm->regs[(size_t)p3] = Register{};
            } else {
                const std::string& t_str = txt.str_val;
                const std::string& p_str = pat.str_val;
                bool matched = like_match(p_str.c_str(), p_str.size(),
                                          t_str.c_str(), t_str.size());
                copy_val_to_reg(vm->regs[(size_t)p3], SVDB_TYPE_INT,
                                matched ? 1 : 0, 0.0, nullptr, 0);
            }
        }
        break;

    /* ── Type conversion ──────────────────────────────────── */
    case SVDB_BC_CAST:
        /* p1 = src reg, p2 = dest reg, p3 = target type (SVDB_TYPE_*) */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        {
            const Register& src = vm->regs[(size_t)p1];
            Register& dst = vm->regs[(size_t)p2];
            if (src.val_type == SVDB_TYPE_NULL) {
                dst = Register{};
            } else if (p3 == SVDB_TYPE_INT) {
                if (src.val_type == SVDB_TYPE_INT)        dst = src;
                else if (src.val_type == SVDB_TYPE_REAL)  copy_val_to_reg(dst, SVDB_TYPE_INT, (int64_t)src.real_val, 0.0, nullptr, 0);
                else if (src.val_type == SVDB_TYPE_TEXT || src.val_type == SVDB_TYPE_BLOB) {
                    try { copy_val_to_reg(dst, SVDB_TYPE_INT, (int64_t)std::stoll(src.str_val), 0.0, nullptr, 0); }
                    catch (...) { copy_val_to_reg(dst, SVDB_TYPE_INT, 0, 0.0, nullptr, 0); }
                } else dst = Register{};
            } else if (p3 == SVDB_TYPE_REAL) {
                if (src.val_type == SVDB_TYPE_REAL)       dst = src;
                else if (src.val_type == SVDB_TYPE_INT)   copy_val_to_reg(dst, SVDB_TYPE_REAL, 0, (double)src.int_val, nullptr, 0);
                else if (src.val_type == SVDB_TYPE_TEXT || src.val_type == SVDB_TYPE_BLOB) {
                    try { copy_val_to_reg(dst, SVDB_TYPE_REAL, 0, std::stod(src.str_val), nullptr, 0); }
                    catch (...) { copy_val_to_reg(dst, SVDB_TYPE_REAL, 0, 0.0, nullptr, 0); }
                } else dst = Register{};
            } else if (p3 == SVDB_TYPE_TEXT) {
                if (src.val_type == SVDB_TYPE_TEXT)       dst = src;
                else if (src.val_type == SVDB_TYPE_INT) {
                    std::string s = std::to_string(src.int_val);
                    copy_val_to_reg(dst, SVDB_TYPE_TEXT, 0, 0.0, s.c_str(), s.size());
                } else if (src.val_type == SVDB_TYPE_REAL) {
                    std::string s = std::to_string(src.real_val);
                    copy_val_to_reg(dst, SVDB_TYPE_TEXT, 0, 0.0, s.c_str(), s.size());
                } else if (src.val_type == SVDB_TYPE_BLOB) {
                    copy_val_to_reg(dst, SVDB_TYPE_TEXT, 0, 0.0, src.str_val.c_str(), src.str_val.size());
                } else dst = Register{};
            } else {
                dst = src; /* unknown target type: pass through */
            }
        }
        break;

    /* ── NULL checks ──────────────────────────────────────── */
    case SVDB_BC_IS_NULL:
        /* p1 = src reg, p2 = dest reg: dest = 1 if src is NULL else 0 */
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        copy_val_to_reg(vm->regs[(size_t)p2], SVDB_TYPE_INT,
                        vm->regs[(size_t)p1].val_type == SVDB_TYPE_NULL ? 1 : 0, 0.0, nullptr, 0);
        break;

    case SVDB_BC_NOT_NULL:
        if (!valid_reg(vm, p1) || !valid_reg(vm, p2)) return -3;
        copy_val_to_reg(vm->regs[(size_t)p2], SVDB_TYPE_INT,
                        vm->regs[(size_t)p1].val_type != SVDB_TYPE_NULL ? 1 : 0, 0.0, nullptr, 0);
        break;

    /* ── Result row ───────────────────────────────────────── */
    case SVDB_BC_RESULT_ROW:
        /* p1 = first column reg, p2 = column count */
        vm->has_result = true;
        return -2;  /* signal caller to collect result row */

    /* ── Load column ─────────────────────────────────────── */
    case SVDB_BC_LOAD_COL:
        /* p1 = dest reg, p2 = column index */
        if (!valid_reg(vm, p1)) return -3;
        vm->regs[(size_t)p1].val_type = SVDB_TYPE_NULL;
        break;

    /* ── Cursor operations ───────────────────────────────── */
    case SVDB_BC_OPEN_READ:
    case SVDB_BC_OPEN_WRITE:
        /* p1 = cursor ID, p2 = table ID */
        /* In full implementation, would open a table/index */
        break;

    case SVDB_BC_REWIND:
        /* p1 = cursor ID */
        /* Reset cursor to beginning */
        break;

    case SVDB_BC_NEXT:
        /* p1 = cursor ID */
        /* Advance cursor to next row */
        break;

    case SVDB_BC_EOF:
        /* p1 = cursor ID, p2 = target PC if not EOF */
        /* Check if cursor is at end */
        if (out_jump_pc && p2 > 0) {
            /* For now, always jump - cursor check would be in full impl */
            *out_jump_pc = p2;
        }
        break;

    case SVDB_BC_COLUMN:
        /* p1 = cursor ID, p2 = column index, p3 = dest reg */
        if (!valid_reg(vm, p3)) return -3;
        vm->regs[(size_t)p3].val_type = SVDB_TYPE_NULL;
        break;

    case SVDB_BC_ROWID:
        /* p1 = cursor ID, p2 = dest reg */
        if (!valid_reg(vm, p2)) return -3;
        vm->regs[(size_t)p2].val_type = SVDB_TYPE_INT;
        vm->regs[(size_t)p2].int_val = 0;
        break;

    case SVDB_BC_SEEK_ROWID:
        /* p1 = cursor ID, p2 = rowid reg */
        /* Search for rowid in table */
        break;

    /* ── Aggregate operations ────────────────────────────── */
    case SVDB_BC_AGG_STEP:
        /* p1 = aggregate function ID, p2 = reg containing value */
        /* Accumulate value into aggregate */
        break;

    case SVDB_BC_AGG_FINAL:
        /* p1 = aggregate function ID, p2 = dest reg */
        /* Finalize aggregate and store result */
        if (!valid_reg(vm, p2)) return -3;
        vm->regs[(size_t)p2].val_type = SVDB_TYPE_NULL;
        break;

    /* ── Coroutine operations ───────────────────────────── */
    case SVDB_BC_INIT_COROUTINE:
        /* p1 = program counter reg */
        break;

    case SVDB_BC_YIELD:
        /* p1 = program counter reg */
        break;

    /* ── Close ─────────────────────────────────────────── */
    case SVDB_BC_CLOSE:
        /* p1 = cursor ID */
        /* Close cursor */
        break;

    default:
        /* Unknown or unimplemented opcode — treat as NOP for extensibility. */
        break;
    }

    return 0;
}

int svdb_bytecode_vm_has_result(svdb_bytecode_vm_t* vm) {
    return (vm && vm->has_result) ? 1 : 0;
}

void svdb_bytecode_vm_reset(svdb_bytecode_vm_t* vm) {
    if (vm) vm->reset();
}

} /* extern "C" */
