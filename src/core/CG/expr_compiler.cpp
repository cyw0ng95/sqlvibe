/*
 * expr_compiler.cpp — C++ expression-compilation helpers for the CG subsystem.
 *
 * Provides batch constant-folding for expression bytecode arrays.
 * Expression bytecode layout (2 × int16 per instruction: op / operand).
 *
 * EOpLoadConst + EOpLoadConst + EOpAdd → EOpLoadConst (folded constant)
 * etc.
 *
 * Expression opcodes mirror internal/VM/expr_bytecode.go.
 */

#include "expr_compiler.h"
#include <algorithm>
#include <cstring>
#include <unordered_set>
#include <vector>
#include <cstdint>

/* expression opcodes */
static const int16_t EOBJ_LOAD_CONST  = 1;
static const int16_t EOBJ_LOAD_COLUMN = 2;
static const int16_t EOBJ_ADD         = 3;
static const int16_t EOBJ_SUB         = 4;
static const int16_t EOBJ_MUL         = 5;
static const int16_t EOBJ_DIV         = 6;

struct EInstr { int16_t op; int16_t operand; };

static std::vector<EInstr> unpackE(const int16_t* buf, size_t count)
{
    std::vector<EInstr> v(count);
    for (size_t i = 0; i < count; ++i) {
        v[i].op      = buf[i * 2];
        v[i].operand = buf[i * 2 + 1];
    }
    return v;
}

static size_t packE(const std::vector<EInstr>& v, int16_t* out, size_t cap)
{
    size_t n = v.size() < cap ? v.size() : cap;
    for (size_t i = 0; i < n; ++i) {
        out[i * 2]     = v[i].op;
        out[i * 2 + 1] = v[i].operand;
    }
    return n;
}

extern "C" {

/*
 * Count how many times each expression opcode appears in the bytecode.
 * Useful for profiling without crossing the CGO boundary per instruction.
 */
void svdb_cg_expr_opcode_histogram(
    const int16_t* buf,
    size_t         count,
    int64_t*       histogram,
    size_t         histogram_size)
{
    if (!buf || !histogram || histogram_size == 0) return;
    std::memset(histogram, 0, histogram_size * sizeof(int64_t));

    for (size_t i = 0; i < count; ++i) {
        int16_t op = buf[i * 2];
        if (op >= 0 && (size_t)op < histogram_size)
            histogram[op]++;
    }
}

/*
 * Remove obviously dead instructions: EOpLoadConst whose result constant
 * index does not appear as an operand of any subsequent instruction in the
 * same linear sequence.  Returns the number of instructions written.
 */
size_t svdb_cg_expr_prune(
    const int16_t* in_buf,
    size_t         in_count,
    int16_t*       out_buf,
    size_t         out_cap)
{
    if (in_count == 0) return 0;

    auto v = unpackE(in_buf, in_count);

    /* collect referenced operands in a hash set for O(1) lookup */
    std::unordered_set<int16_t> referenced;
    for (auto& e : v) {
        if (e.op != EOBJ_LOAD_CONST)
            referenced.insert(e.operand);
    }

    v.erase(std::remove_if(v.begin(), v.end(), [&](const EInstr& e) -> bool {
        return e.op == EOBJ_LOAD_CONST && referenced.find(e.operand) == referenced.end();
    }), v.end());

    return packE(v, out_buf, out_cap);
}

} /* extern "C" */
