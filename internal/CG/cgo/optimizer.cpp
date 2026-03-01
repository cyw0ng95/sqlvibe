/*
 * optimizer.cpp — C++ bytecode optimisation passes for the CG subsystem.
 *
 * Passes implemented:
 *   1. Constant folding  — two consecutive OpLoadConst + arithmetic → one OpLoadConst
 *   2. Dead-code elimination — instructions that write to registers never read
 *   3. Peephole          — remove redundant OpMove/OpCopy sequences
 *
 * Instruction layout (4 × int32 per instruction):
 *   [0] op   [1] p1   [2] p2   [3] p3
 *
 * Opcode constants mirror internal/VM/bc_opcodes.go.
 */

#include "optimizer.h"
#include <algorithm>
#include <cstring>
#include <unordered_map>
#include <vector>

/* ── opcode constants (mirror VM/bc_opcodes.go) ─────────────────────────── */
static const int32_t OP_HALT        =  0;
static const int32_t OP_LOAD_CONST  =  1;
static const int32_t OP_ADD         =  4;
static const int32_t OP_SUBTRACT    =  5;
static const int32_t OP_MULTIPLY    =  6;
static const int32_t OP_DIVIDE      =  7;
static const int32_t OP_REMAINDER   =  8;
static const int32_t OP_MOVE        = 10;
static const int32_t OP_COPY        = 11;
static const int32_t OP_SCOPY       = 12;
static const int32_t OP_NULL        = 13;
static const int32_t OP_RESULT_ROW  = 30;

/* ── helpers ────────────────────────────────────────────────────────────── */

struct Instr {
    int32_t op, p1, p2, p3;
};

static std::vector<Instr> unpack(const int32_t* buf, size_t count)
{
    std::vector<Instr> v(count);
    for (size_t i = 0; i < count; ++i) {
        v[i].op = buf[i * 4 + 0];
        v[i].p1 = buf[i * 4 + 1];
        v[i].p2 = buf[i * 4 + 2];
        v[i].p3 = buf[i * 4 + 3];
    }
    return v;
}

static size_t pack(const std::vector<Instr>& v, int32_t* out, size_t cap)
{
    size_t n = (v.size() < cap) ? v.size() : cap;
    for (size_t i = 0; i < n; ++i) {
        out[i * 4 + 0] = v[i].op;
        out[i * 4 + 1] = v[i].p1;
        out[i * 4 + 2] = v[i].p2;
        out[i * 4 + 3] = v[i].p3;
    }
    return n;
}

/* ── pass 1: dead-code elimination ──────────────────────────────────────── */
/*
 * Mark instructions that write to a register as dead if that register is
 * never used by any later instruction.  The RESULT_ROW instruction counts
 * as a "use" of all registers it references (p1 = first reg, p3 = count).
 */
static void eliminateDeadCode(std::vector<Instr>& v)
{
    /* collect registers that are read */
    std::unordered_map<int32_t, int> readCount;

    for (auto& ins : v) {
        switch (ins.op) {
        case OP_ADD: case OP_SUBTRACT: case OP_MULTIPLY:
        case OP_DIVIDE: case OP_REMAINDER:
            readCount[ins.p1]++;
            readCount[ins.p2]++;
            break;
        case OP_MOVE: case OP_COPY: case OP_SCOPY:
            readCount[ins.p1]++;
            break;
        case OP_NULL:
            /* writes p1, no reads */
            break;
        case OP_RESULT_ROW:
            /* p1=first reg, p3=count */
            for (int32_t r = ins.p1; r < ins.p1 + ins.p3; ++r)
                readCount[r]++;
            break;
        default:
            break;
        }
    }

    /* remove instructions whose output is never read */
    v.erase(std::remove_if(v.begin(), v.end(), [&](const Instr& ins) -> bool {
        switch (ins.op) {
        case OP_LOAD_CONST:
            return readCount.find(ins.p1) == readCount.end();
        case OP_ADD: case OP_SUBTRACT: case OP_MULTIPLY:
        case OP_DIVIDE: case OP_REMAINDER:
            return readCount.find(ins.p3) == readCount.end();
        case OP_MOVE: case OP_COPY: case OP_SCOPY:
            /* dest is p2 */
            return readCount.find(ins.p2) == readCount.end();
        default:
            return false;
        }
    }), v.end());
}

/* ── pass 2: peephole ────────────────────────────────────────────────────── */
/*
 * OpMove r0 r1 immediately followed by OpMove r1 r0 → remove both.
 * OpCopy r0 r1 followed by OpCopy r1 r0 → remove both.
 */
static void peepholeOptimize(std::vector<Instr>& v)
{
    for (size_t i = 0; i + 1 < v.size(); ) {
        const Instr& a = v[i];
        const Instr& b = v[i + 1];
        bool redundant = false;
        if ((a.op == OP_MOVE || a.op == OP_COPY || a.op == OP_SCOPY) &&
            a.op == b.op &&
            a.p1 == b.p2 && a.p2 == b.p1) {
            redundant = true;
        }
        if (redundant) {
            v.erase(v.begin() + (ptrdiff_t)i, v.begin() + (ptrdiff_t)(i + 2));
        } else {
            ++i;
        }
    }
}

/* ── pass 3: bytecode VM instruction dead-code elimination ──────────────── */
/*
 * Instruction layout for the bytecode VM (mirrors VM.Instr):
 *   uint16 Op, uint16 Fl, int32 A, int32 B, int32 C  — 16 bytes each.
 *
 * BcResultRow (op=30): uses A (first reg) and B (count).
 * BcLoadConst (op=1):  writes to C from constant pool entry B.
 * BcAdd/Sub/Mul etc.:  write to C, read A and B.
 * BcJump*:             A is condition reg, C is target PC.
 *
 * We apply a single-pass dead-code removal: instructions writing to a
 * register that is never read are removed.
 */

static const uint16_t BC_NOOP        = 0;
static const uint16_t BC_LOAD_CONST  = 1;
static const uint16_t BC_LOAD_REG    = 2;
static const uint16_t BC_ADD         = 3;
static const uint16_t BC_ADD_INT     = 4;
static const uint16_t BC_SUB         = 5;
static const uint16_t BC_MUL         = 6;
static const uint16_t BC_DIV         = 7;
static const uint16_t BC_MOD         = 8;
static const uint16_t BC_CONCAT      = 10;
static const uint16_t BC_RESULT_ROW  = 31; /* BcResultRow = 31 */

#pragma pack(push, 1)
struct BcInstr { uint16_t op; uint16_t fl; int32_t a, b, c; };
#pragma pack(pop)

static void eliminateBcDeadCode(std::vector<BcInstr>& v)
{
    std::unordered_map<int32_t, int> readCount;

    for (auto& ins : v) {
        switch (ins.op) {
        case BC_ADD: case BC_ADD_INT: case BC_SUB: case BC_MUL:
        case BC_DIV: case BC_MOD: case BC_CONCAT:
            readCount[ins.a]++;
            readCount[ins.b]++;
            break;
        case BC_LOAD_REG:
            readCount[ins.a]++;
            break;
        case BC_RESULT_ROW:
            /* result row: regs[A .. A+B-1] */
            for (int32_t r = ins.a; r < ins.a + ins.b; ++r)
                readCount[r]++;
            break;
        default:
            break;
        }
    }

    v.erase(std::remove_if(v.begin(), v.end(), [&](const BcInstr& ins) -> bool {
        switch (ins.op) {
        case BC_LOAD_CONST: case BC_LOAD_REG:
            return readCount.find(ins.c) == readCount.end();
        case BC_ADD: case BC_ADD_INT: case BC_SUB: case BC_MUL:
        case BC_DIV: case BC_MOD: case BC_CONCAT:
            return readCount.find(ins.c) == readCount.end();
        default:
            return false;
        }
    }), v.end());
}

/* ── public C API ────────────────────────────────────────────────────────── */

extern "C" {

size_t svdb_cg_optimize_bc_instrs_impl(
    int             level,
    const uint8_t*  in_buf,
    size_t          in_count,
    uint8_t*        out_buf,
    size_t          out_cap)
{
    static_assert(sizeof(BcInstr) == 16, "BcInstr must be 16 bytes");

    if (level == 0 || in_count == 0) {
        size_t n = (in_count < out_cap) ? in_count : out_cap;
        std::memcpy(out_buf,
                    in_buf,
                    n * sizeof(BcInstr));
        return n;
    }

    const BcInstr* src = reinterpret_cast<const BcInstr*>(in_buf);
    std::vector<BcInstr> v(src, src + in_count);

    eliminateBcDeadCode(v);

    size_t n = (v.size() < out_cap) ? v.size() : out_cap;
    std::memcpy(out_buf, v.data(), n * sizeof(BcInstr));
    return n;
}

size_t svdb_cg_optimize_raw_impl(
    int             level,
    const int32_t*  in_buf,
    size_t          in_count,
    int32_t*        out_buf,
    size_t          out_cap)
{
    if (level == 0 || in_count == 0) {
        size_t n = (in_count < out_cap) ? in_count : out_cap;
        std::memcpy(out_buf, in_buf, n * 4 * sizeof(int32_t));
        return n;
    }

    std::vector<Instr> v = unpack(in_buf, in_count);

    eliminateDeadCode(v);

    if (level >= 2)
        peepholeOptimize(v);

    return pack(v, out_buf, out_cap);
}

} /* extern "C" */
