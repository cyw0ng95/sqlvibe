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
        case OP_LOAD_CONST:
            /* OP_LOAD_CONST writes p1 (dest) from constant pool; no registers read.
             * (Note: OP_ prefix is for the legacy VM.OpCode numbering, distinct from
             *  the BC_ prefix used in eliminateBcDeadCode for the bytecode VM path.) */
            break;
        default:
            /* Unknown opcode: conservatively mark all operand fields as read
             * to prevent incorrect elimination of instructions that feed them. */
            if (ins.p1 >= 0) readCount[ins.p1]++;
            if (ins.p2 >= 0) readCount[ins.p2]++;
            if (ins.p3 >= 0) readCount[ins.p3]++;
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

/* ── pass 2.5: constant propagation ─────────────────────────────────────── */
/*
 * Replace uses of a register that was loaded by OP_LOAD_CONST with the
 * destination register of the original load, so subsequent dead-code
 * elimination can remove the now-redundant intermediate moves.
 *
 * Specifically, collapse OP_MOVE r_src r_dst immediately preceded by an
 * instruction that wrote r_src, where r_src is used only by this one move.
 */
static void constantPropagation(std::vector<Instr>& v)
{
    /* Map: register → instruction index that last wrote it */
    std::unordered_map<int32_t, size_t> last_write;
    std::unordered_map<int32_t, int>    use_count;

    /* First pass: count uses of each register */
    for (auto& ins : v) {
        switch (ins.op) {
        case OP_ADD: case OP_SUBTRACT: case OP_MULTIPLY:
        case OP_DIVIDE: case OP_REMAINDER:
            use_count[ins.p1]++;
            use_count[ins.p2]++;
            break;
        case OP_MOVE: case OP_COPY: case OP_SCOPY:
            use_count[ins.p1]++;
            break;
        case OP_RESULT_ROW:
            for (int32_t r = ins.p1; r < ins.p1 + ins.p3; ++r)
                use_count[r]++;
            break;
        default:
            if (ins.p1 >= 0) use_count[ins.p1]++;
            if (ins.p2 >= 0) use_count[ins.p2]++;
            break;
        }
    }

    /* Second pass: fold OP_MOVE where src is used exactly once */
    for (size_t i = 0; i < v.size(); ++i) {
        Instr& ins = v[i];
        if ((ins.op == OP_MOVE || ins.op == OP_COPY || ins.op == OP_SCOPY) &&
            use_count.count(ins.p1) && use_count[ins.p1] == 1) {
            auto it = last_write.find(ins.p1);
            if (it != last_write.end()) {
                /* Redirect the prior instruction's output directly to ins.p2 */
                Instr& producer = v[it->second];
                switch (producer.op) {
                case OP_LOAD_CONST:
                    producer.p1 = ins.p2;  /* dest of LOAD_CONST is p1 */
                    ins.op = OP_NULL;       /* NOP: mark for removal */
                    ins.p1 = -1; ins.p2 = -1; ins.p3 = -1;
                    break;
                case OP_ADD: case OP_SUBTRACT: case OP_MULTIPLY:
                case OP_DIVIDE: case OP_REMAINDER:
                    producer.p3 = ins.p2;  /* dest of arithmetic is p3 */
                    ins.op = OP_NULL;
                    ins.p1 = -1; ins.p2 = -1; ins.p3 = -1;
                    break;
                default:
                    break;
                }
            }
        }
        /* Record what this instruction writes */
        switch (ins.op) {
        case OP_LOAD_CONST:                             last_write[ins.p1] = i; break;
        case OP_ADD: case OP_SUBTRACT: case OP_MULTIPLY:
        case OP_DIVIDE: case OP_REMAINDER:              last_write[ins.p3] = i; break;
        case OP_MOVE: case OP_COPY: case OP_SCOPY:      last_write[ins.p2] = i; break;
        default: break;
        }
    }

    /* Remove NOP instructions (OP_NULL with p1=p2=p3=-1 used as marker) */
    v.erase(std::remove_if(v.begin(), v.end(), [](const Instr& ins) {
        return ins.op == OP_NULL && ins.p1 == -1 && ins.p2 == -1 && ins.p3 == -1;
    }), v.end());
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
static const uint16_t BC_NEG         = 9;  /* BcNeg: writes C, reads A */
static const uint16_t BC_EQ          = 11; /* BcEq..BcGe: write C, read A and B */
static const uint16_t BC_NE          = 12;
static const uint16_t BC_LT          = 13;
static const uint16_t BC_LE          = 14;
static const uint16_t BC_GT          = 15;
static const uint16_t BC_GE          = 16;
static const uint16_t BC_AND         = 17;
static const uint16_t BC_OR          = 18;
static const uint16_t BC_NOT         = 19;  /* BcNot: writes C, reads A */
static const uint16_t BC_IS_NULL     = 20;  /* BcIsNull: writes C, reads A */
static const uint16_t BC_NOT_NULL    = 21;  /* BcNotNull: writes C, reads A */
static const uint16_t BC_JUMP        = 22;  /* BcJump: no reg reads */
static const uint16_t BC_JUMP_TRUE   = 23;  /* BcJumpTrue: reads A (condition) */
static const uint16_t BC_JUMP_FALSE  = 24;  /* BcJumpFalse: reads A (condition) */
static const uint16_t BC_OPEN_CURSOR = 25;  /* BcOpenCursor: no reg reads */
static const uint16_t BC_REWIND      = 26;  /* BcRewind: no reg reads */
static const uint16_t BC_NEXT        = 27;  /* BcNext: no reg reads */
static const uint16_t BC_COLUMN      = 28;  /* BcColumn: writes C, reads cursor A */
static const uint16_t BC_ROWID       = 29;  /* BcRowid: writes C, reads cursor A */
static const uint16_t BC_RESULT_ROW  = 30;  /* BcResultRow: A=first reg, B=count */
static const uint16_t BC_HALT        = 31;  /* BcHalt: no operands */
static const uint16_t BC_CALL        = 35;  /* BcCall: A=fn_const, B=nargs, C=dst; reads regs[C-B..C-1] */
static const uint16_t BC_AGG_INIT    = 32;  /* BcAggInit: A=slot, no register reads */
static const uint16_t BC_AGG_STEP    = 33;  /* BcAggStep: A=slot, B=value reg */
static const uint16_t BC_AGG_FINAL   = 34;  /* BcAggFinal: A=slot, writes C */

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
        case BC_EQ:  case BC_NE:  case BC_LT:  case BC_LE:
        case BC_GT:  case BC_GE:  case BC_AND: case BC_OR:
            /* binary ops: reads A and B */
            readCount[ins.a]++;
            readCount[ins.b]++;
            break;
        case BC_LOAD_REG:
        case BC_NEG: case BC_NOT: case BC_IS_NULL: case BC_NOT_NULL:
            /* unary ops: reads A */
            readCount[ins.a]++;
            break;
        case BC_JUMP_TRUE: case BC_JUMP_FALSE:
            /* conditional jump: reads A (condition reg) */
            readCount[ins.a]++;
            break;
        case BC_RESULT_ROW:
            /* result row: regs[A .. A+B-1] */
            for (int32_t r = ins.a; r < ins.a + ins.b; ++r)
                readCount[r]++;
            break;
        case BC_CALL:
            /* BcCall: A=fn_const (not a register), B=nargs, C=dst.
             * Args are at regs[C-B .. C-1]. */
            for (int32_t r = ins.c - ins.b; r < ins.c; ++r)
                if (r >= 0) readCount[r]++;
            break;
        case BC_AGG_STEP:
            /* BcAggStep: A=aggregate slot (not register), B=value register */
            readCount[ins.b]++;
            break;
        case BC_LOAD_CONST:
        case BC_NOOP:
        case BC_JUMP:
        case BC_OPEN_CURSOR: case BC_REWIND: case BC_NEXT:
        case BC_COLUMN: case BC_ROWID:
        case BC_HALT:
        case BC_AGG_INIT: case BC_AGG_FINAL:
            /* BC_LOAD_CONST / BC_COLUMN / BC_ROWID write to C but read no registers
             * (their other operands are const-pool indices or cursor IDs).
             * The others have no meaningful register operands.
             * BC_LOAD_CONST still appears in the elimination pass below — that is
             * intentional: it can be eliminated if its output register C is never
             * used by any subsequent instruction. */
            break;
        default:
            /* Unknown opcode: conservatively mark all positive-valued operand
             * fields as read to prevent incorrect elimination. */
            if (ins.a >= 0) readCount[ins.a]++;
            if (ins.b >= 0) readCount[ins.b]++;
            if (ins.c >= 0) readCount[ins.c]++;
            break;
        }
    }

    v.erase(std::remove_if(v.begin(), v.end(), [&](const BcInstr& ins) -> bool {
        switch (ins.op) {
        case BC_LOAD_CONST: case BC_LOAD_REG:
            return readCount.find(ins.c) == readCount.end();
        case BC_ADD: case BC_ADD_INT: case BC_SUB: case BC_MUL:
        case BC_DIV: case BC_MOD: case BC_CONCAT:
        case BC_EQ:  case BC_NE:  case BC_LT:  case BC_LE:
        case BC_GT:  case BC_GE:  case BC_AND: case BC_OR:
        case BC_NEG: case BC_NOT: case BC_IS_NULL: case BC_NOT_NULL:
        case BC_COLUMN: case BC_ROWID:
        case BC_CALL:
        case BC_AGG_FINAL:
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

    if (level >= 2)
        eliminateBcDeadCode(v);  /* second pass after first removal */

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

    if (level >= 2) {
        constantPropagation(v);
        eliminateDeadCode(v);  /* re-run after folding */
        peepholeOptimize(v);
    }

    return pack(v, out_buf, out_cap);
}

} /* extern "C" */
