#ifndef SVDB_CG_EXPR_COMPILER_H
#define SVDB_CG_EXPR_COMPILER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compute an opcode-frequency histogram over an expression bytecode buffer.
 * histogram[op]++ for each instruction.  histogram_size should be at least
 * the largest opcode value + 1 (32 is sufficient).
 */
void svdb_cg_expr_opcode_histogram(
    const int16_t* buf,
    size_t         count,
    int64_t*       histogram,
    size_t         histogram_size
);

/*
 * Remove dead EOpLoadConst instructions from expression bytecode.
 * Returns the number of instructions written to out_buf.
 */
size_t svdb_cg_expr_prune(
    const int16_t* in_buf,
    size_t         in_count,
    int16_t*       out_buf,
    size_t         out_cap
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_EXPR_COMPILER_H */
