#ifndef SVDB_CG_OPTIMIZER_H
#define SVDB_CG_OPTIMIZER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Internal optimisation helper called from compiler.cpp.
 * level: 0=none, 1=dead-code, 2=dead-code+peephole.
 * Returns number of instructions written to out_buf.
 */
size_t svdb_cg_optimize_bc_instrs_impl(
    int            level,
    const uint8_t* in_buf,
    size_t         in_count,
    uint8_t*       out_buf,
    size_t         out_cap
);

size_t svdb_cg_optimize_raw_impl(
    int            level,
    const int32_t* in_buf,
    size_t         in_count,
    int32_t*       out_buf,
    size_t         out_cap
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_OPTIMIZER_H */
