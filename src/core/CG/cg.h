#ifndef SVDB_CG_H
#define SVDB_CG_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handles */
typedef struct svdb_cg_compiler svdb_cg_compiler_t;
typedef struct svdb_cg_program  svdb_cg_program_t;

/* Compiler lifecycle */
svdb_cg_compiler_t* svdb_cg_create(void);
void                svdb_cg_destroy(svdb_cg_compiler_t* compiler);

/*
 * Compile / optimise a pre-compiled bytecode program represented as JSON.
 *
 * The JSON payload uses the schema produced by the Go CG package:
 *   {
 *     "instructions": [ {"op":N,"p1":N,"p2":N,"p3":N,"p4_int":N,
 *                        "p4_str":"...","p4_type":N}, ... ],
 *     "column_names": ["col1", ...],
 *     "result_reg": N
 *   }
 *
 * The C++ optimizer applies constant-folding, dead-code elimination and
 * peephole passes then returns a new svdb_cg_program_t.  On failure NULL
 * is returned and a NUL-terminated error message is written to error_buf.
 */
svdb_cg_program_t* svdb_cg_compile_select(
    svdb_cg_compiler_t* compiler,
    const char*         bytecode_json,
    size_t              json_len,
    char*               error_buf,
    size_t              error_buf_size
);

svdb_cg_program_t* svdb_cg_compile_insert(
    svdb_cg_compiler_t* compiler,
    const char*         bytecode_json,
    size_t              json_len,
    char*               error_buf,
    size_t              error_buf_size
);

svdb_cg_program_t* svdb_cg_compile_update(
    svdb_cg_compiler_t* compiler,
    const char*         bytecode_json,
    size_t              json_len,
    char*               error_buf,
    size_t              error_buf_size
);

svdb_cg_program_t* svdb_cg_compile_delete(
    svdb_cg_compiler_t* compiler,
    const char*         bytecode_json,
    size_t              json_len,
    char*               error_buf,
    size_t              error_buf_size
);

/*
 * Low-level: optimise a raw bytecode buffer (flat int32 array, 4 int32s per
 * instruction: op/p1/p2/p3).  Used when the caller already has binary
 * bytecode and does not want JSON round-tripping.
 *
 * out_buf must be at least in_count*4 int32s.  Returns the number of
 * instructions written (may be <= in_count after dead-code elimination).
 */
size_t svdb_cg_optimize_raw(
    svdb_cg_compiler_t* compiler,
    const int32_t*      in_buf,
    size_t              in_count,
    int32_t*            out_buf,
    size_t              out_capacity
);

/*
 * Optimise a bytecode-VM instruction array in-place.
 * Each instruction is 16 bytes: {uint16 op, uint16 fl, int32 a, int32 b, int32 c}.
 * Returns the number of instructions retained (may be <= in_count).
 * out_buf must point to a buffer of at least in_count*16 bytes.
 */
size_t svdb_cg_optimize_bc_instrs(
    svdb_cg_compiler_t* compiler,
    const uint8_t*      in_buf,
    size_t              in_count,
    uint8_t*            out_buf,
    size_t              out_capacity
);

/* Program accessors */
const uint8_t* svdb_cg_get_bytecode(svdb_cg_program_t* program, size_t* out_len);
/* Returns a NUL-separated list of column names; out_count receives the count */
const char*    svdb_cg_get_column_names(svdb_cg_program_t* program, size_t* out_count);
int32_t        svdb_cg_get_result_reg(svdb_cg_program_t* program);
/* JSON representation of optimised instructions */
const char*    svdb_cg_get_json(svdb_cg_program_t* program);

/* Program cleanup */
void svdb_cg_program_free(svdb_cg_program_t* program);

/* Optimisation level: 0=none, 1=basic (default), 2=aggressive */
void svdb_cg_set_optimization_level(svdb_cg_compiler_t* compiler, int level);

/* Plan cache */
void               svdb_cg_cache_put(svdb_cg_compiler_t* compiler, const char* sql, svdb_cg_program_t* program);
svdb_cg_program_t* svdb_cg_cache_get(svdb_cg_compiler_t* compiler, const char* sql);
void               svdb_cg_cache_clear(svdb_cg_compiler_t* compiler);
size_t             svdb_cg_cache_size(svdb_cg_compiler_t* compiler);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_CG_H */
