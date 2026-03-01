#ifndef SVDB_VM_DISPATCH_H
#define SVDB_VM_DISPATCH_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Returns the SIMD capability level:
 *   0 = plain C++
 *   1 = SSE4.1
 *   2 = AVX
 *   3 = AVX2
 */
int svdb_dispatch_simd_level(void);

/* Returns 1 if direct-threaded (computed-goto) dispatch is compiled in */
int svdb_dispatch_is_direct_threaded(void);

/*
 * Batch arithmetic opcodes — tight loop over homogeneous int64 pairs.
 * op: 0=add, 1=sub, 2=mul, 3=div, 4=rem
 * Returns 0 on success, non-zero on divide-by-zero.
 */
int svdb_dispatch_arith_int64(
    int            op,
    const int64_t* a,
    const int64_t* b,
    int64_t*       results,
    size_t         count
);

/*
 * Batch arithmetic opcodes for float64.
 */
void svdb_dispatch_arith_float64(
    int           op,
    const double* a,
    const double* b,
    double*       results,
    size_t        count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_DISPATCH_H */
