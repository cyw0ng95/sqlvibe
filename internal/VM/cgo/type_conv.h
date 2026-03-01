#ifndef SVDB_VM_TYPE_CONV_H
#define SVDB_VM_TYPE_CONV_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Batch parse int64 from decimal strings.
 * ok[i] is set to 1 on success, 0 on failure.
 */
void svdb_parse_int64_batch(
    const char** strs,
    int64_t*     results,
    int*         ok,
    size_t       count
);

/*
 * Batch parse float64 from strings.
 * ok[i] is set to 1 on success, 0 on failure.
 */
void svdb_parse_float64_batch(
    const char** strs,
    double*      results,
    int*         ok,
    size_t       count
);

/*
 * Format int64 values to strings.
 * Writes into buf; offsets[i] is the start of each formatted string.
 * Returns total bytes written (including null terminators).
 */
size_t svdb_format_int64_batch(
    const int64_t* values,
    char*          buf,
    size_t*        offsets,
    size_t         count
);

/*
 * Format float64 values to strings.
 * Returns total bytes written.
 */
size_t svdb_format_float64_batch(
    const double* values,
    char*         buf,
    size_t*       offsets,
    size_t        count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_TYPE_CONV_H */
