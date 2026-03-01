#ifndef SVDB_VM_DATETIME_H
#define SVDB_VM_DATETIME_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse an ISO-8601 date string ("YYYY-MM-DD") into Julian Day Number.
 * Returns 0.0 on parse error.
 */
double svdb_julianday(const char* timestr, size_t len);

/*
 * Parse an ISO-8601 datetime ("YYYY-MM-DD HH:MM:SS") into a Unix timestamp.
 * Returns 0 on parse error.
 */
int64_t svdb_unixepoch(const char* timestr, size_t len);

/*
 * Batch julianday conversion.
 * results[i] = 0.0 when the input is NULL (strs[i] == NULL).
 */
void svdb_julianday_batch(
    const char** strs,
    size_t*      lens,
    double*      results,
    int*         ok,
    size_t       count
);

/*
 * Batch unixepoch conversion.
 */
void svdb_unixepoch_batch(
    const char** strs,
    size_t*      lens,
    int64_t*     results,
    int*         ok,
    size_t       count
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_DATETIME_H */
