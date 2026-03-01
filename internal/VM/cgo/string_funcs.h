#ifndef SVDB_VM_STRING_FUNCS_H
#define SVDB_VM_STRING_FUNCS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * In-place ASCII uppercase.
 * Non-ASCII bytes are left unchanged.
 */
void svdb_str_upper(char* s, size_t len);

/*
 * In-place ASCII lowercase.
 */
void svdb_str_lower(char* s, size_t len);

/*
 * Trim leading and/or trailing spaces.
 * Returns new start offset (into s) and sets *out_len.
 */
size_t svdb_str_trim(const char* s, size_t len, int left, int right, size_t* out_len);

/*
 * Batch uppercase: copies strs[i] into out_buf at offsets[i].
 * Returns total bytes written.
 */
size_t svdb_str_upper_batch(
    const char** strs,
    size_t*      lens,
    char*        out_buf,
    size_t*      out_offsets,
    size_t       count
);

/*
 * Batch lowercase.
 */
size_t svdb_str_lower_batch(
    const char** strs,
    size_t*      lens,
    char*        out_buf,
    size_t*      out_offsets,
    size_t       count
);

/*
 * Substring: 1-based start, length=-1 means to end.
 * Writes to out; returns byte count written (no null terminator).
 */
size_t svdb_str_substr(
    const char* s,
    size_t      s_len,
    int64_t     start,   /* 1-based */
    int64_t     length,  /* -1 = to end */
    char*       out,
    size_t      out_cap
);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_STRING_FUNCS_H */
