#include "type_conv.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cerrno>

extern "C" {

void svdb_parse_int64_batch(
    const char** strs,
    int64_t*     results,
    int*         ok,
    size_t       count
) {
    for (size_t i = 0; i < count; ++i) {
        if (!strs[i]) { results[i] = 0; ok[i] = 0; continue; }
        char* end;
        errno = 0;
        long long v = strtoll(strs[i], &end, 10);
        ok[i] = (errno == 0 && end != strs[i] && *end == '\0') ? 1 : 0;
        results[i] = ok[i] ? (int64_t)v : 0;
    }
}

void svdb_parse_float64_batch(
    const char** strs,
    double*      results,
    int*         ok,
    size_t       count
) {
    for (size_t i = 0; i < count; ++i) {
        if (!strs[i]) { results[i] = 0.0; ok[i] = 0; continue; }
        char* end;
        errno = 0;
        double v = strtod(strs[i], &end);
        ok[i] = (errno == 0 && end != strs[i] && *end == '\0') ? 1 : 0;
        results[i] = ok[i] ? v : 0.0;
    }
}

size_t svdb_format_int64_batch(
    const int64_t* values,
    char*          buf,
    size_t*        offsets,
    size_t         count
) {
    size_t pos = 0;
    for (size_t i = 0; i < count; ++i) {
        offsets[i] = pos;
        int n = snprintf(buf + pos, 32, "%lld", (long long)values[i]);
        if (n < 0) n = 0;
        pos += (size_t)n + 1; /* include null terminator */
    }
    return pos;
}

size_t svdb_format_float64_batch(
    const double* values,
    char*         buf,
    size_t*       offsets,
    size_t        count
) {
    size_t pos = 0;
    for (size_t i = 0; i < count; ++i) {
        offsets[i] = pos;
        int n = snprintf(buf + pos, 64, "%g", values[i]);
        if (n < 0) n = 0;
        pos += (size_t)n + 1;
    }
    return pos;
}

} // extern "C"
