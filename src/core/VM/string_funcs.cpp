#include "string_funcs.h"
#include <cstring>
#include <cctype>
#include <algorithm>

extern "C" {

void svdb_str_upper(char* s, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (c >= 'a' && c <= 'z') s[i] = (char)(c - 32);
    }
}

void svdb_str_lower(char* s, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (c >= 'A' && c <= 'Z') s[i] = (char)(c + 32);
    }
}

size_t svdb_str_trim(const char* s, size_t len, int left, int right, size_t* out_len) {
    size_t start = 0;
    size_t end = len;
    if (left) {
        while (start < end && (unsigned char)s[start] == ' ') ++start;
    }
    if (right) {
        while (end > start && (unsigned char)s[end - 1] == ' ') --end;
    }
    *out_len = end - start;
    return start;
}

size_t svdb_str_upper_batch(
    const char** strs,
    size_t*      lens,
    char*        out_buf,
    size_t*      out_offsets,
    size_t       count
) {
    size_t pos = 0;
    for (size_t i = 0; i < count; ++i) {
        out_offsets[i] = pos;
        size_t n = lens[i];
        if (strs[i] && n > 0) {
            memcpy(out_buf + pos, strs[i], n);
            svdb_str_upper(out_buf + pos, n);
        }
        pos += n + 1;
        out_buf[pos - 1] = '\0';
    }
    return pos;
}

size_t svdb_str_lower_batch(
    const char** strs,
    size_t*      lens,
    char*        out_buf,
    size_t*      out_offsets,
    size_t       count
) {
    size_t pos = 0;
    for (size_t i = 0; i < count; ++i) {
        out_offsets[i] = pos;
        size_t n = lens[i];
        if (strs[i] && n > 0) {
            memcpy(out_buf + pos, strs[i], n);
            svdb_str_lower(out_buf + pos, n);
        }
        pos += n + 1;
        out_buf[pos - 1] = '\0';
    }
    return pos;
}

size_t svdb_str_substr(
    const char* s,
    size_t      s_len,
    int64_t     start,
    int64_t     length,
    char*       out,
    size_t      out_cap
) {
    if (!s || s_len == 0 || out_cap == 0) return 0;

    // Convert 1-based start to 0-based
    int64_t idx = start - 1;
    if (idx < 0) {
        // SQLite: negative start counts from end
        idx = (int64_t)s_len + idx;
        if (idx < 0) idx = 0;
    }
    if ((size_t)idx >= s_len) return 0;

    size_t avail = s_len - (size_t)idx;
    size_t n;
    if (length < 0) {
        n = avail;
    } else {
        n = (size_t)length;
        if (n > avail) n = avail;
    }
    if (n > out_cap) n = out_cap;
    memcpy(out, s + idx, n);
    return n;
}

} // extern "C"
