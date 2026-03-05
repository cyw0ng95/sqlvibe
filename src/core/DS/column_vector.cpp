#include "column_vector.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

struct svdb_column_vector {
    std::string name;
    int type;  /* SVDB_TYPE_* */
    std::vector<bool> nulls;
    std::vector<int64_t> ints;
    std::vector<double> floats;
    std::vector<std::string> strings;
    std::vector<std::vector<uint8_t>> blobs;

    svdb_column_vector(const char* n, int t) : name(n ? n : ""), type(t) {}
};

extern "C" {

svdb_column_vector_t* svdb_column_vector_create(const char* name, int type) {
    return new svdb_column_vector(name, type);
}

void svdb_column_vector_destroy(svdb_column_vector_t* cv) {
    if (cv) delete cv;
}

int svdb_column_vector_len(const svdb_column_vector_t* cv) {
    if (!cv) return 0;
    return (int)cv->nulls.size();
}

int svdb_column_vector_is_null(const svdb_column_vector_t* cv, int idx) {
    if (!cv || idx < 0 || idx >= (int)cv->nulls.size()) return 1;
    return cv->nulls[idx] ? 1 : 0;
}

void svdb_column_vector_set_null(svdb_column_vector_t* cv, int idx, int is_null) {
    if (!cv || idx < 0 || idx >= (int)cv->nulls.size()) return;
    cv->nulls[idx] = (is_null != 0);
}

void svdb_column_vector_append_null(svdb_column_vector_t* cv) {
    if (!cv) return;
    cv->nulls.push_back(true);
    switch (cv->type) {
        case SVDB_TYPE_INT:
            cv->ints.push_back(0);
            break;
        case SVDB_TYPE_REAL:
            cv->floats.push_back(0.0);
            break;
        case SVDB_TYPE_TEXT:
            cv->strings.emplace_back();
            break;
        case SVDB_TYPE_BLOB:
            cv->blobs.emplace_back();
            break;
    }
}

void svdb_column_vector_append_int(svdb_column_vector_t* cv, int64_t val) {
    if (!cv) return;
    cv->nulls.push_back(false);
    cv->ints.push_back(val);
}

void svdb_column_vector_append_float(svdb_column_vector_t* cv, double val) {
    if (!cv) return;
    cv->nulls.push_back(false);
    cv->floats.push_back(val);
}

void svdb_column_vector_append_text(svdb_column_vector_t* cv, const char* val, size_t len) {
    if (!cv) return;
    cv->nulls.push_back(false);
    cv->strings.emplace_back(val ? val : "", len);
}

void svdb_column_vector_append_blob(svdb_column_vector_t* cv, const uint8_t* val, size_t len) {
    if (!cv) return;
    cv->nulls.push_back(false);
    cv->blobs.emplace_back(val, val + len);
}

int64_t svdb_column_vector_get_int(const svdb_column_vector_t* cv, int idx) {
    if (!cv || idx < 0 || idx >= (int)cv->ints.size()) return 0;
    return cv->ints[idx];
}

double svdb_column_vector_get_float(const svdb_column_vector_t* cv, int idx) {
    if (!cv || idx < 0 || idx >= (int)cv->floats.size()) return 0.0;
    return cv->floats[idx];
}

const char* svdb_column_vector_get_text(const svdb_column_vector_t* cv, int idx, size_t* out_len) {
    if (!cv || idx < 0 || idx >= (int)cv->strings.size()) {
        if (out_len) *out_len = 0;
        return "";
    }
    if (out_len) *out_len = cv->strings[idx].size();
    return cv->strings[idx].data();
}

const uint8_t* svdb_column_vector_get_blob(const svdb_column_vector_t* cv, int idx, size_t* out_len) {
    if (!cv || idx < 0 || idx >= (int)cv->blobs.size()) {
        if (out_len) *out_len = 0;
        return nullptr;
    }
    if (out_len) *out_len = cv->blobs[idx].size();
    return cv->blobs[idx].data();
}

void svdb_column_vector_clear(svdb_column_vector_t* cv) {
    if (!cv) return;
    cv->nulls.clear();
    cv->ints.clear();
    cv->floats.clear();
    cv->strings.clear();
    cv->blobs.clear();
}

} /* extern "C" */
