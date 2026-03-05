/* vtab_series.cpp — Series Virtual Table Module Implementation */
#include "vtab_series.h"
#include <cstdlib>
#include <cstring>
#include <cerrno>

namespace svdb {

/* ── SeriesModule Implementation ─────────────────────────────── */

VTab* SeriesModule::Create(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return nullptr; /* Requires at least start, stop */
    }
    
    char* endptr = nullptr;
    
    int64_t start = std::strtoll(args[0].c_str(), &endptr, 10);
    if (*endptr != '\0') {
        return nullptr; /* Invalid start value */
    }
    
    int64_t stop = std::strtoll(args[1].c_str(), &endptr, 10);
    if (*endptr != '\0') {
        return nullptr; /* Invalid stop value */
    }
    
    int64_t step = 1;
    if (args.size() >= 3) {
        step = std::strtoll(args[2].c_str(), &endptr, 10);
        if (*endptr != '\0' || step == 0) {
            return nullptr; /* Invalid or zero step value */
        }
    }
    
    return new (std::nothrow) SeriesVTab(start, stop, step);
}

VTab* SeriesModule::Connect(const std::vector<std::string>& args) {
    /* Connect works the same as Create for series */
    return Create(args);
}

/* Auto-register the series module */
static SeriesModule g_series_module;
RegisterModule g_series_module_reg("series", &g_series_module);

/* ── SeriesVTab Implementation ───────────────────────────────── */

SeriesVTab::SeriesVTab(int64_t start, int64_t stop, int64_t step)
    : start_(start), stop_(stop), step_(step) {
}

std::vector<std::string> SeriesVTab::Columns() const {
    return {"value"};
}

VTabCursor* SeriesVTab::Open() {
    return new (std::nothrow) SeriesCursor(start_, stop_, step_);
}

/* ── SeriesCursor Implementation ─────────────────────────────── */

SeriesCursor::SeriesCursor(int64_t start, int64_t stop, int64_t step)
    : start_(start), stop_(stop), step_(step), current_(start) {
}

int SeriesCursor::Filter(int idxNum, const std::string& idxStr,
                         const std::vector<std::string>& args) {
    (void)idxNum;
    (void)idxStr;
    (void)args;
    
    /* Reset to start */
    current_ = start_;
    return 0;
}

int SeriesCursor::Next() {
    current_ += step_;
    return 0;
}

bool SeriesCursor::Eof() const {
    if (step_ > 0) {
        return current_ > stop_;
    }
    return current_ < stop_;
}

int SeriesCursor::Column(int col, int* out_type, int64_t* out_ival,
                         double* out_rval, const char** out_sval,
                         size_t* out_slen) {
    if (col != 0) {
        return -1; /* Only one column */
    }
    
    if (out_type) *out_type = 1; /* SVDB_TYPE_INT */
    if (out_ival) *out_ival = current_;
    if (out_rval) *out_rval = 0.0;
    if (out_sval) *out_sval = nullptr;
    if (out_slen) *out_slen = 0;
    
    return 0;
}

int SeriesCursor::RowID(int64_t* out_rowid) {
    if (step_ != 0) {
        *out_rowid = (current_ - start_) / step_;
    } else {
        *out_rowid = 0;
    }
    return 0;
}

int SeriesCursor::Close() {
    delete this;
    return 0;
}

} /* namespace svdb */
