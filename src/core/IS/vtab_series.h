/* vtab_series.h — Series Virtual Table Module */
#pragma once
#ifndef SVDB_VTAB_SERIES_H
#define SVDB_VTAB_SERIES_H

#include "vtab_registry.h"
#include <cstdint>

namespace svdb {

/**
 * SeriesVTab — Virtual table that generates a series of integers
 * 
 * Usage:
 *   SELECT * FROM series(1, 10)        -- generates 1,2,3,...,10
 *   SELECT * FROM series(0, 100, 10)   -- generates 0,10,20,...,100
 *   CREATE VIRTUAL TABLE t USING series(1, 10)
 * 
 * Schema: value INTEGER
 */
class SeriesVTab : public VTab {
public:
    SeriesVTab(int64_t start, int64_t stop, int64_t step);
    ~SeriesVTab() override = default;
    
    std::vector<std::string> Columns() const override;
    VTabCursor* Open() override;
    
    int64_t Start() const { return start_; }
    int64_t Stop() const { return stop_; }
    int64_t Step() const { return step_; }

private:
    int64_t start_;
    int64_t stop_;
    int64_t step_;
};

/**
 * SeriesCursor — Cursor for series virtual table
 */
class SeriesCursor : public VTabCursor {
public:
    SeriesCursor(int64_t start, int64_t stop, int64_t step);
    ~SeriesCursor() override = default;
    
    int Filter(int idxNum, const std::string& idxStr,
               const std::vector<std::string>& args) override;
    int Next() override;
    bool Eof() const override;
    int Column(int col, int* out_type, int64_t* out_ival,
               double* out_rval, const char** out_sval,
               size_t* out_slen) override;
    int RowID(int64_t* out_rowid) override;
    int Close() override;

private:
    int64_t start_;
    int64_t stop_;
    int64_t step_;
    int64_t current_;
};

/**
 * SeriesModule — Module factory for series virtual table
 */
class SeriesModule : public VTabModule {
public:
    VTab* Create(const std::vector<std::string>& args) override;
    VTab* Connect(const std::vector<std::string>& args) override;
};

/* Auto-register the series module at static initialization */
extern RegisterModule g_series_module_reg;

} /* namespace svdb */

#endif /* SVDB_VTAB_SERIES_H */
