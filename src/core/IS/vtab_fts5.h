/* vtab_fts5.h — FTS5 Virtual Table Module */
#pragma once
#ifndef SVDB_VTAB_FTS5_H
#define SVDB_VTAB_FTS5_H

#include "vtab_registry.h"
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace svdb {

/**
 * FTS5Index — Internal full-text search index
 */
class FTS5Index {
public:
    FTS5Index(int column_count);
    ~FTS5Index();
    
    // Document operations
    int Insert(int64_t doc_id, const std::vector<std::string>& values);
    int Delete(int64_t doc_id);
    void Clear();
    
    // Query operations
    std::vector<int64_t> Search(const std::string& term) const;
    std::vector<int64_t> SearchPrefix(const std::string& prefix) const;
    
    // Statistics for BM25
    int DocCount() const { return doc_count_; }
    int DocLength(int64_t doc_id) const;
    int TermFreq(int64_t doc_id, const std::string& term) const;
    int DocFreq(const std::string& term) const;
    double AvgDocLength() const;
    
    // Column access
    const std::string& GetColumn(int64_t doc_id, int col) const;
    int ColumnCount() const { return column_count_; }
    
    // Iterators for BM25 calculation
    const std::unordered_map<int64_t, int>& GetDocLengths() const { return doc_lengths_; }

private:
    std::vector<std::string> Tokenize(const std::string& text) const;
    
    int column_count_;
    int doc_count_;
    
    // term -> set of doc_ids
    std::unordered_map<std::string, std::unordered_set<int64_t>> posting_list_;
    
    // term -> doc_id -> frequency
    std::unordered_map<std::string, std::unordered_map<int64_t, int>> term_freq_;
    
    // doc_id -> total token count
    std::unordered_map<int64_t, int> doc_lengths_;
    
    // doc_id -> column values
    std::unordered_map<int64_t, std::vector<std::string>> doc_columns_;
};

/**
 * FTS5 VTab types
 */
class FTS5VTab;
class FTS5Cursor;

/**
 * FTS5Cursor — Cursor for FTS5 query results
 */
class FTS5Cursor : public VTabCursor {
public:
    FTS5Cursor(FTS5VTab* vtab);
    ~FTS5Cursor() override;
    
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
    FTS5VTab* vtab_;
    std::vector<int64_t> doc_ids_;
    std::unordered_map<int64_t, double> scores_;
    size_t pos_;
    int64_t current_rowid_;
    bool eof_;
    std::string query_;
};

/**
 * FTS5VTab — FTS5 virtual table instance
 */
class FTS5VTab : public VTab {
public:
    FTS5VTab(const std::vector<std::string>& columns);
    ~FTS5VTab() override;
    
    std::vector<std::string> Columns() const override;
    VTabCursor* Open() override;
    
    // FTS5-specific operations
    int Insert(int64_t doc_id, const std::vector<std::string>& values);
    int Delete(int64_t doc_id);
    
    FTS5Index* GetIndex() { return index_; }
    int ColumnCount() const { return static_cast<int>(columns_.size()); }
    const std::vector<std::string>& GetColumns() const { return columns_; }
    
    // BM25 scoring
    double BM25Score(int64_t doc_id, const std::vector<std::string>& terms,
                    double k1 = 1.2, double b = 0.75) const;

private:
    std::vector<std::string> columns_;
    FTS5Index* index_;
    int64_t next_doc_id_;
};

/**
 * FTS5Module — Module factory for FTS5 virtual table
 */
class FTS5Module : public VTabModule {
public:
    VTab* Create(const std::vector<std::string>& args) override;
    VTab* Connect(const std::vector<std::string>& args) override;
};

/* Auto-register the FTS5 module at static initialization */
extern RegisterModule g_fts5_module_reg;

} /* namespace svdb */

#endif /* SVDB_VTAB_FTS5_H */
