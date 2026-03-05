/* vtab_fts5.cpp — FTS5 Virtual Table Module Implementation */
#include "vtab_fts5.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>

namespace svdb {

/* ── FTS5Index Implementation ─────────────────────────────────── */

FTS5Index::FTS5Index(int column_count)
    : column_count_(column_count), doc_count_(0) {
}

FTS5Index::~FTS5Index() {
    Clear();
}

std::vector<std::string> FTS5Index::Tokenize(const std::string& text) const {
    std::vector<std::string> tokens;
    std::string current;
    
    for (size_t i = 0; i < text.size(); i++) {
        char c = text[i];
        if (std::isalnum(static_cast<unsigned char>(c))) {
            if (std::isupper(static_cast<unsigned char>(c))) {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            current += c;
        } else {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        }
    }
    
    if (!current.empty()) {
        tokens.push_back(current);
    }
    
    return tokens;
}

int FTS5Index::Insert(int64_t doc_id, const std::vector<std::string>& values) {
    if (static_cast<int>(values.size()) != column_count_) {
        return -1;
    }
    
    try {
        int total_tokens = 0;
        std::vector<std::string> columns;
        
        for (int col = 0; col < column_count_; col++) {
            const std::string& text = values[col];
            columns.push_back(text);
            
            // Tokenize and add to index
            std::vector<std::string> tokens = Tokenize(text);
            total_tokens += static_cast<int>(tokens.size());
            
            for (const auto& token : tokens) {
                posting_list_[token].insert(doc_id);
                term_freq_[token][doc_id]++;
            }
        }
        
        doc_lengths_[doc_id] = total_tokens;
        doc_columns_[doc_id] = columns;
        doc_count_++;
        
        return 0;
    } catch (...) {
        return -1;
    }
}

int FTS5Index::Delete(int64_t doc_id) {
    try {
        // Remove from posting lists
        for (auto& pair : term_freq_) {
            const std::string& term = pair.first;
            auto& doc_freq = pair.second;
            
            if (doc_freq.find(doc_id) != doc_freq.end()) {
                doc_freq.erase(doc_id);
                if (posting_list_.find(term) != posting_list_.end()) {
                    posting_list_[term].erase(doc_id);
                }
            }
        }
        
        // Remove doc metadata
        doc_lengths_.erase(doc_id);
        doc_columns_.erase(doc_id);
        doc_count_--;
        
        return 0;
    } catch (...) {
        return -1;
    }
}

void FTS5Index::Clear() {
    posting_list_.clear();
    term_freq_.clear();
    doc_lengths_.clear();
    doc_columns_.clear();
    doc_count_ = 0;
}

std::vector<int64_t> FTS5Index::Search(const std::string& term) const {
    std::vector<int64_t> results;
    
    auto it = posting_list_.find(term);
    if (it != posting_list_.end()) {
        results.insert(results.end(), it->second.begin(), it->second.end());
    }
    
    return results;
}

std::vector<int64_t> FTS5Index::SearchPrefix(const std::string& prefix) const {
    std::vector<int64_t> results;
    std::unordered_set<int64_t> seen;
    
    for (const auto& pair : posting_list_) {
        if (pair.first.size() >= prefix.size() &&
            pair.first.compare(0, prefix.size(), prefix) == 0) {
            for (int64_t doc_id : pair.second) {
                if (seen.find(doc_id) == seen.end()) {
                    results.push_back(doc_id);
                    seen.insert(doc_id);
                }
            }
        }
    }
    
    return results;
}

int FTS5Index::DocLength(int64_t doc_id) const {
    auto it = doc_lengths_.find(doc_id);
    if (it != doc_lengths_.end()) {
        return it->second;
    }
    return 0;
}

int FTS5Index::TermFreq(int64_t doc_id, const std::string& term) const {
    auto tf_it = term_freq_.find(term);
    if (tf_it != term_freq_.end()) {
        auto doc_it = tf_it->second.find(doc_id);
        if (doc_it != tf_it->second.end()) {
            return doc_it->second;
        }
    }
    return 0;
}

int FTS5Index::DocFreq(const std::string& term) const {
    auto it = posting_list_.find(term);
    if (it != posting_list_.end()) {
        return static_cast<int>(it->second.size());
    }
    return 0;
}

const std::string& FTS5Index::GetColumn(int64_t doc_id, int col) const {
    static const std::string empty;
    
    auto doc_it = doc_columns_.find(doc_id);
    if (doc_it != doc_columns_.end()) {
        const auto& columns = doc_it->second;
        if (col >= 0 && col < static_cast<int>(columns.size())) {
            return columns[col];
        }
    }
    
    return empty;
}

double FTS5Index::AvgDocLength() const {
    if (doc_count_ == 0) {
        return 0.0;
    }
    
    double total = 0.0;
    for (const auto& pair : doc_lengths_) {
        total += pair.second;
    }
    return total / doc_count_;
}

/* ── FTS5VTab Implementation ──────────────────────────────────── */

FTS5VTab::FTS5VTab(const std::vector<std::string>& columns)
    : columns_(columns), index_(nullptr), next_doc_id_(1) {
    index_ = new (std::nothrow) FTS5Index(static_cast<int>(columns.size()));
}

FTS5VTab::~FTS5VTab() {
    if (index_) {
        delete index_;
    }
}

std::vector<std::string> FTS5VTab::Columns() const {
    return columns_;
}

VTabCursor* FTS5VTab::Open() {
    return new (std::nothrow) FTS5Cursor(this);
}

int FTS5VTab::Insert(int64_t doc_id, const std::vector<std::string>& values) {
    if (!index_) {
        return -1;
    }
    return index_->Insert(doc_id, values);
}

int FTS5VTab::Delete(int64_t doc_id) {
    if (!index_) {
        return -1;
    }
    return index_->Delete(doc_id);
}

double FTS5VTab::BM25Score(int64_t doc_id, const std::vector<std::string>& terms,
                           double k1, double b) const {
    if (!index_ || terms.empty()) {
        return 0.0;
    }
    
    double score = 0.0;
    int doc_len = index_->DocLength(doc_id);
    int n = index_->DocCount();
    
    // Calculate average document length
    double avg_dl = index_->AvgDocLength();
    
    for (const auto& term : terms) {
        int tf = index_->TermFreq(doc_id, term);
        int df = index_->DocFreq(term);
        
        if (df > 0 && tf > 0) {
            // IDF component
            double idf = std::log((n - df + 0.5) / (df + 0.5) + 1.0);
            
            // TF component with length normalization
            double tf_norm = tf * (k1 + 1.0) / 
                            (tf + k1 * (1.0 - b + b * doc_len / avg_dl));
            
            score += idf * tf_norm;
        }
    }
    
    return score;
}

/* ── FTS5Cursor Implementation ────────────────────────────────── */

FTS5Cursor::FTS5Cursor(FTS5VTab* vtab)
    : vtab_(vtab), pos_(0), current_rowid_(0), eof_(true) {
}

FTS5Cursor::~FTS5Cursor() {
}

int FTS5Cursor::Filter(int idxNum, const std::string& idxStr,
                       const std::vector<std::string>& args) {
    (void)idxNum;
    (void)idxStr;
    
    doc_ids_.clear();
    scores_.clear();
    pos_ = 0;
    eof_ = true;
    
    if (!vtab_ || !vtab_->GetIndex()) {
        return 0;
    }
    
    FTS5Index* index = vtab_->GetIndex();
    
    // Parse query from args
    if (!args.empty()) {
        query_ = args[0];
        
        // Simple term search (could be extended for phrase/prefix)
        std::vector<std::string> terms = vtab_->GetColumns(); // placeholder
        
        // Tokenize query
        std::vector<std::string> query_terms;
        std::string current;
        for (char c : query_) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                if (std::isupper(static_cast<unsigned char>(c))) {
                    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                }
                current += c;
            } else {
                if (!current.empty()) {
                    query_terms.push_back(current);
                    current.clear();
                }
            }
        }
        if (!current.empty()) {
            query_terms.push_back(current);
        }
        
        // Search for each term and combine results
        std::unordered_set<int64_t> result_set;
        for (const auto& term : query_terms) {
            auto matches = index->Search(term);
            for (int64_t doc_id : matches) {
                result_set.insert(doc_id);
            }
        }
        
        // Convert to vector
        doc_ids_.assign(result_set.begin(), result_set.end());
        
        // Calculate BM25 scores
        for (int64_t doc_id : doc_ids_) {
            scores_[doc_id] = vtab_->BM25Score(doc_id, query_terms);
        }
        
        // Sort by score (descending)
        std::sort(doc_ids_.begin(), doc_ids_.end(),
                  [this](int64_t a, int64_t b) {
                      return scores_[a] > scores_[b];
                  });
    }
    
    eof_ = doc_ids_.empty();
    if (!eof_) {
        current_rowid_ = doc_ids_[0];
    }
    
    return 0;
}

int FTS5Cursor::Next() {
    if (pos_ + 1 >= doc_ids_.size()) {
        eof_ = true;
        current_rowid_ = 0;
    } else {
        pos_++;
        current_rowid_ = doc_ids_[pos_];
    }
    return 0;
}

bool FTS5Cursor::Eof() const {
    return eof_;
}

int FTS5Cursor::Column(int col, int* out_type, int64_t* out_ival,
                       double* out_rval, const char** out_sval,
                       size_t* out_slen) {
    if (eof_ || pos_ >= doc_ids_.size()) {
        if (out_type) *out_type = 0; // NULL
        return 0;
    }
    
    int64_t doc_id = doc_ids_[pos_];
    
    // Return score for column 0 if there's a query
    if (!query_.empty() && col == 0) {
        if (out_type) *out_type = 2; // REAL
        if (out_rval) *out_rval = scores_[doc_id];
        if (out_ival) *out_ival = 0;
        if (out_sval) *out_sval = nullptr;
        if (out_slen) *out_slen = 0;
        return 0;
    }
    
    // Return column value
    if (vtab_ && vtab_->GetIndex()) {
        const std::string& value = vtab_->GetIndex()->GetColumn(doc_id, col);
        if (!value.empty()) {
            if (out_type) *out_type = 3; // TEXT
            if (out_sval) *out_sval = value.c_str();
            if (out_slen) *out_slen = value.size();
            if (out_ival) *out_ival = 0;
            if (out_rval) *out_rval = 0.0;
            return 0;
        }
    }
    
    // Return docID as fallback
    if (out_type) *out_type = 1; // INT
    if (out_ival) *out_ival = doc_id;
    if (out_rval) *out_rval = 0.0;
    if (out_sval) *out_sval = nullptr;
    if (out_slen) *out_slen = 0;
    
    return 0;
}

int FTS5Cursor::RowID(int64_t* out_rowid) {
    if (out_rowid) {
        *out_rowid = current_rowid_;
    }
    return 0;
}

int FTS5Cursor::Close() {
    doc_ids_.clear();
    scores_.clear();
    delete this;
    return 0;
}

/* ── FTS5Module Implementation ────────────────────────────────── */

VTab* FTS5Module::Create(const std::vector<std::string>& args) {
    if (args.empty()) {
        return nullptr; // Requires at least one column
    }
    
    // Parse arguments: column names and optional tokenizer
    std::vector<std::string> columns;
    
    for (const auto& arg : args) {
        // Skip tokenizer argument (not used in this implementation)
        if (arg.compare(0, 9, "tokenize=") == 0) {
            continue;
        }
        if (!arg.empty()) {
            columns.push_back(arg);
        }
    }
    
    if (columns.empty()) {
        return nullptr;
    }
    
    return new (std::nothrow) FTS5VTab(columns);
}

VTab* FTS5Module::Connect(const std::vector<std::string>& args) {
    // Connect works the same as Create for FTS5
    return Create(args);
}

/* Auto-register the FTS5 module */
static FTS5Module g_fts5_module;
RegisterModule g_fts5_module_reg("fts5", &g_fts5_module);

} /* namespace svdb */
