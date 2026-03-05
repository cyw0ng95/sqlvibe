#include "fts5.h"
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <algorithm>

// Internal index structure
struct svdb_fts5_index {
    int column_count;
    std::unordered_map<std::string, std::unordered_set<int64_t>> posting_list;  // term -> doc_ids
    std::unordered_map<std::string, std::unordered_map<int64_t, int>> term_freq; // term -> doc_id -> count
    std::unordered_map<int64_t, int> doc_lengths;  // doc_id -> token count
    std::unordered_map<int64_t, std::vector<std::string>> doc_columns;  // doc_id -> column values
    int doc_count;
    
    svdb_fts5_index(int cols) : column_count(cols), doc_count(0) {}
};

// Internal ranker structure
struct svdb_fts5_ranker {
    svdb_fts5_index_t* index;
    double k1;
    double b;
    
    svdb_fts5_ranker(svdb_fts5_index_t* idx, double k, double b_val) 
        : index(idx), k1(k), b(b_val) {}
};

// C API implementation
extern "C" {

// Index functions
svdb_fts5_index_t* svdb_fts5_index_create(int column_count) {
    try {
        return new svdb_fts5_index(column_count);
    } catch (...) {
        return nullptr;
    }
}

void svdb_fts5_index_destroy(svdb_fts5_index_t* index) {
    if (index) {
        delete index;
    }
}

int svdb_fts5_index_add_document(svdb_fts5_index_t* index, int64_t doc_id, 
                                  const char* const* column_values, int column_count) {
    if (!index || !column_values || column_count <= 0) {
        return -1;
    }
    
    try {
        int total_tokens = 0;
        std::vector<std::string> columns;
        
        for (int col = 0; col < column_count && col < index->column_count; col++) {
            if (column_values[col]) {
                columns.push_back(column_values[col]);
                
                // Tokenize and add to index
                std::string text = column_values[col];
                std::string current;
                
                for (size_t i = 0; i < text.size(); i++) {
                    char c = text[i];
                    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                        if (c >= 'A' && c <= 'Z') {
                            c = c + 'a' - 'A';
                        }
                        current += c;
                    } else {
                        if (!current.empty()) {
                            index->posting_list[current].insert(doc_id);
                            index->term_freq[current][doc_id]++;
                            total_tokens++;
                            current.clear();
                        }
                    }
                }
                
                if (!current.empty()) {
                    index->posting_list[current].insert(doc_id);
                    index->term_freq[current][doc_id]++;
                    total_tokens++;
                }
            } else {
                columns.push_back("");
            }
        }
        
        index->doc_lengths[doc_id] = total_tokens;
        index->doc_columns[doc_id] = columns;
        index->doc_count++;
        
        return 0;
    } catch (...) {
        return -1;
    }
}

int64_t* svdb_fts5_index_query(svdb_fts5_index_t* index, const char* query, int* doc_count) {
    if (!index || !query || !doc_count) {
        if (doc_count) *doc_count = 0;
        return nullptr;
    }
    
    try {
        std::unordered_set<int64_t> result_set;
        std::string current;
        
        // Parse query terms
        for (size_t i = 0; i < strlen(query); i++) {
            char c = query[i];
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                if (c >= 'A' && c <= 'Z') {
                    c = c + 'a' - 'A';
                }
                current += c;
            } else {
                if (!current.empty()) {
                    auto it = index->posting_list.find(current);
                    if (it != index->posting_list.end()) {
                        if (result_set.empty()) {
                            result_set = it->second;
                        } else {
                            std::unordered_set<int64_t> intersection;
                            for (int64_t id : result_set) {
                                if (it->second.count(id)) {
                                    intersection.insert(id);
                                }
                            }
                            result_set = intersection;
                        }
                    } else {
                        // Term not found, empty result
                        *doc_count = 0;
                        return nullptr;
                    }
                    current.clear();
                }
            }
        }
        
        if (!current.empty()) {
            auto it = index->posting_list.find(current);
            if (it != index->posting_list.end()) {
                if (result_set.empty()) {
                    result_set = it->second;
                } else {
                    std::unordered_set<int64_t> intersection;
                    for (int64_t id : result_set) {
                        if (it->second.count(id)) {
                            intersection.insert(id);
                        }
                    }
                    result_set = intersection;
                }
            } else {
                *doc_count = 0;
                return nullptr;
            }
        }
        
        if (result_set.empty()) {
            *doc_count = 0;
            return nullptr;
        }
        
        // Convert to array
        int64_t* result = new int64_t[result_set.size()];
        size_t i = 0;
        for (int64_t id : result_set) {
            result[i++] = id;
        }
        *doc_count = static_cast<int>(result_set.size());
        
        return result;
    } catch (...) {
        *doc_count = 0;
        return nullptr;
    }
}

int svdb_fts5_index_get_doc_count(svdb_fts5_index_t* index) {
    if (!index) return 0;
    return index->doc_count;
}

int svdb_fts5_index_get_term_count(svdb_fts5_index_t* index, const char* term) {
    if (!index || !term) return 0;
    
    try {
        std::string t = term;
        for (char& c : t) {
            if (c >= 'A' && c <= 'Z') {
                c = c + 'a' - 'A';
            }
        }
        
        auto it = index->posting_list.find(t);
        if (it != index->posting_list.end()) {
            return static_cast<int>(it->second.size());
        }
        return 0;
    } catch (...) {
        return 0;
    }
}

int svdb_fts5_index_get_doc_length(svdb_fts5_index_t* index, int64_t doc_id) {
    if (!index) return 0;
    
    auto it = index->doc_lengths.find(doc_id);
    if (it != index->doc_lengths.end()) {
        return it->second;
    }
    return 0;
}

double svdb_fts5_index_get_avg_doc_length(svdb_fts5_index_t* index) {
    if (!index || index->doc_count == 0) return 0.0;
    
    try {
        int total = 0;
        for (const auto& kv : index->doc_lengths) {
            total += kv.second;
        }
        return static_cast<double>(total) / index->doc_count;
    } catch (...) {
        return 0.0;
    }
}

// BM25 scoring function
double svdb_fts5_bm25(int doc_len, double avg_dl, int tf, int df, int n, double k1, double b) {
    if (n <= 0 || df <= 0) {
        return 0.0;
    }
    
    // IDF using Robertson-Sparck Jones variant
    double idf = std::log(static_cast<double>(n - df + 1) / static_cast<double>(df + 1));
    
    // TF saturation
    double numerator = static_cast<double>(tf) * (k1 + 1.0);
    double denominator = static_cast<double>(tf) + k1 * (1.0 - b + b * static_cast<double>(doc_len) / avg_dl);
    
    double tf_component = numerator / denominator;
    
    return idf * tf_component;
}

// Ranker functions
svdb_fts5_ranker_t* svdb_fts5_ranker_create(svdb_fts5_index_t* index, double k1, double b) {
    try {
        return new svdb_fts5_ranker(index, k1, b);
    } catch (...) {
        return nullptr;
    }
}

void svdb_fts5_ranker_destroy(svdb_fts5_ranker_t* ranker) {
    if (ranker) {
        delete ranker;
    }
}

double svdb_fts5_ranker_score(svdb_fts5_ranker_t* ranker, int64_t doc_id, 
                               const char* const* terms, int term_count) {
    if (!ranker || !terms || term_count <= 0) {
        return 0.0;
    }
    
    try {
        int doc_len = svdb_fts5_index_get_doc_length(ranker->index, doc_id);
        if (doc_len <= 0) {
            return 0.0;
        }
        
        double avg_dl = svdb_fts5_index_get_avg_doc_length(ranker->index);
        int n = svdb_fts5_index_get_doc_count(ranker->index);
        
        double total_score = 0.0;
        for (int i = 0; i < term_count; i++) {
            std::string term = terms[i];
            for (char& c : term) {
                if (c >= 'A' && c <= 'Z') {
                    c = c + 'a' - 'A';
                }
            }
            
            int df = svdb_fts5_index_get_term_count(ranker->index, term.c_str());
            
            // Get term frequency in doc
            int tf = 0;
            auto tf_it = ranker->index->term_freq.find(term);
            if (tf_it != ranker->index->term_freq.end()) {
                auto doc_it = tf_it->second.find(doc_id);
                if (doc_it != tf_it->second.end()) {
                    tf = doc_it->second;
                }
            }
            
            double score = svdb_fts5_bm25(doc_len, avg_dl, tf, df, n, ranker->k1, ranker->b);
            total_score += score;
        }
        
        return total_score;
    } catch (...) {
        return 0.0;
    }
}

} // extern "C"
