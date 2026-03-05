#include "fts5.h"
#include <cstring>
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>

// Token structure for internal use
struct Token {
    std::string term;
    int start;
    int end;
    int position;
};

// Tokenizer implementation
struct svdb_fts5_tokenizer {
    svdb_fts5_tokenizer_type_t type;
    
    std::vector<Token> tokenize(const std::string& text) {
        std::vector<Token> tokens;
        
        switch (type) {
            case SVDB_FTS5_TOKEN_ASCII:
                return tokenizeASCII(text);
            case SVDB_FTS5_TOKEN_PORTER:
                return tokenizePorter(text);
            case SVDB_FTS5_TOKEN_UNICODE61:
                return tokenizeUnicode61(text);
            default:
                return tokenizeASCII(text);
        }
    }
    
private:
    std::vector<Token> tokenizeASCII(const std::string& text) {
        std::vector<Token> tokens;
        std::string current;
        int start = -1;
        int position = 0;
        
        for (size_t i = 0; i < text.size(); i++) {
            char c = text[i];
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                if (start == -1) {
                    start = static_cast<int>(i);
                }
                // Lowercase
                if (c >= 'A' && c <= 'Z') {
                    c = c + 'a' - 'A';
                }
                current += c;
            } else {
                if (!current.empty()) {
                    tokens.push_back(Token{current, start, static_cast<int>(i), position});
                    position++;
                    current.clear();
                    start = -1;
                }
            }
        }
        
        if (!current.empty()) {
            tokens.push_back(Token{current, start, static_cast<int>(text.size()), position});
        }
        
        return tokens;
    }
    
    std::vector<Token> tokenizePorter(const std::string& text) {
        std::vector<Token> tokens = tokenizeASCII(text);
        for (auto& token : tokens) {
            token.term = stem(token.term);
        }
        return tokens;
    }
    
    std::vector<Token> tokenizeUnicode61(const std::string& text) {
        // Simplified Unicode61 - in production would use proper Unicode library
        std::vector<Token> tokens;
        std::string current;
        int start = -1;
        int position = 0;
        
        for (size_t i = 0; i < text.size(); i++) {
            unsigned char c = static_cast<unsigned char>(text[i]);
            // Simple heuristic: ASCII letters and numbers
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || 
                (c >= '0' && c <= '9') || c >= 0xC0) {
                if (start == -1) {
                    start = static_cast<int>(i);
                }
                // Lowercase ASCII
                if (c >= 'A' && c <= 'Z') {
                    c = c + 'a' - 'A';
                }
                current += static_cast<char>(c);
            } else {
                if (!current.empty()) {
                    tokens.push_back(Token{current, start, static_cast<int>(i), position});
                    position++;
                    current.clear();
                    start = -1;
                }
            }
        }
        
        if (!current.empty()) {
            tokens.push_back(Token{current, start, static_cast<int>(text.size()), position});
        }
        
        return tokens;
    }
    
    // Porter stemmer implementation
    std::string stem(const std::string& word) {
        if (word.length() <= 2) {
            return word;
        }
        
        std::string result = word;
        
        // Step 1a
        if (endsWith(result, "sses")) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "ies")) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "ss")) {
            // Keep ss
        } else if (endsWith(result, "s")) {
            result = result.substr(0, result.length() - 1);
        }
        
        // Step 1b
        if (endsWith(result, "eed")) {
            if (measure(result.substr(0, result.length() - 3)) > 0) {
                result = result.substr(0, result.length() - 1);
            }
        } else if (endsWith(result, "ed") && containsVowel(result.substr(0, result.length() - 2))) {
            result = result.substr(0, result.length() - 2);
            if (endsWith(result, "at") || endsWith(result, "bl") || endsWith(result, "iz")) {
                result += 'e';
            } else if (result.length() >= 2 && result[result.length()-1] == result[result.length()-2] &&
                       result[result.length()-1] != 'l' && result[result.length()-1] != 's' && result[result.length()-1] != 'z') {
                result = result.substr(0, result.length() - 1);
            } else if (measure(result) == 1 && endsWithShort(result)) {
                result += 'e';
            }
        } else if (endsWith(result, "ing") && containsVowel(result.substr(0, result.length() - 3))) {
            result = result.substr(0, result.length() - 3);
            if (endsWith(result, "at") || endsWith(result, "bl") || endsWith(result, "iz")) {
                result += 'e';
            } else if (result.length() >= 2 && result[result.length()-1] == result[result.length()-2] &&
                       result[result.length()-1] != 'l' && result[result.length()-1] != 's' && result[result.length()-1] != 'z') {
                result = result.substr(0, result.length() - 1);
            } else if (measure(result) == 1 && endsWithShort(result)) {
                result += 'e';
            }
        }
        
        // Step 1c
        if (endsWith(result, "y") && containsVowel(result.substr(0, result.length() - 1))) {
            result[result.length() - 1] = 'i';
        }
        
        // Step 2
        if (endsWith(result, "ational") && measure(result.substr(0, result.length() - 7)) > 0) {
            result = result.substr(0, result.length() - 7) + "ate";
        } else if (endsWith(result, "tional") && measure(result.substr(0, result.length() - 6)) > 0) {
            result = result.substr(0, result.length() - 6);
        } else if (endsWith(result, "enci") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4) + "ence";
        } else if (endsWith(result, "anci") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4) + "ance";
        } else if (endsWith(result, "izer") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "abli") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4) + "able";
        } else if (endsWith(result, "alli") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "entli") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "eli") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ousli") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "ization") && measure(result.substr(0, result.length() - 7)) > 0) {
            result = result.substr(0, result.length() - 7) + "ize";
        } else if (endsWith(result, "ation") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5) + "ate";
        } else if (endsWith(result, "ator") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4) + "ate";
        } else if (endsWith(result, "alism") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "iveness") && measure(result.substr(0, result.length() - 7)) > 0) {
            result = result.substr(0, result.length() - 7);
        } else if (endsWith(result, "fulness") && measure(result.substr(0, result.length() - 7)) > 0) {
            result = result.substr(0, result.length() - 7);
        } else if (endsWith(result, "ousness") && measure(result.substr(0, result.length() - 7)) > 0) {
            result = result.substr(0, result.length() - 7);
        } else if (endsWith(result, "aliti") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "iviti") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5) + "ive";
        } else if (endsWith(result, "biliti") && measure(result.substr(0, result.length() - 6)) > 0) {
            result = result.substr(0, result.length() - 6) + "ble";
        }
        
        // Step 3
        if (endsWith(result, "icate") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "ative") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "alize") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "iciti") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "ical") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ful") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ness") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        }
        
        // Step 4
        if (endsWith(result, "al") && measure(result.substr(0, result.length() - 2)) > 0) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "ance") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ence") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "er") && measure(result.substr(0, result.length() - 2)) > 0) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "ic") && measure(result.substr(0, result.length() - 2)) > 0) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "able") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ible") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ant") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ement") && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 5);
        } else if (endsWith(result, "ment") && measure(result.substr(0, result.length() - 4)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ent") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (result.length() >= 5 && result.substr(result.length() - 5) == "ision" && measure(result.substr(0, result.length() - 5)) > 0) {
            result = result.substr(0, result.length() - 4);
        } else if (endsWith(result, "ion") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ou") && measure(result.substr(0, result.length() - 2)) > 0) {
            result = result.substr(0, result.length() - 2);
        } else if (endsWith(result, "ism") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ate") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "iti") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ous") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ive") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        } else if (endsWith(result, "ize") && measure(result.substr(0, result.length() - 3)) > 0) {
            result = result.substr(0, result.length() - 3);
        }
        
        // Step 5a
        if (endsWith(result, "e")) {
            if (measure(result.substr(0, result.length() - 1)) > 1) {
                result = result.substr(0, result.length() - 1);
            } else if (measure(result.substr(0, result.length() - 1)) == 1 && !endsWithShort(result.substr(0, result.length() - 1))) {
                result = result.substr(0, result.length() - 1);
            }
        }
        
        // Step 5b
        if (measure(result) > 1 && result.length() >= 2 && 
            result[result.length()-1] == 'l' && result[result.length()-2] == 'l') {
            result = result.substr(0, result.length() - 1);
        }
        
        return result;
    }
    
    bool endsWith(const std::string& str, const std::string& suffix) {
        if (suffix.length() > str.length()) return false;
        return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
    }
    
    bool containsVowel(const std::string& str) {
        for (char c : str) {
            if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' || c == 'y') {
                return true;
            }
        }
        return false;
    }
    
    int measure(const std::string& str) {
        int m = 0;
        bool inConsonant = false;
        bool inVowel = false;
        
        for (size_t i = 0; i < str.length(); i++) {
            char c = str[i];
            bool isVowel = (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' || 
                           (c == 'y' && i > 0));
            
            if (isVowel) {
                if (!inVowel) {
                    m++;
                }
                inVowel = true;
                inConsonant = false;
            } else {
                inConsonant = true;
                inVowel = false;
            }
        }
        
        return m;
    }
    
    bool endsWithShort(const std::string& str) {
        if (str.length() < 2) return false;
        char c = str[str.length() - 1];
        return (c == 'w' || c == 'x' || c == 'y') && containsVowel(str.substr(0, str.length() - 1));
    }
};

// C API implementation
extern "C" {

svdb_fts5_tokenizer_t* svdb_fts5_tokenizer_create(svdb_fts5_tokenizer_type_t type) {
    try {
        return new svdb_fts5_tokenizer{type};
    } catch (...) {
        return nullptr;
    }
}

void svdb_fts5_tokenizer_destroy(svdb_fts5_tokenizer_t* tokenizer) {
    if (tokenizer) {
        delete tokenizer;
    }
}

svdb_fts5_token_t* svdb_fts5_tokenize(svdb_fts5_tokenizer_t* tokenizer, const char* text, int* token_count) {
    if (!tokenizer || !text || !token_count) {
        if (token_count) *token_count = 0;
        return nullptr;
    }
    
    try {
        std::vector<Token> tokens = tokenizer->tokenize(text);
        *token_count = static_cast<int>(tokens.size());
        
        if (tokens.empty()) {
            return nullptr;
        }
        
        svdb_fts5_token_t* result = new svdb_fts5_token_t[tokens.size()];
        for (size_t i = 0; i < tokens.size(); i++) {
            result[i].term = strdup(tokens[i].term.c_str());
            result[i].start = tokens[i].start;
            result[i].end = tokens[i].end;
            result[i].position = tokens[i].position;
        }
        
        return result;
    } catch (...) {
        *token_count = 0;
        return nullptr;
    }
}

void svdb_fts5_token_free(svdb_fts5_token_t* token) {
    if (token) {
        if (token->term) {
            free(token->term);
        }
    }
}

void svdb_fts5_free_string(char* str) {
    if (str) {
        free(str);
    }
}

void svdb_fts5_free_int64_array(int64_t* arr) {
    if (arr) {
        delete[] arr;
    }
}

} // extern "C"
