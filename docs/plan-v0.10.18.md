# Plan v0.10.18 - C++ FTS5 Extension (Hybrid Go+C++)

## Summary

Refactor ext/fts5 to use C++ for full-text search operations, following the same hybrid Go+C++ pattern as v0.10.16 and v0.10.17.

## Background

### Current State
- ext/fts5: ~2311 lines of Go code
- Pure Go tokenizer and ranking implementation
- FTS5 virtual table with MATCH queries

### Why C++?
- **Performance**: FTS is I/O and CPU intensive
- **Libraries**: Existing C++ libraries (Snowball, Lucene)
- **SIMD**: Can accelerate text processing

### Dependencies
- **Snowball**: Stemming algorithm (C library)
- **mmap**: Memory-mapped file access

---

## 1. Architecture

### Hybrid Structure

```
ext/fts5/
├── fts5.go           # Go: Extension registration (always)
├── fts5_cgo.go     # [+build SVDB_ENABLE_CGO] CGO bridge
├── fts5_pure.go    # [-build SVDB_ENABLE_CGO] Pure Go implementation
├── fts5.h           # C++: Header declarations
├── fts5.cpp         # C++: Implementation
├── tokenizer.h      # C++: Tokenizer interface
├── tokenizer.cpp    # C++: Snowball tokenizer
├── rank.h          # C++: Ranking algorithms
├── rank.cpp        # C++: BM25, TF-IDF
└── CMakeLists.txt  # C++ build config
```

### Library Name
- `libsvdb_ext_fts5.so`

---

## 2. C++ Implementation

### fts5.h

```cpp
#ifndef SVDB_FTS5_H
#define SVDB_FTS5_H

#include <cstdint>
#include <string>
#include <vector>

extern "C" {

// Tokenizer
typedef struct {
    char* token;
    int32_t start;
    int32_t end;
    int32_t pos;
} FTS5Token;

void* fts5_tokenizer_create(const char* language);
void fts5_tokenizer_destroy(void* tokenizer);
int fts5_tokenize(void* tokenizer, const char* text, int32_t len, 
                  void (*callback)(void*, FTS5Token*), void* ctx);

// Index operations
void* fts5_index_create(const char* path);
void fts5_index_destroy(void* index);
int fts5_index_add(void* index, const char* docid, const char* text);
int fts5_index_delete(void* index, const char* docid);
int fts5_index_flush(void* index);

// Query
typedef struct {
    char* docid;
    float rank;
} FTS5Match;

void* fts5_query_create(void* index, const char* query);
void fts5_query_destroy(void* query);
int fts5_query_next(void* query, FTS5Match* out);
int64_t fts5_query_count(void* query);

// Ranking
float fts5_rank_bm25(void* index, const char* query, const char* docid);
float fts5_rank_tf(void* index, const char* query, const char* docid);

// Snapshot/Checkpoint
int fts5_index_checkpoint(void* index, const char* path);
int fts5_index_restore(void* index, const char* path);

} // extern "C"

#endif
```

### tokenizer.cpp (Snowball)

```cpp
#include "tokenizer.h"
#include "libstemmer.h"

struct StemmerTokenizer {
    sb_stemmer* stemmer;
    
    StemmerTokenizer(const char* language) {
        stemmer = sb_stemmer_new(language, nullptr);
    }
    
    ~StemmerTokenizer() {
        if (stemmer) sb_stemmer_delete(stemmer);
    }
    
    void tokenize(const char* text, int len, TokenCallback cb) {
        // 1. Tokenize into words
        // 2. Apply stemming via Snowball
        // 3. Return tokens with positions
    }
};
```

### rank.cpp (BM25)

```cpp
#include "rank.h"

// BM25 ranking algorithm
float calculate_bm25(
    int64_t doc_len,      // Document length
    int64_t term_freq,    // Term frequency in document  
    int64_t doc_count,    // Total documents
    int64_t term_doc_freq // Documents containing term
) {
    double k1 = 1.5;
    double b = 0.75;
    double avgdl = 100; // Average document length
    
    double idf = log((doc_count - term_doc_freq + 0.5) / (term_doc_freq + 0.5));
    double tf = (term_freq * (k1 + 1)) / (term_freq + k1 * (1 - b + b * doc_len / avgdl));
    
    return idf * tf;
}
```

---

## 3. Build System

### 3.1 Root CMakeLists.txt

Update to include FTS5:

```cmake
# Add to existing CMakeLists.txt
add_subdirectory(ext/fts5)
```

### 3.2 ext/fts5/CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(svdb_ext_fts5)

set(CMAKE_CXX_STANDARD 17)

# Find or fetch Snowball stemmer
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(SNOWBALL QUIET snowballstem)
endif()

if(NOT SNOWBALL_FOUND)
    # Use bundled snowball
    add_library(snowball STATIC IMPORTED)
    set_target_properties(snowball PROPERTIES
        IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/ext/fts5/libstemmer/libstemmer.a
    )
endif()

add_library(svdb_ext_fts5 SHARED
    fts5.cpp
    tokenizer.cpp
    rank.cpp
)

target_link_libraries(svdb_ext_fts5 PUBLIC
    ${SNOWBALL_LIBRARIES}
    m  # math library
)

target_include_directories(svdb_ext_fts5 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${SNOWBALL_INCLUDE_DIRS}
)

# Install to .build/cmake/lib
install(TARGETS svdb_ext_fts5
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/.build/cmake/lib
)
```

---

## 4. Go Integration

### fts5_cgo.go

```go
// +build SVDB_ENABLE_CGO

package fts5

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_fts5
#cgo CFLAGS: -I${SRCDIR}/include
#include "fts5.h"
#include <stdlib.h>
*/
import "C"

type FTS5Extension struct{}

func (e *FTS5Extension) VTabModule() string { return "fts5" }

func (e *FTS5Extension) Create(arg string) (VTab, error) {
    // Use C++ index
    path := C.CString(arg)
    defer C.free(unsafe.Pointer(path))
    
    idx := C.fts5_index_create(path)
    if idx == nil {
        return nil, errors.New("failed to create FTS5 index")
    }
    
    return &FTS5VTab{index: idx}, nil
}

type FTS5VTab struct {
    index unsafe.Pointer
}

func (v *FTS5VTab) Close() error {
    if v.index != nil {
        C.fts5_index_destroy(v.index)
    }
    return nil
}

func (v *FTS5VTab) Filter(query string) ([]Row, error) {
    cQuery := C.CString(query)
    defer C.free(unsafe.Pointer(cQuery))
    
    q := C.fts5_query_create(v.index, cQuery)
    if q == nil {
        return nil, errors.New("failed to create query")
    }
    defer C.fts5_query_destroy(q)
    
    var results []Row
    for {
        var match C.FTS5Match
        if C.fts5_query_next(q, &match) == 0 {
            break
        }
        results = append(results, Row{
            DocID: C.GoString(match.docid),
            Rank:  float64(match.rank),
        })
    }
    
    return results, nil
}
```

---

## 5. Performance Comparison

### Expected Performance

| Operation | Go (ms) | C++ (ms) | Speedup |
|----------|---------|----------|---------|
| Tokenize 1MB text | 50 | 10 | 5x |
| Index 10K docs | 5000 | 1000 | 5x |
| BM25 Ranking | 100 | 20 | 5x |
| Query | 10 | 2 | 5x |

### Benchmark Scenarios

| Scenario | Documents | Go | C++ |
|----------|-----------|-----|------|
| Index build | 100K | 50s | 10s |
| Search | 1M docs | 100ms | 20ms |
| Re-index | 100K | 30s | 6s |

---

## 6. Dependencies

### External Libraries

| Library | Purpose | License |
|---------|---------|---------|
| Snowball | Stemming | BSD |
| mmio | Memory-mapped I/O | MIT |

### Fetch Strategy
- Use CMake FetchContent
- Fall back to system libraries
- Bundle minimal stemmer if needed

---

## 7. Implementation Order

1. Create C++ headers (fts5.h, tokenizer.h, rank.h)
2. Implement C++ tokenizer with Snowball
3. Implement C++ indexing
4. Implement BM25 ranking
5. Create fts5_cgo.go with build tags
6. Ensure fts5_pure.go works without CGO
7. Build and test both variants
8. Benchmark comparison

---

## 8. Success Criteria

- [ ] C++ FTS5 library builds with Snowball
- [ ] Tokenizer works via CGO
- [ ] Index create/update/delete via CGO
- [ ] BM25 ranking works via CGO
- [ ] Pure Go fallback works without CGO
- [ ] Performance improvement demonstrated (5x)
- [ ] Build system works with -n flag

---

## 9. Notes

- Uses same pattern as v0.10.16 (math) and v0.10.17 (json)
- Snowball provides high-quality stemming
- BM25 is industry-standard ranking algorithm
- Memory-mapped I/O for large indices
