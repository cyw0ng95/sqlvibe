//go:build SVDB_ENABLE_CGO
// +build SVDB_ENABLE_CGO

package fts5

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb_ext_fts5
#cgo CFLAGS: -I${SRCDIR}
#include "fts5.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"unsafe"
)

// CGO tokenizer wrapper
type cgoTokenizer struct {
	handle *C.svdb_fts5_tokenizer_t
	typ    TokenizerType
}

func newCGOTokenizer(typ TokenizerType) *cgoTokenizer {
	var cType C.svdb_fts5_tokenizer_type_t
	switch typ {
	case TokenizerASCII:
		cType = C.SVDB_FTS5_TOKEN_ASCII
	case TokenizerPorter:
		cType = C.SVDB_FTS5_TOKEN_PORTER
	case TokenizerUnicode61:
		cType = C.SVDB_FTS5_TOKEN_UNICODE61
	default:
		cType = C.SVDB_FTS5_TOKEN_ASCII
	}

	handle := C.svdb_fts5_tokenizer_create(cType)
	return &cgoTokenizer{
		handle: handle,
		typ:    typ,
	}
}

func (t *cgoTokenizer) Tokenize(text string) []Token {
	if t.handle == nil {
		return nil
	}

	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))

	var tokenCount C.int
	cTokens := C.svdb_fts5_tokenize(t.handle, cText, &tokenCount)
	if cTokens == nil || tokenCount == 0 {
		return nil
	}
	defer C.free(unsafe.Pointer(cTokens))

	tokens := make([]Token, int(tokenCount))
	tokSlice := unsafe.Slice(cTokens, int(tokenCount))
	for i := 0; i < int(tokenCount); i++ {
		tokens[i] = Token{
			Term:     C.GoString(tokSlice[i].term),
			Start:    int(tokSlice[i].start),
			End:      int(tokSlice[i].end),
			Position: int(tokSlice[i].position),
		}
		C.svdb_fts5_token_free(&tokSlice[i])
	}

	return tokens
}

func (t *cgoTokenizer) Close() {
	if t.handle != nil {
		C.svdb_fts5_tokenizer_destroy(t.handle)
		t.handle = nil
	}
}

// CGO ranker wrapper
type cgoRanker struct {
	index  *FTS5Index
	handle *C.svdb_fts5_ranker_t
}

func newCGORanker(index *FTS5Index, k1, b float64) *cgoRanker {
	// The ranker needs access to the index, which is still in Go
	// For now, we use the pure Go ranker
	// In a full implementation, the index would also be in C++
	return nil
}

func (r *cgoRanker) ScoreDocument(docID int64, terms []string) float64 {
	if r.handle == nil {
		return 0
	}

	cTerms := make([]*C.char, len(terms))
	for i, term := range terms {
		cTerms[i] = C.CString(term)
	}
	defer func() {
		for _, cTerm := range cTerms {
			C.free(unsafe.Pointer(cTerm))
		}
	}()

	score := C.svdb_fts5_ranker_score(r.handle, C.int64_t(docID), &cTerms[0], C.int(len(terms)))
	return float64(score)
}

func (r *cgoRanker) Close() {
	if r.handle != nil {
		C.svdb_fts5_ranker_destroy(r.handle)
		r.handle = nil
	}
}

// Helper function to convert Go string slice to C array
func goStringsToC(strings []string) []*C.char {
	cStrings := make([]*C.char, len(strings))
	for i, s := range strings {
		cStrings[i] = C.CString(s)
	}
	return cStrings
}

func freeCStringArray(cStrings []*C.char) {
	for _, cStr := range cStrings {
		if cStr != nil {
			C.free(unsafe.Pointer(cStr))
		}
	}
}

// BM25 scoring via CGO
func cgoBM25(docLen int, avgDL float64, tf int, df int, n int, k1, b float64) float64 {
	score := C.svdb_fts5_bm25(
		C.int(docLen),
		C.double(avgDL),
		C.int(tf),
		C.int(df),
		C.int(n),
		C.double(k1),
		C.double(b),
	)
	return float64(score)
}
