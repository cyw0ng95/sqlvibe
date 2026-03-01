//go:build !SVDB_ENABLE_CGO
// +build !SVDB_ENABLE_CGO

package fts5

import "unsafe"

// cgoTokenizer is a stub type for pure Go builds.
type cgoTokenizer struct {
	handle unsafe.Pointer
	typ    TokenizerType
}

func (t *cgoTokenizer) Close() {}

func (t *cgoTokenizer) Tokenize(text string) []Token {
	return nil
}

// cgoRanker is a stub type for pure Go builds.
type cgoRanker struct {
	index  *FTS5Index
	handle unsafe.Pointer
}

// Pure Go tokenizer factory
func newCGOTokenizer(typ TokenizerType) *cgoTokenizer {
	// In pure Go build, return nil - the Go implementation will be used
	return nil
}

// Pure Go ranker factory
func newCGORanker(index *FTS5Index, k1, b float64) *cgoRanker {
	// In pure Go build, return nil - the Go implementation will be used
	return nil
}

// Pure Go BM25 - delegates to Go implementation
func cgoBM25(docLen int, avgDL float64, tf int, df int, n int, k1, b float64) float64 {
	// In pure Go build, use the Go implementation
	return BM25(docLen, avgDL, tf, df, n, BM25Params{K1: k1, B: b})
}
