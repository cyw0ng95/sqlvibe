package fts5

import (
	"math"
	"strings"
)

// BM25Params holds BM25 algorithm parameters.
type BM25Params struct {
	K1 float64 // term frequency saturation parameter (default 1.2)
	B  float64 // length normalization parameter (default 0.75)
}

// DefaultBM25Params returns default BM25 parameters.
func DefaultBM25Params() BM25Params {
	return BM25Params{
		K1: 1.2,
		B:  0.75,
	}
}

// BM25 calculates the BM25 relevance score for a document.
// Parameters:
// - docLen: length of the document (token count)
// - avgDL: average document length across the corpus
// - tf: term frequency in the document
// - df: document frequency (number of docs containing the term)
// - N: total number of documents
func BM25(docLen int, avgDL float64, tf int, df int, N int, params BM25Params) float64 {
	if N <= 0 || df <= 0 {
		return 0
	}

	// Inverse Document Frequency (IDF)
	// Using the Robertson-Sparck Jones variant
	idf := math.Log(float64(N-df+1) / float64(df+1))

	// Term Frequency saturation
	k1 := params.K1
	b := params.B

	numerator := float64(tf) * (k1 + 1)
	denominator := float64(tf) + k1*(1-b+b*float64(docLen)/avgDL)

	tfComponent := numerator / denominator

	return idf * tfComponent
}

// Ranker provides ranking functionality for FTS5 results.
type Ranker struct {
	index  *FTS5Index
	params BM25Params
}

// NewRanker creates a new ranker.
func NewRanker(index *FTS5Index, params BM25Params) *Ranker {
	return &Ranker{
		index:  index,
		params: params,
	}
}

// ScoreDocument calculates the BM25 score for a document given query terms.
func (r *Ranker) ScoreDocument(docID int64, terms []string) float64 {
	meta, ok := r.index.GetDocMeta(docID)
	if !ok {
		return 0
	}

	N := int(r.index.GetDocCount())
	avgDL := r.averageDocLength()

	totalScore := 0.0
	for _, term := range terms {
		term = strings.ToLower(term)
		df := r.index.GetTermCount(term)
		tf := r.termFrequencyInDoc(docID, term)

		score := BM25(meta.TokenCount, avgDL, tf, df, N, r.params)
		totalScore += score
	}

	return totalScore
}

// ScoreDocuments calculates scores for multiple documents.
func (r *Ranker) ScoreDocuments(docIDs []int64, terms []string) map[int64]float64 {
	scores := make(map[int64]float64)
	for _, docID := range docIDs {
		scores[docID] = r.ScoreDocument(docID, terms)
	}
	return scores
}

// termFrequencyInDoc counts how many times a term appears in a document.
func (r *Ranker) termFrequencyInDoc(docID int64, term string) int {
	occurrences := r.index.QueryTerm(term)
	count := 0
	for _, occ := range occurrences {
		if occ.DocID == docID {
			count++
		}
	}
	return count
}

// averageDocLength calculates the average document length.
func (r *Ranker) averageDocLength() float64 {
	total := 0
	count := 0

	// This could be optimized by storing running totals
	for _, meta := range r.index.docs {
		total += meta.TokenCount
		count++
	}

	if count == 0 {
		return 1
	}

	return float64(total) / float64(count)
}

// Highlighter provides text highlighting functionality.
type Highlighter struct {
	index       *FTS5Index
	openTag     string
	closeTag    string
	tokenizer   Tokenizer
}

// NewHighlighter creates a new highlighter.
func NewHighlighter(index *FTS5Index, tokenizer Tokenizer) *Highlighter {
	return &Highlighter{
		index:     index,
		openTag:   "<b>",
		closeTag:  "</b>",
		tokenizer: tokenizer,
	}
}

// SetTags sets the highlight tags.
func (h *Highlighter) SetTags(open, close string) {
	h.openTag = open
	h.closeTag = close
}

// Highlight returns text with matched terms highlighted.
func (h *Highlighter) Highlight(text string, terms []string) string {
	// Create a map of terms to highlight (lowercase for case-insensitive)
	highlightTerms := make(map[string]bool)
	for _, term := range terms {
		highlightTerms[strings.ToLower(term)] = true
	}

	// Tokenize and rebuild with highlights
	tokens := h.tokenizer.Tokenize(text)
	var result strings.Builder
	lastEnd := 0

	for _, tok := range tokens {
		if highlightTerms[tok.Term] {
			// Add text before this token
			result.WriteString(text[lastEnd:tok.Start])
			// Add highlighted term
			result.WriteString(h.openTag)
			result.WriteString(text[tok.Start:tok.End])
			result.WriteString(h.closeTag)
			lastEnd = tok.End
		}
	}

	// Add remaining text
	result.WriteString(text[lastEnd:])
	return result.String()
}

// SnippetExtractor extracts relevant snippets from text.
type SnippetExtractor struct {
	index      *FTS5Index
	tokenizer  Tokenizer
	windowSize int
	ellipsis   string
}

// NewSnippetExtractor creates a new snippet extractor.
func NewSnippetExtractor(index *FTS5Index, tokenizer Tokenizer) *SnippetExtractor {
	return &SnippetExtractor{
		index:      index,
		tokenizer:  tokenizer,
		windowSize: 50, // words around match
		ellipsis:   "...",
	}
}

// SetWindowSize sets the snippet window size.
func (s *SnippetExtractor) SetWindowSize(size int) {
	s.windowSize = size
}

// SetEllipsis sets the ellipsis string.
func (s *SnippetExtractor) SetEllipsis(e string) {
	s.ellipsis = e
}

// Snippet returns a snippet of text containing the first match.
func (s *SnippetExtractor) Snippet(text string, terms []string) string {
	tokens := s.tokenizer.Tokenize(text)

	// Find first matching token
	matchTerms := make(map[string]bool)
	for _, term := range terms {
		matchTerms[strings.ToLower(term)] = true
	}

	matchPos := -1
	for i, tok := range tokens {
		if matchTerms[tok.Term] {
			matchPos = i
			break
		}
	}

	if matchPos == -1 {
		// No match, return beginning of text
		if len(tokens) > s.windowSize {
			return s.joinTokens(tokens[:s.windowSize]) + s.ellipsis
		}
		return s.joinTokens(tokens)
	}

	// Calculate window around match
	start := matchPos - s.windowSize/2
	end := matchPos + s.windowSize/2

	if start < 0 {
		start = 0
	}
	if end > len(tokens) {
		end = len(tokens)
	}

	var result strings.Builder
	if start > 0 {
		result.WriteString(s.ellipsis)
	}
	result.WriteString(s.joinTokens(tokens[start:end]))
	if end < len(tokens) {
		result.WriteString(s.ellipsis)
	}

	return result.String()
}

// joinTokens joins tokens back into text.
func (s *SnippetExtractor) joinTokens(tokens []Token) string {
	if len(tokens) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(tokens[0].Term)
	for i := 1; i < len(tokens); i++ {
		result.WriteString(" ")
		result.WriteString(tokens[i].Term)
	}
	return result.String()
}
