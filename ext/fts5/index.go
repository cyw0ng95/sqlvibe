package fts5

import (
	"strings"
	"sync"
)

// DocInfo represents a document's occurrence in the index.
type DocInfo struct {
	DocID    int64
	Column   int
	Position int
}

// DocMeta stores metadata about a document.
type DocMeta struct {
	TokenCount int
	Lengths    []int // token count per column
}

// FTS5Index is the inverted index for full-text search.
type FTS5Index struct {
	mu        sync.RWMutex
	terms     map[string][]DocInfo // term -> [doc occurrences]
	docs      map[int64]DocMeta    // docID -> metadata
	docCount  int64
	tokenizer Tokenizer
	columns   []string
}

// NewFTS5Index creates a new FTS5 index.
func NewFTS5Index(columns []string, tokenizer Tokenizer) *FTS5Index {
	return &FTS5Index{
		terms:     make(map[string][]DocInfo),
		docs:      make(map[int64]DocMeta),
		tokenizer: tokenizer,
		columns:   columns,
	}
}

// Insert adds a document to the index.
func (idx *FTS5Index) Insert(docID int64, values []string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	meta := DocMeta{
		Lengths: make([]int, len(idx.columns)),
	}

	for col, text := range values {
		tokens := idx.tokenizer.Tokenize(text)
		meta.Lengths[col] += len(tokens)
		meta.TokenCount += len(tokens)

		for _, tok := range tokens {
			idx.terms[tok.Term] = append(idx.terms[tok.Term], DocInfo{
				DocID:    docID,
				Column:   col,
				Position: tok.Position,
			})
		}
	}

	idx.docs[docID] = meta
	idx.docCount++
}

// Delete removes a document from the index.
func (idx *FTS5Index) Delete(docID int64, values []string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for col, text := range values {
		tokens := idx.tokenizer.Tokenize(text)
		for _, tok := range tokens {
			idx.removeTermOccurrence(tok.Term, docID, col, tok.Position)
		}
	}

	delete(idx.docs, docID)
	if idx.docCount > 0 {
		idx.docCount--
	}
}

// removeTermOccurrence removes a specific occurrence from the term list.
func (idx *FTS5Index) removeTermOccurrence(term string, docID int64, col int, pos int) {
	occurrences, ok := idx.terms[term]
	if !ok {
		return
	}

	for i, occ := range occurrences {
		if occ.DocID == docID && occ.Column == col && occ.Position == pos {
			// Remove this occurrence
			idx.terms[term] = append(occurrences[:i], occurrences[i+1:]...)
			// Clean up empty term lists
			if len(idx.terms[term]) == 0 {
				delete(idx.terms, term)
			}
			return
		}
	}
}

// QueryTerm finds all documents containing a term.
func (idx *FTS5Index) QueryTerm(term string) []DocInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Lowercase for case-insensitive search
	term = strings.ToLower(term)
	return idx.terms[term]
}

// QueryPrefix finds all documents with terms starting with prefix.
func (idx *FTS5Index) QueryPrefix(prefix string) []DocInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix = strings.ToLower(prefix)
	var results []DocInfo

	for term, occurrences := range idx.terms {
		if strings.HasPrefix(term, prefix) {
			results = append(results, occurrences...)
		}
	}

	return results
}

// QueryPhrase finds documents containing the exact phrase.
func (idx *FTS5Index) QueryPhrase(terms []string) []DocInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(terms) == 0 {
		return nil
	}

	// Get occurrences for first term
	firstTerm := strings.ToLower(terms[0])
	firstOccs, ok := idx.terms[firstTerm]
	if !ok {
		return nil
	}

	// For each occurrence of first term, check if subsequent terms follow
	var results []DocInfo
	for _, firstOcc := range firstOccs {
		if idx.matchPhrase(firstOcc, terms[1:]) {
			results = append(results, firstOcc)
		}
	}

	return results
}

// matchPhrase checks if the phrase follows the given occurrence.
func (idx *FTS5Index) matchPhrase(first DocInfo, remainingTerms []string) bool {
	for i, term := range remainingTerms {
		term = strings.ToLower(term)
		occurrences := idx.terms[term]
		found := false

		for _, occ := range occurrences {
			if occ.DocID == first.DocID &&
				occ.Column == first.Column &&
				occ.Position == first.Position+i+1 {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// GetDocMeta returns metadata for a document.
func (idx *FTS5Index) GetDocMeta(docID int64) (DocMeta, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	meta, ok := idx.docs[docID]
	return meta, ok
}

// GetDocCount returns the total number of documents.
func (idx *FTS5Index) GetDocCount() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.docCount
}

// GetTermCount returns the number of documents containing a term.
func (idx *FTS5Index) GetTermCount(term string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	term = strings.ToLower(term)
	occurrences := idx.terms[term]

	// Count unique documents
	seen := make(map[int64]bool)
	for _, occ := range occurrences {
		seen[occ.DocID] = true
	}

	return len(seen)
}

// GetColumns returns the column names.
func (idx *FTS5Index) GetColumns() []string {
	return idx.columns
}

// Clear removes all documents from the index.
func (idx *FTS5Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.terms = make(map[string][]DocInfo)
	idx.docs = make(map[int64]DocMeta)
	idx.docCount = 0
}
