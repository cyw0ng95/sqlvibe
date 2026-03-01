package fts5

import (
	"strings"
	"unicode"
)

// MatchOp represents the type of match operation.
type MatchOp int

const (
	MatchTerm MatchOp = iota
	MatchAnd
	MatchOr
	MatchNot
	MatchPhrase
	MatchPrefix
)

// MatchExpr represents a parsed MATCH expression.
type MatchExpr struct {
	Op       MatchOp
	Term     string
	Column   string // empty means all columns
	Children []*MatchExpr
}

// QueryParser parses MATCH expressions.
type QueryParser struct {
	input     string
	pos       int
	tokenizer Tokenizer
}

// NewQueryParser creates a new query parser.
func NewQueryParser(tokenizer Tokenizer) *QueryParser {
	return &QueryParser{
		tokenizer: tokenizer,
	}
}

// Parse parses a MATCH expression string.
func (p *QueryParser) Parse(query string) (*MatchExpr, error) {
	p.input = query
	p.pos = 0
	return p.parseOr()
}

// parseOr parses OR expressions.
func (p *QueryParser) parseOr() (*MatchExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.skipSpace() {
		if p.peekMatch("OR") {
			p.pos += 2
			right, err := p.parseAnd()
			if err != nil {
				return nil, err
			}
			left = &MatchExpr{
				Op:       MatchOr,
				Children: []*MatchExpr{left, right},
			}
		} else {
			break
		}
	}

	return left, nil
}

// parseAnd parses AND expressions.
func (p *QueryParser) parseAnd() (*MatchExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.skipSpace() {
		// Check for AND operator or implicit AND (space between terms)
		if p.peekMatch("AND") {
			p.pos += 3
			right, err := p.parseNot()
			if err != nil {
				return nil, err
			}
			left = &MatchExpr{
				Op:       MatchAnd,
				Children: []*MatchExpr{left, right},
			}
		} else if p.skipSpace() && !p.isAtEnd() && p.current() != ')' && p.current() != '"' {
			// Implicit AND between terms
			right, err := p.parseNot()
			if err != nil {
				return nil, err
			}
			left = &MatchExpr{
				Op:       MatchAnd,
				Children: []*MatchExpr{left, right},
			}
		} else {
			break
		}
	}

	return left, nil
}

// parseNot parses NOT expressions.
func (p *QueryParser) parseNot() (*MatchExpr, error) {
	p.skipSpace()

	if p.peekMatch("NOT") {
		p.pos += 3
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &MatchExpr{
			Op:       MatchNot,
			Children: []*MatchExpr{expr},
		}, nil
	}

	return p.parsePrimary()
}

// parsePrimary parses primary expressions (terms, phrases, prefixes).
func (p *QueryParser) parsePrimary() (*MatchExpr, error) {
	p.skipSpace()

	if p.isAtEnd() {
		return nil, nil
	}

	// Check for column filter: column:term
	if col := p.parseColumnFilter(); col != "" {
		p.skipSpace()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if expr != nil {
			expr.Column = col
		}
		return expr, nil
	}

	// Check for phrase
	if p.current() == '"' {
		return p.parsePhrase()
	}

	// Parse term
	return p.parseTerm()
}

// parseColumnFilter checks for column: prefix.
func (p *QueryParser) parseColumnFilter() string {
	start := p.pos
	for p.pos < len(p.input) && unicode.IsLetter(rune(p.input[p.pos])) {
		p.pos++
	}

	if p.pos > start && p.pos < len(p.input) && p.input[p.pos] == ':' {
		col := p.input[start:p.pos]
		p.pos++ // skip ':'
		return col
	}

	p.pos = start
	return ""
}

// parsePhrase parses quoted phrases.
func (p *QueryParser) parsePhrase() (*MatchExpr, error) {
	if p.current() != '"' {
		return nil, nil
	}
	p.pos++ // skip opening quote

	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] != '"' {
		p.pos++
	}

	if p.pos >= len(p.input) {
		return nil, nil // unclosed quote
	}

	phrase := p.input[start:p.pos]
	p.pos++ // skip closing quote

	// Tokenize the phrase
	tokens := p.tokenizer.Tokenize(phrase)
	terms := make([]string, len(tokens))
	for i, tok := range tokens {
		terms[i] = tok.Term
	}

	return &MatchExpr{
		Op:    MatchPhrase,
		Term:  terms[0], // primary term for indexing
		Children: nil,
	}, nil
}

// parseTerm parses a single term.
func (p *QueryParser) parseTerm() (*MatchExpr, error) {
	start := p.pos

	for p.pos < len(p.input) {
		r := rune(p.input[p.pos])
		// Term can contain letters, numbers, and * for prefix
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '*' || r == '_' {
			p.pos++
		} else {
			break
		}
	}

	if p.pos == start {
		return nil, nil
	}

	term := p.input[start:p.pos]

	// Check for prefix search (term*)
	if strings.HasSuffix(term, "*") {
		return &MatchExpr{
			Op:   MatchPrefix,
			Term: strings.TrimSuffix(term, "*"),
		}, nil
	}

	return &MatchExpr{
		Op:   MatchTerm,
		Term: term,
	}, nil
}

// Helper methods

func (p *QueryParser) skipSpace() bool {
	start := p.pos
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
	return p.pos > start
}

func (p *QueryParser) peekMatch(s string) bool {
	if p.pos+len(s) > len(p.input) {
		return false
	}
	return strings.ToUpper(p.input[p.pos:p.pos+len(s)]) == s
}

func (p *QueryParser) current() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *QueryParser) isAtEnd() bool {
	return p.pos >= len(p.input)
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	DocIDs    []int64
	Scores    map[int64]float64 // docID -> BM25 score
	Highlights map[int64][]string // docID -> highlighted terms
}

// ExecuteQuery executes a match expression against the index.
func ExecuteQuery(idx *FTS5Index, expr *MatchExpr) *QueryResult {
	result := &QueryResult{
		Scores: make(map[int64]float64),
	}

	if expr == nil {
		return result
	}

	docIDs := executeMatchExpr(idx, expr)
	result.DocIDs = docIDs

	return result
}

// executeMatchExpr executes a match expression and returns matching doc IDs.
func executeMatchExpr(idx *FTS5Index, expr *MatchExpr) []int64 {
	switch expr.Op {
	case MatchTerm:
		occurrences := idx.QueryTerm(expr.Term)
		return uniqueDocIDs(occurrences)

	case MatchPrefix:
		occurrences := idx.QueryPrefix(expr.Term)
		return uniqueDocIDs(occurrences)

	case MatchPhrase:
		// For phrase, we need to get all terms in the phrase
		// This is simplified - full implementation would tokenize the phrase
		occurrences := idx.QueryTerm(expr.Term)
		return uniqueDocIDs(occurrences)

	case MatchAnd:
		if len(expr.Children) < 2 {
			return nil
		}
		left := executeMatchExpr(idx, expr.Children[0])
		right := executeMatchExpr(idx, expr.Children[1])
		return intersect(left, right)

	case MatchOr:
		if len(expr.Children) < 2 {
			return nil
		}
		left := executeMatchExpr(idx, expr.Children[0])
		right := executeMatchExpr(idx, expr.Children[1])
		return union(left, right)

	case MatchNot:
		if len(expr.Children) < 1 {
			return nil
		}
		// NOT requires context of all docs - simplified here
		return nil

	default:
		return nil
	}
}

// uniqueDocIDs returns unique document IDs from occurrences.
func uniqueDocIDs(occurrences []DocInfo) []int64 {
	seen := make(map[int64]bool)
	var result []int64

	for _, occ := range occurrences {
		if !seen[occ.DocID] {
			seen[occ.DocID] = true
			result = append(result, occ.DocID)
		}
	}

	return result
}

// intersect returns the intersection of two doc ID slices.
func intersect(a, b []int64) []int64 {
	setA := make(map[int64]bool)
	for _, id := range a {
		setA[id] = true
	}

	var result []int64
	for _, id := range b {
		if setA[id] {
			result = append(result, id)
		}
	}

	return result
}

// union returns the union of two doc ID slices.
func union(a, b []int64) []int64 {
	seen := make(map[int64]bool)
	var result []int64

	for _, id := range a {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}

	for _, id := range b {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}

	return result
}
