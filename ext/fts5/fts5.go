package fts5

import (
	"fmt"
	"strings"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
)

// FTS5Module implements the FTS5 virtual table module.
type FTS5Module struct {
	DS.TableModule
}

// FTS5Table represents an FTS5 virtual table instance.
type FTS5Table struct {
	name      string
	columns   []string
	index     *FTS5Index
	tokenizer Tokenizer
	ranker    *Ranker
	docID     int64
}

// fts5Cursor is the cursor for iterating over FTS5 query results.
type fts5Cursor struct {
	table    *FTS5Table
	docIDs   []int64
	scores   map[int64]float64
	pos      int
	match    *MatchExpr
	terms    []string
	column   int // -1 for all columns
	rowID    int64
	eof      bool
}

// Create creates a new FTS5 table.
func (m *FTS5Module) Create(args []string) (DS.VTab, error) {
	return m.create(args)
}

// Connect connects to an existing FTS5 table.
func (m *FTS5Module) Connect(args []string) (DS.VTab, error) {
	return m.create(args)
}

func (m *FTS5Module) create(args []string) (*FTS5Table, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("fts5: requires at least one column name")
	}

	// Parse arguments: column names and optional tokenizer
	columns := make([]string, 0)
	tokenizerName := ""

	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "tokenize=") {
			tokenizerName = strings.TrimPrefix(arg, "tokenize=")
		} else if arg != "" {
			columns = append(columns, arg)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("fts5: no columns specified")
	}

	// Create tokenizer
	tokenizer := GetTokenizer(tokenizerName)

	// Create index
	index := NewFTS5Index(columns, tokenizer)

	return &FTS5Table{
		name:      "fts5",
		columns:   columns,
		index:     index,
		tokenizer: tokenizer,
		ranker:    NewRanker(index, DefaultBM25Params()),
		docID:     1,
	}, nil
}

// BestIndex handles query planning.
func (t *FTS5Table) BestIndex(info *DS.IndexInfo) error {
	// Check for MATCH constraint
	for i, constraint := range info.Constraints {
		if constraint.Usable && constraint.Op == '=' {
			info.IdxNum = i
			info.IdxStr = "match"
			info.EstimatedRows = 10
			info.EstimatedCost = 1.0
			return nil
		}
	}

	info.EstimatedRows = 100
	info.EstimatedCost = 10.0
	return nil
}

// Open creates a new cursor.
func (t *FTS5Table) Open() (DS.VTabCursor, error) {
	return &fts5Cursor{
		table:  t,
		scores: make(map[int64]float64),
		pos:    -1,
		column: -1,
	}, nil
}

// Columns returns the column names.
func (t *FTS5Table) Columns() []string {
	// Return content columns plus hidden rowid column
	result := make([]string, len(t.columns))
	copy(result, t.columns)
	return result
}

// Disconnect disconnects from the table.
func (t *FTS5Table) Disconnect() error {
	return nil
}

// Destroy destroys the table.
func (t *FTS5Table) Destroy() error {
	t.index.Clear()
	return nil
}

// Insert adds a document to the FTS5 table.
func (t *FTS5Table) Insert(values []string) int64 {
	docID := t.docID
	t.docID++
	t.index.Insert(docID, values)
	return docID
}

// Delete removes a document from the FTS5 table.
func (t *FTS5Table) Delete(docID int64, values []string) {
	t.index.Delete(docID, values)
}

// Filter initializes the cursor with a query.
func (c *fts5Cursor) Filter(idxNum int, idxStr string, args []interface{}) error {
	c.pos = 0
	c.docIDs = nil
	c.scores = make(map[int64]float64)

	// Parse MATCH expression from args
	if len(args) > 0 {
		if query, ok := args[0].(string); ok {
			parser := NewQueryParser(c.table.tokenizer)
			match, err := parser.Parse(query)
			if err != nil {
				return err
			}
			c.match = match

			// Execute query
			result := ExecuteQuery(c.table.index, match)
			c.docIDs = result.DocIDs

			// Calculate scores
			terms := c.extractTerms(match)
			c.scores = c.table.ranker.ScoreDocuments(c.docIDs, terms)
		}
	}

	// If no query, return all documents
	if c.docIDs == nil {
		c.docIDs = []int64{}
		// In a real implementation, we'd iterate all docs
	}

	c.eof = len(c.docIDs) == 0
	if !c.eof {
		c.rowID = c.docIDs[0]
	}

	return nil
}

// Next moves to the next row.
func (c *fts5Cursor) Next() error {
	c.pos++
	if c.pos >= len(c.docIDs) {
		c.eof = true
		c.rowID = 0
	} else {
		c.rowID = c.docIDs[c.pos]
	}
	return nil
}

// Column returns the value for a column.
func (c *fts5Cursor) Column(col int) (interface{}, error) {
	if c.eof || c.pos >= len(c.docIDs) {
		return nil, nil
	}

	docID := c.docIDs[c.pos]

	// For now, return the score for column 0 if there's a match query
	// In a full implementation, we'd store the actual document content
	if c.match != nil && col == 0 {
		return c.scores[docID], nil
	}

	// Return docID as a placeholder for content
	return docID, nil
}

// RowID returns the current row ID.
func (c *fts5Cursor) RowID() (int64, error) {
	return c.rowID, nil
}

// Eof returns whether the cursor is at end.
func (c *fts5Cursor) Eof() bool {
	return c.eof
}

// Close closes the cursor.
func (c *fts5Cursor) Close() error {
	return nil
}

// extractTerms extracts search terms from a match expression.
func (c *fts5Cursor) extractTerms(expr *MatchExpr) []string {
	if expr == nil {
		return nil
	}

	var terms []string
	c.collectTerms(expr, &terms)
	return terms
}

func (c *fts5Cursor) collectTerms(expr *MatchExpr, terms *[]string) {
	if expr == nil {
		return
	}

	switch expr.Op {
	case MatchTerm, MatchPrefix:
		*terms = append(*terms, expr.Term)
	case MatchPhrase:
		*terms = append(*terms, expr.Term)
	case MatchAnd, MatchOr:
		for _, child := range expr.Children {
			c.collectTerms(child, terms)
		}
	case MatchNot:
		if len(expr.Children) > 0 {
			c.collectTerms(expr.Children[0], terms)
		}
	}
}

// GetBM25 returns the BM25 score for the current row.
func (c *fts5Cursor) GetBM25() float64 {
	if c.eof || c.pos >= len(c.docIDs) {
		return 0
	}
	return c.scores[c.docIDs[c.pos]]
}

// GetHighlight returns highlighted text for a column.
func (c *fts5Cursor) GetHighlight(col int, text string) string {
	if c.match == nil {
		return text
	}

	highlighter := NewHighlighter(c.table.index, c.table.tokenizer)
	terms := c.extractTerms(c.match)
	return highlighter.Highlight(text, terms)
}

// GetSnippet returns a snippet for a column.
func (c *fts5Cursor) GetSnippet(col int, text string) string {
	if c.match == nil {
		if len(text) > 100 {
			return text[:100] + "..."
		}
		return text
	}

	extractor := NewSnippetExtractor(c.table.index, c.table.tokenizer)
	terms := c.extractTerms(c.match)
	return extractor.Snippet(text, terms)
}
