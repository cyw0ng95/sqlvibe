# FTS5 Full-Text Search Extension

FTS5 (Full-Text Search 5) is a virtual table module that provides full-text search capabilities for sqlvibe.

## Features

- **Full-Text Search**: Search through text content efficiently using inverted indexes
- **Multiple Tokenizers**: ASCII, Porter (stemming), and Unicode61 tokenizers
- **MATCH Query Syntax**: Support for term, prefix, phrase, and boolean queries
- **BM25 Ranking**: Industry-standard relevance scoring
- **Highlighting**: Mark matched terms in results
- **Snippets**: Extract relevant excerpts from matching documents

## Usage

### Creating an FTS5 Table

```sql
-- Basic FTS5 table
CREATE VIRTUAL TABLE articles USING fts5(title, content);

-- With Porter stemmer
CREATE VIRTUAL TABLE docs USING fts5(title, body, tokenize=porter);

-- With Unicode61 tokenizer (multi-language support)
CREATE VIRTUAL TABLE multilang USING fts5(text, tokenize=unicode61);
```

### Inserting Documents

```sql
INSERT INTO articles (title, content) VALUES 
    ('Hello World', 'This is a test document about greetings'),
    ('Goodbye World', 'Another document about farewells'),
    ('Test Document', 'Testing the search functionality');
```

### Searching

```sql
-- Simple term search
SELECT * FROM articles WHERE articles MATCH 'hello';

-- Prefix search (terms starting with 'test')
SELECT * FROM articles WHERE articles MATCH 'test*';

-- Boolean AND
SELECT * FROM articles WHERE articles MATCH 'hello AND world';

-- Boolean OR
SELECT * FROM articles WHERE articles MATCH 'hello OR goodbye';

-- Phrase search (exact word order)
SELECT * FROM articles WHERE articles MATCH '"test document"';

-- Column-specific search
SELECT * FROM articles WHERE articles MATCH 'title:hello';
```

### Ranking Results

```sql
-- Search with BM25 ranking
SELECT *, bm25(articles) as score 
FROM articles 
WHERE articles MATCH 'test' 
ORDER BY score DESC;
```

### Highlighting Matches

```sql
-- Highlight matched terms
SELECT highlight(articles, content, '<mark>', '</mark>') 
FROM articles 
WHERE articles MATCH 'test';
```

### Getting Snippets

```sql
-- Get snippet with context around matches
SELECT snippet(articles, content, 50) 
FROM articles 
WHERE articles MATCH 'test';
```

## Tokenizers

### ASCII (default)
- Splits by whitespace and punctuation
- Lowercases all terms
- Only keeps ASCII letters and numbers

### Porter
- Applies Porter stemming algorithm
- Reduces words to root form (e.g., "running" → "run")
- Useful for matching word variations

### Unicode61
- Classifies characters by Unicode category
- Handles multi-language text
- Supports emoji and special characters

## MATCH Query Syntax

| Pattern | Description | Example |
|---------|-------------|---------|
| `term` | Exact term match | `hello` |
| `term*` | Prefix search | `test*` |
| `"phrase"` | Phrase search | `"hello world"` |
| `term1 AND term2` | Both terms required | `hello AND world` |
| `term1 OR term2` | Either term matches | `hello OR goodbye` |
| `NOT term` | Exclude term | `hello NOT world` |
| `column:term` | Search specific column | `title:hello` |

## BM25 Ranking

The BM25 algorithm calculates relevance scores based on:
- **Term Frequency (TF)**: How often the term appears in the document
- **Inverse Document Frequency (IDF)**: How rare the term is across all documents
- **Document Length**: Normalizes for document length

Default parameters:
- `k1 = 1.2` (term frequency saturation)
- `b = 0.75` (length normalization)

## API Reference

### Go API

```go
import "github.com/cyw0ng95/sqlvibe/ext/fts5"

// Create tokenizer
tokenizer := fts5.NewASCIITokenizer()
tokenizer := fts5.NewPorterTokenizer()
tokenizer := fts5.NewUnicode61Tokenizer()

// Create index
index := fts5.NewFTS5Index([]string{"title", "content"}, tokenizer)

// Insert document
index.Insert(docID, []string{"Title text", "Content text"})

// Query
parser := fts5.NewQueryParser(tokenizer)
expr, _ := parser.Parse("hello AND world")
result := fts5.ExecuteQuery(index, expr)

// Rank results
ranker := fts5.NewRanker(index, fts5.DefaultBM25Params())
scores := ranker.ScoreDocuments(result.DocIDs, []string{"hello"})

// Highlight
highlighter := fts5.NewHighlighter(index, tokenizer)
highlighted := highlighter.Highlight(text, []string{"hello"})

// Snippet
extractor := fts5.NewSnippetExtractor(index, tokenizer)
snippet := extractor.Snippet(text, []string{"hello"})
```

## Performance

- Index insertion: O(n) where n is the number of tokens
- Term query: O(1) average case (hash table lookup)
- Prefix query: O(m) where m is the number of terms
- Phrase query: O(p) where p is the phrase length

## Limitations

- In-memory index (not persisted to disk)
- Simplified Porter stemmer implementation
- Basic query parser (no nested parentheses)

## Future Enhancements

- Disk-based index persistence
- Full Porter stemming algorithm
- Advanced query syntax (parentheses, proximity)
- Index compression
- FTS3/FTS4 compatibility mode
