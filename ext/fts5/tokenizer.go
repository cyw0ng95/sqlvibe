package fts5

import (
	"strings"
	"unicode"
)

// Token represents a tokenized term with position information.
type Token struct {
	Term     string
	Start    int
	End      int
	Position int
}

// Tokenizer defines the interface for tokenizing text.
type Tokenizer interface {
	Tokenize(text string) []Token
}

// ASCIITokenizer implements simple ASCII tokenization.
// - Splits by whitespace and punctuation
// - Lowercases all terms
type ASCIITokenizer struct{}

func NewASCIITokenizer() *ASCIITokenizer {
	return &ASCIITokenizer{}
}

func (t *ASCIITokenizer) Tokenize(text string) []Token {
	var tokens []Token
	var current strings.Builder
	start := -1
	position := 0

	for i, r := range text {
		// ASCII: only keep a-z, A-Z, 0-9
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			if start == -1 {
				start = i
			}
			// Lowercase
			if r >= 'A' && r <= 'Z' {
				r = r + 'a' - 'A'
			}
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, Token{
					Term:     current.String(),
					Start:    start,
					End:      i,
					Position: position,
				})
				position++
				current.Reset()
				start = -1
			}
		}
	}

	// Don't forget the last token
	if current.Len() > 0 {
		tokens = append(tokens, Token{
			Term:     current.String(),
			Start:    start,
			End:      len(text),
			Position: position,
		})
	}

	return tokens
}

// PorterTokenizer implements Porter stemming algorithm.
// Reduces words to their root form.
type PorterTokenizer struct {
	ascii *ASCIITokenizer
}

func NewPorterTokenizer() *PorterTokenizer {
	return &PorterTokenizer{
		ascii: NewASCIITokenizer(),
	}
}

func (t *PorterTokenizer) Tokenize(text string) []Token {
	rawTokens := t.ascii.Tokenize(text)
	tokens := make([]Token, len(rawTokens))
	for i, tok := range rawTokens {
		tokens[i] = Token{
			Term:     stem(tok.Term),
			Start:    tok.Start,
			End:      tok.End,
			Position: tok.Position,
		}
	}
	return tokens
}

// Unicode61Tokenizer implements Unicode text category classification.
// - Classifies characters by Unicode category
// - Tokenizes by letter/number boundaries
// - Handles multi-language text
type Unicode61Tokenizer struct{}

func NewUnicode61Tokenizer() *Unicode61Tokenizer {
	return &Unicode61Tokenizer{}
}

func (t *Unicode61Tokenizer) Tokenize(text string) []Token {
	var tokens []Token
	var current strings.Builder
	start := -1
	position := 0

	for i, r := range text {
		// Unicode61: token characters are letters and numbers
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if start == -1 {
				start = i
			}
			// Fold to lowercase
			current.WriteRune(unicode.ToLower(r))
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, Token{
					Term:     current.String(),
					Start:    start,
					End:      i,
					Position: position,
				})
				position++
				current.Reset()
				start = -1
			}
		}
	}

	// Don't forget the last token
	if current.Len() > 0 {
		tokens = append(tokens, Token{
			Term:     current.String(),
			Start:    start,
			End:      len(text),
			Position: position,
		})
	}

	return tokens
}

// Porter Stemming Algorithm Implementation
// Based on the original Porter stemmer

func stem(word string) string {
	if len(word) <= 2 {
		return word
	}

	// Step 1a
	word = step1a(word)

	// Step 1b
	word = step1b(word)

	// Step 1c
	word = step1c(word)

	// Step 2
	word = step2(word)

	// Step 3
	word = step3(word)

	// Step 4
	word = step4(word)

	// Step 5a
	word = step5a(word)

	// Step 5b
	word = step5b(word)

	return word
}

func step1a(word string) string {
	if strings.HasSuffix(word, "sses") {
		return word[:len(word)-2]
	}
	if strings.HasSuffix(word, "ies") {
		return word[:len(word)-2]
	}
	if strings.HasSuffix(word, "ss") {
		return word
	}
	if strings.HasSuffix(word, "s") {
		return word[:len(word)-1]
	}
	return word
}

func step1b(word string) string {
	if strings.HasSuffix(word, "eed") {
		if measure(word[:len(word)-3]) > 0 {
			return word[:len(word)-1]
		}
		return word
	}

	if strings.HasSuffix(word, "ed") {
		if measure(word[:len(word)-2]) > 0 {
			return step1b2(word[:len(word)-2])
		}
		return word
	}

	if strings.HasSuffix(word, "ing") {
		if measure(word[:len(word)-3]) > 0 {
			return step1b2(word[:len(word)-3])
		}
		return word
	}

	return word
}

func step1b2(word string) string {
	if strings.HasSuffix(word, "at") {
		return word + "e"
	}
	if strings.HasSuffix(word, "bl") {
		return word + "e"
	}
	if strings.HasSuffix(word, "iz") {
		return word + "e"
	}
	if len(word) >= 2 {
		last := word[len(word)-1]
		if last == 'l' || last == 's' || last == 'z' {
			return word
		}
		if len(word) >= 2 {
			lastTwo := word[len(word)-2:]
			if isConsonant(lastTwo[0]) && !isConsonant(lastTwo[1]) && isConsonant(last) {
				if last != 'l' && last != 's' && last != 'z' {
					return word + string(last)
				}
			}
		}
	}
	return word
}

func step1c(word string) string {
	if len(word) > 0 && word[len(word)-1] == 'y' {
		if measure(word[:len(word)-1]) > 0 {
			return word[:len(word)-1] + "i"
		}
	}
	return word
}

func step2(word string) string {
	suffixes := []struct {
		suffix string
		repl   string
	}{
		{"ational", "ate"},
		{"tional", "tion"},
		{"enci", "ence"},
		{"anci", "ance"},
		{"izer", "ize"},
		{"abli", "able"},
		{"alli", "al"},
		{"entli", "ent"},
		{"eli", "e"},
		{"ousli", "ous"},
		{"ization", "ize"},
		{"ation", "ate"},
		{"ator", "ate"},
		{"alism", "al"},
		{"iveness", "ive"},
		{"fulness", "ful"},
		{"ousness", "ous"},
		{"aliti", "al"},
		{"iviti", "ive"},
		{"biliti", "ble"},
	}

	for _, s := range suffixes {
		if strings.HasSuffix(word, s.suffix) {
			if measure(word[:len(word)-len(s.suffix)]) > 0 {
				return word[:len(word)-len(s.suffix)] + s.repl
			}
		}
	}
	return word
}

func step3(word string) string {
	suffixes := []struct {
		suffix string
		repl   string
	}{
		{"icate", "ic"},
		{"ative", ""},
		{"alize", "al"},
		{"iciti", "ic"},
		{"ical", "ic"},
		{"ful", ""},
		{"ness", ""},
	}

	for _, s := range suffixes {
		if strings.HasSuffix(word, s.suffix) {
			if measure(word[:len(word)-len(s.suffix)]) > 0 {
				return word[:len(word)-len(s.suffix)] + s.repl
			}
		}
	}
	return word
}

func step4(word string) string {
	suffixes := []string{
		"al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement",
		"ment", "ent", "ou", "ism", "ate", "iti", "ous", "ive", "ize",
	}

	for _, suffix := range suffixes {
		if strings.HasSuffix(word, suffix) {
			if measure(word[:len(word)-len(suffix)]) > 1 {
				return word[:len(word)-len(suffix)]
			}
			return word
		}
	}

	if strings.HasSuffix(word, "ion") {
		base := word[:len(word)-3]
		if measure(base) > 1 {
			if len(base) > 0 && (base[len(base)-1] == 's' || base[len(base)-1] == 't') {
				return base
			}
		}
	}

	return word
}

func step5a(word string) string {
	if strings.HasSuffix(word, "e") {
		m := measure(word[:len(word)-1])
		if m > 1 {
			return word[:len(word)-1]
		}
		if m == 1 {
			if len(word) >= 2 {
				lastTwo := word[len(word)-2:]
				if !isConsonant(lastTwo[0]) && isConsonant(lastTwo[1]) {
					return word[:len(word)-1]
				}
			}
		}
	}
	return word
}

func step5b(word string) string {
	if len(word) > 1 && strings.HasSuffix(word, "ll") {
		if measure(word[:len(word)-1]) > 1 {
			return word[:len(word)-1]
		}
	}
	return word
}

func measure(word string) int {
	m := 0
	inConsonant := false

	for _, r := range word {
		isCons := isConsonantRune(r)
		if isCons && !inConsonant {
			inConsonant = true
		} else if !isCons && inConsonant {
			m++
			inConsonant = false
		}
	}

	return m
}

func isConsonant(r byte) bool {
	return r != 'a' && r != 'e' && r != 'i' && r != 'o' && r != 'u'
}

func isConsonantRune(r rune) bool {
	return r != 'a' && r != 'e' && r != 'i' && r != 'o' && r != 'u' &&
		r != 'A' && r != 'E' && r != 'I' && r != 'O' && r != 'U'
}

// GetTokenizer returns a tokenizer by name.
func GetTokenizer(name string) Tokenizer {
	switch name {
	case "porter":
		return NewPorterTokenizer()
	case "unicode61":
		return NewUnicode61Tokenizer()
	case "ascii", "simple", "":
		return NewASCIITokenizer()
	default:
		return NewASCIITokenizer()
	}
}
