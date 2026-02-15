package QP

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenInvalid TokenType = iota
	TokenEOF
	TokenIdentifier
	TokenString
	TokenNumber
	TokenKeyword
	TokenOperator
	TokenLeftParen
	TokenRightParen
	TokenComma
	TokenSemicolon
	TokenDot
	TokenAsterisk
	TokenEq
	TokenNe
	TokenLt
	TokenLe
	TokenGt
	TokenGe
	TokenPlus
	TokenMinus
	TokenSlash
	TokenPercent
	TokenConcat
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenLike
	TokenBetween
	TokenIs
	TokenIsNot
)

var keywords = map[string]TokenType{
	"SELECT":        TokenKeyword,
	"FROM":          TokenKeyword,
	"WHERE":         TokenKeyword,
	"AND":           TokenAnd,
	"OR":            TokenOr,
	"NOT":           TokenNot,
	"IN":            TokenIn,
	"LIKE":          TokenLike,
	"BETWEEN":       TokenBetween,
	"IS":            TokenIs,
	"NULL":          TokenKeyword,
	"TRUE":          TokenKeyword,
	"FALSE":         TokenKeyword,
	"INSERT":        TokenKeyword,
	"INTO":          TokenKeyword,
	"VALUES":        TokenKeyword,
	"UPDATE":        TokenKeyword,
	"SET":           TokenKeyword,
	"DELETE":        TokenKeyword,
	"CREATE":        TokenKeyword,
	"TABLE":         TokenKeyword,
	"INDEX":         TokenKeyword,
	"DROP":          TokenKeyword,
	"ALTER":         TokenKeyword,
	"ADD":           TokenKeyword,
	"COLUMN":        TokenKeyword,
	"PRIMARY":       TokenKeyword,
	"KEY":           TokenKeyword,
	"UNIQUE":        TokenKeyword,
	"DEFAULT":       TokenKeyword,
	"CHECK":         TokenKeyword,
	"CONSTRAINT":    TokenKeyword,
	"FOREIGN":       TokenKeyword,
	"REFERENCES":    TokenKeyword,
	"JOIN":          TokenKeyword,
	"INNER":         TokenKeyword,
	"LEFT":          TokenKeyword,
	"RIGHT":         TokenKeyword,
	"OUTER":         TokenKeyword,
	"CROSS":         TokenKeyword,
	"ON":            TokenKeyword,
	"AS":            TokenKeyword,
	"ORDER":         TokenKeyword,
	"BY":            TokenKeyword,
	"GROUP":         TokenKeyword,
	"HAVING":        TokenKeyword,
	"LIMIT":         TokenKeyword,
	"OFFSET":        TokenKeyword,
	"ASC":           TokenKeyword,
	"DESC":          TokenKeyword,
	"UNION":         TokenKeyword,
	"ALL":           TokenKeyword,
	"EXCEPT":        TokenKeyword,
	"INTERSECT":     TokenKeyword,
	"CASE":          TokenKeyword,
	"WHEN":          TokenKeyword,
	"THEN":          TokenKeyword,
	"ELSE":          TokenKeyword,
	"END":           TokenKeyword,
	"EXISTS":        TokenKeyword,
	"CAST":          TokenKeyword,
	"COUNT":         TokenKeyword,
	"SUM":           TokenKeyword,
	"AVG":           TokenKeyword,
	"MIN":           TokenKeyword,
	"MAX":           TokenKeyword,
	"COALESCE":      TokenKeyword,
	"IFNULL":        TokenKeyword,
	"DISTINCT":      TokenKeyword,
	"INTEGER":       TokenKeyword,
	"TEXT":          TokenKeyword,
	"REAL":          TokenKeyword,
	"BLOB":          TokenKeyword,
	"VARCHAR":       TokenKeyword,
	"BOOLEAN":       TokenKeyword,
	"BEGIN":         TokenKeyword,
	"COMMIT":        TokenKeyword,
	"ROLLBACK":      TokenKeyword,
	"TRANSACTION":   TokenKeyword,
	"PRAGMA":        TokenKeyword,
	"IF":            TokenKeyword,
	"AUTOINCREMENT": TokenKeyword,
	"ROWID":         TokenKeyword,
}

type Token struct {
	Type     TokenType
	Literal  string
	Location int
}

type Tokenizer struct {
	input  string
	pos    int
	start  int
	tokens []Token
}

func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{
		input:  input,
		pos:    0,
		start:  0,
		tokens: make([]Token, 0),
	}
}

func (t *Tokenizer) Tokenize() ([]Token, error) {
	for {
		t.skipWhitespace()
		if t.pos >= len(t.input) {
			t.addToken(TokenEOF, "")
			break
		}

		ch := t.input[t.pos]

		if unicode.IsLetter(rune(ch)) || ch == '_' {
			if err := t.readIdentifier(); err != nil {
				return nil, err
			}
		} else if unicode.IsDigit(rune(ch)) {
			if err := t.readNumber(); err != nil {
				return nil, err
			}
		} else if ch == '"' || ch == '\'' {
			if err := t.readString(); err != nil {
				return nil, err
			}
		} else {
			if err := t.readOperator(); err != nil {
				return nil, err
			}
		}
	}
	return t.tokens, nil
}

func (t *Tokenizer) skipWhitespace() {
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			t.pos++
		} else if ch == '-' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '-' {
			for t.pos < len(t.input) && t.input[t.pos] != '\n' {
				t.pos++
			}
		} else if ch == '/' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '/' {
			for t.pos < len(t.input) && t.input[t.pos] != '\n' {
				t.pos++
			}
		} else if ch == '/' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '*' {
			t.pos += 2
			for t.pos+1 < len(t.input) {
				if t.input[t.pos] == '*' && t.input[t.pos+1] == '/' {
					t.pos += 2
					break
				}
				t.pos++
			}
		} else {
			break
		}
	}
}

func (t *Tokenizer) readIdentifier() error {
	t.start = t.pos
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			t.pos++
		} else {
			break
		}
	}

	literal := t.input[t.start:t.pos]
	upper := strings.ToUpper(literal)

	if tokenType, ok := keywords[upper]; ok {
		t.addToken(tokenType, literal)
	} else {
		t.addToken(TokenIdentifier, literal)
	}
	return nil
}

func (t *Tokenizer) readNumber() error {
	t.start = t.pos
	hasDot := false

	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if unicode.IsDigit(rune(ch)) {
			t.pos++
		} else if ch == '.' && !hasDot {
			hasDot = true
			t.pos++
		} else if ch == 'e' || ch == 'E' {
			t.pos++
			if t.pos < len(t.input) && (t.input[t.pos] == '+' || t.input[t.pos] == '-') {
				t.pos++
			}
		} else {
			break
		}
	}

	t.addToken(TokenNumber, t.input[t.start:t.pos])
	return nil
}

func (t *Tokenizer) readString() error {
	quote := t.input[t.pos]
	t.pos++

	t.start = t.pos
	for t.pos < len(t.input) {
		if t.input[t.pos] == quote {
			break
		}
		if t.input[t.pos] == '\\' && t.pos+1 < len(t.input) {
			t.pos += 2
		} else {
			t.pos++
		}
	}

	if t.pos >= len(t.input) {
		return fmt.Errorf("unterminated string at position %d", t.start)
	}

	literal := t.input[t.start:t.pos]
	t.pos++
	t.addToken(TokenString, literal)
	return nil
}

func (t *Tokenizer) readOperator() error {
	ch := t.input[t.pos]
	t.pos++

	switch ch {
	case '(':
		t.addToken(TokenLeftParen, "(")
	case ')':
		t.addToken(TokenRightParen, ")")
	case ',':
		t.addToken(TokenComma, ",")
	case ';':
		t.addToken(TokenSemicolon, ";")
	case '.':
		t.addToken(TokenDot, ".")
	case '*':
		t.addToken(TokenAsterisk, "*")
	case '=':
		t.addToken(TokenEq, "=")
	case '<':
		if t.pos < len(t.input) {
			next := t.input[t.pos]
			if next == '=' {
				t.pos++
				t.addToken(TokenLe, "<=")
			} else if next == '>' {
				t.pos++
				t.addToken(TokenNe, "<>")
			} else if next == '<' {
				t.pos++
				t.addToken(TokenConcat, "<<")
			} else {
				t.addToken(TokenLt, "<")
			}
		} else {
			t.addToken(TokenLt, "<")
		}
	case '>':
		if t.pos < len(t.input) && t.input[t.pos] == '=' {
			t.pos++
			t.addToken(TokenGe, ">=")
		} else {
			t.addToken(TokenGt, ">")
		}
	case '!':
		if t.pos < len(t.input) && t.input[t.pos] == '=' {
			t.pos++
			t.addToken(TokenNe, "!=")
		} else {
			return fmt.Errorf("invalid operator '!' at position %d", t.pos)
		}
	case '+':
		t.addToken(TokenPlus, "+")
	case '-':
		t.addToken(TokenMinus, "-")
	case '/':
		t.addToken(TokenSlash, "/")
	case '%':
		t.addToken(TokenPercent, "%")
	case '|':
		if t.pos < len(t.input) && t.input[t.pos] == '|' {
			t.pos++
			t.addToken(TokenConcat, "||")
		} else {
			return fmt.Errorf("invalid operator '|' at position %d", t.pos)
		}
	default:
		return fmt.Errorf("invalid character '%c' at position %d", ch, t.pos)
	}
	return nil
}

func (t *Tokenizer) addToken(tokenType TokenType, literal string) {
	t.tokens = append(t.tokens, Token{
		Type:     tokenType,
		Literal:  literal,
		Location: t.start,
	})
}

func (t *Tokenizer) String() string {
	var sb strings.Builder
	for i, tok := range t.tokens {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("{Type: %d, Literal: %q}", tok.Type, tok.Literal))
	}
	return sb.String()
}
