package QP

import (
	"fmt"
	"strings"
	"unicode"
)

// hexValTable is a lookup table for hex character values.
// Values > 15 indicate invalid hex characters.
var hexValTable [256]byte

func init() {
	// Mark all entries as invalid (255)
	for i := range hexValTable {
		hexValTable[i] = 255
	}
	for i := byte('0'); i <= '9'; i++ {
		hexValTable[i] = i - '0'
	}
	for i := byte('A'); i <= 'F'; i++ {
		hexValTable[i] = i - 'A' + 10
	}
	for i := byte('a'); i <= 'f'; i++ {
		hexValTable[i] = i - 'a' + 10
	}
}

type TokenType int

const (
	TokenInvalid TokenType = iota
	TokenEOF
	TokenIdentifier
	TokenString
	TokenHexString
	TokenNumber
	TokenKeyword
	TokenExplain
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
	TokenNotIn
	TokenLike
	TokenNotLike
	TokenGlob
	TokenBetween
	TokenNotBetween
	TokenIs
	TokenIsNot
	TokenExists
	TokenInSubquery
	TokenAll
	TokenAny
	TokenCast
)

var keywords = map[string]TokenType{
	"SELECT":            TokenKeyword,
	"FROM":              TokenKeyword,
	"WHERE":             TokenKeyword,
	"AND":               TokenAnd,
	"OR":                TokenOr,
	"NOT":               TokenNot,
	"IN":                TokenIn,
	"LIKE":              TokenLike,
	"GLOB":              TokenGlob,
	"BETWEEN":           TokenBetween,
	"IS":                TokenIs,
	"NULL":              TokenKeyword,
	"TRUE":              TokenKeyword,
	"FALSE":             TokenKeyword,
	"INSERT":            TokenKeyword,
	"INTO":              TokenKeyword,
	"VALUES":            TokenKeyword,
	"UPDATE":            TokenKeyword,
	"SET":               TokenKeyword,
	"DELETE":            TokenKeyword,
	"ON":                TokenKeyword,
	"CONFLICT":          TokenKeyword,
	"DO":                TokenKeyword,
	"NOTHING":           TokenKeyword,
	"CREATE":            TokenKeyword,
	"TABLE":             TokenKeyword,
	"INDEX":             TokenKeyword,
	"DROP":              TokenKeyword,
	"ALTER":             TokenKeyword,
	"ADD":               TokenKeyword,
	"COLUMN":            TokenKeyword,
	"PRIMARY":           TokenKeyword,
	"KEY":               TokenKeyword,
	"UNIQUE":            TokenKeyword,
	"DEFAULT":           TokenKeyword,
	"CHECK":             TokenKeyword,
	"CONSTRAINT":        TokenKeyword,
	"FOREIGN":           TokenKeyword,
	"REFERENCES":        TokenKeyword,
	"JOIN":              TokenKeyword,
	"INNER":             TokenKeyword,
	"LEFT":              TokenKeyword,
	"RIGHT":             TokenKeyword,
	"OUTER":             TokenKeyword,
	"CROSS":             TokenKeyword,
	"NATURAL":           TokenKeyword,
	"USING":             TokenKeyword,
	"AS":                TokenKeyword,
	"ORDER":             TokenKeyword,
	"BY":                TokenKeyword,
	"GROUP":             TokenKeyword,
	"HAVING":            TokenKeyword,
	"LIMIT":             TokenKeyword,
	"OFFSET":            TokenKeyword,
	"ASC":               TokenKeyword,
	"DESC":              TokenKeyword,
	"UNION":             TokenKeyword,
	"ALL":               TokenAll,
	"EXCEPT":            TokenKeyword,
	"INTERSECT":         TokenKeyword,
	"WITH":              TokenKeyword,
	"RECURSIVE":         TokenKeyword,
	"CASE":              TokenKeyword,
	"WHEN":              TokenKeyword,
	"THEN":              TokenKeyword,
	"ELSE":              TokenKeyword,
	"END":               TokenKeyword,
	"EXISTS":            TokenExists,
	"ANY":               TokenAny,
	"SOME":              TokenAny,
	"CAST":              TokenCast,
	"EXPLAIN":           TokenExplain,
	"COUNT":             TokenKeyword,
	"SUM":               TokenKeyword,
	"AVG":               TokenKeyword,
	"MIN":               TokenKeyword,
	"MAX":               TokenKeyword,
	"COALESCE":          TokenKeyword,
	"IFNULL":            TokenKeyword,
	"DISTINCT":          TokenKeyword,
	"INTEGER":           TokenKeyword,
	"TEXT":              TokenKeyword,
	"REAL":              TokenKeyword,
	"BLOB":              TokenKeyword,
	"VARCHAR":           TokenKeyword,
	"CHAR":              TokenKeyword,
	"CHARACTER":         TokenKeyword,
	"BOOLEAN":           TokenKeyword,
	"BEGIN":             TokenKeyword,
	"COMMIT":            TokenKeyword,
	"ROLLBACK":          TokenKeyword,
	"BACKUP":            TokenKeyword,
	"DATABASE":          TokenKeyword,
	"INCREMENTAL":       TokenKeyword,
	"TO":                TokenKeyword,
	"TRANSACTION":       TokenKeyword,
	"PRAGMA":            TokenKeyword,
	"IF":                TokenKeyword,
	"AUTOINCREMENT":     TokenKeyword,
	"ROWID":             TokenKeyword,
	"DATE":              TokenKeyword,
	"TIME":              TokenKeyword,
	"TIMESTAMP":         TokenKeyword,
	"DATETIME":          TokenKeyword,
	"CURRENT_DATE":      TokenKeyword,
	"CURRENT_TIME":      TokenKeyword,
	"CURRENT_TIMESTAMP": TokenKeyword,
	"LOCALTIME":         TokenKeyword,
	"LOCALTIMESTAMP":    TokenKeyword,
	"OVER":              TokenKeyword,
	"PARTITION":         TokenKeyword,
	"LAG":               TokenKeyword,
	"LEAD":              TokenKeyword,
	"FIRST_VALUE":       TokenKeyword,
	"LAST_VALUE":        TokenKeyword,
	"ROW_NUMBER":        TokenKeyword,
	"RANK":              TokenKeyword,
	"DENSE_RANK":        TokenKeyword,
	"NTILE":             TokenKeyword,
	"UNBOUNDED":         TokenKeyword,
	"PRECEDING":         TokenKeyword,
	"FOLLOWING":         TokenKeyword,
	"CURRENT":           TokenKeyword,
	"ROWS":              TokenKeyword,
	"RANGE":             TokenKeyword,
	"PERCENT_RANK":      TokenKeyword,
	"CUME_DIST":         TokenKeyword,
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
	// Pre-allocate token slice: estimate ~1 token per 8 chars, minimum 16
	estimated := len(input) / 8
	if estimated < 16 {
		estimated = 16
	}
	return &Tokenizer{
		input:  input,
		pos:    0,
		start:  0,
		tokens: make([]Token, 0, estimated),
	}
}

// lookupKeyword uses a switch on string length + content for O(1) keyword lookup
// without map hash computation on the common case. Falls back to the map for
// less-frequent long keywords.
func lookupKeyword(s string) (TokenType, bool) {
	switch len(s) {
	case 2:
		switch s {
		case "OR":
			return TokenOr, true
		case "IN":
			return TokenIn, true
		case "IS":
			return TokenIs, true
		case "AS":
			return TokenKeyword, true
		case "BY":
			return TokenKeyword, true
		case "ON":
			return TokenKeyword, true
		case "DO":
			return TokenKeyword, true
		case "IF":
			return TokenKeyword, true
		}
	case 3:
		switch s {
		case "NOT":
			return TokenNot, true
		case "AND":
			return TokenAnd, true
		case "ANY":
			return TokenAny, true
		case "ALL":
			return TokenAll, true
		case "SET":
			return TokenKeyword, true
		case "ADD":
			return TokenKeyword, true
		case "KEY":
			return TokenKeyword, true
		case "ASC":
			return TokenKeyword, true
		case "END":
			return TokenKeyword, true
		case "MIN":
			return TokenKeyword, true
		case "MAX":
			return TokenKeyword, true
		case "AVG":
			return TokenKeyword, true
		case "SUM":
			return TokenKeyword, true
		}
	case 4:
		switch s {
		case "LIKE":
			return TokenLike, true
		case "GLOB":
			return TokenGlob, true
		case "CAST":
			return TokenCast, true
		case "SOME":
			return TokenAny, true
		case "LEAD":
			return TokenKeyword, true
		case "RANK":
			return TokenKeyword, true
		case "NULL":
			return TokenKeyword, true
		case "TRUE":
			return TokenKeyword, true
		case "FROM":
			return TokenKeyword, true
		case "INTO":
			return TokenKeyword, true
		case "JOIN":
			return TokenKeyword, true
		case "LEFT":
			return TokenKeyword, true
		case "DESC":
			return TokenKeyword, true
		case "WITH":
			return TokenKeyword, true
		case "CASE":
			return TokenKeyword, true
		case "WHEN":
			return TokenKeyword, true
		case "THEN":
			return TokenKeyword, true
		case "ELSE":
			return TokenKeyword, true
		case "DROP":
			return TokenKeyword, true
		case "TEXT":
			return TokenKeyword, true
		case "REAL":
			return TokenKeyword, true
		case "BLOB":
			return TokenKeyword, true
		case "CHAR":
			return TokenKeyword, true
		case "DATE":
			return TokenKeyword, true
		case "TIME":
			return TokenKeyword, true
		case "OVER":
			return TokenKeyword, true
		case "ROWS":
			return TokenKeyword, true
		}
	case 5:
		switch s {
		case "NTILE":
			return TokenKeyword, true
		case "WHERE":
			return TokenKeyword, true
		case "ORDER":
			return TokenKeyword, true
		case "GROUP":
			return TokenKeyword, true
		case "LIMIT":
			return TokenKeyword, true
		case "INNER":
			return TokenKeyword, true
		case "OUTER":
			return TokenKeyword, true
		case "CROSS":
			return TokenKeyword, true
		case "UNION":
			return TokenKeyword, true
		case "USING":
			return TokenKeyword, true
		case "BEGIN":
			return TokenKeyword, true
		case "TABLE":
			return TokenKeyword, true
		case "INDEX":
			return TokenKeyword, true
		case "ROWID":
			return TokenKeyword, true
		case "FALSE":
			return TokenKeyword, true
		case "RANGE":
			return TokenKeyword, true
		}
	case 6:
		switch s {
		case "EXISTS":
			return TokenExists, true
		case "SELECT":
			return TokenKeyword, true
		case "INSERT":
			return TokenKeyword, true
		case "UPDATE":
			return TokenKeyword, true
		case "DELETE":
			return TokenKeyword, true
		case "CREATE":
			return TokenKeyword, true
		case "HAVING":
			return TokenKeyword, true
		case "OFFSET":
			return TokenKeyword, true
		case "UNIQUE":
			return TokenKeyword, true
		case "COLUMN":
			return TokenKeyword, true
		case "EXCEPT":
			return TokenKeyword, true
		case "COMMIT":
			return TokenKeyword, true
		case "PRAGMA":
			return TokenKeyword, true
		case "VALUES":
			return TokenKeyword, true
		}
	case 7:
		switch s {
		case "BETWEEN":
			return TokenBetween, true
		case "EXPLAIN":
			return TokenExplain, true
		case "NATURAL":
			return TokenKeyword, true
		case "PRIMARY":
			return TokenKeyword, true
		case "DEFAULT":
			return TokenKeyword, true
		case "FOREIGN":
			return TokenKeyword, true
		case "NOTHING":
			return TokenKeyword, true
		case "BOOLEAN":
			return TokenKeyword, true
		case "INTEGER":
			return TokenKeyword, true
		case "VARCHAR":
			return TokenKeyword, true
		case "CURRENT":
			return TokenKeyword, true
		case "LEADING":
			return TokenKeyword, true
		}
	}
	// Fallback to map for longer/less common keywords
	if tokenType, ok := keywords[s]; ok {
		return tokenType, true
	}
	return TokenIdentifier, false
}

func (t *Tokenizer) Tokenize() ([]Token, error) {
	for {
		t.skipWhitespace()
		if t.pos >= len(t.input) {
			t.addToken(TokenEOF, "")
			break
		}

		ch := t.input[t.pos]

		// Check for hex string (x'...') BEFORE checking for identifier
		if (ch == 'x' || ch == 'X') && t.pos+1 < len(t.input) && (t.input[t.pos+1] == '\'' || t.input[t.pos+1] == '"') {
			if err := t.readHexString(); err != nil {
				return nil, err
			}
		} else if unicode.IsLetter(rune(ch)) || ch == '_' {
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

	if tokenType, ok := lookupKeyword(upper); ok {
		// Store keywords in uppercase for consistent parser checks
		t.addToken(tokenType, upper)
	} else {
		// Unquoted identifiers are case-insensitive in SQLite (store as lowercase)
		t.addToken(TokenIdentifier, strings.ToLower(literal))
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
	var sb strings.Builder
	for t.pos < len(t.input) {
		if t.input[t.pos] == quote {
			// Check for doubled quote escape (e.g., '' inside single-quoted string)
			if t.pos+1 < len(t.input) && t.input[t.pos+1] == quote {
				sb.WriteByte(quote)
				t.pos += 2
				continue
			}
			break
		}
		sb.WriteByte(t.input[t.pos])
		t.pos++
	}

	if t.pos >= len(t.input) {
		return fmt.Errorf("unterminated string at position %d", t.start)
	}

	literal := sb.String()
	t.pos++
	t.addToken(TokenString, literal)
	return nil
}

func (t *Tokenizer) readHexString() error {
	// Skip the 'x' character and get the quote
	t.pos++
	quote := t.input[t.pos]
	t.pos++

	t.start = t.pos
	for t.pos < len(t.input) {
		if t.input[t.pos] == quote {
			break
		}
		t.pos++
	}

	if t.pos >= len(t.input) {
		return fmt.Errorf("unterminated hex string at position %d", t.start)
	}

	hexStr := t.input[t.start:t.pos]
	t.pos++

	bytes, err := parseHexString(hexStr)
	if err != nil {
		return err
	}
	t.addToken(TokenHexString, string(bytes))
	return nil
}

func parseHexString(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("invalid hex string: odd length")
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi := hexValTable[s[i]]
		lo := hexValTable[s[i+1]]
		if hi > 15 || lo > 15 {
			return nil, fmt.Errorf("invalid hex string: %s", s[i:i+2])
		}
		result[i/2] = (hi << 4) | lo
	}
	return result, nil
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
