// Package QP - minimal Token types for C++ wrapper compatibility
package QP

// TokenType represents the type of a token.
type TokenType int

const (
	TokenInvalid TokenType = iota
	TokenEOF
	TokenIdentifier
	TokenString
	TokenHexString
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
	TokenBitAnd
	TokenBitOr
	TokenBitNot
	TokenShiftLeft
	TokenShiftRight
	TokenPlus
	TokenMinus
	TokenSlash
	TokenRemainder
	TokenConcat
	TokenBetween
	TokenIn
	TokenIs
	TokenLike
	TokenGlob
	TokenMatch
	TokenRegexp
	TokenAnd
	TokenOr
	TokenNot
	TokenExists
	TokenCase
	TokenWhen
	TokenThen
	TokenElse
	TokenEnd
	TokenNull
	TokenTrue
	TokenFalse
	TokenCurrentDate
	TokenCurrentTime
	TokenCurrentTimestamp
	TokenPlaceholderPos
	TokenPlaceholderNamed
)

// Token represents a lexical token.
type Token struct {
	Type     TokenType
	Literal  string
	Location int
}

// keywords maps keyword strings to their TokenType.
var keywords = map[string]TokenType{
	"SELECT":           TokenKeyword,
	"FROM":             TokenKeyword,
	"WHERE":            TokenKeyword,
	"INSERT":           TokenKeyword,
	"INTO":             TokenKeyword,
	"VALUES":           TokenKeyword,
	"UPDATE":           TokenKeyword,
	"SET":              TokenKeyword,
	"DELETE":           TokenKeyword,
	"CREATE":           TokenKeyword,
	"TABLE":            TokenKeyword,
	"INDEX":            TokenKeyword,
	"DROP":             TokenKeyword,
	"ALTER":            TokenKeyword,
	"ADD":              TokenKeyword,
	"PRIMARY":          TokenKeyword,
	"KEY":              TokenKeyword,
	"UNIQUE":           TokenKeyword,
	"NOT":              TokenKeyword,
	"NULL":             TokenKeyword,
	"DEFAULT":          TokenKeyword,
	"CHECK":            TokenKeyword,
	"FOREIGN":          TokenKeyword,
	"REFERENCES":       TokenKeyword,
	"ON":               TokenKeyword,
	"CASCADE":          TokenKeyword,
	"RESTRICT":         TokenKeyword,
	"SETNULL":          TokenKeyword,
	"SETDEFAULT":       TokenKeyword,
	"NO":               TokenKeyword,
	"ACTION":           TokenKeyword,
	"DEFERRABLE":       TokenKeyword,
	"INITIALLY":        TokenKeyword,
	"DEFERRED":         TokenKeyword,
	"IMMEDIATE":        TokenKeyword,
	"EXCLUSIVE":        TokenKeyword,
	"BEGIN":            TokenKeyword,
	"TRANSACTION":      TokenKeyword,
	"COMMIT":           TokenKeyword,
	"ROLLBACK":         TokenKeyword,
	"SAVEPOINT":        TokenKeyword,
	"RELEASE":          TokenKeyword,
	"EXPLAIN":          TokenKeyword,
	"PLAN":             TokenKeyword,
	"QUERY":            TokenKeyword,
	"ANALYZE":          TokenKeyword,
	"VACUUM":           TokenKeyword,
	"PRAGMA":           TokenKeyword,
	"WITH":             TokenKeyword,
	"RECURSIVE":        TokenKeyword,
	"AS":               TokenKeyword,
	"ALL":              TokenKeyword,
	"DISTINCT":         TokenKeyword,
	"GROUP":            TokenKeyword,
	"BY":               TokenKeyword,
	"HAVING":           TokenKeyword,
	"ORDER":            TokenKeyword,
	"ASC":              TokenKeyword,
	"DESC":             TokenKeyword,
	"LIMIT":            TokenKeyword,
	"OFFSET":           TokenKeyword,
	"UNION":            TokenKeyword,
	"INTERSECT":        TokenKeyword,
	"EXCEPT":           TokenKeyword,
	"JOIN":             TokenKeyword,
	"INNER":            TokenKeyword,
	"LEFT":             TokenKeyword,
	"RIGHT":            TokenKeyword,
	"OUTER":            TokenKeyword,
	"CROSS":            TokenKeyword,
	"NATURAL":          TokenKeyword,
	"USING":            TokenKeyword,
	"AND":              TokenAnd,
	"OR":               TokenOr,
	"IS":               TokenIs,
	"IN":               TokenIn,
	"LIKE":             TokenLike,
	"GLOB":             TokenGlob,
	"MATCH":            TokenMatch,
	"REGEXP":           TokenRegexp,
	"BETWEEN":          TokenBetween,
	"NULLS":            TokenKeyword,
	"FIRST":            TokenKeyword,
	"LAST":             TokenKeyword,
	"CASE":             TokenCase,
	"WHEN":             TokenWhen,
	"THEN":             TokenThen,
	"ELSE":             TokenElse,
	"END":              TokenEnd,
	"TRUE":             TokenTrue,
	"FALSE":            TokenFalse,
	"CURRENT_DATE":     TokenCurrentDate,
	"CURRENT_TIME":     TokenCurrentTime,
	"CURRENT_TIMESTAMP": TokenCurrentTimestamp,
	"INTEGER":          TokenKeyword,
	"REAL":             TokenKeyword,
	"TEXT":             TokenKeyword,
	"BLOB":             TokenKeyword,
	"INT":              TokenKeyword,
	"VARCHAR":          TokenKeyword,
	"CHAR":             TokenKeyword,
	"DOUBLE":           TokenKeyword,
	"FLOAT":            TokenKeyword,
	"BOOLEAN":          TokenKeyword,
	"DATE":             TokenKeyword,
	"DATETIME":         TokenKeyword,
	"WINDOW":           TokenKeyword,
	"OVER":             TokenKeyword,
	"PARTITION":        TokenKeyword,
	"RANGE":            TokenKeyword,
	"ROWS":             TokenKeyword,
	"UNBOUNDED":        TokenKeyword,
	"PRECEDING":        TokenKeyword,
	"FOLLOWING":        TokenKeyword,
	"CURRENT":          TokenKeyword,
	"ROW":              TokenKeyword,
	"EXISTS":           TokenExists,
	"CAST":             TokenKeyword,
	"FILTER":           TokenKeyword,
	"GROUPS":           TokenKeyword,
	"OTHERS":           TokenKeyword,
	"DO":               TokenKeyword,
	"RAISE":            TokenKeyword,
	"IGNORE":           TokenKeyword,
	"REPLACE":          TokenKeyword,
	"ABORT":            TokenKeyword,
	"FAIL":             TokenKeyword,
	"CONSTRAINT":       TokenKeyword,
	"IF":               TokenKeyword,
	"TEMP":             TokenKeyword,
	"TEMPORARY":        TokenKeyword,
	"VIEW":             TokenKeyword,
	"TRIGGER":          TokenKeyword,
	"BEFORE":           TokenKeyword,
	"AFTER":            TokenKeyword,
	"EACH":             TokenKeyword,
	"FOR":              TokenKeyword,
	"OF":               TokenKeyword,
	"NEW":              TokenKeyword,
	"OLD":              TokenKeyword,
	"DATABASE":         TokenKeyword,
	"ATTACH":           TokenKeyword,
	"DETACH":           TokenKeyword,
	"VIRTUAL":          TokenKeyword,
	"MODULE":           TokenKeyword,
	"REINDEX":          TokenKeyword,
	"RENAME":           TokenKeyword,
	"TO":               TokenKeyword,
	"COLUMN":           TokenKeyword,
	"CONFLICT":         TokenKeyword,
	"COMMITTED":        TokenKeyword,
	"ISOLATION":        TokenKeyword,
	"LEVEL":            TokenKeyword,
	"READ":             TokenKeyword,
	"WRITE":            TokenKeyword,
	"SERIALIZABLE":     TokenKeyword,
}
