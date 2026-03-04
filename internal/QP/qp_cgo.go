package QP

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/QP
#include "tokenizer.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"unsafe"
)

// TokenizeC tokenizes SQL using the C++ tokenizer.
// Returns a slice of tokens or an error.
func TokenizeC(sql string) ([]Token, error) {
	sqlLen := len(sql)
	if sqlLen == 0 {
		return []Token{{Type: TokenEOF, Literal: "", Location: 0}}, nil
	}

	// Count tokens first
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	tokenCount := C.svdb_token_count(cSQL, C.size_t(sqlLen))
	if tokenCount == 0 {
		return nil, ErrTokenize
	}

	// Allocate token array
	tokens := make([]C.svdb_token_t, tokenCount)

	// Tokenize
	actualCount := C.svdb_tokenize(
		cSQL,
		C.size_t(sqlLen),
		&tokens[0],
		C.size_t(tokenCount),
	)

	// Convert to Go tokens
	result := make([]Token, 0, actualCount)
	for i := C.size_t(0); i < actualCount; i++ {
		tok := tokens[i]
		tokenType := cTokenTypeToGo(C.int(tok._type))
		text := sql[tok.start:tok.end]

		// Convert keyword identifiers
		if tokenType == TokenIdentifier {
			if kwType, ok := keywords[text]; ok {
				tokenType = kwType
			}
		}

		result = append(result, Token{
			Type:     tokenType,
			Literal:  text,
			Location: int(tok.start),
		})
	}

	return result, nil
}

// cTokenTypeToGo converts C token type to Go TokenType.
func cTokenTypeToGo(cType C.int) TokenType {
	switch cType {
	case C.SVDB_TOK_EOF:
		return TokenEOF
	case C.SVDB_TOK_IDENTIFIER:
		return TokenIdentifier
	case C.SVDB_TOK_INTEGER:
		return TokenNumber
	case C.SVDB_TOK_FLOAT:
		return TokenNumber
	case C.SVDB_TOK_STRING:
		return TokenString
	case C.SVDB_TOK_KEYWORD:
		return TokenKeyword
	case C.SVDB_TOK_PUNCT:
		return TokenLeftParen // Refined by position
	case C.SVDB_TOK_OPERATOR:
		return TokenOperator
	case C.SVDB_TOK_PARAM:
		return TokenPlaceholderPos
	case C.SVDB_TOK_NAMED_PARAM:
		return TokenPlaceholderNamed
	case C.SVDB_TOK_COMMENT:
		return TokenKeyword // Comments treated as keywords for skipping
	case C.SVDB_TOK_WHITESPACE:
		return TokenKeyword // Whitespace treated as keywords for skipping
	default:
		return TokenInvalid
	}
}

// Errors
var (
	ErrTokenize = &qpError{"tokenization failed"}
)

// qpError represents a QP-related error.
type qpError struct {
	Msg string
}

func (e *qpError) Error() string {
	return "QP: " + e.Msg
}
