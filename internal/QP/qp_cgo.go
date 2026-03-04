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

// ParseC parses SQL using the C++ parser and returns statement info.
// This is a high-level wrapper around CParser for easy migration.
func ParseC(sql string) (*ParsedStmt, error) {
	if len(sql) == 0 {
		return nil, &qpError{"empty SQL input"}
	}

	parser := NewCParser(sql)
	if parser == nil {
		return nil, &qpError{"failed to create parser"}
	}

	node := parser.Parse()
	if node == nil {
		errMsg := parser.Error()
		if errMsg == "" {
			errMsg = "parse error"
		}
		return nil, &qpError{errMsg}
	}

	stmt := &ParsedStmt{
		Type:       stmtTypeFromInt(node.Type()),
		Table:      node.Table(),
		Where:      node.Where(),
		SQL:        sql,
		node:       node,
		parser:     parser,
	}

	// Extract columns
	colCount := node.ColumnCount()
	if colCount > 0 {
		stmt.Columns = make([]string, colCount)
		for i := 0; i < colCount; i++ {
			stmt.Columns[i] = node.Column(i)
		}
	}

	// Extract INSERT values
	valueRowCount := node.ValueRowCount()
	if valueRowCount > 0 {
		stmt.Values = make([][]string, valueRowCount)
		for i := 0; i < valueRowCount; i++ {
			valCount := node.ValueCount(i)
			stmt.Values[i] = make([]string, valCount)
			for j := 0; j < valCount; j++ {
				stmt.Values[i][j] = node.Value(i, j)
			}
		}
	}

	return stmt, nil
}

// ParsedStmt holds parsed SQL statement information.
type ParsedStmt struct {
	Type    StmtType
	Table   string
	Columns []string
	Values  [][]string
	Where   string
	SQL     string

	node   *CASTNode
	parser *CParser
}

// Free frees the parsed statement resources.
func (s *ParsedStmt) Free() {
	if s.node != nil {
		// Node freed by parser
	}
	if s.parser != nil {
		// Parser finalizer handles cleanup
	}
}

// StmtType represents the type of SQL statement.
type StmtType int

const (
	StmtUnknown StmtType = iota
	StmtSelect
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtCreate
	StmtDrop
	StmtAlter
	StmtBegin
	StmtCommit
	StmtRollback
)

func stmtTypeFromInt(t int) StmtType {
	switch t {
	case ASTSelect:
		return StmtSelect
	case ASTInsert:
		return StmtInsert
	case ASTUpdate:
		return StmtUpdate
	case ASTDelete:
		return StmtDelete
	case ASTCreate:
		return StmtCreate
	case ASTDrop:
		return StmtDrop
	case ASTExpr:
		return StmtUnknown
	default:
		return StmtUnknown
	}
}
