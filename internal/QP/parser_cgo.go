package QP

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/QP
#include "parser.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// CParser is a thin CGO wrapper around the C++ SQL parser.
// It provides statement-type detection and basic structure extraction
// (table name, column list, WHERE clause, INSERT VALUES).
//
// The Go parser (internal/QP/parser.go) remains the primary parser
// for full SQL support. CParser is used for lightweight pre-processing
// and incremental C++ migration.
type CParser struct {
	ptr unsafe.Pointer // *C.svdb_parser_t
}

// AST node type constants — must match src/core/QP/parser.h.
const (
	ASTUnknown = 0
	ASTSelect  = 1
	ASTInsert  = 2
	ASTUpdate  = 3
	ASTDelete  = 4
	ASTCreate  = 5
	ASTDrop    = 6
	ASTExpr    = 7
)

// CASTNode wraps the C++ AST node and provides accessors.
type CASTNode struct {
	ptr unsafe.Pointer // *C.svdb_ast_node_t
}

// NewCParser creates a new C++ parser for the given SQL string.
func NewCParser(sql string) *CParser {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	p := &CParser{
		ptr: unsafe.Pointer(C.svdb_parser_create(cs, C.size_t(len(sql)))),
	}
	runtime.SetFinalizer(p, func(x *CParser) {
		if x.ptr != nil {
			C.svdb_parser_destroy((*C.svdb_parser_t)(x.ptr))
			x.ptr = nil
		}
	})
	return p
}

// Parse parses the SQL and returns a CASTNode, or nil on error.
func (p *CParser) Parse() *CASTNode {
	if p.ptr == nil {
		return nil
	}
	nodePtr := C.svdb_parser_parse((*C.svdb_parser_t)(p.ptr))
	if nodePtr == nil {
		return nil
	}
	node := &CASTNode{ptr: unsafe.Pointer(nodePtr)}
	runtime.SetFinalizer(node, func(n *CASTNode) {
		if n.ptr != nil {
			C.svdb_ast_node_free((*C.svdb_ast_node_t)(n.ptr))
			n.ptr = nil
		}
	})
	return node
}

// Error returns the last parse error message (empty if none).
func (p *CParser) Error() string {
	if p.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_parser_error((*C.svdb_parser_t)(p.ptr)))
}

// Type returns the AST node type (ASTSelect, ASTInsert, etc.).
func (n *CASTNode) Type() int {
	if n.ptr == nil {
		return ASTUnknown
	}
	return int(C.svdb_ast_node_type((*C.svdb_ast_node_t)(n.ptr)))
}

// Table returns the primary table name extracted from the statement.
func (n *CASTNode) Table() string {
	if n.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_ast_get_table((*C.svdb_ast_node_t)(n.ptr)))
}

// ColumnCount returns the number of columns extracted from the statement.
func (n *CASTNode) ColumnCount() int {
	if n.ptr == nil {
		return 0
	}
	return int(C.svdb_ast_get_column_count((*C.svdb_ast_node_t)(n.ptr)))
}

// Column returns the column name at index i (0-based).
func (n *CASTNode) Column(i int) string {
	if n.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_ast_get_column((*C.svdb_ast_node_t)(n.ptr), C.int(i)))
}

// Columns returns all extracted column names.
func (n *CASTNode) Columns() []string {
	count := n.ColumnCount()
	cols := make([]string, count)
	for i := 0; i < count; i++ {
		cols[i] = n.Column(i)
	}
	return cols
}

// ValueRowCount returns the number of INSERT VALUES rows.
func (n *CASTNode) ValueRowCount() int {
	if n.ptr == nil {
		return 0
	}
	return int(C.svdb_ast_get_value_row_count((*C.svdb_ast_node_t)(n.ptr)))
}

// ValueCount returns the number of values in a specific row.
func (n *CASTNode) ValueCount(rowIdx int) int {
	if n.ptr == nil {
		return 0
	}
	return int(C.svdb_ast_get_value_count((*C.svdb_ast_node_t)(n.ptr), C.int(rowIdx)))
}

// Value returns the value string at (rowIdx, colIdx).
func (n *CASTNode) Value(rowIdx, colIdx int) string {
	if n.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_ast_get_value((*C.svdb_ast_node_t)(n.ptr), C.int(rowIdx), C.int(colIdx)))
}

// Where returns the WHERE clause text (empty if none).
func (n *CASTNode) Where() string {
	if n.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_ast_get_where((*C.svdb_ast_node_t)(n.ptr)))
}

// SQL returns the original SQL text stored in the node.
func (n *CASTNode) SQL() string {
	if n.ptr == nil {
		return ""
	}
	return C.GoString(C.svdb_ast_get_sql((*C.svdb_ast_node_t)(n.ptr)))
}

// ParseSQL is a convenience function that creates a parser, parses the SQL,
// and returns the CASTNode and any error message.
func ParseSQL(sql string) (*CASTNode, string) {
	p := NewCParser(sql)
	node := p.Parse()
	if node == nil {
		return nil, p.Error()
	}
	return node, ""
}
