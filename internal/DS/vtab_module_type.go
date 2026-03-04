// Package DS - minimal VTab types for C++ wrapper compatibility
package DS

// VTabModule represents a virtual table module.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual vtab handling is done in C++.
type VTabModule interface {
	Create(args []string) (VTab, error)
	Connect(args []string) (VTab, error)
}

// VTab represents a virtual table.
type VTab interface {
	Columns() []string
	Open() (VTabCursor, error)
}

// VTabCursor represents a virtual table cursor.
type VTabCursor interface {
	Close() error
	Filter(idxNum int, idxStr string, args []interface{}) error
	Eof() bool
	Next() error
	Column(col int) (interface{}, error)
}

// IndexInfo represents index information for virtual tables.
type IndexInfo struct {
	Constraints   []IndexConstraint
	IdxNum        int
	IdxStr        string
	EstimatedRows int64
	EstimatedCost float64
}

// IndexConstraint represents a constraint in IndexInfo.
type IndexConstraint struct {
	Usable bool
	Op     int
}

// TableModule is the table module type.
type TableModule struct{}
