package DS

// VTabCursor defines the cursor interface for iterating over virtual table rows.
type VTabCursor interface {
	Filter(idxNum int, idxStr string, args []interface{}) error
	Next() error
	Column(col int) (interface{}, error)
	RowID() (int64, error)
	Eof() bool
	Close() error
}

// VTab represents a virtual table instance.
type VTab interface {
	BestIndex(info *IndexInfo) error
	Open() (VTabCursor, error)
	Columns() []string
	Disconnect() error
	Destroy() error
}

// VTabModule is the factory for creating/connecting to virtual tables.
type VTabModule interface {
	Create(args []string) (VTab, error)
	Connect(args []string) (VTab, error)
}

// IndexInfo holds query plan information passed to BestIndex.
type IndexInfo struct {
	Constraints   []IndexConstraint
	OrderBy       []IndexOrderBy
	IdxNum        int
	IdxStr        string
	EstimatedRows int64
	EstimatedCost float64
	OutputUsed    []bool
}

// IndexConstraint describes a WHERE constraint.
type IndexConstraint struct {
	Column int
	Op     byte // '=' '<' '>' etc.
	Usable bool
}

// IndexOrderBy describes an ORDER BY term.
type IndexOrderBy struct {
	Column int
	Desc   bool
}
