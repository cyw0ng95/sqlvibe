package DS

import "fmt"

// RowStoreCursor wraps RowStore to implement VTabCursor.
type RowStoreCursor struct {
	store *RowStore
	rows  []Row
	pos   int
}

// NewRowStoreCursor creates a RowStoreCursor for the given RowStore.
func NewRowStoreCursor(rs *RowStore) *RowStoreCursor {
	return &RowStoreCursor{store: rs}
}

func (c *RowStoreCursor) Filter(idxNum int, idxStr string, args []interface{}) error {
	c.rows = c.store.Scan()
	c.pos = 0
	return nil
}

func (c *RowStoreCursor) Next() error {
	c.pos++
	return nil
}

func (c *RowStoreCursor) Column(col int) (interface{}, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, fmt.Errorf("RowStoreCursor: position %d out of range", c.pos)
	}
	return dsValueToInterface(c.rows[c.pos].Get(col)), nil
}

func (c *RowStoreCursor) RowID() (int64, error) {
	return int64(c.pos), nil
}

func (c *RowStoreCursor) Eof() bool {
	return c.pos >= len(c.rows)
}

func (c *RowStoreCursor) Close() error {
	c.rows = nil
	return nil
}

// HybridStoreCursor wraps HybridStore to implement VTabCursor.
type HybridStoreCursor struct {
	store *HybridStore
	rows  [][]Value
	pos   int
}

// NewHybridStoreCursor creates a HybridStoreCursor for the given HybridStore.
func NewHybridStoreCursor(hs *HybridStore) *HybridStoreCursor {
	return &HybridStoreCursor{store: hs}
}

func (c *HybridStoreCursor) Filter(idxNum int, idxStr string, args []interface{}) error {
	c.rows = c.store.Scan()
	c.pos = 0
	return nil
}

func (c *HybridStoreCursor) Next() error {
	c.pos++
	return nil
}

func (c *HybridStoreCursor) Column(col int) (interface{}, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, fmt.Errorf("HybridStoreCursor: position %d out of range", c.pos)
	}
	row := c.rows[c.pos]
	if col < 0 || col >= len(row) {
		return nil, fmt.Errorf("HybridStoreCursor: column %d out of range", col)
	}
	return dsValueToInterface(row[col]), nil
}

func (c *HybridStoreCursor) RowID() (int64, error) {
	return int64(c.pos), nil
}

func (c *HybridStoreCursor) Eof() bool {
	return c.pos >= len(c.rows)
}

func (c *HybridStoreCursor) Close() error {
	c.rows = nil
	return nil
}

// dsValueToInterface converts a DS.Value to a plain Go interface{} for VTabCursor consumers.
func dsValueToInterface(v Value) interface{} {
	switch v.Type {
	case TypeNull:
		return nil
	case TypeInt:
		return v.Int
	case TypeFloat:
		return v.Float
	case TypeString:
		return v.Str
	case TypeBytes:
		return v.Bytes
	case TypeBool:
		return v.Int != 0
	default:
		return nil
	}
}
