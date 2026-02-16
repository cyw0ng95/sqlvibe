package VM

type Cursor struct {
	ID       int
	TableID  int
	RowID    int64
	EOF      bool
	Index    int
	IndexKey int64
}

type CursorArray struct {
	cursors []*Cursor
}

func NewCursorArray() *CursorArray {
	return &CursorArray{
		cursors: make([]*Cursor, 0),
	}
}

func (ca *CursorArray) Open(tableID int) int {
	cursor := &Cursor{
		ID:      len(ca.cursors),
		TableID: tableID,
		RowID:   0,
		EOF:     false,
		Index:   -1,
	}
	ca.cursors = append(ca.cursors, cursor)
	return cursor.ID
}

func (ca *CursorArray) Close(id int) error {
	if id >= 0 && id < len(ca.cursors) {
		ca.cursors[id] = nil
		return nil
	}
	return nil
}

func (ca *CursorArray) Get(id int) *Cursor {
	if id >= 0 && id < len(ca.cursors) {
		return ca.cursors[id]
	}
	return nil
}

func (ca *CursorArray) SetRowID(id int, rowID int64) {
	if id >= 0 && id < len(ca.cursors) {
		ca.cursors[id].RowID = rowID
	}
}

func (ca *CursorArray) SetEOF(id int, eof bool) {
	if id >= 0 && id < len(ca.cursors) {
		ca.cursors[id].EOF = eof
	}
}

func (ca *CursorArray) SetIndex(id int, idx int) {
	if id >= 0 && id < len(ca.cursors) {
		ca.cursors[id].Index = idx
	}
}

func (ca *CursorArray) Reset() {
	for i := range ca.cursors {
		ca.cursors[i] = nil
	}
	ca.cursors = make([]*Cursor, 0)
}
