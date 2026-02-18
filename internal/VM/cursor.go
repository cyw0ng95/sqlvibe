package VM

type Cursor struct {
	ID        int
	TableID   int
	TableName string
	RowID     int64
	EOF       bool
	Index     int
	IndexKey  int64
	Data      []map[string]interface{}
	Columns   []string
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

func (ca *CursorArray) OpenTable(tableName string, data []map[string]interface{}, columns []string) int {
	cursor := &Cursor{
		ID:        len(ca.cursors),
		TableName: tableName,
		RowID:     0,
		EOF:       len(data) == 0,
		Index:     -1,
		Data:      data,
		Columns:   columns,
	}
	ca.cursors = append(ca.cursors, cursor)
	return cursor.ID
}

func (ca *CursorArray) OpenTableAtID(cursorID int, tableName string, data []map[string]interface{}, columns []string) {
	// Ensure cursors array is large enough
	for len(ca.cursors) <= cursorID {
		ca.cursors = append(ca.cursors, nil)
	}
	cursor := &Cursor{
		ID:        cursorID,
		TableName: tableName,
		RowID:     0,
		EOF:       len(data) == 0,
		Index:     -1,
		Data:      data,
		Columns:   columns,
	}
	ca.cursors[cursorID] = cursor
}

func (ca *CursorArray) Next(id int) (map[string]interface{}, bool) {
	if id < 0 || id >= len(ca.cursors) || ca.cursors[id] == nil {
		return nil, true
	}
	cursor := ca.cursors[id]
	cursor.Index++
	if cursor.Index >= len(cursor.Data) {
		cursor.EOF = true
		return nil, true
	}
	return cursor.Data[cursor.Index], false
}

func (ca *CursorArray) GetRow(id int) (map[string]interface{}, bool) {
	if id < 0 || id >= len(ca.cursors) || ca.cursors[id] == nil {
		return nil, true
	}
	cursor := ca.cursors[id]
	if cursor.Index < 0 || cursor.Index >= len(cursor.Data) {
		return nil, true
	}
	return cursor.Data[cursor.Index], false
}

func (ca *CursorArray) GetColumn(id int, colName string) interface{} {
	if row, ok := ca.GetRow(id); ok {
		return nil
	} else {
		return row[colName]
	}
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
