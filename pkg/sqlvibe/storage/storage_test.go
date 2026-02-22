package storage

import (
	"testing"
)

// ---- Value tests ----

func TestValue_Constructors(t *testing.T) {
	if !NullValue().IsNull() {
		t.Fatal("NullValue should be null")
	}
	if IntValue(42).Int != 42 {
		t.Fatal("IntValue")
	}
	if FloatValue(3.14).Float != 3.14 {
		t.Fatal("FloatValue")
	}
	if StringValue("hi").Str != "hi" {
		t.Fatal("StringValue")
	}
	if BoolValue(true).Int != 1 {
		t.Fatal("BoolValue true")
	}
	if BoolValue(false).Int != 0 {
		t.Fatal("BoolValue false")
	}
	bv := BytesValue([]byte{1, 2})
	if len(bv.Bytes) != 2 {
		t.Fatal("BytesValue")
	}
}

func TestValue_Compare(t *testing.T) {
	if Compare(NullValue(), NullValue()) != 0 {
		t.Fatal("null == null")
	}
	if Compare(NullValue(), IntValue(0)) != -1 {
		t.Fatal("null < int")
	}
	if Compare(IntValue(1), IntValue(2)) != -1 {
		t.Fatal("1 < 2")
	}
	if Compare(IntValue(2), IntValue(1)) != 1 {
		t.Fatal("2 > 1")
	}
	if Compare(IntValue(1), FloatValue(1.0)) != 0 {
		t.Fatal("int(1) == float(1.0)")
	}
	if Compare(StringValue("abc"), StringValue("abd")) != -1 {
		t.Fatal("abc < abd")
	}
}

func TestValue_Equal(t *testing.T) {
	if NullValue().Equal(NullValue()) {
		t.Fatal("null should not equal null (SQL semantics)")
	}
	if !IntValue(5).Equal(IntValue(5)) {
		t.Fatal("5 == 5")
	}
	if IntValue(5).Equal(IntValue(6)) {
		t.Fatal("5 != 6")
	}
}

func TestValue_String(t *testing.T) {
	if NullValue().String() != "NULL" {
		t.Fatal("null string")
	}
	if IntValue(7).String() != "7" {
		t.Fatal("int string")
	}
	if BoolValue(true).String() != "true" {
		t.Fatal("bool string")
	}
}

// ---- Row tests ----

func TestRow_NullBitmap(t *testing.T) {
	r := NewRow([]Value{IntValue(1), NullValue(), IntValue(3)})
	if r.IsNull(0) {
		t.Fatal("col 0 should not be null")
	}
	if !r.IsNull(1) {
		t.Fatal("col 1 should be null")
	}

	r.SetNull(0)
	if !r.IsNull(0) {
		t.Fatal("col 0 should be null after SetNull")
	}
	r.ClearNull(0)
	if r.IsNull(0) {
		t.Fatal("col 0 should not be null after ClearNull")
	}

	if r.Get(1).Type != TypeNull {
		t.Fatal("Get on null column should return NullValue")
	}
}

func TestRow_SetGet(t *testing.T) {
	r := NewRow([]Value{IntValue(10), IntValue(20)})
	r.Set(0, NullValue())
	if !r.IsNull(0) {
		t.Fatal("Set null should update bitmap")
	}
	r.Set(1, IntValue(99))
	if r.Get(1).Int != 99 {
		t.Fatal("Get after Set")
	}
	if r.Len() != 2 {
		t.Fatal("Len")
	}
}

// ---- ColumnVector tests ----

func TestColumnVector_Basic(t *testing.T) {
	cv := NewColumnVector("age", TypeInt)
	cv.Append(IntValue(10))
	cv.Append(IntValue(20))
	cv.AppendNull()

	if cv.Len() != 3 {
		t.Fatalf("expected len 3, got %d", cv.Len())
	}
	if cv.Get(0).Int != 10 {
		t.Fatal("get 0")
	}
	if cv.Get(2).Type != TypeNull {
		t.Fatal("get null")
	}
	if !cv.IsNull(2) {
		t.Fatal("IsNull 2")
	}

	cv.SetNull(0, true)
	if !cv.IsNull(0) {
		t.Fatal("SetNull")
	}
}

func TestColumnVector_Slice(t *testing.T) {
	cv := NewColumnVector("x", TypeInt)
	for i := 0; i < 5; i++ {
		cv.Append(IntValue(int64(i)))
	}
	sl := cv.Slice(1, 3)
	if sl.Len() != 2 {
		t.Fatalf("slice len expected 2, got %d", sl.Len())
	}
	if sl.Get(0).Int != 1 {
		t.Fatal("slice get 0")
	}
}

func TestColumnVector_Reset(t *testing.T) {
	cv := NewColumnVector("x", TypeInt)
	cv.Append(IntValue(1))
	cv.Reset()
	if cv.Len() != 0 {
		t.Fatal("reset")
	}
}

// ---- RoaringBitmap tests ----

func TestRoaringBitmap_AddRemoveContains(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(1)
	rb.Add(100)
	rb.Add(1000000)
	if !rb.Contains(1) || !rb.Contains(100) || !rb.Contains(1000000) {
		t.Fatal("contains after add")
	}
	if rb.Cardinality() != 3 {
		t.Fatalf("cardinality expected 3, got %d", rb.Cardinality())
	}
	rb.Remove(100)
	if rb.Contains(100) {
		t.Fatal("contains after remove")
	}
	if rb.Cardinality() != 2 {
		t.Fatal("cardinality after remove")
	}
}

func TestRoaringBitmap_ToSlice(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(5)
	rb.Add(3)
	rb.Add(7)
	s := rb.ToSlice()
	if len(s) != 3 || s[0] != 3 || s[1] != 5 || s[2] != 7 {
		t.Fatalf("ToSlice: %v", s)
	}
}

func TestRoaringBitmap_And(t *testing.T) {
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	for i := uint32(0); i < 10; i++ {
		a.Add(i)
	}
	for i := uint32(5); i < 15; i++ {
		b.Add(i)
	}
	and := a.And(b)
	s := and.ToSlice()
	if len(s) != 5 {
		t.Fatalf("And expected 5 elements, got %d: %v", len(s), s)
	}
}

func TestRoaringBitmap_Or(t *testing.T) {
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	a.Add(1)
	b.Add(2)
	or := a.Or(b)
	if or.Cardinality() != 2 {
		t.Fatal("Or cardinality")
	}
}

func TestRoaringBitmap_AndNot(t *testing.T) {
	a := NewRoaringBitmap()
	b := NewRoaringBitmap()
	for i := uint32(0); i < 5; i++ {
		a.Add(i)
		b.Add(i + 3)
	}
	diff := a.AndNot(b)
	s := diff.ToSlice()
	if len(s) != 3 { // 0,1,2 remain
		t.Fatalf("AndNot expected 3 elements, got %d: %v", len(s), s)
	}
}

func TestRoaringBitmap_IsEmpty(t *testing.T) {
	rb := NewRoaringBitmap()
	if !rb.IsEmpty() {
		t.Fatal("empty bitmap")
	}
	rb.Add(1)
	if rb.IsEmpty() {
		t.Fatal("non-empty bitmap")
	}
}

func TestRoaringBitmap_Clone(t *testing.T) {
	rb := NewRoaringBitmap()
	rb.Add(1)
	rb.Add(2)
	c := rb.Clone()
	c.Remove(1)
	if !rb.Contains(1) {
		t.Fatal("clone should be independent")
	}
}

func TestRoaringBitmap_LargeArrayContainer(t *testing.T) {
	rb := NewRoaringBitmap()
	// Add more than 4096 elements to trigger bitmap container
	for i := uint32(0); i < 5000; i++ {
		rb.Add(i)
	}
	if rb.Cardinality() != 5000 {
		t.Fatalf("expected 5000, got %d", rb.Cardinality())
	}
	if !rb.Contains(4999) {
		t.Fatal("contains 4999")
	}
	rb.Remove(4999)
	if rb.Contains(4999) {
		t.Fatal("removed 4999")
	}
}

// ---- SkipList tests ----

func TestSkipList_InsertFind(t *testing.T) {
	sl := NewSkipList()
	sl.Insert(IntValue(10), 0)
	sl.Insert(IntValue(20), 1)
	sl.Insert(IntValue(10), 2) // duplicate key, different row

	idxs := sl.Find(IntValue(10))
	if len(idxs) != 2 {
		t.Fatalf("expected 2 rows for key 10, got %d", len(idxs))
	}
	if sl.Len() != 2 { // 2 unique keys
		t.Fatalf("expected 2 unique keys, got %d", sl.Len())
	}
}

func TestSkipList_Delete(t *testing.T) {
	sl := NewSkipList()
	sl.Insert(IntValue(5), 0)
	sl.Insert(IntValue(5), 1)
	sl.Delete(IntValue(5), 0)

	idxs := sl.Find(IntValue(5))
	if len(idxs) != 1 || idxs[0] != 1 {
		t.Fatalf("after delete: %v", idxs)
	}
	sl.Delete(IntValue(5), 1)
	if sl.Len() != 0 {
		t.Fatal("skip list should be empty")
	}
}

func TestSkipList_Range(t *testing.T) {
	sl := NewSkipList()
	for i := int64(0); i < 10; i++ {
		sl.Insert(IntValue(i), uint32(i))
	}
	idxs := sl.Range(IntValue(3), IntValue(6), true)
	if len(idxs) != 4 {
		t.Fatalf("range [3,6] expected 4 elements, got %d: %v", len(idxs), idxs)
	}
}

func TestSkipList_MinMax(t *testing.T) {
	sl := NewSkipList()
	_, ok := sl.Min()
	if ok {
		t.Fatal("empty min should return false")
	}
	sl.Insert(IntValue(5), 0)
	sl.Insert(IntValue(1), 1)
	sl.Insert(IntValue(9), 2)
	min, _ := sl.Min()
	max, _ := sl.Max()
	if min.Int != 1 || max.Int != 9 {
		t.Fatalf("min=%d max=%d", min.Int, max.Int)
	}
}

// ---- Arena tests ----

func TestArena_AllocReset(t *testing.T) {
	a := NewArena(1024)
	b1 := a.Alloc(64)
	b1[0] = 0xAB
	b2 := a.Alloc(128)
	_ = b2
	if a.BytesUsed() < 64+128 {
		t.Fatal("bytes used")
	}
	a.Reset()
	if a.BytesUsed() != 0 {
		t.Fatal("reset bytes used")
	}
}

func TestArena_LargeAlloc(t *testing.T) {
	a := NewArena(128)
	b := a.Alloc(1000) // larger than initial size
	if len(b) != 1000 {
		t.Fatal("large alloc")
	}
}

// ---- RowStore tests ----

func TestRowStore_InsertScanDelete(t *testing.T) {
	rs := NewRowStore([]string{"id", "name"}, []ValueType{TypeInt, TypeString})
	r0 := NewRow([]Value{IntValue(1), StringValue("alice")})
	r1 := NewRow([]Value{IntValue(2), StringValue("bob")})
	idx0 := rs.Insert(r0)
	idx1 := rs.Insert(r1)

	if rs.RowCount() != 2 || rs.LiveCount() != 2 {
		t.Fatal("counts after insert")
	}

	got := rs.Get(idx0)
	if got.Get(1).Str != "alice" {
		t.Fatal("get row 0")
	}

	rs.Delete(idx1)
	if rs.LiveCount() != 1 {
		t.Fatal("live count after delete")
	}

	rows := rs.Scan()
	if len(rows) != 1 {
		t.Fatal("scan after delete")
	}

	rs.Update(idx0, NewRow([]Value{IntValue(1), StringValue("ALICE")}))
	updated := rs.Get(idx0)
	if updated.Get(1).Str != "ALICE" {
		t.Fatal("update")
	}
}

func TestRowStore_ColIndex(t *testing.T) {
	rs := NewRowStore([]string{"a", "b", "c"}, []ValueType{TypeInt, TypeInt, TypeInt})
	if rs.ColIndex("b") != 1 {
		t.Fatal("ColIndex b")
	}
	if rs.ColIndex("z") != -1 {
		t.Fatal("ColIndex missing")
	}
}

func TestRowStore_ToColumnVectors(t *testing.T) {
	rs := NewRowStore([]string{"x"}, []ValueType{TypeInt})
	rs.Insert(NewRow([]Value{IntValue(7)}))
	rs.Insert(NewRow([]Value{IntValue(8)}))
	vecs := rs.ToColumnVectors()
	if len(vecs) != 1 || vecs[0].Len() != 2 {
		t.Fatal("ToColumnVectors")
	}
}

// ---- ColumnStore tests ----

func TestColumnStore_AppendGetFilter(t *testing.T) {
	cs := NewColumnStore([]string{"val"}, []ValueType{TypeInt})
	cs.AppendRow([]Value{IntValue(10)})
	cs.AppendRow([]Value{IntValue(20)})
	cs.AppendRow([]Value{IntValue(30)})

	row := cs.GetRow(1)
	if row[0].Int != 20 {
		t.Fatal("GetRow 1")
	}

	rb := cs.Filter("val", func(v Value) bool { return v.Int > 15 })
	s := rb.ToSlice()
	if len(s) != 2 {
		t.Fatalf("filter expected 2, got %d", len(s))
	}
}

func TestColumnStore_Delete(t *testing.T) {
	cs := NewColumnStore([]string{"v"}, []ValueType{TypeInt})
	cs.AppendRow([]Value{IntValue(1)})
	cs.AppendRow([]Value{IntValue(2)})
	cs.DeleteRow(0)
	if cs.LiveCount() != 1 {
		t.Fatal("live count")
	}
	rows := cs.ToRows()
	if len(rows) != 1 || rows[0].Get(0).Int != 2 {
		t.Fatal("ToRows after delete")
	}
}

func TestColumnStore_ColIndex(t *testing.T) {
	cs := NewColumnStore([]string{"a", "b"}, []ValueType{TypeInt, TypeString})
	if cs.ColIndex("a") != 0 || cs.ColIndex("b") != 1 || cs.ColIndex("z") != -1 {
		t.Fatal("ColIndex")
	}
}

// ---- IndexEngine tests ----

func TestIndexEngine_BitmapLookup(t *testing.T) {
	ie := NewIndexEngine()
	ie.AddBitmapIndex("status")
	ie.IndexRow(0, "status", StringValue("active"))
	ie.IndexRow(1, "status", StringValue("inactive"))
	ie.IndexRow(2, "status", StringValue("active"))

	rb := ie.LookupEqual("status", StringValue("active"))
	s := rb.ToSlice()
	if len(s) != 2 {
		t.Fatalf("bitmap lookup expected 2, got %d", len(s))
	}
}

func TestIndexEngine_SkipListRange(t *testing.T) {
	ie := NewIndexEngine()
	ie.AddSkipListIndex("age")
	for i := int64(0); i < 10; i++ {
		ie.IndexRow(uint32(i), "age", IntValue(i*10))
	}
	rb := ie.LookupRange("age", IntValue(20), IntValue(50), true)
	s := rb.ToSlice()
	if len(s) != 4 { // 20, 30, 40, 50
		t.Fatalf("range expected 4 got %d: %v", len(s), s)
	}
}

func TestIndexEngine_UnindexRow(t *testing.T) {
	ie := NewIndexEngine()
	ie.AddBitmapIndex("cat")
	ie.IndexRow(0, "cat", StringValue("A"))
	ie.UnindexRow(0, "cat", StringValue("A"))
	rb := ie.LookupEqual("cat", StringValue("A"))
	if rb.Cardinality() != 0 {
		t.Fatal("unindex should remove from bitmap")
	}
}

// ---- HybridStore tests ----

func TestHybridStore_RoundTrip(t *testing.T) {
	hs := NewHybridStore([]string{"id", "score"}, []ValueType{TypeInt, TypeFloat})
	hs.Insert([]Value{IntValue(1), FloatValue(9.5)})
	hs.Insert([]Value{IntValue(2), FloatValue(7.0)})
	hs.Insert([]Value{IntValue(3), FloatValue(8.0)})

	if hs.RowCount() != 3 || hs.LiveCount() != 3 {
		t.Fatal("counts")
	}

	rows := hs.Scan()
	if len(rows) != 3 {
		t.Fatal("scan")
	}
}

func TestHybridStore_DeleteUpdate(t *testing.T) {
	hs := NewHybridStore([]string{"x"}, []ValueType{TypeInt})
	idx := hs.Insert([]Value{IntValue(10)})
	hs.Update(idx, []Value{IntValue(99)})
	rows := hs.Scan()
	if len(rows) != 1 || rows[0][0].Int != 99 {
		t.Fatal("update")
	}

	hs.Delete(idx)
	if hs.LiveCount() != 0 {
		t.Fatal("delete")
	}
}

func TestHybridStore_ScanWhere_NoIndex(t *testing.T) {
	hs := NewHybridStore([]string{"name"}, []ValueType{TypeString})
	hs.Insert([]Value{StringValue("alice")})
	hs.Insert([]Value{StringValue("bob")})
	hs.Insert([]Value{StringValue("alice")})

	rows := hs.ScanWhere("name", StringValue("alice"))
	if len(rows) != 2 {
		t.Fatalf("expected 2, got %d", len(rows))
	}
}

func TestHybridStore_ScanWhere_WithIndex(t *testing.T) {
	hs := NewHybridStore([]string{"cat"}, []ValueType{TypeString})
	hs.Insert([]Value{StringValue("A")})
	hs.Insert([]Value{StringValue("B")})
	hs.Insert([]Value{StringValue("A")})
	hs.CreateIndex("cat", false) // bitmap index

	rows := hs.ScanWhere("cat", StringValue("A"))
	if len(rows) != 2 {
		t.Fatalf("expected 2 (indexed), got %d", len(rows))
	}
}

func TestHybridStore_ScanRange(t *testing.T) {
	hs := NewHybridStore([]string{"n"}, []ValueType{TypeInt})
	for i := int64(0); i < 10; i++ {
		hs.Insert([]Value{IntValue(i)})
	}
	hs.CreateIndex("n", true) // skip-list

	rows := hs.ScanRange("n", IntValue(3), IntValue(6))
	if len(rows) != 4 {
		t.Fatalf("range expected 4, got %d", len(rows))
	}
}

func TestHybridStore_Columns(t *testing.T) {
	hs := NewHybridStore([]string{"a", "b"}, []ValueType{TypeInt, TypeString})
	cols := hs.Columns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Fatal("columns")
	}
	if hs.ColIndex("b") != 1 || hs.ColIndex("z") != -1 {
		t.Fatal("ColIndex")
	}
}
