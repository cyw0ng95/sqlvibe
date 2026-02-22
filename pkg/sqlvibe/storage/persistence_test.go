package storage

import (
	"os"
	"testing"
)

func tmpFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "sqlvibe-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

func makeStore(t *testing.T) (*HybridStore, []string, []ValueType) {
	t.Helper()
	cols := []string{"id", "name", "score", "active", "tag"}
	types := []ValueType{TypeInt, TypeString, TypeFloat, TypeBool, TypeString}
	hs := NewHybridStore(cols, types)

	hs.Insert([]Value{IntValue(1), StringValue("alice"), FloatValue(9.5), BoolValue(true), NullValue()})
	hs.Insert([]Value{IntValue(2), StringValue("bob"), FloatValue(7.0), BoolValue(false), StringValue("vip")})
	hs.Insert([]Value{IntValue(3), NullValue(), FloatValue(8.25), BoolValue(true), StringValue("staff")})
	return hs, cols, types
}

func makeSchema(cols []string, types []ValueType) map[string]interface{} {
	typInts := make([]int, len(types))
	for i, t := range types {
		typInts[i] = int(t)
	}
	return map[string]interface{}{
		"column_names": cols,
		"column_types": typInts,
	}
}

// ---- Round-trip test ----

func TestPersistence_RoundTrip(t *testing.T) {
	path := tmpFile(t)
	hs, cols, types := makeStore(t)
	schema := makeSchema(cols, types)

	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatalf("WriteDatabase: %v", err)
	}

	hs2, schema2, err := ReadDatabase(path)
	if err != nil {
		t.Fatalf("ReadDatabase: %v", err)
	}

	// Verify schema preserved.
	cn, _ := schema2["column_names"].([]interface{})
	if len(cn) != len(cols) {
		t.Fatalf("column_names length: got %d, want %d", len(cn), len(cols))
	}
	for i, c := range cn {
		if c.(string) != cols[i] {
			t.Fatalf("column_names[%d]: got %v, want %v", i, c, cols[i])
		}
	}

	// Verify row count.
	if hs2.LiveCount() != hs.LiveCount() {
		t.Fatalf("row count: got %d, want %d", hs2.LiveCount(), hs.LiveCount())
	}

	// Verify row data.
	orig := hs.Scan()
	read := hs2.Scan()
	if len(orig) != len(read) {
		t.Fatalf("scan length: got %d, want %d", len(read), len(orig))
	}
	for ri, row := range orig {
		rrow := read[ri]
		for ci, v := range row {
			rv := rrow[ci]
			if v.IsNull() && rv.IsNull() {
				continue
			}
			if v.IsNull() != rv.IsNull() {
				t.Errorf("row %d col %d: null mismatch", ri, ci)
				continue
			}
			if Compare(v, rv) != 0 {
				t.Errorf("row %d col %d: got %v, want %v", ri, ci, rv, v)
			}
		}
	}
}

// ---- Type-specific round-trip tests ----

func TestPersistence_IntColumn(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"n"}, []ValueType{TypeInt})
	hs.Insert([]Value{IntValue(0)})
	hs.Insert([]Value{IntValue(-1)})
	hs.Insert([]Value{IntValue(1<<62 - 1)})

	schema := makeSchema([]string{"n"}, []ValueType{TypeInt})
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := hs2.Scan()
	if rows[0][0].Int != 0 || rows[1][0].Int != -1 || rows[2][0].Int != 1<<62-1 {
		t.Fatalf("int values mismatch: %v", rows)
	}
}

func TestPersistence_FloatColumn(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"f"}, []ValueType{TypeFloat})
	hs.Insert([]Value{FloatValue(3.141592653589793)})
	hs.Insert([]Value{FloatValue(-0.0)})
	hs.Insert([]Value{FloatValue(1e300)})

	schema := makeSchema([]string{"f"}, []ValueType{TypeFloat})
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := hs2.Scan()
	if rows[0][0].Float != 3.141592653589793 {
		t.Fatalf("float precision lost: %v", rows[0][0].Float)
	}
	if rows[2][0].Float != 1e300 {
		t.Fatalf("large float lost: %v", rows[2][0].Float)
	}
}

func TestPersistence_StringColumn(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"s"}, []ValueType{TypeString})
	hs.Insert([]Value{StringValue("")})
	hs.Insert([]Value{StringValue("hello, 世界")})
	hs.Insert([]Value{StringValue("abc")})

	schema := makeSchema([]string{"s"}, []ValueType{TypeString})
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := hs2.Scan()
	if rows[0][0].Str != "" || rows[1][0].Str != "hello, 世界" || rows[2][0].Str != "abc" {
		t.Fatalf("string values mismatch: %v", rows)
	}
}

func TestPersistence_BoolColumn(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"b"}, []ValueType{TypeBool})
	hs.Insert([]Value{BoolValue(true)})
	hs.Insert([]Value{BoolValue(false)})

	schema := makeSchema([]string{"b"}, []ValueType{TypeBool})
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := hs2.Scan()
	if rows[0][0].Int != 1 || rows[1][0].Int != 0 {
		t.Fatalf("bool values mismatch: %v", rows)
	}
}

func TestPersistence_NullValues(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"a", "b"}, []ValueType{TypeInt, TypeString})
	hs.Insert([]Value{NullValue(), StringValue("x")})
	hs.Insert([]Value{IntValue(42), NullValue()})
	hs.Insert([]Value{NullValue(), NullValue()})

	schema := makeSchema([]string{"a", "b"}, []ValueType{TypeInt, TypeString})
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := hs2.Scan()
	if !rows[0][0].IsNull() {
		t.Error("row 0 col 0 should be null")
	}
	if rows[0][1].Str != "x" {
		t.Errorf("row 0 col 1: got %v", rows[0][1])
	}
	if rows[1][0].Int != 42 {
		t.Errorf("row 1 col 0: got %v", rows[1][0])
	}
	if !rows[1][1].IsNull() {
		t.Error("row 1 col 1 should be null")
	}
	if !rows[2][0].IsNull() || !rows[2][1].IsNull() {
		t.Error("row 2 should be all nulls")
	}
}

func TestPersistence_EmptyStore(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"x"}, []ValueType{TypeInt})
	schema := makeSchema([]string{"x"}, []ValueType{TypeInt})

	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	hs2, _, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if hs2.LiveCount() != 0 {
		t.Fatalf("expected 0 rows, got %d", hs2.LiveCount())
	}
}

// ---- CRC validation tests ----

func TestPersistence_CorruptFileCRC(t *testing.T) {
	path := tmpFile(t)
	hs, cols, types := makeStore(t)
	if err := WriteDatabase(path, hs, makeSchema(cols, types)); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the column data section.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit somewhere in the middle of the file.
	mid := len(raw) / 2
	raw[mid] ^= 0xFF
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}

	_, _, err = ReadDatabase(path)
	if err == nil {
		t.Fatal("expected CRC error for corrupted file, got nil")
	}
}

func TestPersistence_CorruptHeaderCRC(t *testing.T) {
	path := tmpFile(t)
	hs, cols, types := makeStore(t)
	if err := WriteDatabase(path, hs, makeSchema(cols, types)); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt a header byte (inside the first 248 bytes that are CRC-protected).
	raw[30] ^= 0x01
	// Also fix footer CRC so the file-level CRC check won't trigger first.
	// (We just want to verify header CRC detection; but file CRC will trigger
	// first since it covers everything. So we just verify an error is returned.)
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}

	_, _, err = ReadDatabase(path)
	if err == nil {
		t.Fatal("expected error for corrupted header, got nil")
	}
}

func TestPersistence_ExtraSchema(t *testing.T) {
	path := tmpFile(t)
	hs := NewHybridStore([]string{"k"}, []ValueType{TypeInt})
	hs.Insert([]Value{IntValue(99)})

	schema := map[string]interface{}{
		"column_names": []string{"k"},
		"column_types": []int{int(TypeInt)},
		"tables":       []interface{}{map[string]interface{}{"name": "t"}},
		"version":      "1.0",
	}
	if err := WriteDatabase(path, hs, schema); err != nil {
		t.Fatal(err)
	}
	_, schema2, err := ReadDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if schema2["version"] != "1.0" {
		t.Errorf("extra schema field lost: version = %v", schema2["version"])
	}
}
