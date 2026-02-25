package VM

import (
	"testing"
	"unsafe"
)

func TestVmVal_Size(t *testing.T) {
	var v VmVal
	size := unsafe.Sizeof(v)
	if size != 32 {
		t.Errorf("VmVal size = %d, want 32", size)
	}
}

func TestVmVal_Constructors(t *testing.T) {
	if !VmNull().IsNull() {
		t.Error("VmNull().IsNull() should be true")
	}
	if VmInt(42).Int() != 42 {
		t.Error("VmInt(42).Int() should be 42")
	}
	if VmFloat(3.14).Float() != 3.14 {
		t.Error("VmFloat(3.14).Float() should be 3.14")
	}
	if VmText("hi").Text() != "hi" {
		t.Error("VmText(hi).Text() should be hi")
	}
	if string(VmBlob([]byte{1, 2, 3}).Blob()) != string([]byte{1, 2, 3}) {
		t.Error("VmBlob round-trip failed")
	}
}

func TestVmVal_Arithmetic(t *testing.T) {
	a := VmInt(10)
	b := VmInt(3)

	if AddVmVal(a, b).Int() != 13 {
		t.Error("Add failed")
	}
	if SubVmVal(a, b).Int() != 7 {
		t.Error("Sub failed")
	}
	if MulVmVal(a, b).Int() != 30 {
		t.Error("Mul failed")
	}
	if DivVmVal(a, b).Int() != 3 {
		t.Error("Div failed")
	}
	if ModVmVal(a, b).Int() != 1 {
		t.Error("Mod failed")
	}
	if NegVmVal(a).Int() != -10 {
		t.Error("Neg failed")
	}
}

func TestVmVal_NullPropagation(t *testing.T) {
	n := VmNull()
	v := VmInt(5)

	if !AddVmVal(n, v).IsNull() {
		t.Error("NULL + int should be NULL")
	}
	if !AddVmVal(v, n).IsNull() {
		t.Error("int + NULL should be NULL")
	}
	if !MulVmVal(n, v).IsNull() {
		t.Error("NULL * int should be NULL")
	}
	if !NegVmVal(n).IsNull() {
		t.Error("NEG NULL should be NULL")
	}
}

func TestVmVal_Concat(t *testing.T) {
	a := VmText("hello")
	b := VmText(" world")
	c := ConcatVmVal(a, b)
	if c.Text() != "hello world" {
		t.Errorf("Concat = %q, want %q", c.Text(), "hello world")
	}
	if !ConcatVmVal(VmNull(), b).IsNull() {
		t.Error("NULL || text should be NULL")
	}
}

func TestVmVal_ToFromInterface(t *testing.T) {
	cases := []interface{}{
		nil,
		int64(42),
		float64(3.14),
		"hello",
		[]byte{1, 2, 3},
		true,
		false,
	}
	for _, c := range cases {
		v := FromInterface(c)
		got := v.ToInterface()
		// Check type equivalence for basic types
		switch exp := c.(type) {
		case nil:
			if got != nil {
				t.Errorf("nil round-trip: got %v", got)
			}
		case int64:
			if got != exp {
				t.Errorf("int64 round-trip: got %v want %v", got, exp)
			}
		case float64:
			if got != exp {
				t.Errorf("float64 round-trip: got %v want %v", got, exp)
			}
		case string:
			if got != exp {
				t.Errorf("string round-trip: got %v want %v", got, exp)
			}
		}
	}
}

func TestVmVal_Compare(t *testing.T) {
	if CompareVmVal(VmInt(1), VmInt(2)) != -1 {
		t.Error("1 < 2 should be -1")
	}
	if CompareVmVal(VmInt(2), VmInt(2)) != 0 {
		t.Error("2 == 2 should be 0")
	}
	if CompareVmVal(VmInt(3), VmInt(2)) != 1 {
		t.Error("3 > 2 should be 1")
	}
}

func BenchmarkVmVal_NoAlloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = VmNull()
		_ = VmInt(42)
		_ = VmFloat(3.14)
	}
}
