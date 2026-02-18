package DS

import (
	"bytes"
	"testing"
)

func TestVarintEncoding(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  int // expected bytes
	}{
		{"zero", 0, 1},
		{"small", 127, 1},
		{"medium", 128, 2},
		{"large", 16384, 3},
		{"negative", -1, 9},
		{"max_int64", 9223372036854775807, 9},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 10)
			n := PutVarint(buf, tt.value)
			
			if n != tt.want {
				t.Errorf("PutVarint() bytes = %d, want %d", n, tt.want)
			}
			
			// Decode and verify
			got, bytesRead := GetVarint(buf)
			if bytesRead != n {
				t.Errorf("GetVarint() bytes = %d, want %d", bytesRead, n)
			}
			if got != tt.value {
				t.Errorf("GetVarint() = %d, want %d", got, tt.value)
			}
		})
	}
}

func TestVarintLen(t *testing.T) {
	tests := []int64{0, 1, 127, 128, 16383, 16384, -1, -128, 9223372036854775807}
	
	for _, v := range tests {
		buf := make([]byte, 10)
		n := PutVarint(buf, v)
		length := VarintLen(v)
		
		if length != n {
			t.Errorf("VarintLen(%d) = %d, want %d", v, length, n)
		}
	}
}

func TestSerialTypes(t *testing.T) {
	tests := []struct {
		name       string
		value      interface{}
		wantType   int
		wantLength int
	}{
		{"null", nil, SerialTypeNull, 0},
		{"zero", int64(0), SerialTypeZero, 0},
		{"one", int64(1), SerialTypeOne, 0},
		{"int8", int64(100), SerialTypeInt8, 1},
		{"int16", int64(1000), SerialTypeInt16, 2},
		{"int32", int64(100000), SerialTypeInt32, 4},
		{"int64", int64(10000000000), SerialTypeInt64, 8},
		{"float", float64(3.14), SerialTypeFloat64, 8},
		{"text", "hello", 13 + 2*5, 5},
		{"blob", []byte{1, 2, 3}, 12 + 2*3, 3},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := GetSerialType(tt.value)
			if st != tt.wantType {
				t.Errorf("GetSerialType() = %d, want %d", st, tt.wantType)
			}
			
			length := SerialTypeLen(st)
			if length != tt.wantLength {
				t.Errorf("SerialTypeLen() = %d, want %d", length, tt.wantLength)
			}
		})
	}
}

func TestRecordEncoding(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{
			name:   "simple",
			values: []interface{}{int64(1), "hello", int64(42)},
		},
		{
			name:   "with_null",
			values: []interface{}{nil, int64(100), "test"},
		},
		{
			name:   "all_types",
			values: []interface{}{nil, int64(0), int64(1), int64(127), float64(3.14), "text", []byte{1, 2, 3}},
		},
		{
			name:   "empty",
			values: []interface{}{},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeRecord(tt.values)
			
			// Decode
			decoded, n, err := DecodeRecord(encoded)
			if err != nil {
				t.Fatalf("DecodeRecord() error = %v", err)
			}
			
			if n != len(encoded) {
				t.Errorf("DecodeRecord() consumed %d bytes, want %d", n, len(encoded))
			}
			
			// Compare values
			if len(decoded) != len(tt.values) {
				t.Fatalf("DecodeRecord() returned %d values, want %d", len(decoded), len(tt.values))
			}
			
			for i := range tt.values {
				if !valuesEqual(decoded[i], tt.values[i]) {
					t.Errorf("DecodeRecord()[%d] = %v, want %v", i, decoded[i], tt.values[i])
				}
			}
		})
	}
}

func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			return av == bv
		}
		if bv, ok := b.(int); ok {
			return av == int64(bv)
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
		if bv, ok := b.(float32); ok {
			return av == float64(bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Equal(av, bv)
		}
	}
	
	return false
}

func BenchmarkVarintEncode(b *testing.B) {
	buf := make([]byte, 10)
	for i := 0; i < b.N; i++ {
		PutVarint(buf, int64(i))
	}
}

func BenchmarkVarintDecode(b *testing.B) {
	buf := make([]byte, 10)
	PutVarint(buf, 12345678)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetVarint(buf)
	}
}

func BenchmarkRecordEncode(b *testing.B) {
	values := []interface{}{int64(1), "hello world", int64(42), float64(3.14159)}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeRecord(values)
	}
}

func BenchmarkRecordDecode(b *testing.B) {
	values := []interface{}{int64(1), "hello world", int64(42), float64(3.14159)}
	encoded := EncodeRecord(values)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeRecord(encoded)
	}
}
