package DS

import (
	"testing"
)

// BenchmarkVarint_Put benchmarks varint encoding.
func BenchmarkVarint_Put(b *testing.B) {
	buf := make([]byte, 9)
	values := []int64{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutVarint(buf, values[i%len(values)])
	}
}

// BenchmarkVarint_Get benchmarks varint decoding.
func BenchmarkVarint_Get(b *testing.B) {
	// Pre-encode some values
	samples := []int64{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	bufs := make([][]byte, len(samples))
	for i, v := range samples {
		buf := make([]byte, 9)
		PutVarint(buf, v)
		bufs[i] = buf
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetVarint(bufs[i%len(bufs)])
	}
}

// BenchmarkVarint_Len benchmarks varint length calculation.
func BenchmarkVarint_Len(b *testing.B) {
	values := []int64{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarintLen(values[i%len(values)])
	}
}

// BenchmarkRecord_Encode benchmarks record encoding.
func BenchmarkRecord_Encode(b *testing.B) {
	values := []interface{}{int64(42), "hello world", float64(3.14), nil, int64(1000000)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeRecord(values)
	}
}

// BenchmarkRecord_EncodePooled benchmarks pooled record encoding.
func BenchmarkRecord_EncodePooled(b *testing.B) {
	values := []interface{}{int64(42), "hello world", float64(3.14), nil, int64(1000000)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeRecordPooled(values)
	}
}

// BenchmarkRecord_Decode benchmarks record decoding.
func BenchmarkRecord_Decode(b *testing.B) {
	values := []interface{}{int64(42), "hello world", float64(3.14), nil, int64(1000000)}
	encoded := EncodeRecord(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeRecord(encoded)
	}
}
