package DS

import (
	"bytes"
	"testing"
)

func TestTableLeafCell(t *testing.T) {
	tests := []struct {
		name         string
		rowid        int64
		payload      []byte
		overflowPage uint32
	}{
		{"simple", 1, []byte("hello world"), 0},
		{"with_overflow", 42, []byte("large payload data"), 100},
		{"empty_payload", 999, []byte{}, 0},
		{"large_rowid", 9223372036854775807, []byte("test"), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeTableLeafCell(tt.rowid, tt.payload, tt.overflowPage)

			// Decode
			decoded, err := DecodeTableLeafCell(encoded)
			if err != nil {
				t.Fatalf("DecodeTableLeafCell() error = %v", err)
			}

			// Verify
			if decoded.Type != CellTypeTableLeaf {
				t.Errorf("Type = %v, want %v", decoded.Type, CellTypeTableLeaf)
			}
			if decoded.Rowid != tt.rowid {
				t.Errorf("Rowid = %d, want %d", decoded.Rowid, tt.rowid)
			}
			if !bytes.Equal(decoded.Payload, tt.payload) {
				t.Errorf("Payload = %v, want %v", decoded.Payload, tt.payload)
			}
			if decoded.OverflowPage != tt.overflowPage {
				t.Errorf("OverflowPage = %d, want %d", decoded.OverflowPage, tt.overflowPage)
			}
		})
	}
}

func TestTableInteriorCell(t *testing.T) {
	tests := []struct {
		name      string
		leftChild uint32
		rowid     int64
	}{
		{"simple", 10, 100},
		{"large", 12345, 9876543210},
		{"zero_child", 0, 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeTableInteriorCell(tt.leftChild, tt.rowid)

			// Decode
			decoded, err := DecodeTableInteriorCell(encoded)
			if err != nil {
				t.Fatalf("DecodeTableInteriorCell() error = %v", err)
			}

			// Verify
			if decoded.Type != CellTypeTableInterior {
				t.Errorf("Type = %v, want %v", decoded.Type, CellTypeTableInterior)
			}
			if decoded.LeftChild != tt.leftChild {
				t.Errorf("LeftChild = %d, want %d", decoded.LeftChild, tt.leftChild)
			}
			if decoded.Rowid != tt.rowid {
				t.Errorf("Rowid = %d, want %d", decoded.Rowid, tt.rowid)
			}
		})
	}
}

func TestIndexLeafCell(t *testing.T) {
	tests := []struct {
		name         string
		key          []byte
		overflowPage uint32
	}{
		{"simple", []byte("index_key"), 0},
		{"with_overflow", []byte("very_long_index_key_data"), 200},
		{"empty", []byte{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeIndexLeafCell(tt.key, tt.overflowPage)

			// Decode
			decoded, err := DecodeIndexLeafCell(encoded)
			if err != nil {
				t.Fatalf("DecodeIndexLeafCell() error = %v", err)
			}

			// Verify
			if decoded.Type != CellTypeIndexLeaf {
				t.Errorf("Type = %v, want %v", decoded.Type, CellTypeIndexLeaf)
			}
			if !bytes.Equal(decoded.Key, tt.key) {
				t.Errorf("Key = %v, want %v", decoded.Key, tt.key)
			}
			if decoded.OverflowPage != tt.overflowPage {
				t.Errorf("OverflowPage = %d, want %d", decoded.OverflowPage, tt.overflowPage)
			}
		})
	}
}

func TestIndexInteriorCell(t *testing.T) {
	tests := []struct {
		name         string
		leftChild    uint32
		key          []byte
		overflowPage uint32
	}{
		{"simple", 50, []byte("key"), 0},
		{"with_overflow", 100, []byte("long_key_data"), 300},
		{"zero_child", 0, []byte("test"), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeIndexInteriorCell(tt.leftChild, tt.key, tt.overflowPage)

			// Decode
			decoded, err := DecodeIndexInteriorCell(encoded)
			if err != nil {
				t.Fatalf("DecodeIndexInteriorCell() error = %v", err)
			}

			// Verify
			if decoded.Type != CellTypeIndexInterior {
				t.Errorf("Type = %v, want %v", decoded.Type, CellTypeIndexInterior)
			}
			if decoded.LeftChild != tt.leftChild {
				t.Errorf("LeftChild = %d, want %d", decoded.LeftChild, tt.leftChild)
			}
			if !bytes.Equal(decoded.Key, tt.key) {
				t.Errorf("Key = %v, want %v", decoded.Key, tt.key)
			}
			if decoded.OverflowPage != tt.overflowPage {
				t.Errorf("OverflowPage = %d, want %d", decoded.OverflowPage, tt.overflowPage)
			}
		})
	}
}

func TestCalculateLocalPayloadSize(t *testing.T) {
	tests := []struct {
		name        string
		usableSize  int
		payloadSize int
		isLeaf      bool
		wantMin     int // Minimum expected local size
		wantMax     int // Maximum expected local size
	}{
		{"small_leaf", 4096, 100, true, 100, 100},
		{"large_leaf", 4096, 5000, true, 100, 4061},
		{"small_interior", 4096, 100, false, 100, 100},
		{"large_interior", 4096, 5000, false, 100, 4092},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateLocalPayloadSize(tt.usableSize, tt.payloadSize, tt.isLeaf)

			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("CalculateLocalPayloadSize() = %d, want between %d and %d",
					got, tt.wantMin, tt.wantMax)
			}

			// Local size should never exceed payload size
			if got > tt.payloadSize {
				t.Errorf("Local size %d exceeds payload size %d", got, tt.payloadSize)
			}
		})
	}
}

func TestCellSize(t *testing.T) {
	tests := []struct {
		name string
		cell *CellData
		want int
	}{
		{
			name: "table_leaf",
			cell: &CellData{
				Type:    CellTypeTableLeaf,
				Rowid:   100,
				Payload: []byte("test payload"),
			},
			want: VarintLen(12) + VarintLen(100) + 12,
		},
		{
			name: "table_interior",
			cell: &CellData{
				Type:      CellTypeTableInterior,
				LeftChild: 50,
				Rowid:     200,
			},
			want: 4 + VarintLen(200),
		},
		{
			name: "index_leaf",
			cell: &CellData{
				Type: CellTypeIndexLeaf,
				Key:  []byte("index_key"),
			},
			want: VarintLen(9) + 9,
		},
		{
			name: "index_interior",
			cell: &CellData{
				Type:      CellTypeIndexInterior,
				LeftChild: 75,
				Key:       []byte("key"),
			},
			want: 4 + VarintLen(3) + 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CellSize(tt.cell)
			if got != tt.want {
				t.Errorf("CellSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func BenchmarkEncodeTableLeafCell(b *testing.B) {
	payload := []byte("benchmark payload data for testing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeTableLeafCell(int64(i+1), payload, 0)
	}
}

func BenchmarkDecodeTableLeafCell(b *testing.B) {
	payload := []byte("benchmark payload data for testing")
	encoded := EncodeTableLeafCell(12345, payload, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeTableLeafCell(encoded)
	}
}

func BenchmarkEncodeIndexInteriorCell(b *testing.B) {
	key := []byte("benchmark_index_key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeIndexInteriorCell(uint32(i), key, 0)
	}
}

func BenchmarkDecodeIndexInteriorCell(b *testing.B) {
	key := []byte("benchmark_index_key")
	encoded := EncodeIndexInteriorCell(100, key, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeIndexInteriorCell(encoded)
	}
}
