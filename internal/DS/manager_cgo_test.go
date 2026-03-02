package DS

import (
	"encoding/binary"
	"testing"
)

// TestManagerIsValidPageSize tests power-of-2 page size validation.
func TestManagerIsValidPageSize(t *testing.T) {
	validSizes := []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
	for _, sz := range validSizes {
		if !ManagerIsValidPageSize(sz) {
			t.Errorf("ManagerIsValidPageSize(%d) = false; want true", sz)
		}
	}
	invalidSizes := []uint32{0, 256, 511, 513, 1000, 65537, 131072}
	for _, sz := range invalidSizes {
		if ManagerIsValidPageSize(sz) {
			t.Errorf("ManagerIsValidPageSize(%d) = true; want false", sz)
		}
	}
}

// TestManagerPageOffset tests file byte offset computation.
func TestManagerPageOffset(t *testing.T) {
	if got := ManagerPageOffset(0, 4096); got != -1 {
		t.Errorf("ManagerPageOffset(0, 4096) = %d; want -1", got)
	}
	if got := ManagerPageOffset(1, 4096); got != 0 {
		t.Errorf("ManagerPageOffset(1, 4096) = %d; want 0", got)
	}
	if got := ManagerPageOffset(2, 4096); got != 4096 {
		t.Errorf("ManagerPageOffset(2, 4096) = %d; want 4096", got)
	}
	if got := ManagerPageOffset(3, 4096); got != 8192 {
		t.Errorf("ManagerPageOffset(3, 4096) = %d; want 8192", got)
	}
}

// buildSQLiteHeader builds a minimal 100-byte SQLite-compatible database header.
func buildSQLiteHeader(pageSize uint32) []byte {
	hdr := make([]byte, 100)
	copy(hdr[:16], "SQLite format 3\x00")
	// page size at bytes 16-17 (big-endian uint16; 1 means 65536)
	ps := uint16(pageSize)
	if pageSize == 65536 {
		ps = 1
	}
	binary.BigEndian.PutUint16(hdr[16:18], ps)
	// num pages at bytes 28-31 (big-endian uint32)
	binary.BigEndian.PutUint32(hdr[28:32], 0)
	return hdr
}

// TestManagerHeaderMagicValid tests SQLite magic string validation.
func TestManagerHeaderMagicValid(t *testing.T) {
	hdr := buildSQLiteHeader(4096)
	if !ManagerHeaderMagicValid(hdr) {
		t.Error("ManagerHeaderMagicValid returned false for valid magic")
	}
	hdr[0] = 'X' // corrupt magic
	if ManagerHeaderMagicValid(hdr) {
		t.Error("ManagerHeaderMagicValid returned true for corrupted magic")
	}
	if ManagerHeaderMagicValid(nil) {
		t.Error("ManagerHeaderMagicValid returned true for nil")
	}
	if ManagerHeaderMagicValid(make([]byte, 4)) {
		t.Error("ManagerHeaderMagicValid returned true for too-short buffer")
	}
}

// TestManagerReadWriteHeaderPageSize tests page size header round-trip.
func TestManagerReadWriteHeaderPageSize(t *testing.T) {
	hdr := buildSQLiteHeader(4096)
	if got := ManagerReadHeaderPageSize(hdr); got != 4096 {
		t.Errorf("ManagerReadHeaderPageSize = %d; want 4096", got)
	}

	// Write a new page size.
	if !ManagerWriteHeaderPageSize(hdr, 8192) {
		t.Fatal("ManagerWriteHeaderPageSize returned false")
	}
	if got := ManagerReadHeaderPageSize(hdr); got != 8192 {
		t.Errorf("After write, ManagerReadHeaderPageSize = %d; want 8192", got)
	}
}

// TestManagerReadWriteHeaderPageSize_65536 tests 65536 special encoding.
func TestManagerReadWriteHeaderPageSize_65536(t *testing.T) {
	hdr := buildSQLiteHeader(65536)
	if got := ManagerReadHeaderPageSize(hdr); got != 65536 {
		t.Errorf("ManagerReadHeaderPageSize for 65536 = %d; want 65536", got)
	}
	if !ManagerWriteHeaderPageSize(hdr, 65536) {
		t.Fatal("ManagerWriteHeaderPageSize(65536) returned false")
	}
	if got := ManagerReadHeaderPageSize(hdr); got != 65536 {
		t.Errorf("After write, ManagerReadHeaderPageSize = %d; want 65536", got)
	}
}

// TestManagerWriteHeaderPageSize_Invalid tests that invalid sizes are rejected.
func TestManagerWriteHeaderPageSize_Invalid(t *testing.T) {
	hdr := buildSQLiteHeader(4096)
	if ManagerWriteHeaderPageSize(hdr, 511) {
		t.Error("expected false for invalid page size 511")
	}
	if ManagerWriteHeaderPageSize(hdr, 0) {
		t.Error("expected false for invalid page size 0")
	}
}

// TestManagerReadWriteHeaderNumPages tests num-pages header round-trip.
func TestManagerReadWriteHeaderNumPages(t *testing.T) {
	hdr := buildSQLiteHeader(4096)
	if got := ManagerReadHeaderNumPages(hdr); got != 0 {
		t.Errorf("ManagerReadHeaderNumPages = %d; want 0", got)
	}
	if !ManagerWriteHeaderNumPages(hdr, 42) {
		t.Fatal("ManagerWriteHeaderNumPages returned false")
	}
	if got := ManagerReadHeaderNumPages(hdr); got != 42 {
		t.Errorf("After write, ManagerReadHeaderNumPages = %d; want 42", got)
	}
}

// TestManagerHeaderOps_TooShort tests graceful handling of short buffers.
func TestManagerHeaderOps_TooShort(t *testing.T) {
	short := make([]byte, 10)
	if ManagerReadHeaderPageSize(short) != 0 {
		t.Error("expected 0 for too-short buffer in ReadHeaderPageSize")
	}
	if ManagerWriteHeaderPageSize(short, 4096) {
		t.Error("expected false for too-short buffer in WriteHeaderPageSize")
	}
	if ManagerReadHeaderNumPages(short) != 0 {
		t.Error("expected 0 for too-short buffer in ReadHeaderNumPages")
	}
	if ManagerWriteHeaderNumPages(short, 1) {
		t.Error("expected false for too-short buffer in WriteHeaderNumPages")
	}
}
