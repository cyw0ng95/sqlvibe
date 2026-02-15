package DS

import (
	"bytes"
	"testing"
)

func TestNewPage(t *testing.T) {
	page := NewPage(1, 4096)
	if page.Num != 1 {
		t.Errorf("expected page number 1, got %d", page.Num)
	}
	if len(page.Data) != 4096 {
		t.Errorf("expected data size 4096, got %d", len(page.Data))
	}
	if page.IsDirty {
		t.Error("new page should not be dirty")
	}
}

func TestPageSetType(t *testing.T) {
	page := NewPage(1, 4096)
	page.SetType(PageTypeLeafTbl)
	if page.Type != PageTypeLeafTbl {
		t.Errorf("expected type leaf-table, got %s", page.Type)
	}
	if !page.IsDirty {
		t.Error("page should be dirty after SetType")
	}
}

func TestParseHeader(t *testing.T) {
	header := NewDatabaseHeader(4096)
	data := make([]byte, 100)
	header.WriteTo(data)

	parsed, err := ParseHeader(data)
	if err != nil {
		t.Fatalf("failed to parse header: %v", err)
	}

	if parsed.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", parsed.PageSize)
	}
	if parsed.TextEncoding != 1 {
		t.Errorf("expected UTF-8 encoding, got %d", parsed.TextEncoding)
	}
	if parsed.WriteVersion != 1 {
		t.Errorf("expected write version 1, got %d", parsed.WriteVersion)
	}
}

func TestParseHeaderInvalidMagic(t *testing.T) {
	data := make([]byte, 100)
	data[0] = 'X'

	_, err := ParseHeader(data)
	if err != ErrInvalidHeader {
		t.Errorf("expected ErrInvalidHeader, got %v", err)
	}
}

func TestParseHeaderTooShort(t *testing.T) {
	data := make([]byte, 50)

	_, err := ParseHeader(data)
	if err != ErrInvalidHeader {
		t.Errorf("expected ErrInvalidHeader, got %v", err)
	}
}

func TestNewDatabaseHeader(t *testing.T) {
	header := NewDatabaseHeader(4096)
	if string(header.Magic[:]) != SQLiteMagic {
		t.Error("magic string mismatch")
	}
	if header.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", header.PageSize)
	}
	if header.TextEncoding != 1 {
		t.Errorf("expected UTF-8 encoding, got %d", header.TextEncoding)
	}
}

func TestDatabaseHeaderRoundTrip(t *testing.T) {
	original := NewDatabaseHeader(4096)
	original.DatabaseSize = 100
	original.UserVersion = 42

	buf := make([]byte, 100)
	original.WriteTo(buf)

	parsed, err := ParseHeader(buf)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if parsed.DatabaseSize != original.DatabaseSize {
		t.Errorf("database size mismatch: %d vs %d", parsed.DatabaseSize, original.DatabaseSize)
	}
	if parsed.UserVersion != original.UserVersion {
		t.Errorf("user version mismatch: %d vs %d", parsed.UserVersion, original.UserVersion)
	}
}

func TestIsValidPageSize(t *testing.T) {
	tests := []struct {
		size     int
		expected bool
	}{
		{512, true},
		{1024, true},
		{2048, true},
		{4096, true},
		{8192, true},
		{16384, true},
		{65536, true},
		{256, false},
		{1000, false},
		{70000, false},
		{0, false},
	}

	for _, tt := range tests {
		result := IsValidPageSize(tt.size)
		if result != tt.expected {
			t.Errorf("IsValidPageSize(%d) = %v, expected %v", tt.size, result, tt.expected)
		}
	}
}

func TestPageTypeString(t *testing.T) {
	tests := []struct {
		pt       PageType
		expected string
	}{
		{PageTypeLockByte, "lock-byte"},
		{PageTypeFreelist, "freelist"},
		{PageTypePointerMap, "pointer-map"},
		{PageTypeInteriorIdx, "interior-index"},
		{PageTypeInteriorTbl, "interior-table"},
		{PageTypeLeafIdx, "leaf-index"},
		{PageTypeLeafTbl, "leaf-table"},
		{0x00, "unknown"},
	}

	for _, tt := range tests {
		result := tt.pt.String()
		if result != tt.expected {
			t.Errorf("PageType(%d).String() = %q, expected %q", tt.pt, result, tt.expected)
		}
	}
}

func TestHeaderGetTextEncoding(t *testing.T) {
	h := &DatabaseHeader{TextEncoding: 1}
	if h.GetTextEncoding() != "UTF-8" {
		t.Errorf("expected UTF-8, got %s", h.GetTextEncoding())
	}

	h.TextEncoding = 2
	if h.GetTextEncoding() != "UTF-16le" {
		t.Errorf("expected UTF-16le, got %s", h.GetTextEncoding())
	}
}

func TestPageDataCopy(t *testing.T) {
	page := NewPage(1, 100)
	original := []byte("hello world")
	page.SetData(original)

	if !bytes.Equal(page.Data[:len(original)], original) {
		t.Error("data not copied correctly")
	}
}
