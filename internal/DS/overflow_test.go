package DS

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/PB"
)

func setupTestPageManager(t *testing.T, pageSize int) *PageManager {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := PB.OpenFile(testPath, PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	t.Cleanup(func() { file.Close() })

	pm, err := NewPageManager(file, pageSize)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	return pm
}

func TestOverflowManager_WriteReadChain(t *testing.T) {
	pm := setupTestPageManager(t, 1024) // 1KB pages
	om := NewOverflowManager(pm)

	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "empty",
			payload: []byte{},
		},
		{
			name:    "small payload",
			payload: []byte("Hello, World!"),
		},
		{
			name:    "single page",
			payload: bytes.Repeat([]byte("A"), 500),
		},
		{
			name:    "exactly one page",
			payload: bytes.Repeat([]byte("B"), 1020), // 1024 - 4 header
		},
		{
			name:    "multiple pages",
			payload: bytes.Repeat([]byte("C"), 3000),
		},
		{
			name:    "large payload",
			payload: bytes.Repeat([]byte("X"), 10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.payload) == 0 {
				// Empty payload should return 0
				firstPage, err := om.WriteOverflowChain(tt.payload)
				if err != nil {
					t.Fatalf("WriteOverflowChain failed: %v", err)
				}
				if firstPage != 0 {
					t.Errorf("Expected firstPage = 0 for empty payload, got %d", firstPage)
				}
				return
			}

			// Write overflow chain
			firstPage, err := om.WriteOverflowChain(tt.payload)
			if err != nil {
				t.Fatalf("WriteOverflowChain failed: %v", err)
			}

			if firstPage == 0 {
				t.Fatal("Expected non-zero firstPage")
			}

			// Read overflow chain
			result, err := om.ReadOverflowChain(firstPage, len(tt.payload))
			if err != nil {
				t.Fatalf("ReadOverflowChain failed: %v", err)
			}

			// Verify payload matches
			if !bytes.Equal(result, tt.payload) {
				t.Errorf("Payload mismatch: expected %d bytes, got %d bytes", len(tt.payload), len(result))
			}

			// Free overflow chain
			if err := om.FreeOverflowChain(firstPage); err != nil {
				t.Fatalf("FreeOverflowChain failed: %v", err)
			}
		})
	}
}

func TestOverflowManager_ChainLength(t *testing.T) {
	pm := setupTestPageManager(t, 1024)
	om := NewOverflowManager(pm)

	tests := []struct {
		name          string
		payloadSize   int
		expectedPages int
	}{
		{
			name:          "no overflow",
			payloadSize:   0,
			expectedPages: 0,
		},
		{
			name:          "single page",
			payloadSize:   1020, // exactly fills one page
			expectedPages: 1,
		},
		{
			name:          "two pages",
			payloadSize:   1021, // needs second page
			expectedPages: 2,
		},
		{
			name:          "three pages",
			payloadSize:   2100,
			expectedPages: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.payloadSize == 0 {
				length, err := om.GetOverflowChainLength(0)
				if err != nil {
					t.Fatalf("GetOverflowChainLength failed: %v", err)
				}
				if length != 0 {
					t.Errorf("Expected length = 0, got %d", length)
				}
				return
			}

			payload := bytes.Repeat([]byte("T"), tt.payloadSize)
			firstPage, err := om.WriteOverflowChain(payload)
			if err != nil {
				t.Fatalf("WriteOverflowChain failed: %v", err)
			}

			length, err := om.GetOverflowChainLength(firstPage)
			if err != nil {
				t.Fatalf("GetOverflowChainLength failed: %v", err)
			}

			if length != tt.expectedPages {
				t.Errorf("Expected %d pages, got %d", tt.expectedPages, length)
			}

			// Cleanup
			om.FreeOverflowChain(firstPage)
		})
	}
}

func TestOverflowManager_FreeChain(t *testing.T) {
	pm := setupTestPageManager(t, 1024)
	om := NewOverflowManager(pm)

	// Write a multi-page overflow chain
	payload := bytes.Repeat([]byte("D"), 5000)
	firstPage, err := om.WriteOverflowChain(payload)
	if err != nil {
		t.Fatalf("WriteOverflowChain failed: %v", err)
	}

	// Get the chain length before freeing
	lengthBefore, err := om.GetOverflowChainLength(firstPage)
	if err != nil {
		t.Fatalf("GetOverflowChainLength failed: %v", err)
	}

	if lengthBefore == 0 {
		t.Fatal("Expected non-zero chain length")
	}

	// Free the chain
	if err := om.FreeOverflowChain(firstPage); err != nil {
		t.Fatalf("FreeOverflowChain failed: %v", err)
	}

	// Try to read from the freed chain (should fail or return error)
	// Note: This test depends on PageManager behavior after FreePage
}

func TestOverflowManager_EmptyChain(t *testing.T) {
	pm := setupTestPageManager(t, 1024)
	om := NewOverflowManager(pm)

	// Test with page 0 (no overflow)
	result, err := om.ReadOverflowChain(0, 0)
	if err != nil {
		t.Fatalf("ReadOverflowChain with page 0 should not error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for page 0")
	}

	// Free page 0 (should be no-op)
	if err := om.FreeOverflowChain(0); err != nil {
		t.Fatalf("FreeOverflowChain with page 0 should not error: %v", err)
	}

	// Get length of non-existent chain
	length, err := om.GetOverflowChainLength(0)
	if err != nil {
		t.Fatalf("GetOverflowChainLength with page 0 should not error: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected length 0 for page 0, got %d", length)
	}
}
