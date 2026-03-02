package DS

import (
	"encoding/json"
	"testing"
)

// TestWALEntryTotalSize tests total-size computation.
func TestWALEntryTotalSize(t *testing.T) {
	if got := WALEntryTotalSize(0); got != 4 {
		t.Errorf("WALEntryTotalSize(0) = %d; want 4", got)
	}
	if got := WALEntryTotalSize(10); got != 14 {
		t.Errorf("WALEntryTotalSize(10) = %d; want 14", got)
	}
	if got := WALEntryTotalSize(100); got != 104 {
		t.Errorf("WALEntryTotalSize(100) = %d; want 104", got)
	}
}

// TestWALEncodeDecodeRoundtrip tests encode → decode round-trip.
func TestWALEncodeDecodeRoundtrip(t *testing.T) {
	body := []byte(`{"op":1,"vals":[1,2,3]}`)
	totalSize := WALEntryTotalSize(len(body))
	buf := make([]byte, totalSize)

	written := WALEncodeEntry(buf, body)
	if written == 0 {
		t.Fatal("WALEncodeEntry returned 0")
	}
	if written != totalSize {
		t.Errorf("written = %d; want %d", written, totalSize)
	}

	// Decode length prefix.
	bodyLen := WALDecodeEntryLength(buf)
	if int(bodyLen) != len(body) {
		t.Errorf("decoded length = %d; want %d", bodyLen, len(body))
	}

	// Decode body.
	decoded := WALDecodeEntryBody(buf, 0)
	if string(decoded) != string(body) {
		t.Errorf("decoded body = %q; want %q", decoded, body)
	}
}

// TestWALIsValidEntry tests valid/invalid entry detection.
func TestWALIsValidEntry(t *testing.T) {
	body := []byte(`{"x":42}`)
	buf := make([]byte, WALEntryTotalSize(len(body)))
	WALEncodeEntry(buf, body)

	if !WALIsValidEntry(buf) {
		t.Error("WALIsValidEntry should return true for valid entry")
	}
	if WALIsValidEntry(nil) {
		t.Error("WALIsValidEntry should return false for nil")
	}
	// Truncated buffer should be invalid.
	if WALIsValidEntry(buf[:3]) {
		t.Error("WALIsValidEntry should return false for truncated buffer")
	}
}

// TestWALCreateInsertRecord tests insert record encoding.
func TestWALCreateInsertRecord(t *testing.T) {
	jsonVals := []byte(`[{"t":1,"i":42}]`)
	buf := make([]byte, 256)
	n := WALCreateInsertRecord(buf, jsonVals)
	if n == 0 {
		t.Fatal("WALCreateInsertRecord returned 0")
	}
	body := WALDecodeEntryBody(buf[:n], 0)
	if len(body) == 0 {
		t.Fatal("could not decode insert record body")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("invalid JSON in insert record: %v — body: %q", err, body)
	}
	op, _ := m["op"].(float64)
	if int(op) != CWALInsert {
		t.Errorf("insert record op = %v; want %d", op, CWALInsert)
	}
}

// TestWALCreateDeleteRecord tests delete record encoding.
func TestWALCreateDeleteRecord(t *testing.T) {
	buf := make([]byte, 256)
	n := WALCreateDeleteRecord(buf, 7)
	if n == 0 {
		t.Fatal("WALCreateDeleteRecord returned 0")
	}
	body := WALDecodeEntryBody(buf[:n], 0)
	if len(body) == 0 {
		t.Fatal("could not decode delete record body")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("invalid JSON in delete record: %v — body: %q", err, body)
	}
	op, _ := m["op"].(float64)
	if int(op) != CWALDelete {
		t.Errorf("delete record op = %v; want %d", op, CWALDelete)
	}
	idx, _ := m["idx"].(float64)
	if int(idx) != 7 {
		t.Errorf("delete record idx = %v; want 7", idx)
	}
}

// TestWALCreateUpdateRecord tests update record encoding.
func TestWALCreateUpdateRecord(t *testing.T) {
	jsonVals := []byte(`[{"t":3,"s":"new"}]`)
	buf := make([]byte, 256)
	n := WALCreateUpdateRecord(buf, 3, jsonVals)
	if n == 0 {
		t.Fatal("WALCreateUpdateRecord returned 0")
	}
	body := WALDecodeEntryBody(buf[:n], 0)
	if len(body) == 0 {
		t.Fatal("could not decode update record body")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("invalid JSON in update record: %v — body: %q", err, body)
	}
	op, _ := m["op"].(float64)
	if int(op) != CWALUpdate {
		t.Errorf("update record op = %v; want %d", op, CWALUpdate)
	}
	idx, _ := m["idx"].(float64)
	if int(idx) != 3 {
		t.Errorf("update record idx = %v; want 3", idx)
	}
}

// TestWALEncode_BufferTooSmall tests graceful handling of undersized buffers.
func TestWALEncode_BufferTooSmall(t *testing.T) {
	body := []byte(`{"op":1,"vals":[1,2,3]}`)
	buf := make([]byte, 3) // too small
	n := WALEncodeEntry(buf, body)
	if n != 0 {
		t.Errorf("expected 0 for buffer too small, got %d", n)
	}
}

// TestWALDecodeEntryBody_Empty tests empty/nil input handling.
func TestWALDecodeEntryBody_Empty(t *testing.T) {
	if WALDecodeEntryBody(nil, 0) != nil {
		t.Error("expected nil for nil input")
	}
	if WALDecodeEntryBody([]byte{}, 0) != nil {
		t.Error("expected nil for empty input")
	}
}
