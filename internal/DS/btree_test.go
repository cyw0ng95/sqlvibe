package DS

import (
	"bytes"
	"testing"
)

func TestBTree_NewAndBasics(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	
	// Allocate root page
	rootPage, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate root page: %v", err)
	}

	bt := NewBTree(pm, rootPage, true)

	if bt.RootPage() != rootPage {
		t.Errorf("expected root page %d, got %d", rootPage, bt.RootPage())
	}

	if !bt.IsTable() {
		t.Error("expected table B-Tree")
	}
}

func TestBTree_SearchEmpty(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, true) // Empty tree

	result, err := bt.Search([]byte{1, 2, 3})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if result != nil {
		t.Error("expected nil result for empty tree")
	}
}

func TestBTree_InsertAndSearch(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, true)

	// Insert a key-value pair
	key := make([]byte, 9)
	PutVarint(key, 42)
	value := []byte("Hello, World!")

	if err := bt.Insert(key, value); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Search for the key
	result, err := bt.Search(key)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if !bytes.Equal(result, value) {
		t.Errorf("expected value %v, got %v", value, result)
	}

	// Search for non-existent key
	key2 := make([]byte, 9)
	PutVarint(key2, 100)
	result2, err := bt.Search(key2)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if result2 != nil {
		t.Error("expected nil for non-existent key")
	}
}

func TestBTree_MultipleInserts(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, true)

	// Insert multiple key-value pairs
	entries := []struct {
		rowid int64
		value string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{5, "Eve"},
		{4, "David"}, // Insert out of order
	}

	for _, entry := range entries {
		key := make([]byte, 9)
		PutVarint(key, entry.rowid)
		if err := bt.Insert(key, []byte(entry.value)); err != nil {
			t.Fatalf("insert rowid=%d failed: %v", entry.rowid, err)
		}
	}

	// Search for all inserted keys
	for _, entry := range entries {
		key := make([]byte, 9)
		PutVarint(key, entry.rowid)
		result, err := bt.Search(key)
		if err != nil {
			t.Fatalf("search rowid=%d failed: %v", entry.rowid, err)
		}

		if !bytes.Equal(result, []byte(entry.value)) {
			t.Errorf("rowid=%d: expected %s, got %s", entry.rowid, entry.value, string(result))
		}
	}
}

func TestBTree_Cursor(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, true)

	// Insert some data
	for i := int64(1); i <= 5; i++ {
		key := make([]byte, 9)
		PutVarint(key, i)
		value := []byte{byte(i)}
		if err := bt.Insert(key, value); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Create cursor and iterate
	cursor := bt.NewCursor()
	if err := cursor.First(); err != nil {
		t.Fatalf("cursor.First() failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("cursor should be valid after First()")
	}

	// Read first entry
	key, err := cursor.Key()
	if err != nil {
		t.Fatalf("cursor.Key() failed: %v", err)
	}

	firstRowid, _ := GetVarint(key)
	if firstRowid != 1 {
		t.Errorf("expected first rowid=1, got %d", firstRowid)
	}

	value, err := cursor.Value()
	if err != nil {
		t.Fatalf("cursor.Value() failed: %v", err)
	}

	if len(value) != 1 || value[0] != 1 {
		t.Errorf("expected value [1], got %v", value)
	}
}

func TestBTree_CursorEmpty(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, true)

	cursor := bt.NewCursor()
	if err := cursor.First(); err != nil {
		t.Fatalf("cursor.First() on empty tree failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("cursor should not be valid on empty tree")
	}
}

func TestBTree_IndexTree(t *testing.T) {
	pm := setupTestPageManager(t, 4096)
	bt := NewBTree(pm, 0, false) // Index tree

	// Insert index entries
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for _, key := range keys {
		if err := bt.Insert(key, nil); err != nil {
			t.Fatalf("insert key=%s failed: %v", string(key), err)
		}
	}

	// Search for keys
	for _, key := range keys {
		result, err := bt.Search(key)
		if err != nil {
			t.Fatalf("search key=%s failed: %v", string(key), err)
		}

		// Index trees return the key itself
		if !bytes.Equal(result, key) {
			t.Errorf("key=%s: expected %v, got %v", string(key), key, result)
		}
	}
}
