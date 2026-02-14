package ds

import (
	"path/filepath"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/pb"
)

func TestBTreeCreate(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := pb.OpenFile(testPath, pb.O_CREATE|pb.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	bt := NewBTree(pm, 0, true)
	if !bt.IsTable() {
		t.Error("expected table B-Tree")
	}
	if bt.RootPage() != 0 {
		t.Error("expected root page 0 for new B-Tree")
	}
}

func TestBTreeInsert(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := pb.OpenFile(testPath, pb.O_CREATE|pb.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	bt := NewBTree(pm, 0, true)

	err = bt.Insert([]byte{0x01}, []byte("value1"))
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	if bt.RootPage() == 0 {
		t.Error("root page should not be 0 after insert")
	}
}

func TestBTreeSearch(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := pb.OpenFile(testPath, pb.O_CREATE|pb.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	bt := NewBTree(pm, 0, true)

	bt.Insert([]byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte("value1"))
	bt.Insert([]byte{0, 0, 0, 0, 0, 0, 0, 2}, []byte("value2"))

	result, err := bt.Search([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if string(result) != "value1" {
		t.Errorf("expected value1, got %s", string(result))
	}
}

func TestBTreeCursor(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	file, err := pb.OpenFile(testPath, pb.O_CREATE|pb.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	bt := NewBTree(pm, 0, true)

	bt.Insert([]byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte("value1"))
	bt.Insert([]byte{0, 0, 0, 0, 0, 0, 0, 2}, []byte("value2"))

	cursor, err := bt.First()
	if err != nil {
		t.Fatalf("failed to get first: %v", err)
	}
	if cursor == nil {
		t.Fatal("cursor should not be nil")
	}
	defer cursor.Close()

	key, val, err := cursor.Next()
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	t.Logf("key=%x, val=%s", key, val)
}
