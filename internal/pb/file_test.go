package pb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileOpen(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	f, err := OpenFile(testPath, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Error("file was not created")
	}
}

func TestFileReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	f, err := OpenFile(testPath, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	data := []byte("hello world")
	n, err := f.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != len(data) {
		t.Errorf("wrote %d bytes, expected %d", n, len(data))
	}

	buf := make([]byte, len(data))
	n, err = f.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("read %q, expected %q", string(buf), string(data))
	}
}

func TestFileSize(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	f, err := OpenFile(testPath, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	data := make([]byte, 1000)
	f.WriteAt(data, 0)

	size, err := f.Size()
	if err != nil {
		t.Fatalf("failed to get size: %v", err)
	}
	if size != 1000 {
		t.Errorf("size is %d, expected 1000", size)
	}
}

func TestFileLock(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.db")

	f, err := OpenFile(testPath, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	err = f.Lock(LockShared)
	if err != nil {
		t.Fatalf("failed to lock: %v", err)
	}

	err = f.Unlock()
	if err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}
}
