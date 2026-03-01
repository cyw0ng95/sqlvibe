package PB

import (
	"path/filepath"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/SF/vfs"
)

// --- MemoryVFS ---------------------------------------------------------------

func TestMemoryVFS_Name(t *testing.T) {
	mvfs := NewMemoryVFS()
	if mvfs.Name() != "memory" {
		t.Errorf("expected 'memory', got %q", mvfs.Name())
	}
}

func TestMemoryVFS_DeleteAccessFullPathname(t *testing.T) {
	mvfs := NewMemoryVFS()

	// Access non-existent file
	exists, err := mvfs.Access(":memory:", 0)
	if err != nil {
		t.Fatalf("Access: %v", err)
	}
	// The file may or may not exist depending on init()
	_ = exists

	// Open to create
	f, err := mvfs.Open("testfile", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Access created file
	exists, err = mvfs.Access("testfile", 0)
	if err != nil {
		t.Fatalf("Access after create: %v", err)
	}
	if !exists {
		t.Error("expected file to exist")
	}

	// FullPathname
	path, err := mvfs.FullPathname("testfile")
	if err != nil {
		t.Fatalf("FullPathname: %v", err)
	}
	if path != "testfile" {
		t.Errorf("expected 'testfile', got %q", path)
	}

	// Delete
	err = mvfs.Delete("testfile")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Access after delete
	exists, err = mvfs.Access("testfile", 0)
	if err != nil {
		t.Fatalf("Access after delete: %v", err)
	}
	if exists {
		t.Error("expected file not to exist after delete")
	}

	// Delete non-existent file
	err = mvfs.Delete("nonexistent")
	if err == nil {
		t.Error("expected error deleting non-existent file")
	}
}

func TestMemoryVFS_Randomness(t *testing.T) {
	mvfs := NewMemoryVFS()
	buf := make([]byte, 16)
	err := mvfs.Randomness(buf)
	if err != nil {
		t.Fatalf("Randomness: %v", err)
	}
}

func TestMemoryVFS_Sleep(t *testing.T) {
	mvfs := NewMemoryVFS()
	err := mvfs.Sleep(1) // 1 microsecond
	if err != nil {
		t.Fatalf("Sleep: %v", err)
	}
}

func TestMemoryVFS_CurrentTime(t *testing.T) {
	mvfs := NewMemoryVFS()
	jd, err := mvfs.CurrentTime()
	if err != nil {
		t.Fatalf("CurrentTime: %v", err)
	}
	// Julian day for 2020+ should be > 2458849
	if jd < 2458849 {
		t.Errorf("Julian day too small: %f", jd)
	}
}

// --- MemoryFile extra methods -------------------------------------------------

func TestMemoryFile_Truncate(t *testing.T) {
	mvfs := NewMemoryVFS()
	f, err := mvfs.Open("trunc_test", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Write data
	data := []byte("hello world")
	_, err = f.Write(data, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	size, _ := f.FileSize()
	if size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}

	// Truncate smaller
	err = f.Truncate(5)
	if err != nil {
		t.Fatalf("Truncate(5): %v", err)
	}
	size, _ = f.FileSize()
	if size != 5 {
		t.Errorf("expected size 5 after truncate, got %d", size)
	}

	// Truncate larger (zero-extend)
	err = f.Truncate(20)
	if err != nil {
		t.Fatalf("Truncate(20): %v", err)
	}
	size, _ = f.FileSize()
	if size != 20 {
		t.Errorf("expected size 20 after extend, got %d", size)
	}
}

func TestMemoryFile_LockUnlock(t *testing.T) {
	mvfs := NewMemoryVFS()
	f, err := mvfs.Open("lock_test", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Lock shared
	err = f.Lock(vfs.LockShared)
	if err != nil {
		t.Fatalf("Lock(Shared): %v", err)
	}

	// Lock same level again (no-op)
	err = f.Lock(vfs.LockShared)
	if err != nil {
		t.Fatalf("Lock(Shared) again: %v", err)
	}

	// CheckReservedLock
	reserved, err := f.CheckReservedLock()
	if err != nil {
		t.Fatalf("CheckReservedLock: %v", err)
	}
	if reserved {
		t.Error("expected not reserved with shared lock")
	}

	// Upgrade to exclusive
	err = f.Lock(vfs.LockExclusive)
	if err != nil {
		t.Fatalf("Lock(Exclusive): %v", err)
	}

	reserved, _ = f.CheckReservedLock()
	if !reserved {
		t.Error("expected reserved with exclusive lock")
	}

	// Unlock
	err = f.Unlock(vfs.LockNone)
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestMemoryFile_FileControl(t *testing.T) {
	mvfs := NewMemoryVFS()
	f, err := mvfs.Open("fc_test", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	err = f.FileControl(0, nil)
	if err != nil {
		t.Fatalf("FileControl: %v", err)
	}
}

func TestMemoryFile_SectorSizeDeviceCharacteristics(t *testing.T) {
	mvfs := NewMemoryVFS()
	f, err := mvfs.Open("sc_test", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if f.SectorSize() <= 0 {
		t.Errorf("expected positive sector size, got %d", f.SectorSize())
	}

	if f.DeviceCharacteristics() == 0 {
		t.Error("expected non-zero device characteristics")
	}
}

// --- UnixVFS -----------------------------------------------------------------

func TestUnixVFS_Name(t *testing.T) {
	uvfs := NewUnixVFS()
	if uvfs.Name() != "unix" {
		t.Errorf("expected 'unix', got %q", uvfs.Name())
	}
}

func TestUnixVFS_DeleteAccessFullPathname(t *testing.T) {
	dir := t.TempDir()
	uvfs := NewUnixVFS()
	path := filepath.Join(dir, "test.db")

	// Access non-existent
	exists, err := uvfs.Access(path, 0)
	if err != nil {
		t.Fatalf("Access: %v", err)
	}
	if exists {
		t.Error("expected not exists")
	}

	// Open to create
	f, err := uvfs.Open(path, vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Access existing
	exists, err = uvfs.Access(path, vfs.AccessExists)
	if err != nil {
		t.Fatalf("Access after create: %v", err)
	}
	if !exists {
		t.Error("expected exists after create")
	}

	// FullPathname
	abs, err := uvfs.FullPathname(path)
	if err != nil {
		t.Fatalf("FullPathname: %v", err)
	}
	if abs == "" {
		t.Error("expected non-empty absolute path")
	}

	// Delete
	f.Close()
	err = uvfs.Delete(path)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestUnixVFS_Randomness(t *testing.T) {
	uvfs := NewUnixVFS()
	buf := make([]byte, 8)
	err := uvfs.Randomness(buf)
	if err != nil {
		t.Fatalf("Randomness: %v", err)
	}
}

func TestUnixVFS_Sleep(t *testing.T) {
	uvfs := NewUnixVFS()
	err := uvfs.Sleep(1)
	if err != nil {
		t.Fatalf("Sleep: %v", err)
	}
}

func TestUnixVFS_CurrentTime(t *testing.T) {
	uvfs := NewUnixVFS()
	jd, err := uvfs.CurrentTime()
	if err != nil {
		t.Fatalf("CurrentTime: %v", err)
	}
	if jd < 2458849 {
		t.Errorf("Julian day too small: %f", jd)
	}
}

// --- UnixFile extra methods --------------------------------------------------

func TestUnixFile_CheckReservedLockFileControlSectorSizeDevChar(t *testing.T) {
	dir := t.TempDir()
	uvfs := NewUnixVFS()
	path := filepath.Join(dir, "unix_extra.db")

	f, err := uvfs.Open(path, vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// CheckReservedLock — no lock held, should be false
	reserved, err := f.CheckReservedLock()
	if err != nil {
		t.Fatalf("CheckReservedLock: %v", err)
	}
	if reserved {
		t.Error("expected CheckReservedLock=false with no lock held")
	}

	// FileControl
	err = f.FileControl(0, nil)
	if err != nil {
		t.Fatalf("FileControl: %v", err)
	}

	// SectorSize
	if f.SectorSize() <= 0 {
		t.Errorf("expected positive sector size, got %d", f.SectorSize())
	}

	// DeviceCharacteristics
	dc := f.DeviceCharacteristics()
	if dc == 0 {
		t.Error("expected non-zero DeviceCharacteristics")
	}
}

func TestUnixFile_CloseWithLock(t *testing.T) {
	dir := t.TempDir()
	uvfs := NewUnixVFS()
	path := filepath.Join(dir, "close_lock.db")

	f, err := uvfs.Open(path, vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Acquire a lock, then close (Close should release it)
	_ = f.Lock(vfs.LockShared)
	err = f.Close()
	if err != nil {
		t.Fatalf("Close with lock: %v", err)
	}
}

func TestUnixFile_FileSize(t *testing.T) {
	dir := t.TempDir()
	uvfs := NewUnixVFS()
	path := filepath.Join(dir, "size.db")

	f, err := uvfs.Open(path, vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	data := make([]byte, 100)
	f.Write(data, 0)
	size, err := f.FileSize()
	if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
	if size != 100 {
		t.Errorf("expected 100, got %d", size)
	}
}

// --- parseVFSURI + OpenFile (additional URI formats) -------------------------

func TestParseVFSURI_FileURI(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "uri_test.db")

	f, err := OpenFile("file:"+path, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile with file: URI: %v", err)
	}
	f.Close()
}

func TestParseVFSURI_FileURIWithVFS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vfs_uri.db")

	f, err := OpenFile("file:"+path+"?vfs=unix", O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile with file:?vfs=unix URI: %v", err)
	}
	f.Close()
}

func TestOpenFile_VFSNotFound(t *testing.T) {
	_, err := OpenFile("file:test.db?vfs=nosuchvfs", O_CREATE|O_RDWR)
	if err == nil {
		t.Error("expected error for unknown VFS")
	}
}

func TestOpenFile_Open(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "open_iface.db")

	f, err := OpenFile(path, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	// vfsFile.Open delegates to OpenFile
	f2, err := f.Open(path, O_RDWR)
	if err != nil {
		t.Fatalf("f.Open: %v", err)
	}
	f2.Close()
}

func TestOpenFile_LockUpgradeAndUnlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lock_upgrade.db")

	f, err := OpenFile(path, O_CREATE|O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	// Unlock when already unlocked (no-op)
	err = f.Unlock()
	if err != nil {
		t.Fatalf("Unlock when not locked: %v", err)
	}

	// Lock reserved
	err = f.Lock(LockReserved)
	if err != nil {
		t.Fatalf("Lock(Reserved): %v", err)
	}

	// Lock same level (no-op — curLock >= lockType)
	err = f.Lock(LockReserved)
	if err != nil {
		t.Fatalf("Lock(Reserved) again: %v", err)
	}

	err = f.Unlock()
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}
