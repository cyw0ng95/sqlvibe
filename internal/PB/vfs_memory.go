package PB

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/sqlvibe/sqlvibe/internal/SF/vfs"
)

// MemoryVFS implements an in-memory VFS for testing and :memory: databases
type MemoryVFS struct {
	mu    sync.RWMutex
	files map[string]*MemoryFile
}

// NewMemoryVFS creates a new memory-based VFS
func NewMemoryVFS() *MemoryVFS {
	return &MemoryVFS{
		files: make(map[string]*MemoryFile),
	}
}

func (mvfs *MemoryVFS) Name() string {
	return "memory"
}

func (mvfs *MemoryVFS) Open(name string, flags int) (vfs.File, error) {
	mvfs.mu.Lock()
	defer mvfs.mu.Unlock()

	// Check if file exists
	file, exists := mvfs.files[name]

	// Handle create flag
	if !exists {
		if flags&vfs.OpenCreate == 0 && flags&vfs.OpenReadOnly != 0 {
			return nil, errors.New("file does not exist")
		}
		file = &MemoryFile{
			name:     name,
			data:     make([]byte, 0),
			lockType: vfs.LockNone,
		}
		mvfs.files[name] = file
	}

	// Return a copy of the file handle
	return file, nil
}

func (mvfs *MemoryVFS) Delete(name string) error {
	mvfs.mu.Lock()
	defer mvfs.mu.Unlock()

	if _, exists := mvfs.files[name]; !exists {
		return errors.New("file does not exist")
	}

	delete(mvfs.files, name)
	return nil
}

func (mvfs *MemoryVFS) Access(name string, flags int) (bool, error) {
	mvfs.mu.RLock()
	defer mvfs.mu.RUnlock()

	_, exists := mvfs.files[name]
	return exists, nil
}

func (mvfs *MemoryVFS) FullPathname(name string) (string, error) {
	return name, nil
}

func (mvfs *MemoryVFS) Randomness(buf []byte) error {
	_, err := rand.Read(buf)
	return err
}

func (mvfs *MemoryVFS) Sleep(microseconds int) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

func (mvfs *MemoryVFS) CurrentTime() (float64, error) {
	// Julian day number: days since noon, January 1, 4713 BC
	const unixEpochJulian = 2440587.5
	now := time.Now()
	seconds := float64(now.Unix())
	days := seconds / 86400.0
	return unixEpochJulian + days, nil
}

// MemoryFile represents an in-memory file
type MemoryFile struct {
	mu       sync.RWMutex
	name     string
	data     []byte
	lockType int
}

func (f *MemoryFile) Close() error {
	return nil
}

func (f *MemoryFile) Read(p []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if offset >= int64(len(f.data)) {
		return 0, nil
	}

	n := copy(p, f.data[offset:])
	return n, nil
}

func (f *MemoryFile) Write(p []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	// Extend data if necessary
	end := offset + int64(len(p))
	if end > int64(len(f.data)) {
		newData := make([]byte, end)
		copy(newData, f.data)
		f.data = newData
	}

	n := copy(f.data[offset:], p)
	return n, nil
}

func (f *MemoryFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if size < 0 {
		return errors.New("negative size")
	}

	if size > int64(len(f.data)) {
		newData := make([]byte, size)
		copy(newData, f.data)
		f.data = newData
	} else {
		f.data = f.data[:size]
	}

	return nil
}

func (f *MemoryFile) Sync(flags int) error {
	// Nothing to sync for memory file
	return nil
}

func (f *MemoryFile) FileSize() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return int64(len(f.data)), nil
}

func (f *MemoryFile) Lock(lockType int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Simple lock escalation check
	if lockType <= f.lockType {
		return nil // Already have stronger or equal lock
	}

	f.lockType = lockType
	return nil
}

func (f *MemoryFile) Unlock(lockType int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if lockType >= f.lockType {
		f.lockType = vfs.LockNone
	}

	return nil
}

func (f *MemoryFile) CheckReservedLock() (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.lockType >= vfs.LockReserved, nil
}

func (f *MemoryFile) FileControl(op int, arg interface{}) error {
	// No special file controls for memory files
	return nil
}

func (f *MemoryFile) SectorSize() int {
	// Memory has no sectors, return a reasonable default
	return 512
}

func (f *MemoryFile) DeviceCharacteristics() int {
	// Memory files are fast and atomic
	const (
		DeviceCharacteristicsAtomic512  = 0x00000001
		DeviceCharacteristicsAtomic1K   = 0x00000002
		DeviceCharacteristicsAtomic2K   = 0x00000004
		DeviceCharacteristicsAtomic4K   = 0x00000008
		DeviceCharacteristicsAtomic8K   = 0x00000010
		DeviceCharacteristicsAtomic16K  = 0x00000020
		DeviceCharacteristicsAtomic32K  = 0x00000040
		DeviceCharacteristicsAtomic64K  = 0x00000080
		DeviceCharacteristicsSafeAppend = 0x00000200
		DeviceCharacteristicsSequential = 0x00000400
	)

	return DeviceCharacteristicsAtomic512 |
		DeviceCharacteristicsAtomic1K |
		DeviceCharacteristicsAtomic2K |
		DeviceCharacteristicsAtomic4K |
		DeviceCharacteristicsAtomic8K |
		DeviceCharacteristicsAtomic16K |
		DeviceCharacteristicsAtomic32K |
		DeviceCharacteristicsAtomic64K
}

func init() {
	// Register memory VFS on package initialization
	memVFS := NewMemoryVFS()
	vfs.RegisterVFS("memory", memVFS, false)
}
