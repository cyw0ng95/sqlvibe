package PB

import (
	"errors"
	"sync"
)

type memoryFile struct {
	mu       sync.RWMutex
	data     []byte
	locked   bool
	lockType LockType
}

func (f *memoryFile) Open(path string, flag int) (File, error) {
	return &memoryFile{
		data: make([]byte, 0),
	}, nil
}

func (f *memoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if off >= int64(len(f.data)) {
		return 0, errors.New("read beyond end of file")
	}

	n = copy(p, f.data[off:])
	if n < len(p) {
		return n, errors.New("short read")
	}
	return n, nil
}

func (f *memoryFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Extend buffer if needed
	endPos := off + int64(len(p))
	if endPos > int64(len(f.data)) {
		newData := make([]byte, endPos)
		copy(newData, f.data)
		f.data = newData
	}

	n = copy(f.data[off:], p)
	return n, nil
}

func (f *memoryFile) Sync() error {
	// In-memory files don't need syncing
	return nil
}

func (f *memoryFile) Close() error {
	// Clear the data to free memory
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = nil
	return nil
}

func (f *memoryFile) Lock(lockType LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if lockType == LockNone {
		return f.Unlock()
	}

	if f.locked && f.lockType >= lockType {
		return nil
	}

	f.locked = true
	f.lockType = lockType
	return nil
}

func (f *memoryFile) Unlock() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.locked = false
	f.lockType = LockNone
	return nil
}

func (f *memoryFile) Size() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return int64(len(f.data)), nil
}

func (f *memoryFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if size > int64(len(f.data)) {
		newData := make([]byte, size)
		copy(newData, f.data)
		f.data = newData
	} else {
		f.data = f.data[:size]
	}
	return nil
}
