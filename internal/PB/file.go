package PB

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/sqlvibe/sqlvibe/internal/SF/vfs"
	"github.com/sqlvibe/sqlvibe/internal/util"
)

var (
	ErrInvalidFlag = errors.New("invalid file flag")
	ErrNotLocked   = errors.New("file not locked")
	ErrLocked      = errors.New("file already locked")
)

const (
	O_CREATE = vfs.OpenCreate
	O_EXCL   = vfs.OpenExclusive
	O_TRUNC  = 0x0200 // Not directly mapped
	O_RDWR   = vfs.OpenReadWrite
	O_RDONLY = vfs.OpenReadOnly
	O_WRONLY = vfs.OpenReadWrite // Map to read-write for now
)

type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockReserved
	LockExclusive
)

// File interface for database file operations
// This is the legacy interface that will be maintained for compatibility
type File interface {
	Open(path string, flag int) (File, error)
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Sync() error
	Close() error
	Lock(lockType LockType) error
	Unlock() error
	Size() (int64, error)
	Truncate(size int64) error
}

// vfsFile wraps a VFS File to implement the PB.File interface
type vfsFile struct {
	vfsHandle vfs.File
	vfs       vfs.VFS
	path      string
	lock      sync.Mutex
	curLock   LockType
}

func (f *vfsFile) Open(path string, flag int) (File, error) {
	// This should not be called on an existing file handle
	return OpenFile(path, flag)
}

func (f *vfsFile) ReadAt(p []byte, off int64) (n int, err error) {
	util.AssertNotNil(p, "buffer")
	util.Assert(off >= 0, "offset cannot be negative: %d", off)
	return f.vfsHandle.Read(p, off)
}

func (f *vfsFile) WriteAt(p []byte, off int64) (n int, err error) {
	util.AssertNotNil(p, "buffer")
	util.Assert(off >= 0, "offset cannot be negative: %d", off)
	return f.vfsHandle.Write(p, off)
}

func (f *vfsFile) Sync() error {
	return f.vfsHandle.Sync(vfs.SyncNormal)
}

func (f *vfsFile) Close() error {
	return f.vfsHandle.Close()
}

func (f *vfsFile) Lock(lockType LockType) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if lockType == LockNone {
		return f.Unlock()
	}

	if f.curLock >= lockType {
		return nil
	}

	vfsLockType := vfs.LockShared
	if lockType >= LockReserved {
		vfsLockType = vfs.LockReserved
	}
	if lockType == LockExclusive {
		vfsLockType = vfs.LockExclusive
	}

	err := f.vfsHandle.Lock(vfsLockType)
	if err != nil {
		return err
	}
	f.curLock = lockType
	return nil
}

func (f *vfsFile) Unlock() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.curLock == LockNone {
		return nil
	}

	err := f.vfsHandle.Unlock(vfs.LockNone)
	if err != nil {
		return err
	}
	f.curLock = LockNone
	return nil
}

func (f *vfsFile) Size() (int64, error) {
	return f.vfsHandle.FileSize()
}

func (f *vfsFile) Truncate(size int64) error {
	util.Assert(size >= 0, "truncate size cannot be negative: %d", size)
	return f.vfsHandle.Truncate(size)
}

// parseVFSURI parses a database URI to extract the VFS name and actual path
// Supports formats like:
//   - ":memory:" -> memory VFS, ":memory:" path
//   - "file:test.db" -> default VFS, "test.db" path
//   - "file:test.db?vfs=unix" -> unix VFS, "test.db" path
//   - "test.db" -> default VFS, "test.db" path
func parseVFSURI(uri string) (vfsName string, path string, err error) {
	// Handle :memory: special case
	if uri == ":memory:" {
		return "memory", ":memory:", nil
	}

	// Check if it's a URI with scheme
	if strings.Contains(uri, ":") && !strings.HasPrefix(uri, "/") && !strings.HasPrefix(uri, ".") {
		// Try to parse as URL
		u, parseErr := url.Parse(uri)
		if parseErr == nil && u.Scheme == "file" {
			path = u.Path
			if path == "" {
				path = u.Opaque
			}

			// Check for vfs parameter
			query := u.Query()
			if vfsParam := query.Get("vfs"); vfsParam != "" {
				return vfsParam, path, nil
			}

			// No VFS specified, use default
			return "", path, nil
		}
	}

	// Plain path, use default VFS
	return "", uri, nil
}

// OpenFile opens a file using the VFS system
// Supports :memory: databases and VFS selection via URI
func OpenFile(uri string, flag int) (File, error) {
	util.Assert(uri != "", "URI cannot be empty")
	
	// Parse URI to get VFS name and path
	vfsName, path, err := parseVFSURI(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URI: %w", err)
	}

	// Get the appropriate VFS
	var selectedVFS vfs.VFS
	if vfsName == "" {
		// Use default VFS
		selectedVFS, err = vfs.DefaultVFS()
		if err != nil {
			return nil, fmt.Errorf("no default VFS available: %w", err)
		}
	} else {
		// Use specified VFS
		selectedVFS, err = vfs.FindVFS(vfsName)
		if err != nil {
			return nil, fmt.Errorf("VFS '%s' not found: %w", vfsName, err)
		}
	}

	// Convert PB flags to VFS flags
	vfsFlags := 0
	if flag&O_RDONLY != 0 {
		vfsFlags |= vfs.OpenReadOnly
	} else if flag&O_RDWR != 0 {
		vfsFlags |= vfs.OpenReadWrite
	}
	if flag&O_CREATE != 0 {
		vfsFlags |= vfs.OpenCreate
	}
	if flag&O_EXCL != 0 {
		vfsFlags |= vfs.OpenExclusive
	}

	// Open file using VFS
	vfsHandle, err := selectedVFS.Open(path, vfsFlags)
	if err != nil {
		return nil, err
	}

	return &vfsFile{
		vfsHandle: vfsHandle,
		vfs:       selectedVFS,
		path:      path,
		curLock:   LockNone,
	}, nil
}

