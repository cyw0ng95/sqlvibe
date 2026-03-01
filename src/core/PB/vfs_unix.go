//go:build unix || linux || darwin

package PB

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cyw0ng95/sqlvibe/internal/SF/vfs"
)

// UnixVFS implements a Unix/POSIX file system VFS
type UnixVFS struct{}

// NewUnixVFS creates a new Unix VFS
func NewUnixVFS() *UnixVFS {
	return &UnixVFS{}
}

func (uvfs *UnixVFS) Name() string {
	return "unix"
}

func (uvfs *UnixVFS) Open(name string, flags int) (vfs.File, error) {
	osFlags := 0

	if flags&vfs.OpenReadOnly != 0 {
		osFlags = os.O_RDONLY
	} else if flags&vfs.OpenReadWrite != 0 {
		osFlags = os.O_RDWR
	}

	if flags&vfs.OpenCreate != 0 {
		osFlags |= os.O_CREATE
	}

	if flags&vfs.OpenExclusive != 0 {
		osFlags |= os.O_EXCL
	}

	f, err := os.OpenFile(name, osFlags, 0644)
	if err != nil {
		return nil, err
	}

	return &UnixFile{
		file:     f,
		name:     name,
		lockType: vfs.LockNone,
	}, nil
}

func (uvfs *UnixVFS) Delete(name string) error {
	return os.Remove(name)
}

func (uvfs *UnixVFS) Access(name string, flags int) (bool, error) {
	_, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	// File exists
	if flags == vfs.AccessExists {
		return true, nil
	}

	// Check read/write permissions
	mode := 0
	if flags == vfs.AccessReadWrite {
		mode = 0600
	} else if flags == vfs.AccessReadOnly {
		mode = 0400
	}

	return syscall.Access(name, uint32(mode)) == nil, nil
}

func (uvfs *UnixVFS) FullPathname(name string) (string, error) {
	return filepath.Abs(name)
}

func (uvfs *UnixVFS) Randomness(buf []byte) error {
	_, err := rand.Read(buf)
	return err
}

func (uvfs *UnixVFS) Sleep(microseconds int) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

func (uvfs *UnixVFS) CurrentTime() (float64, error) {
	// Julian day number: days since noon, January 1, 4713 BC
	const unixEpochJulian = 2440587.5
	now := time.Now()
	seconds := float64(now.Unix())
	days := seconds / 86400.0
	return unixEpochJulian + days, nil
}

// UnixFile represents a Unix file
type UnixFile struct {
	file     *os.File
	name     string
	lockType int
}

func (f *UnixFile) Close() error {
	// Release any locks
	if f.lockType != vfs.LockNone {
		f.Unlock(vfs.LockNone)
	}
	return f.file.Close()
}

func (f *UnixFile) Read(p []byte, offset int64) (int, error) {
	return f.file.ReadAt(p, offset)
}

func (f *UnixFile) Write(p []byte, offset int64) (int, error) {
	return f.file.WriteAt(p, offset)
}

func (f *UnixFile) Truncate(size int64) error {
	return f.file.Truncate(size)
}

func (f *UnixFile) Sync(flags int) error {
	return f.file.Sync()
}

func (f *UnixFile) FileSize() (int64, error) {
	info, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (f *UnixFile) Lock(lockType int) error {
	fd := int(f.file.Fd())

	// Convert lock type to flock type
	how := syscall.LOCK_SH // Shared lock
	if lockType >= vfs.LockReserved {
		how = syscall.LOCK_EX // Exclusive lock
	}

	// Use non-blocking lock
	how |= syscall.LOCK_NB

	err := syscall.Flock(fd, how)
	if err != nil {
		return err
	}

	f.lockType = lockType
	return nil
}

func (f *UnixFile) Unlock(lockType int) error {
	fd := int(f.file.Fd())

	err := syscall.Flock(fd, syscall.LOCK_UN)
	if err != nil {
		return err
	}

	f.lockType = vfs.LockNone
	return nil
}

func (f *UnixFile) CheckReservedLock() (bool, error) {
	return f.lockType >= vfs.LockReserved, nil
}

func (f *UnixFile) FileControl(op int, arg interface{}) error {
	// No special file controls for basic Unix files
	return nil
}

func (f *UnixFile) SectorSize() int {
	// Most Unix systems use 512-byte sectors
	return 512
}

func (f *UnixFile) DeviceCharacteristics() int {
	// Unix files support atomic writes up to a certain size
	const (
		DeviceCharacteristicsAtomic512 = 0x00000001
		DeviceCharacteristicsAtomic1K  = 0x00000002
	)

	return DeviceCharacteristicsAtomic512 | DeviceCharacteristicsAtomic1K
}

func init() {
	// Register Unix VFS as default on Unix systems
	unixVFS := NewUnixVFS()
	vfs.RegisterVFS("unix", unixVFS, true)
}
