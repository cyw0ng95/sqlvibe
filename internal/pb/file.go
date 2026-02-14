package pb

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

var (
	ErrInvalidFlag = errors.New("invalid file flag")
	ErrNotLocked   = errors.New("file not locked")
	ErrLocked      = errors.New("file already locked")
)

const (
	O_CREATE = os.O_CREATE
	O_EXCL   = os.O_EXCL
	O_TRUNC  = os.O_TRUNC
	O_RDWR   = os.O_RDWR
	O_RDONLY = os.O_RDONLY
	O_WRONLY = os.O_WRONLY
)

type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockReserved
	LockExclusive
)

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

type osFile struct {
	f       *os.File
	lock    sync.Mutex
	curLock LockType
}

func (f *osFile) Open(path string, flag int) (File, error) {
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: file}, nil
}

func (f *osFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.f.ReadAt(p, off)
}

func (f *osFile) WriteAt(p []byte, off int64) (n int, err error) {
	return f.f.WriteAt(p, off)
}

func (f *osFile) Sync() error {
	return f.f.Sync()
}

func (f *osFile) Close() error {
	return f.f.Close()
}

func (f *osFile) Lock(lockType LockType) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if lockType == LockNone {
		return f.Unlock()
	}

	if f.curLock >= lockType {
		return nil
	}

	flockType := syscall.LOCK_SH
	if lockType == LockExclusive {
		flockType = syscall.LOCK_EX
	}

	err := syscall.Flock(int(f.f.Fd()), flockType|syscall.LOCK_NB)
	if err != nil {
		return err
	}
	f.curLock = lockType
	return nil
}

func (f *osFile) Unlock() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.curLock == LockNone {
		return nil
	}

	err := syscall.Flock(int(f.f.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}
	f.curLock = LockNone
	return nil
}

func (f *osFile) Size() (int64, error) {
	stat, err := f.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (f *osFile) Truncate(size int64) error {
	return f.f.Truncate(size)
}

func OpenFile(path string, flag int) (File, error) {
	f := &osFile{}
	return f.Open(path, flag)
}
