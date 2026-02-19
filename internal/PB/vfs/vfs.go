package vfs

import (
"errors"
"sync"
)

// VFS (Virtual File System) interface provides an abstraction layer for file operations.
// This design is inspired by SQLite's VFS architecture and enables multiple storage
// backends (real files, memory, custom implementations).
//
// Reference: https://www.sqlite.org/vfs.html
type VFS interface {
// Name returns the name of this VFS implementation (e.g., "unix", "memory")
Name() string

// Open opens a file with the given name and flags
// Returns a File interface for performing operations
Open(name string, flags int) (File, error)

// Delete removes a file
Delete(name string) error

// Access checks if a file exists and is accessible with given flags
Access(name string, flags int) (bool, error)

// FullPathname returns the absolute pathname of a file
FullPathname(name string) (string, error)

// Randomness fills the buffer with random bytes
// Used for generating unique identifiers
Randomness(buf []byte) error

// Sleep suspends execution for at least the specified number of microseconds
Sleep(microseconds int) error

// CurrentTime returns the current time as a Julian day number
CurrentTime() (float64, error)
}

// File interface represents an open file
type File interface {
// Close closes the file
Close() error

// Read reads len(p) bytes from the file at the given offset
Read(p []byte, offset int64) (int, error)

// Write writes len(p) bytes to the file at the given offset
Write(p []byte, offset int64) (int, error)

// Truncate changes the size of the file
Truncate(size int64) error

// Sync commits the current contents of the file to stable storage
Sync(flags int) error

// FileSize returns the current size of the file
FileSize() (int64, error)

// Lock acquires a lock on the file
Lock(lockType int) error

// Unlock releases a lock on the file
Unlock(lockType int) error

// CheckReservedLock checks if any lock is held on the file
CheckReservedLock() (bool, error)

// FileControl provides access to file-specific operations
FileControl(op int, arg interface{}) error

// SectorSize returns the sector size of the device that holds the file
SectorSize() int

// DeviceCharacteristics returns device characteristics flags
DeviceCharacteristics() int
}

// File open flags (SQLite-compatible)
const (
OpenReadOnly     = 0x00000001 // Read-only
OpenReadWrite    = 0x00000002 // Read-write
OpenCreate       = 0x00000004 // Create if not exists
OpenDeleteOnClose = 0x00000008 // Delete on close
OpenExclusive    = 0x00000010 // Exclusive access
OpenMainDB       = 0x00000100 // Main database file
OpenTempDB       = 0x00000200 // Temporary database file
OpenTransientDB  = 0x00000400 // Transient database
OpenMainJournal  = 0x00000800 // Main journal file
OpenTempJournal  = 0x00001000 // Temporary journal
OpenSubJournal   = 0x00002000 // Sub-journal
OpenMasterJournal = 0x00004000 // Master journal
OpenWAL          = 0x00080000 // Write-ahead log
)

// Lock types (SQLite-compatible)
const (
LockNone      = 0 // No lock
LockShared    = 1 // Shared lock (read)
LockReserved  = 2 // Reserved lock (intent to write)
LockPending   = 3 // Pending lock (waiting for readers to finish)
LockExclusive = 4 // Exclusive lock (writing)
)

// Sync flags
const (
SyncNormal   = 0x00000002 // Normal sync
SyncFull     = 0x00000003 // Full sync (including directory)
SyncDataOnly = 0x00000010 // Sync data only, not metadata
)

// Access flags
const (
AccessExists    = 0 // Check if file exists
AccessReadWrite = 1 // Check if file is readable and writable
AccessReadOnly  = 2 // Check if file is readable
)

// VFS Registry for managing multiple VFS implementations
var (
vfsRegistry     = make(map[string]VFS)
defaultVFSName  string
vfsRegistryLock sync.RWMutex
)

var (
ErrVFSNotFound      = errors.New("VFS not found")
ErrVFSAlreadyExists = errors.New("VFS already registered")
ErrNoDefaultVFS     = errors.New("no default VFS registered")
)

// RegisterVFS registers a VFS implementation
// If makeDefault is true, this VFS becomes the default
func RegisterVFS(name string, vfs VFS, makeDefault bool) error {
if name == "" {
return errors.New("VFS name cannot be empty")
}
if vfs == nil {
return errors.New("VFS cannot be nil")
}

vfsRegistryLock.Lock()
defer vfsRegistryLock.Unlock()

if _, exists := vfsRegistry[name]; exists {
return ErrVFSAlreadyExists
}

vfsRegistry[name] = vfs

if makeDefault || defaultVFSName == "" {
defaultVFSName = name
}

return nil
}

// FindVFS finds a VFS by name
func FindVFS(name string) (VFS, error) {
vfsRegistryLock.RLock()
defer vfsRegistryLock.RUnlock()

vfs, ok := vfsRegistry[name]
if !ok {
return nil, ErrVFSNotFound
}

return vfs, nil
}

// DefaultVFS returns the default VFS
func DefaultVFS() (VFS, error) {
vfsRegistryLock.RLock()
defer vfsRegistryLock.RUnlock()

if defaultVFSName == "" {
return nil, ErrNoDefaultVFS
}

vfs, ok := vfsRegistry[defaultVFSName]
if !ok {
return nil, ErrNoDefaultVFS
}

return vfs, nil
}

// UnregisterVFS removes a VFS from the registry
func UnregisterVFS(name string) error {
vfsRegistryLock.Lock()
defer vfsRegistryLock.Unlock()

if _, ok := vfsRegistry[name]; !ok {
return ErrVFSNotFound
}

delete(vfsRegistry, name)

// If this was the default, clear the default
if defaultVFSName == name {
defaultVFSName = ""
// Set a new default if any VFS remains
for n := range vfsRegistry {
defaultVFSName = n
break
}
}

return nil
}

// ListVFS returns all registered VFS names
func ListVFS() []string {
vfsRegistryLock.RLock()
defer vfsRegistryLock.RUnlock()

names := make([]string, 0, len(vfsRegistry))
for name := range vfsRegistry {
names = append(names, name)
}

return names
}
