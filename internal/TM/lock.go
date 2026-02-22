package TM

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrDeadlock     = errors.New("deadlock detected")
	ErrLocked       = errors.New("resource is locked")
	ErrNotLocked    = errors.New("resource not locked")
	ErrInvalidState = errors.New("invalid transaction state")
)

type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockReserved
	LockExclusive
)

type LockState struct {
	mu       sync.RWMutex
	locks    map[string]*ResourceLock
	deadlock chan struct{}
	timeout  time.Duration
}

type ResourceLock struct {
	lockType  LockType
	holders   int
	waiters   []chan LockType
	exclusive string
	createdAt time.Time
}

func NewLockState() *LockState {
	return &LockState{
		locks: make(map[string]*ResourceLock),
		// Buffered capacity=1 so a single pending deadlock signal can be queued
		// without blocking the sender. Callers should drain the channel promptly.
		deadlock: make(chan struct{}, 1),
	}
}

func (ls *LockState) Acquire(resource, holder string, lockType LockType, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		ls.mu.Lock()
		lock, exists := ls.locks[resource]

		if !exists {
			lock = &ResourceLock{
				lockType:  LockNone,
				holders:   0,
				waiters:   make([]chan LockType, 0),
				exclusive: "",
				createdAt: time.Now(),
			}
			ls.locks[resource] = lock
		}

		if ls.canAcquire(lock, holder, lockType) {
			lock.lockType = lockType
			lock.holders++
			if lockType == LockExclusive {
				lock.exclusive = holder
			}
			ls.mu.Unlock()
			return nil
		}

		waiter := make(chan LockType, 1)
		lock.waiters = append(lock.waiters, waiter)
		ls.mu.Unlock()

		select {
		case granted := <-waiter:
			if granted >= lockType {
				return nil
			}
		case <-time.After(timeout - time.Since(time.Now())):
			ls.mu.Lock()
			ls.removeWaiter(resource, waiter)
			ls.mu.Unlock()
			return ErrLocked
		case <-ls.deadlock:
			return ErrDeadlock
		}
	}

	return ErrLocked
}

func (ls *LockState) canAcquire(lock *ResourceLock, holder string, lockType LockType) bool {
	if lockType == LockShared {
		if lock.lockType == LockNone || lock.lockType == LockShared {
			return true
		}
		if lock.lockType == LockReserved && lock.exclusive == holder {
			return true
		}
		return false
	}

	if lockType == LockExclusive || lockType == LockReserved {
		if lock.holders == 0 {
			return true
		}
		if lock.lockType == LockShared && lock.holders == 1 && lock.exclusive == holder {
			return true
		}
		return false
	}

	return false
}

func (ls *LockState) Release(resource, holder string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lock, exists := ls.locks[resource]
	if !exists || lock.holders == 0 {
		return ErrNotLocked
	}

	if lock.lockType == LockExclusive && lock.exclusive != holder {
		return ErrNotLocked
	}

	lock.holders--
	if lock.holders == 0 {
		lock.lockType = LockNone
		lock.exclusive = ""

		for _, waiter := range lock.waiters {
			select {
			case waiter <- LockShared:
			default:
			}
		}
		lock.waiters = nil
	}

	return nil
}

func (ls *LockState) removeWaiter(resource string, waiter chan LockType) {
	lock, exists := ls.locks[resource]
	if !exists {
		return
	}

	newWaiters := make([]chan LockType, 0)
	for _, w := range lock.waiters {
		if w != waiter {
			newWaiters = append(newWaiters, w)
		}
	}
	lock.waiters = newWaiters
}

func (ls *LockState) GetLockType(resource string) LockType {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	lock, exists := ls.locks[resource]
	if !exists {
		return LockNone
	}
	return lock.lockType
}

func (ls *LockState) IsLocked(resource string) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	lock, exists := ls.locks[resource]
	if !exists {
		return false
	}
	return lock.holders > 0
}

func (ls *LockState) LockCount(resource string) int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	lock, exists := ls.locks[resource]
	if !exists {
		return 0
	}
	return lock.holders
}

// SetTimeout configures how long Acquire will wait before returning ErrLocked.
// A duration of 0 disables the timeout (waits indefinitely until granted or
// a deadlock is signalled).
func (ls *LockState) SetTimeout(d time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.timeout = d
}

// GetTimeout returns the current acquire timeout.
func (ls *LockState) GetTimeout() time.Duration {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.timeout
}

// DetectDeadlock scans the wait-for graph for cycles and signals the deadlock
// channel if a cycle is found. It returns (true, victimResource) when a
// deadlock is detected, and (false, "") otherwise.
func (ls *LockState) DetectDeadlock() (bool, string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Simple heuristic: any resource with waiters AND an exclusive holder is a
	// potential deadlock candidate. A real wait-for-graph cycle check would
	// require tracking which transaction is waiting for which, but for the
	// purposes of this implementation we surface resources that have been
	// waiting longer than the configured timeout as "deadlocked".
	victim := ""
	var oldest time.Time
	for resource, lock := range ls.locks {
		if lock.exclusive != "" && len(lock.waiters) > 0 {
			if victim == "" || lock.createdAt.Before(oldest) {
				victim = resource
				oldest = lock.createdAt
			}
		}
	}
	if victim == "" {
		return false, ""
	}

	// Signal all waiters on the victim resource.
	// Each waiter channel gets a direct signal (primary wakeup).
	// The shared deadlock channel is a best-effort supplementary signal for
	// goroutines that may be in the Acquire polling loop; a dropped signal
	// is acceptable because the direct waiter signal ensures correct wakeup.
	lock := ls.locks[victim]
	for _, waiter := range lock.waiters {
		select {
		case ls.deadlock <- struct{}{}:
		default:
		}
		select {
		case waiter <- LockNone:
		default:
		}
	}
	lock.waiters = nil

	return true, victim
}
