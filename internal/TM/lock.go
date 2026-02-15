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
		locks:    make(map[string]*ResourceLock),
		deadlock: make(chan struct{}),
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
