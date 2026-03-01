package TM

import (
	"sync"
	"sync/atomic"
)

// VersionedValue represents a single versioned entry in the MVCC store.
type VersionedValue struct {
	CommitID uint64
	Value    interface{}
	Deleted  bool
}

// Snapshot represents a read-consistent view of the store at a particular
// commit watermark.
type Snapshot struct {
	CommitID   uint64
	ActiveTxns map[uint64]bool
}

// MVCCStore is a lightweight multi-version store that maps string keys to a
// chain of VersionedValues. Readers supply a Snapshot to see only the
// versions that are visible to them; writers append new versions and receive
// a commit ID.
type MVCCStore struct {
	mu       sync.RWMutex
	versions map[string][]*VersionedValue
	commitID uint64 // monotonically increasing; atomic access
}

// NewMVCCStore returns an empty MVCCStore.
func NewMVCCStore() *MVCCStore {
	return &MVCCStore{
		versions: make(map[string][]*VersionedValue),
	}
}

// Snapshot returns a read-consistent snapshot at the current commit watermark.
func (m *MVCCStore) Snapshot() *Snapshot {
	cid := atomic.LoadUint64(&m.commitID)
	return &Snapshot{CommitID: cid, ActiveTxns: make(map[uint64]bool)}
}

// Get returns the most recent value for key that is visible under snapshot.
// Returns (nil, false) when the key has no visible version or was deleted.
func (m *MVCCStore) Get(key string, snap *Snapshot) (interface{}, bool) {
	m.mu.RLock()
	chain := m.versions[key]
	m.mu.RUnlock()

	// Walk from newest to oldest to find the first visible version.
	for i := len(chain) - 1; i >= 0; i-- {
		v := chain[i]
		if v.CommitID > snap.CommitID {
			continue // written after our snapshot
		}
		if snap.ActiveTxns[v.CommitID] {
			continue // written by a still-active transaction
		}
		if v.Deleted {
			return nil, false
		}
		return v.Value, true
	}
	return nil, false
}

// Put appends a new version for key and returns its commit ID. The caller is
// responsible for ensuring that concurrent writes to the same key are
// serialised (e.g. under an exclusive lock at the table level).
func (m *MVCCStore) Put(key string, value interface{}) uint64 {
	cid := atomic.AddUint64(&m.commitID, 1)
	vv := &VersionedValue{CommitID: cid, Value: value}

	m.mu.Lock()
	m.versions[key] = append(m.versions[key], vv)
	m.mu.Unlock()

	return cid
}

// Delete marks key as deleted at a new commit ID and returns that ID.
func (m *MVCCStore) Delete(key string) uint64 {
	cid := atomic.AddUint64(&m.commitID, 1)
	vv := &VersionedValue{CommitID: cid, Deleted: true}

	m.mu.Lock()
	m.versions[key] = append(m.versions[key], vv)
	m.mu.Unlock()

	return cid
}

// GC removes versions that are older than the oldest needed snapshot.
// keepBelow defines the minimum commit ID that should be retained; all
// versions with CommitID < keepBelow (except the last one per key that is
// <= keepBelow) are pruned.
func (m *MVCCStore) GC(keepBelow uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	pruned := 0
	for key, chain := range m.versions {
		// Find the last version whose CommitID < keepBelow (the baseline).
		baseline := -1
		for i, v := range chain {
			if v.CommitID < keepBelow {
				baseline = i
			}
		}
		// Keep from baseline onward (the baseline itself plus anything newer).
		if baseline > 0 {
			pruned += baseline
			m.versions[key] = chain[baseline:]
		}
		// If the chain consists only of a single deleted baseline, remove key.
		if len(m.versions[key]) == 1 && m.versions[key][0].Deleted &&
			m.versions[key][0].CommitID < keepBelow {
			delete(m.versions, key)
		}
	}
	return pruned
}

// CommitID returns the current monotonic commit counter.
func (m *MVCCStore) CommitID() uint64 {
	return atomic.LoadUint64(&m.commitID)
}
