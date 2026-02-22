package sqlvibe

import (
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// ---- ShardedMap -------------------------------------------------------

const numShards = 16

// fnv32Pool reuses FNV-32a hash instances to avoid per-call allocation.
var fnv32Pool = sync.Pool{
	New: func() interface{} { return fnv.New32a() },
}

// ShardedMap is a concurrent map partitioned into numShards shards to reduce
// lock contention on multicore workloads.
type ShardedMap struct {
	shards [numShards]shardedMapShard
}

type shardedMapShard struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewShardedMap creates a ready-to-use ShardedMap.
func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{}
	for i := range sm.shards {
		sm.shards[i].data = make(map[string]interface{})
	}
	return sm
}

func (sm *ShardedMap) shard(key string) *shardedMapShard {
	h := fnv32Pool.Get().(hash.Hash32)
	h.Reset()
	_, _ = h.Write([]byte(key))
	idx := h.Sum32() % numShards
	fnv32Pool.Put(h)
	return &sm.shards[idx]
}

// Get returns the value for key and whether it was found.
func (sm *ShardedMap) Get(key string) (interface{}, bool) {
	s := sm.shard(key)
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

// Set stores val under key.
func (sm *ShardedMap) Set(key string, val interface{}) {
	s := sm.shard(key)
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// Delete removes key from the map.
func (sm *ShardedMap) Delete(key string) {
	s := sm.shard(key)
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Keys returns all keys in the map (snapshot; order is not guaranteed).
func (sm *ShardedMap) Keys() []string {
	var keys []string
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k := range s.data {
			keys = append(keys, k)
		}
		s.mu.RUnlock()
	}
	return keys
}

// ---- AtomicCounter ----------------------------------------------------

// AtomicCounter is a thread-safe int64 counter backed by atomic operations.
type AtomicCounter struct {
	val int64
}

// Add atomically adds n to the counter and returns the new value.
func (ac *AtomicCounter) Add(n int64) int64 {
	return atomic.AddInt64(&ac.val, n)
}

// Get atomically reads the current counter value.
func (ac *AtomicCounter) Get() int64 {
	return atomic.LoadInt64(&ac.val)
}

// Set atomically sets the counter to n.
func (ac *AtomicCounter) Set(n int64) {
	atomic.StoreInt64(&ac.val, n)
}

// ---- LockMetrics ------------------------------------------------------

// LockMetrics tracks lock acquisition and contention statistics.
type LockMetrics struct {
	Acquisitions AtomicCounter
	Contentions  AtomicCounter
	WaitNs       AtomicCounter
}

// RecordAcquisition increments the acquisition counter.
func (lm *LockMetrics) RecordAcquisition() {
	lm.Acquisitions.Add(1)
}

// RecordContention increments the contention counter and accumulates wait time.
func (lm *LockMetrics) RecordContention(waitNs int64) {
	lm.Contentions.Add(1)
	lm.WaitNs.Add(waitNs)
}
