package storage

import (
	"math/rand"
	"time"
)

const maxSkipListLevel = 16

type skipNode struct {
	key     Value
	rowIdxs []uint32    // all row indices associated with this key
	forward []*skipNode // one per level
}

// SkipList provides O(log n) ordered key → row index mapping.
type SkipList struct {
	head   *skipNode
	levels int
	length int // unique keys
	rng    *rand.Rand
}

// NewSkipList creates an empty SkipList.
func NewSkipList() *SkipList {
	head := &skipNode{forward: make([]*skipNode, maxSkipListLevel)}
	return &SkipList{head: head, levels: 1, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// Len returns the number of unique keys.
func (sl *SkipList) Len() int { return sl.length }

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxSkipListLevel && sl.rng.Float64() < 0.5 {
		level++
	}
	return level
}

// Insert adds the key → rowIdx mapping.
func (sl *SkipList) Insert(key Value, rowIdx uint32) {
	update := make([]*skipNode, maxSkipListLevel)
	cur := sl.head
	for i := sl.levels - 1; i >= 0; i-- {
		for cur.forward[i] != nil && Compare(cur.forward[i].key, key) < 0 {
			cur = cur.forward[i]
		}
		update[i] = cur
	}

	next := update[0].forward[0]
	if next != nil && Compare(next.key, key) == 0 {
		// Key already exists – just append rowIdx if not duplicate
		for _, r := range next.rowIdxs {
			if r == rowIdx {
				return
			}
		}
		next.rowIdxs = append(next.rowIdxs, rowIdx)
		return
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.levels {
		for i := sl.levels; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.levels = newLevel
	}

	n := &skipNode{
		key:     key,
		rowIdxs: []uint32{rowIdx},
		forward: make([]*skipNode, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	sl.length++
}

// Delete removes the key → rowIdx pair.
func (sl *SkipList) Delete(key Value, rowIdx uint32) {
	update := make([]*skipNode, maxSkipListLevel)
	cur := sl.head
	for i := sl.levels - 1; i >= 0; i-- {
		for cur.forward[i] != nil && Compare(cur.forward[i].key, key) < 0 {
			cur = cur.forward[i]
		}
		update[i] = cur
	}

	target := update[0].forward[0]
	if target == nil || Compare(target.key, key) != 0 {
		return
	}

	// Remove rowIdx from target
	newIdxs := target.rowIdxs[:0]
	for _, r := range target.rowIdxs {
		if r != rowIdx {
			newIdxs = append(newIdxs, r)
		}
	}
	target.rowIdxs = newIdxs

	if len(target.rowIdxs) > 0 {
		return // still has other rows for this key
	}

	// Remove the node entirely
	for i := 0; i < sl.levels; i++ {
		if update[i].forward[i] != target {
			break
		}
		update[i].forward[i] = target.forward[i]
	}
	for sl.levels > 1 && sl.head.forward[sl.levels-1] == nil {
		sl.levels--
	}
	sl.length--
}

// Find returns all row indices for key.
func (sl *SkipList) Find(key Value) []uint32 {
	cur := sl.head
	for i := sl.levels - 1; i >= 0; i-- {
		for cur.forward[i] != nil && Compare(cur.forward[i].key, key) < 0 {
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]
	if cur == nil || Compare(cur.key, key) != 0 {
		return nil
	}
	out := make([]uint32, len(cur.rowIdxs))
	copy(out, cur.rowIdxs)
	return out
}

// Range returns all row indices for keys in [lo, hi] (or (lo, hi) if !inclusive).
func (sl *SkipList) Range(lo, hi Value, inclusive bool) []uint32 {
	cur := sl.head
	for i := sl.levels - 1; i >= 0; i-- {
		for cur.forward[i] != nil && Compare(cur.forward[i].key, lo) < 0 {
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]

	var out []uint32
	for cur != nil {
		cmp := Compare(cur.key, hi)
		if cmp > 0 {
			break
		}
		if cmp == 0 && !inclusive {
			break
		}
		cmpLo := Compare(cur.key, lo)
		if cmpLo == 0 && !inclusive {
			cur = cur.forward[0]
			continue
		}
		out = append(out, cur.rowIdxs...)
		cur = cur.forward[0]
	}
	return out
}

// Min returns the smallest key. ok is false if empty.
func (sl *SkipList) Min() (Value, bool) {
	n := sl.head.forward[0]
	if n == nil {
		return NullValue(), false
	}
	return n.key, true
}

// Max returns the largest key. ok is false if empty.
func (sl *SkipList) Max() (Value, bool) {
	cur := sl.head
	for i := sl.levels - 1; i >= 0; i-- {
		for cur.forward[i] != nil {
			cur = cur.forward[i]
		}
	}
	if cur == sl.head {
		return NullValue(), false
	}
	return cur.key, true
}
