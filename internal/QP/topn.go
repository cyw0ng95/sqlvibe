package QP

import "container/heap"

// TopN collects the N smallest rows according to a user-supplied comparator.
// It is used to implement ORDER BY … LIMIT N without materialising all rows
// first: rows are streamed in and only the top-N are retained in a bounded
// max-heap of size N.  The root of the heap is always the "worst" row seen so
// far, so new rows are inserted only when they are better (i.e. the comparator
// says they should come before the current worst).
type TopN struct {
	n    int
	heap *topNHeap
}

// topNHeap is a max-heap by the user comparator.  The maximum element is at
// the root so it can be quickly replaced when a better candidate arrives.
type topNHeap struct {
	data [][]interface{}
	// less(a, b) reports whether a is "better than" b (i.e. should appear
	// earlier in the final ORDER BY result).
	less func(a, b []interface{}) bool
}

func (h *topNHeap) Len() int { return len(h.data) }

// Less returns true when h.data[j] is "worse" than h.data[i], keeping the
// worst row at the root (max-heap).
func (h *topNHeap) Less(i, j int) bool {
	return h.less(h.data[j], h.data[i])
}

func (h *topNHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *topNHeap) Push(x interface{}) { h.data = append(h.data, x.([]interface{})) }

func (h *topNHeap) Pop() interface{} {
	n := len(h.data)
	x := h.data[n-1]
	h.data = h.data[:n-1]
	return x
}

// NewTopN creates a TopN accumulator that retains the n best rows according to
// less.  less(a, b) must return true when a should appear before b in the
// ORDER BY result (i.e. a is "better").
func NewTopN(n int, less func(a, b []interface{}) bool) *TopN {
	return &TopN{
		n: n,
		heap: &topNHeap{
			data: make([][]interface{}, 0, n),
			less: less,
		},
	}
}

// Push considers row for inclusion in the top-N result set.  The row is kept
// when the accumulator holds fewer than N rows, or when row is better than the
// current worst row.
func (tn *TopN) Push(row []interface{}) {
	if tn.heap.Len() < tn.n {
		heap.Push(tn.heap, row)
	} else if tn.n > 0 && tn.heap.less(row, tn.heap.data[0]) {
		// row is better than the current worst: replace the worst.
		tn.heap.data[0] = row
		heap.Fix(tn.heap, 0)
	}
}

// Result returns the accumulated rows in sorted order (best first).
// After calling Result the TopN accumulator should not be used further.
func (tn *TopN) Result() [][]interface{} {
	n := tn.heap.Len()
	out := make([][]interface{}, n)
	for i := n - 1; i >= 0; i-- {
		out[i] = heap.Pop(tn.heap).([]interface{})
	}
	return out
}
