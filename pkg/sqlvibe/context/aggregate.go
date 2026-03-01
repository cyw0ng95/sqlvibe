package context

import "fmt"

// AnyValueAcc accumulates the first non-NULL value seen in a group, implementing ANY_VALUE().
type AnyValueAcc struct {
	val    interface{}
	hasVal bool
}

// Accumulate updates the accumulator with val, keeping only the first non-NULL value.
func (a *AnyValueAcc) Accumulate(val interface{}) {
	if !a.hasVal && val != nil {
		a.val = val
		a.hasVal = true
	}
}

// Result returns the accumulated any_value result (nil if no non-NULL values were seen).
func (a *AnyValueAcc) Result() interface{} {
	if !a.hasVal {
		return nil
	}
	return a.val
}

// ModeAcc accumulates frequency counts for values in a group, implementing MODE().
type ModeAcc struct {
	freq  map[string]int
	vals  map[string]interface{}
	order []string
}

// NewModeAcc creates a new ModeAcc ready for accumulation.
func NewModeAcc() *ModeAcc {
	return &ModeAcc{
		freq: make(map[string]int),
		vals: make(map[string]interface{}),
	}
}

// Accumulate adds val to the frequency count. NULL values are ignored.
func (m *ModeAcc) Accumulate(val interface{}) {
	if val == nil {
		return
	}
	key := fmt.Sprintf("%v", val)
	if _, exists := m.freq[key]; !exists {
		m.order = append(m.order, key)
		m.vals[key] = val
	}
	m.freq[key]++
}

// Result returns the value with the highest frequency.
// On ties, the value that appeared first is returned.
// Returns nil if no values were accumulated.
func (m *ModeAcc) Result() interface{} {
	if len(m.order) == 0 {
		return nil
	}
	bestKey := m.order[0]
	bestCount := m.freq[bestKey]
	for _, key := range m.order[1:] {
		if m.freq[key] > bestCount {
			bestCount = m.freq[key]
			bestKey = key
		}
	}
	return m.vals[bestKey]
}
