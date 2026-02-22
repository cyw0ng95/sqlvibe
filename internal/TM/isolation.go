package TM

import (
	"fmt"
	"strings"
)

// IsolationLevel defines the transaction isolation level.
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads — the transaction can see uncommitted
	// changes made by other transactions.
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted (default) — only committed data is visible.
	ReadCommitted
	// Serializable — all reads are snapshot-consistent; no phantom reads.
	Serializable
)

func (il IsolationLevel) String() string {
	switch il {
	case ReadUncommitted:
		return "READ UNCOMMITTED"
	case ReadCommitted:
		return "READ COMMITTED"
	case Serializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// ParseIsolationLevel parses a string such as "READ COMMITTED" or
// "SERIALIZABLE" (case-insensitive) into the corresponding IsolationLevel.
func ParseIsolationLevel(s string) (IsolationLevel, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "READ UNCOMMITTED", "READ_UNCOMMITTED":
		return ReadUncommitted, nil
	case "READ COMMITTED", "READ_COMMITTED":
		return ReadCommitted, nil
	case "SERIALIZABLE":
		return Serializable, nil
	default:
		return ReadCommitted, fmt.Errorf("unknown isolation level %q", s)
	}
}

// IsolationConfig holds per-database isolation settings.
type IsolationConfig struct {
	Level        IsolationLevel
	BusyTimeout  int // milliseconds; 0 = no timeout
}

// NewIsolationConfig returns the default configuration (READ COMMITTED,
// no timeout).
func NewIsolationConfig() *IsolationConfig {
	return &IsolationConfig{
		Level:       ReadCommitted,
		BusyTimeout: 0,
	}
}

// SetIsolationLevel changes the isolation level and returns an error if the
// level string is invalid.
func (ic *IsolationConfig) SetIsolationLevel(level string) error {
	il, err := ParseIsolationLevel(level)
	if err != nil {
		return err
	}
	ic.Level = il
	return nil
}

// GetIsolationLevel returns the current isolation level as a string.
func (ic *IsolationConfig) GetIsolationLevel() string {
	return ic.Level.String()
}

// LockState returns the lock state associated with the isolation config.
// For SERIALIZABLE mode this should be connected to a shared LockState so
// that phantom prevention can be enforced.
func (ic *IsolationConfig) LockState() *LockState {
	return NewLockState()
}
