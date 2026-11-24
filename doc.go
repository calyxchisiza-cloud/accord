// Package accord keeps one replicated log for a small cluster.
// A write is visible only after a majority of nodes have stored it.
// The state machine is a key-value map plus a fencing lock.
package accord

import "errors"

const (
	maxValue = 64 << 10
	maxLog   = 10000
)

var (
	ErrNotLeader = errors.New("not the leader")
	ErrNoQuorum  = errors.New("no majority")
	ErrHeld      = errors.New("lock is held")
	ErrNotHeld   = errors.New("lock is not held")
	ErrNotFound  = errors.New("key not found")
	ErrInvalid   = errors.New("name is not valid")
	ErrClosed    = errors.New("node is closed")
	ErrTimeout   = errors.New("timed out")
	ErrTooLarge  = errors.New("value is too large")
	ErrFull      = errors.New("log is full")
)

// Entry is one command in the replicated log.
type Entry struct {
	Term  uint64 `json:"term"`
	Kind  string `json:"kind,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Owner string `json:"owner,omitempty"`
}
