package sqlvibe

import (
	"context"
	stderrors "errors"

	"github.com/cyw0ng95/sqlvibe/internal/SF/errors"
)

// RowCallback is reserved for future streaming result delivery (v0.9.14+).
// It will allow callers to receive result rows one at a time without materialising
// the full result set in memory.
type RowCallback func(cols []string, row []interface{}) error

// execState carries per-query execution context threaded through the call stack.
// It is a lightweight struct created once per ExecContext/QueryContext invocation.
type execState struct {
	ctx         context.Context
	rowsChecked int64 // counter for periodic context checks
}

// newExecState creates a new execState for the given context.
func newExecState(ctx context.Context) *execState {
	return &execState{ctx: ctx}
}

// check returns a sqlvibe error if the context has been cancelled or timed out.
// Returns nil if execution should continue.
func (s *execState) check() error {
	if s == nil {
		return nil
	}
	select {
	case <-s.ctx.Done():
		return wrapCtxErr(s.ctx.Err())
	default:
		return nil
	}
}

// checkEvery256 increments the row counter and checks context every 256 rows.
// Inline this in hot paths to amortise the context check overhead.
func (s *execState) checkEvery256() error {
	if s == nil {
		return nil
	}
	s.rowsChecked++
	if s.rowsChecked%256 == 0 {
		return s.check()
	}
	return nil
}

// wrapCtxErr maps context errors to sqlvibe native error codes.
// context.DeadlineExceeded → errors.SVDB_QUERY_TIMEOUT
// context.Canceled → returned as-is (user cancellation)
func wrapCtxErr(err error) error {
	if err == nil {
		return nil
	}
	if stderrors.Is(err, context.DeadlineExceeded) {
		return errors.Errorf(errors.SVDB_QUERY_TIMEOUT, "query timeout: %v", err)
	}
	return err
}
