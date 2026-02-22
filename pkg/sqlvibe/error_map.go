package sqlvibe

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
)

// ToError converts an arbitrary error to a *Error with an appropriate code.
// If err is already a *Error it is returned unchanged.
// If err is nil, nil is returned.
func ToError(err error) *Error {
	if err == nil {
		return nil
	}

	// Already a sqlvibe error — return as-is.
	var se *Error
	if errors.As(err, &se) {
		return se
	}

	// Map well-known standard library sentinel errors.
	switch {
	case errors.Is(err, io.EOF):
		return &Error{Code: SVDB_DONE, Message: err.Error(), Err: err}
	case errors.Is(err, io.ErrUnexpectedEOF):
		return &Error{Code: SVDB_CORRUPT, Message: err.Error(), Err: err}
	case errors.Is(err, io.ErrShortWrite):
		return &Error{Code: SVDB_IOERR_WRITE, Message: err.Error(), Err: err}
	case errors.Is(err, io.ErrShortBuffer):
		return &Error{Code: SVDB_IOERR_SHORT_READ, Message: err.Error(), Err: err}
	case errors.Is(err, io.ErrNoProgress):
		return &Error{Code: SVDB_IOERR, Message: err.Error(), Err: err}
	case errors.Is(err, os.ErrNotExist):
		return &Error{Code: SVDB_NOTFOUND, Message: err.Error(), Err: err}
	case errors.Is(err, os.ErrExist):
		return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
	case errors.Is(err, os.ErrPermission):
		return &Error{Code: SVDB_PERM, Message: err.Error(), Err: err}
	case errors.Is(err, os.ErrClosed):
		return &Error{Code: SVDB_IOERR_CLOSE, Message: err.Error(), Err: err}
	case errors.Is(err, os.ErrProcessDone):
		return &Error{Code: SVDB_DONE, Message: err.Error(), Err: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Code: SVDB_BUSY_TIMEOUT, Message: err.Error(), Err: err}
	case errors.Is(err, context.Canceled):
		return &Error{Code: SVDB_INTERRUPT, Message: err.Error(), Err: err}
	}

	// Fallback: inspect the error message for common patterns.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "unique") || strings.Contains(msg, "duplicate"):
		return &Error{Code: SVDB_CONSTRAINT_UNIQUE, Message: err.Error(), Err: err}
	case strings.Contains(msg, "not null") || strings.Contains(msg, "notnull"):
		return &Error{Code: SVDB_CONSTRAINT_NOTNULL, Message: err.Error(), Err: err}
	case strings.Contains(msg, "foreign key") || strings.Contains(msg, "foreignkey"):
		return &Error{Code: SVDB_CONSTRAINT_FOREIGNKEY, Message: err.Error(), Err: err}
	case strings.Contains(msg, "primary key") || strings.Contains(msg, "primarykey"):
		return &Error{Code: SVDB_CONSTRAINT_PRIMARYKEY, Message: err.Error(), Err: err}
	case strings.Contains(msg, "check constraint") || strings.Contains(msg, "constraint check"):
		return &Error{Code: SVDB_CONSTRAINT_CHECK, Message: err.Error(), Err: err}
	case strings.Contains(msg, "constraint"):
		return &Error{Code: SVDB_CONSTRAINT, Message: err.Error(), Err: err}
	case strings.Contains(msg, "no such table") || strings.Contains(msg, "table not found") ||
		strings.Contains(msg, "unknown table"):
		return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
	case strings.Contains(msg, "already exists"):
		return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
	case strings.Contains(msg, "syntax error") || strings.Contains(msg, "parse error"):
		return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
	case strings.Contains(msg, "permission denied") || strings.Contains(msg, "access denied"):
		return &Error{Code: SVDB_PERM, Message: err.Error(), Err: err}
	case strings.Contains(msg, "no space") || strings.Contains(msg, "disk full"):
		return &Error{Code: SVDB_FULL, Message: err.Error(), Err: err}
	case strings.Contains(msg, "out of memory") || strings.Contains(msg, "oom"):
		return &Error{Code: SVDB_NOMEM, Message: err.Error(), Err: err}
	case strings.Contains(msg, "read-only") || strings.Contains(msg, "readonly"):
		return &Error{Code: SVDB_READONLY, Message: err.Error(), Err: err}
	case strings.Contains(msg, "corrupt") || strings.Contains(msg, "malformed"):
		return &Error{Code: SVDB_CORRUPT, Message: err.Error(), Err: err}
	case strings.Contains(msg, "lock") || strings.Contains(msg, "locked"):
		return &Error{Code: SVDB_LOCKED, Message: err.Error(), Err: err}
	case strings.Contains(msg, "busy") || strings.Contains(msg, "timeout"):
		return &Error{Code: SVDB_BUSY, Message: err.Error(), Err: err}
	case strings.Contains(msg, "interrupted") || strings.Contains(msg, "canceled"):
		return &Error{Code: SVDB_INTERRUPT, Message: err.Error(), Err: err}
	case strings.Contains(msg, "io error") || strings.Contains(msg, "i/o error"):
		return &Error{Code: SVDB_IOERR, Message: err.Error(), Err: err}
	case strings.Contains(msg, "schema"):
		return &Error{Code: SVDB_SCHEMA, Message: err.Error(), Err: err}
	case strings.Contains(msg, "range") || strings.Contains(msg, "out of range"):
		return &Error{Code: SVDB_RANGE, Message: err.Error(), Err: err}
	case strings.Contains(msg, "too big") || strings.Contains(msg, "too large"):
		return &Error{Code: SVDB_TOOBIG, Message: err.Error(), Err: err}
	case strings.Contains(msg, "mismatch") || strings.Contains(msg, "type mismatch"):
		return &Error{Code: SVDB_MISMATCH, Message: err.Error(), Err: err}
	case strings.Contains(msg, "authorization") || strings.Contains(msg, "unauthorized"):
		return &Error{Code: SVDB_AUTH, Message: err.Error(), Err: err}
	default:
		return &Error{Code: SVDB_ERROR, Message: err.Error(), Err: err}
	}
}
