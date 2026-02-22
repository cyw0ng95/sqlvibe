package sqlvibe

import (
	"errors"
	"fmt"
)

// Error is a structured sqlvibe error carrying an error code, a human-readable
// message, and an optional wrapped underlying error.
type Error struct {
	Code    ErrorCode
	Message string
	Err     error // wrapped underlying error (may be nil)
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code.String(), e.Message)
}

// Unwrap returns the wrapped error for use with errors.Is / errors.As.
func (e *Error) Unwrap() error {
	return e.Err
}

// Is reports whether this error matches target.  Two *Error values match when
// their Codes are equal.
func (e *Error) Is(target error) bool {
	if t, ok := target.(*Error); ok {
		return e.Code == t.Code
	}
	return false
}

// NewError creates a new *Error with the given code and message.
func NewError(code ErrorCode, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// Errorf creates a new *Error with the given code and a formatted message.
func Errorf(code ErrorCode, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ErrorCodeOf returns the ErrorCode of err.
// Returns SVDB_OK for nil, the code from any *Error in the chain, or SVDB_ERROR
// for any other non-nil error.
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return SVDB_OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return SVDB_ERROR
}

// IsErrorCode reports whether err carries the given error code.
func IsErrorCode(err error, code ErrorCode) bool {
	return ErrorCodeOf(err) == code
}
