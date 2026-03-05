// Package errors provides error types and SQLSTATE utilities for sqlvibe.
// It classifies runtime errors returned by the sqlvibe engine into typed
// errors with well-known error codes and SQLSTATE values.
package errors

import (
	"context"
	"fmt"
	"strings"
)

// ErrorCode identifies the category of a sqlvibe engine error.
type ErrorCode int

const (
	// SVDB_OK indicates no error.
	SVDB_OK ErrorCode = iota
	// SVDB_QUERY_TIMEOUT is returned when a query or statement execution
	// exceeds its configured timeout or the provided context deadline fires.
	SVDB_QUERY_TIMEOUT
	// SVDB_OOM_LIMIT is returned when the result set would exceed the
	// per-database max_memory limit configured via PRAGMA max_memory.
	SVDB_OOM_LIMIT
	// SVDB_CONSTRAINT_UNIQUE is returned on a UNIQUE or PRIMARY KEY violation.
	SVDB_CONSTRAINT_UNIQUE
	// SVDB_CONSTRAINT_INTEGRITY is returned on any integrity constraint violation.
	SVDB_CONSTRAINT_INTEGRITY
	// SVDB_GENERIC is returned for other, unclassified errors.
	SVDB_GENERIC
)

// Error is the structured error type returned by the sqlvibe engine.
type Error struct {
	// Code is the machine-readable error category.
	Code ErrorCode
	// Msg is the human-readable error message.
	Msg string
}

func (e *Error) Error() string {
	return e.Msg
}

// Unwrap allows errors.Is/As to traverse wrapped errors.
func (e *Error) Unwrap() error {
	switch e.Code {
	case SVDB_QUERY_TIMEOUT:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// fromError classifies a raw error returned by sqlvibe and wraps it as *Error.
// If err is already an *Error it is returned unchanged.
func fromError(err error) *Error {
	if err == nil {
		return nil
	}
	if se, ok := err.(*Error); ok {
		return se
	}
	msg := err.Error()
	lmsg := strings.ToLower(msg)
	switch {
	case isTimeoutError(err, lmsg):
		return &Error{Code: SVDB_QUERY_TIMEOUT, Msg: msg}
	case strings.Contains(lmsg, "oom") || strings.Contains(lmsg, "out of memory") ||
		strings.Contains(lmsg, "memory limit") || strings.Contains(lmsg, "max_memory"):
		return &Error{Code: SVDB_OOM_LIMIT, Msg: msg}
	case strings.Contains(lmsg, "unique constraint") || strings.Contains(lmsg, "uniqueconstraint"):
		return &Error{Code: SVDB_CONSTRAINT_UNIQUE, Msg: msg}
	case strings.Contains(lmsg, "constraint"):
		return &Error{Code: SVDB_CONSTRAINT_INTEGRITY, Msg: msg}
	default:
		return &Error{Code: SVDB_GENERIC, Msg: msg}
	}
}

func isTimeoutError(err error, lmsg string) bool {
	if err == context.DeadlineExceeded || err == context.Canceled {
		return true
	}
	return strings.Contains(lmsg, "timeout") || strings.Contains(lmsg, "deadline") ||
		strings.Contains(lmsg, "canceled") || strings.Contains(lmsg, "cancelled") ||
		strings.Contains(lmsg, "context")
}

// SQLState is a 5-character SQLSTATE code as defined by SQL:1999.
type SQLState string

func (s SQLState) String() string { return string(s) }

// Well-known SQLSTATE values.
const (
	// SQLState_OK indicates success (00000).
	SQLState_OK SQLState = "00000"
	// SQLState_UniqueViolation is SQLSTATE 23505: unique constraint violation.
	SQLState_UniqueViolation SQLState = "23505"
	// SQLState_IntegrityConstraintViolation is SQLSTATE 23000: integrity constraint violation.
	SQLState_IntegrityConstraintViolation SQLState = "23000"
	// SQLState_QueryCanceled is SQLSTATE 57014: query canceled.
	SQLState_QueryCanceled SQLState = "57014"
	// SQLState_DivisionByZero is SQLSTATE 22012.
	SQLState_DivisionByZero SQLState = "22012"
	// SQLState_DataException is SQLSTATE 22000.
	SQLState_DataException SQLState = "22000"
	// SQLState_NoDataFound is SQLSTATE 02000.
	SQLState_NoDataFound SQLState = "02000"
	// SQLState_GenericError is SQLSTATE HY000.
	SQLState_GenericError SQLState = "HY000"
)

// SQLStateOf returns the SQLSTATE code for the given error.
// It inspects the error message to determine the appropriate SQLSTATE.
// Returns SQLState_OK ("00000") when err is nil.
func SQLStateOf(err error) SQLState {
	if err == nil {
		return SQLState_OK
	}
	se := fromError(err)
	switch se.Code {
	case SVDB_CONSTRAINT_UNIQUE:
		return SQLState_UniqueViolation
	case SVDB_CONSTRAINT_INTEGRITY:
		return SQLState_IntegrityConstraintViolation
	case SVDB_QUERY_TIMEOUT:
		return SQLState_QueryCanceled
	default:
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "unique") {
			return SQLState_UniqueViolation
		}
		if strings.Contains(msg, "constraint") {
			return SQLState_IntegrityConstraintViolation
		}
		if strings.Contains(msg, "division by zero") || strings.Contains(msg, "divide by zero") {
			return SQLState_DivisionByZero
		}
		return SQLState_GenericError
	}
}

// Wrap wraps a raw error as an *Error. Returns nil if err is nil.
func Wrap(err error) *Error {
	return fromError(err)
}

// New creates a new *Error with the given code and message.
func New(code ErrorCode, format string, args ...interface{}) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}
