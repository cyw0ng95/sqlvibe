package errors

import "errors"

// SQLSTATE codes (SQL standard ISO/IEC 9075)
const (
	SQLState_OK                           = "00000"
	SQLState_StringDataRightTruncated     = "22001"
	SQLState_NumericValueOutOfRange       = "22003"
	SQLState_IntegrityConstraintViolation = "23000"
	SQLState_NotNullViolation             = "23502"
	SQLState_ForeignKeyViolation          = "23503"
	SQLState_UniqueViolation              = "23505"
	SQLState_CheckViolation               = "23514"
)

// WithSQLState returns a copy of e with the given SQLState code.
func WithSQLState(e *Error, state string) *Error {
	return &Error{Code: e.Code, Message: e.Message, Err: e.Err, SQLState: state}
}

// SQLStateOf returns the SQLSTATE code for the error, or "00000" if nil.
func SQLStateOf(err error) string {
	if err == nil {
		return SQLState_OK
	}
	var e *Error
	if errors.As(err, &e) && e.SQLState != "" {
		return e.SQLState
	}
	// Map known error codes to SQLSTATE
	code := ErrorCodeOf(err)
	switch code {
	case SVDB_CONSTRAINT_UNIQUE:
		return SQLState_UniqueViolation
	case SVDB_CONSTRAINT_PRIMARYKEY:
		return SQLState_UniqueViolation
	case SVDB_CONSTRAINT_FOREIGNKEY:
		return SQLState_ForeignKeyViolation
	case SVDB_CONSTRAINT_NOTNULL:
		return SQLState_NotNullViolation
	case SVDB_CONSTRAINT_CHECK:
		return SQLState_CheckViolation
	case SVDB_CONSTRAINT:
		return SQLState_IntegrityConstraintViolation
	default:
		return "HY000" // General error
	}
}
