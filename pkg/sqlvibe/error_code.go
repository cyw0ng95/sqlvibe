package sqlvibe

import "fmt"

// ErrorCode represents a sqlvibe error code following the SQLite error code convention.
type ErrorCode int32

// Primary error codes (0–28, 100–101).
const (
	SVDB_OK        ErrorCode = 0
	SVDB_ERROR     ErrorCode = 1
	SVDB_INTERNAL  ErrorCode = 2
	SVDB_PERM      ErrorCode = 3
	SVDB_ABORT     ErrorCode = 4
	SVDB_BUSY      ErrorCode = 5
	SVDB_LOCKED    ErrorCode = 6
	SVDB_NOMEM     ErrorCode = 7
	SVDB_READONLY  ErrorCode = 8
	SVDB_INTERRUPT ErrorCode = 9
	SVDB_IOERR     ErrorCode = 10
	SVDB_CORRUPT   ErrorCode = 11
	SVDB_NOTFOUND  ErrorCode = 12
	SVDB_FULL      ErrorCode = 13
	SVDB_CANTOPEN  ErrorCode = 14
	SVDB_PROTOCOL  ErrorCode = 15
	SVDB_EMPTY     ErrorCode = 16
	SVDB_SCHEMA    ErrorCode = 17
	SVDB_TOOBIG    ErrorCode = 18

	SVDB_CONSTRAINT ErrorCode = 19
	SVDB_MISMATCH   ErrorCode = 20
	SVDB_MISUSE     ErrorCode = 21
	SVDB_NOLFS      ErrorCode = 22
	SVDB_AUTH       ErrorCode = 23
	SVDB_FORMAT     ErrorCode = 24
	SVDB_RANGE      ErrorCode = 25
	SVDB_NOTADB     ErrorCode = 26
	SVDB_NOTICE     ErrorCode = 27
	SVDB_WARNING    ErrorCode = 28

	SVDB_ROW  ErrorCode = 100
	SVDB_DONE ErrorCode = 101
)

// Extended error codes (256+).  The low byte encodes the primary code.
const (
	SVDB_OK_LOAD_PERMANENTLY ErrorCode = 256

	// CONSTRAINT extended codes
	SVDB_CONSTRAINT_CHECK       ErrorCode = 275  // 19 | (1 << 8)
	SVDB_CONSTRAINT_COMMITHOOK  ErrorCode = 531  // 19 | (2 << 8)
	SVDB_CONSTRAINT_FOREIGNKEY  ErrorCode = 787  // 19 | (3 << 8)
	SVDB_CONSTRAINT_FUNCTION    ErrorCode = 1043 // 19 | (4 << 8)
	SVDB_CONSTRAINT_NOTNULL     ErrorCode = 1299 // 19 | (5 << 8)
	SVDB_CONSTRAINT_PRIMARYKEY  ErrorCode = 1555 // 19 | (6 << 8)
	SVDB_CONSTRAINT_TRIGGER     ErrorCode = 1811 // 19 | (7 << 8)
	SVDB_CONSTRAINT_UNIQUE      ErrorCode = 2067 // 19 | (8 << 8)
	SVDB_CONSTRAINT_VTAB        ErrorCode = 2323 // 19 | (9 << 8)
	SVDB_CONSTRAINT_ROWID       ErrorCode = 2579 // 19 | (10 << 8)
	SVDB_CONSTRAINT_DATATYPE    ErrorCode = 3091 // 19 | (12 << 8)
	SVDB_CONSTRAINT_PINNED      ErrorCode = 2835 // 19 | (11 << 8)

	// BUSY extended codes
	SVDB_BUSY_RECOVERY   ErrorCode = 261 // 5 | (1 << 8)
	SVDB_BUSY_SNAPSHOT   ErrorCode = 517 // 5 | (2 << 8)
	SVDB_BUSY_TIMEOUT    ErrorCode = 773 // 5 | (3 << 8)

	// LOCKED extended codes
	SVDB_LOCKED_SHAREDCACHE  ErrorCode = 262 // 6 | (1 << 8)
	SVDB_LOCKED_VTAB         ErrorCode = 518 // 6 | (2 << 8)

	// IOERR extended codes
	SVDB_IOERR_READ             ErrorCode = 266  // 10 | (1 << 8)
	SVDB_IOERR_SHORT_READ       ErrorCode = 522  // 10 | (2 << 8)
	SVDB_IOERR_WRITE            ErrorCode = 778  // 10 | (3 << 8)
	SVDB_IOERR_FSYNC            ErrorCode = 1034 // 10 | (4 << 8)
	SVDB_IOERR_DIR_FSYNC        ErrorCode = 1290 // 10 | (5 << 8)
	SVDB_IOERR_TRUNCATE         ErrorCode = 1546 // 10 | (6 << 8)
	SVDB_IOERR_FSTAT            ErrorCode = 1802 // 10 | (7 << 8)
	SVDB_IOERR_UNLOCK           ErrorCode = 2058 // 10 | (8 << 8)
	SVDB_IOERR_RDLOCK           ErrorCode = 2314 // 10 | (9 << 8)
	SVDB_IOERR_DELETE           ErrorCode = 2570 // 10 | (10 << 8)
	SVDB_IOERR_BLOCKED          ErrorCode = 2826 // 10 | (11 << 8)
	SVDB_IOERR_NOMEM            ErrorCode = 3082 // 10 | (12 << 8)
	SVDB_IOERR_ACCESS           ErrorCode = 3338 // 10 | (13 << 8)
	SVDB_IOERR_CHECKRESERVEDLOCK ErrorCode = 3594 // 10 | (14 << 8)
	SVDB_IOERR_LOCK             ErrorCode = 3850 // 10 | (15 << 8)
	SVDB_IOERR_CLOSE            ErrorCode = 4106 // 10 | (16 << 8)
	SVDB_IOERR_DIR_CLOSE        ErrorCode = 4362 // 10 | (17 << 8)
	SVDB_IOERR_SHMOPEN          ErrorCode = 4618 // 10 | (18 << 8)
	SVDB_IOERR_SHMSIZE          ErrorCode = 4874 // 10 | (19 << 8)
	SVDB_IOERR_SHMLOCK          ErrorCode = 5130 // 10 | (20 << 8)
	SVDB_IOERR_SHMMAP           ErrorCode = 5386 // 10 | (21 << 8)
	SVDB_IOERR_SEEK             ErrorCode = 5642 // 10 | (22 << 8)
	SVDB_IOERR_DELETE_NOENT     ErrorCode = 5898 // 10 | (23 << 8)
	SVDB_IOERR_MMAP             ErrorCode = 6154 // 10 | (24 << 8)
	SVDB_IOERR_GETTEMPPATH      ErrorCode = 6410 // 10 | (25 << 8)
	SVDB_IOERR_CONVPATH         ErrorCode = 6666 // 10 | (26 << 8)
	SVDB_IOERR_VNODE            ErrorCode = 6922 // 10 | (27 << 8)
	SVDB_IOERR_AUTH             ErrorCode = 7178 // 10 | (28 << 8)
	SVDB_IOERR_BEGIN_ATOMIC     ErrorCode = 7434 // 10 | (29 << 8)
	SVDB_IOERR_COMMIT_ATOMIC    ErrorCode = 7690 // 10 | (30 << 8)
	SVDB_IOERR_ROLLBACK_ATOMIC  ErrorCode = 7946 // 10 | (31 << 8)
	SVDB_IOERR_DATA             ErrorCode = 8202 // 10 | (32 << 8)
	SVDB_IOERR_CORRUPTFS        ErrorCode = 8458 // 10 | (33 << 8)

	// ABORT extended codes
	SVDB_ABORT_ROLLBACK ErrorCode = 516 // 4 | (2 << 8)

	// CANTOPEN extended codes
	SVDB_CANTOPEN_NOTEMPDIR   ErrorCode = 270 // 14 | (1 << 8)
	SVDB_CANTOPEN_ISDIR       ErrorCode = 526 // 14 | (2 << 8)
	SVDB_CANTOPEN_FULLPATH    ErrorCode = 782 // 14 | (3 << 8)
	SVDB_CANTOPEN_CONVPATH    ErrorCode = 1038 // 14 | (4 << 8)
	SVDB_CANTOPEN_DIRTYWAL    ErrorCode = 1294 // 14 | (5 << 8)
	SVDB_CANTOPEN_SYMLINK     ErrorCode = 1550 // 14 | (6 << 8)

	// CORRUPT extended codes
	SVDB_CORRUPT_VTAB      ErrorCode = 267 // 11 | (1 << 8)
	SVDB_CORRUPT_SEQUENCE  ErrorCode = 523 // 11 | (2 << 8)
	SVDB_CORRUPT_INDEX     ErrorCode = 779 // 11 | (3 << 8)

	// READONLY extended codes
	SVDB_READONLY_RECOVERY       ErrorCode = 264 // 8 | (1 << 8)
	SVDB_READONLY_CANTLOCK       ErrorCode = 520 // 8 | (2 << 8)
	SVDB_READONLY_ROLLBACK       ErrorCode = 776 // 8 | (3 << 8)
	SVDB_READONLY_DBMOVED        ErrorCode = 1032 // 8 | (4 << 8)
	SVDB_READONLY_CANTINIT       ErrorCode = 1288 // 8 | (5 << 8)
	SVDB_READONLY_DIRECTORY      ErrorCode = 1544 // 8 | (6 << 8)

	// NOTICE extended codes
	SVDB_NOTICE_RECOVER_WAL    ErrorCode = 283 // 27 | (1 << 8)
	SVDB_NOTICE_RECOVER_ROLLBACK ErrorCode = 539 // 27 | (2 << 8)
	SVDB_NOTICE_RBU            ErrorCode = 795 // 27 | (3 << 8)

	// WARNING extended codes
	SVDB_WARNING_AUTOINDEX ErrorCode = 284 // 28 | (1 << 8)

	// AUTH extended codes
	SVDB_AUTH_USER ErrorCode = 279 // 23 | (1 << 8)

	// INTERRUPT extended codes (query timeout)
	SVDB_QUERY_TIMEOUT ErrorCode = 265 // 9 | (1 << 8)

	// NOMEM extended codes (memory limit exceeded)
	SVDB_OOM_LIMIT ErrorCode = 263 // 7 | (1 << 8)

	// SCHEMA extended codes (ALTER TABLE conflicts)
	SVDB_ALTER_CONFLICT ErrorCode = 273 // 17 | (1 << 8)
)

// primaryCodeNames maps primary code values to their string names.
var primaryCodeNames = map[ErrorCode]string{
	SVDB_OK:         "SVDB_OK",
	SVDB_ERROR:      "SVDB_ERROR",
	SVDB_INTERNAL:   "SVDB_INTERNAL",
	SVDB_PERM:       "SVDB_PERM",
	SVDB_ABORT:      "SVDB_ABORT",
	SVDB_BUSY:       "SVDB_BUSY",
	SVDB_LOCKED:     "SVDB_LOCKED",
	SVDB_NOMEM:      "SVDB_NOMEM",
	SVDB_READONLY:   "SVDB_READONLY",
	SVDB_INTERRUPT:  "SVDB_INTERRUPT",
	SVDB_IOERR:      "SVDB_IOERR",
	SVDB_CORRUPT:    "SVDB_CORRUPT",
	SVDB_NOTFOUND:   "SVDB_NOTFOUND",
	SVDB_FULL:       "SVDB_FULL",
	SVDB_CANTOPEN:   "SVDB_CANTOPEN",
	SVDB_PROTOCOL:   "SVDB_PROTOCOL",
	SVDB_EMPTY:      "SVDB_EMPTY",
	SVDB_SCHEMA:     "SVDB_SCHEMA",
	SVDB_TOOBIG:     "SVDB_TOOBIG",
	SVDB_CONSTRAINT: "SVDB_CONSTRAINT",
	SVDB_MISMATCH:   "SVDB_MISMATCH",
	SVDB_MISUSE:     "SVDB_MISUSE",
	SVDB_NOLFS:      "SVDB_NOLFS",
	SVDB_AUTH:       "SVDB_AUTH",
	SVDB_FORMAT:     "SVDB_FORMAT",
	SVDB_RANGE:      "SVDB_RANGE",
	SVDB_NOTADB:     "SVDB_NOTADB",
	SVDB_NOTICE:     "SVDB_NOTICE",
	SVDB_WARNING:    "SVDB_WARNING",
	SVDB_ROW:        "SVDB_ROW",
	SVDB_DONE:       "SVDB_DONE",
}

// extendedCodeNames maps extended code values to their string names.
var extendedCodeNames = map[ErrorCode]string{
	SVDB_OK_LOAD_PERMANENTLY:     "SVDB_OK_LOAD_PERMANENTLY",
	SVDB_CONSTRAINT_CHECK:        "SVDB_CONSTRAINT_CHECK",
	SVDB_CONSTRAINT_COMMITHOOK:   "SVDB_CONSTRAINT_COMMITHOOK",
	SVDB_CONSTRAINT_FOREIGNKEY:   "SVDB_CONSTRAINT_FOREIGNKEY",
	SVDB_CONSTRAINT_FUNCTION:     "SVDB_CONSTRAINT_FUNCTION",
	SVDB_CONSTRAINT_NOTNULL:      "SVDB_CONSTRAINT_NOTNULL",
	SVDB_CONSTRAINT_PRIMARYKEY:   "SVDB_CONSTRAINT_PRIMARYKEY",
	SVDB_CONSTRAINT_TRIGGER:      "SVDB_CONSTRAINT_TRIGGER",
	SVDB_CONSTRAINT_UNIQUE:       "SVDB_CONSTRAINT_UNIQUE",
	SVDB_CONSTRAINT_VTAB:         "SVDB_CONSTRAINT_VTAB",
	SVDB_CONSTRAINT_ROWID:        "SVDB_CONSTRAINT_ROWID",
	SVDB_CONSTRAINT_DATATYPE:     "SVDB_CONSTRAINT_DATATYPE",
	SVDB_CONSTRAINT_PINNED:       "SVDB_CONSTRAINT_PINNED",
	SVDB_BUSY_RECOVERY:           "SVDB_BUSY_RECOVERY",
	SVDB_BUSY_SNAPSHOT:           "SVDB_BUSY_SNAPSHOT",
	SVDB_BUSY_TIMEOUT:            "SVDB_BUSY_TIMEOUT",
	SVDB_LOCKED_SHAREDCACHE:      "SVDB_LOCKED_SHAREDCACHE",
	SVDB_LOCKED_VTAB:             "SVDB_LOCKED_VTAB",
	SVDB_IOERR_READ:              "SVDB_IOERR_READ",
	SVDB_IOERR_SHORT_READ:        "SVDB_IOERR_SHORT_READ",
	SVDB_IOERR_WRITE:             "SVDB_IOERR_WRITE",
	SVDB_IOERR_FSYNC:             "SVDB_IOERR_FSYNC",
	SVDB_IOERR_DIR_FSYNC:         "SVDB_IOERR_DIR_FSYNC",
	SVDB_IOERR_TRUNCATE:          "SVDB_IOERR_TRUNCATE",
	SVDB_IOERR_FSTAT:             "SVDB_IOERR_FSTAT",
	SVDB_IOERR_UNLOCK:            "SVDB_IOERR_UNLOCK",
	SVDB_IOERR_RDLOCK:            "SVDB_IOERR_RDLOCK",
	SVDB_IOERR_DELETE:            "SVDB_IOERR_DELETE",
	SVDB_IOERR_BLOCKED:           "SVDB_IOERR_BLOCKED",
	SVDB_IOERR_NOMEM:             "SVDB_IOERR_NOMEM",
	SVDB_IOERR_ACCESS:            "SVDB_IOERR_ACCESS",
	SVDB_IOERR_CHECKRESERVEDLOCK: "SVDB_IOERR_CHECKRESERVEDLOCK",
	SVDB_IOERR_LOCK:              "SVDB_IOERR_LOCK",
	SVDB_IOERR_CLOSE:             "SVDB_IOERR_CLOSE",
	SVDB_IOERR_DIR_CLOSE:         "SVDB_IOERR_DIR_CLOSE",
	SVDB_IOERR_SHMOPEN:           "SVDB_IOERR_SHMOPEN",
	SVDB_IOERR_SHMSIZE:           "SVDB_IOERR_SHMSIZE",
	SVDB_IOERR_SHMLOCK:           "SVDB_IOERR_SHMLOCK",
	SVDB_IOERR_SHMMAP:            "SVDB_IOERR_SHMMAP",
	SVDB_IOERR_SEEK:              "SVDB_IOERR_SEEK",
	SVDB_IOERR_DELETE_NOENT:      "SVDB_IOERR_DELETE_NOENT",
	SVDB_IOERR_MMAP:              "SVDB_IOERR_MMAP",
	SVDB_IOERR_GETTEMPPATH:       "SVDB_IOERR_GETTEMPPATH",
	SVDB_IOERR_CONVPATH:          "SVDB_IOERR_CONVPATH",
	SVDB_IOERR_VNODE:             "SVDB_IOERR_VNODE",
	SVDB_IOERR_AUTH:              "SVDB_IOERR_AUTH",
	SVDB_IOERR_BEGIN_ATOMIC:      "SVDB_IOERR_BEGIN_ATOMIC",
	SVDB_IOERR_COMMIT_ATOMIC:     "SVDB_IOERR_COMMIT_ATOMIC",
	SVDB_IOERR_ROLLBACK_ATOMIC:   "SVDB_IOERR_ROLLBACK_ATOMIC",
	SVDB_IOERR_DATA:              "SVDB_IOERR_DATA",
	SVDB_IOERR_CORRUPTFS:         "SVDB_IOERR_CORRUPTFS",
	SVDB_ABORT_ROLLBACK:          "SVDB_ABORT_ROLLBACK",
	SVDB_CANTOPEN_NOTEMPDIR:      "SVDB_CANTOPEN_NOTEMPDIR",
	SVDB_CANTOPEN_ISDIR:          "SVDB_CANTOPEN_ISDIR",
	SVDB_CANTOPEN_FULLPATH:       "SVDB_CANTOPEN_FULLPATH",
	SVDB_CANTOPEN_CONVPATH:       "SVDB_CANTOPEN_CONVPATH",
	SVDB_CANTOPEN_DIRTYWAL:       "SVDB_CANTOPEN_DIRTYWAL",
	SVDB_CANTOPEN_SYMLINK:        "SVDB_CANTOPEN_SYMLINK",
	SVDB_CORRUPT_VTAB:            "SVDB_CORRUPT_VTAB",
	SVDB_CORRUPT_SEQUENCE:        "SVDB_CORRUPT_SEQUENCE",
	SVDB_CORRUPT_INDEX:           "SVDB_CORRUPT_INDEX",
	SVDB_READONLY_RECOVERY:       "SVDB_READONLY_RECOVERY",
	SVDB_READONLY_CANTLOCK:       "SVDB_READONLY_CANTLOCK",
	SVDB_READONLY_ROLLBACK:       "SVDB_READONLY_ROLLBACK",
	SVDB_READONLY_DBMOVED:        "SVDB_READONLY_DBMOVED",
	SVDB_READONLY_CANTINIT:       "SVDB_READONLY_CANTINIT",
	SVDB_READONLY_DIRECTORY:      "SVDB_READONLY_DIRECTORY",
	SVDB_NOTICE_RECOVER_WAL:      "SVDB_NOTICE_RECOVER_WAL",
	SVDB_NOTICE_RECOVER_ROLLBACK: "SVDB_NOTICE_RECOVER_ROLLBACK",
	SVDB_NOTICE_RBU:              "SVDB_NOTICE_RBU",
	SVDB_WARNING_AUTOINDEX:       "SVDB_WARNING_AUTOINDEX",
	SVDB_AUTH_USER:               "SVDB_AUTH_USER",
	SVDB_QUERY_TIMEOUT:           "SVDB_QUERY_TIMEOUT",
	SVDB_OOM_LIMIT:               "SVDB_OOM_LIMIT",
	SVDB_ALTER_CONFLICT:          "SVDB_ALTER_CONFLICT",
}

// String returns the string representation of the error code.
func (c ErrorCode) String() string {
	if name, ok := primaryCodeNames[c]; ok {
		return name
	}
	if name, ok := extendedCodeNames[c]; ok {
		return name
	}
	return fmt.Sprintf("SVDB_UNKNOWN(%d)", int32(c))
}

// Primary extracts the base (primary) error code from an extended error code.
// For primary codes this returns the code unchanged.
func (c ErrorCode) Primary() ErrorCode {
	return ErrorCode(int32(c) & 0xFF)
}
