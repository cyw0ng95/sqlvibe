package errors

import (
	"testing"
)

// ---- ErrorCode.String() -----------------------------------------------

func TestErrorCodeString(t *testing.T) {
	tests := []struct {
		code ErrorCode
		want string
	}{
		{SVDB_OK, "SVDB_OK"},
		{SVDB_ERROR, "SVDB_ERROR"},
		{SVDB_INTERNAL, "SVDB_INTERNAL"},
		{SVDB_PERM, "SVDB_PERM"},
		{SVDB_ABORT, "SVDB_ABORT"},
		{SVDB_BUSY, "SVDB_BUSY"},
		{SVDB_LOCKED, "SVDB_LOCKED"},
		{SVDB_NOMEM, "SVDB_NOMEM"},
		{SVDB_READONLY, "SVDB_READONLY"},
		{SVDB_INTERRUPT, "SVDB_INTERRUPT"},
		{SVDB_IOERR, "SVDB_IOERR"},
		{SVDB_CORRUPT, "SVDB_CORRUPT"},
		{SVDB_NOTFOUND, "SVDB_NOTFOUND"},
		{SVDB_FULL, "SVDB_FULL"},
		{SVDB_CANTOPEN, "SVDB_CANTOPEN"},
		{SVDB_PROTOCOL, "SVDB_PROTOCOL"},
		{SVDB_EMPTY, "SVDB_EMPTY"},
		{SVDB_SCHEMA, "SVDB_SCHEMA"},
		{SVDB_TOOBIG, "SVDB_TOOBIG"},
		{SVDB_CONSTRAINT, "SVDB_CONSTRAINT"},
		{SVDB_MISMATCH, "SVDB_MISMATCH"},
		{SVDB_MISUSE, "SVDB_MISUSE"},
		{SVDB_NOLFS, "SVDB_NOLFS"},
		{SVDB_AUTH, "SVDB_AUTH"},
		{SVDB_FORMAT, "SVDB_FORMAT"},
		{SVDB_RANGE, "SVDB_RANGE"},
		{SVDB_NOTADB, "SVDB_NOTADB"},
		{SVDB_NOTICE, "SVDB_NOTICE"},
		{SVDB_WARNING, "SVDB_WARNING"},
		{SVDB_ROW, "SVDB_ROW"},
		{SVDB_DONE, "SVDB_DONE"},
		// Extended codes
		{SVDB_OK_LOAD_PERMANENTLY, "SVDB_OK_LOAD_PERMANENTLY"},
		{SVDB_CONSTRAINT_CHECK, "SVDB_CONSTRAINT_CHECK"},
		{SVDB_CONSTRAINT_NOTNULL, "SVDB_CONSTRAINT_NOTNULL"},
		{SVDB_CONSTRAINT_PRIMARYKEY, "SVDB_CONSTRAINT_PRIMARYKEY"},
		{SVDB_CONSTRAINT_UNIQUE, "SVDB_CONSTRAINT_UNIQUE"},
		{SVDB_CONSTRAINT_FOREIGNKEY, "SVDB_CONSTRAINT_FOREIGNKEY"},
		{SVDB_BUSY_RECOVERY, "SVDB_BUSY_RECOVERY"},
		{SVDB_BUSY_TIMEOUT, "SVDB_BUSY_TIMEOUT"},
		{SVDB_IOERR_READ, "SVDB_IOERR_READ"},
		{SVDB_IOERR_WRITE, "SVDB_IOERR_WRITE"},
		{SVDB_IOERR_FSYNC, "SVDB_IOERR_FSYNC"},
		{SVDB_IOERR_SHORT_READ, "SVDB_IOERR_SHORT_READ"},
		{SVDB_IOERR_TRUNCATE, "SVDB_IOERR_TRUNCATE"},
		{SVDB_IOERR_DELETE, "SVDB_IOERR_DELETE"},
		{SVDB_IOERR_SEEK, "SVDB_IOERR_SEEK"},
		{SVDB_IOERR_MMAP, "SVDB_IOERR_MMAP"},
		{SVDB_NOTICE_RECOVER_WAL, "SVDB_NOTICE_RECOVER_WAL"},
		{SVDB_WARNING_AUTOINDEX, "SVDB_WARNING_AUTOINDEX"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.code.String(); got != tt.want {
				t.Errorf("ErrorCode(%d).String() = %q, want %q", int32(tt.code), got, tt.want)
			}
		})
	}
}

func TestErrorCodeStringUnknown(t *testing.T) {
	unknown := ErrorCode(9999)
	s := unknown.String()
	if s == "" {
		t.Error("expected non-empty string for unknown code")
	}
	// Should contain the numeric value somewhere.
	if s == "SVDB_OK" {
		t.Errorf("unexpected: unknown code returned SVDB_OK")
	}
}

// ---- ErrorCode.Primary() ----------------------------------------------

func TestErrorCodePrimary(t *testing.T) {
	tests := []struct {
		code    ErrorCode
		primary ErrorCode
	}{
		{SVDB_CONSTRAINT_CHECK, SVDB_CONSTRAINT},
		{SVDB_CONSTRAINT_NOTNULL, SVDB_CONSTRAINT},
		{SVDB_CONSTRAINT_PRIMARYKEY, SVDB_CONSTRAINT},
		{SVDB_CONSTRAINT_UNIQUE, SVDB_CONSTRAINT},
		{SVDB_CONSTRAINT_FOREIGNKEY, SVDB_CONSTRAINT},
		{SVDB_IOERR_READ, SVDB_IOERR},
		{SVDB_IOERR_WRITE, SVDB_IOERR},
		{SVDB_IOERR_FSYNC, SVDB_IOERR},
		{SVDB_IOERR_MMAP, SVDB_IOERR},
		{SVDB_BUSY_RECOVERY, SVDB_BUSY},
		{SVDB_BUSY_TIMEOUT, SVDB_BUSY},
		{SVDB_BUSY_SNAPSHOT, SVDB_BUSY},
		{SVDB_LOCKED_SHAREDCACHE, SVDB_LOCKED},
		{SVDB_READONLY_RECOVERY, SVDB_READONLY},
		{SVDB_READONLY_ROLLBACK, SVDB_READONLY},
		{SVDB_CANTOPEN_ISDIR, SVDB_CANTOPEN},
		{SVDB_CANTOPEN_FULLPATH, SVDB_CANTOPEN},
		{SVDB_CORRUPT_VTAB, SVDB_CORRUPT},
		{SVDB_CORRUPT_INDEX, SVDB_CORRUPT},
		{SVDB_ABORT_ROLLBACK, SVDB_ABORT},
		{SVDB_NOTICE_RECOVER_WAL, SVDB_NOTICE},
		{SVDB_WARNING_AUTOINDEX, SVDB_WARNING},
		// Primary codes are unchanged.
		{SVDB_OK, SVDB_OK},
		{SVDB_ERROR, SVDB_ERROR},
		{SVDB_BUSY, SVDB_BUSY},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			if got := tt.code.Primary(); got != tt.primary {
				t.Errorf("%s.Primary() = %s, want %s", tt.code, got, tt.primary)
			}
		})
	}
}

// ---- Error struct -----------------------------------------------------

func TestNewError(t *testing.T) {
	e := NewError(SVDB_NOTFOUND, "table foo not found")
	if e.Code != SVDB_NOTFOUND {
		t.Errorf("Code = %v, want SVDB_NOTFOUND", e.Code)
	}
	if e.Message != "table foo not found" {
		t.Errorf("Message = %q, want %q", e.Message, "table foo not found")
	}
	if e.Err != nil {
		t.Errorf("Err should be nil")
	}
}

func TestErrorfMessage(t *testing.T) {
	e := Errorf(SVDB_ERROR, "column %s missing in table %s", "id", "users")
	want := "column id missing in table users"
	if e.Message != want {
		t.Errorf("Message = %q, want %q", e.Message, want)
	}
}
