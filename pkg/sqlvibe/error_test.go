package sqlvibe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

func TestErrorErrorString(t *testing.T) {
	e := NewError(SVDB_BUSY, "database is busy")
	s := e.Error()
	if s == "" {
		t.Fatal("Error() returned empty string")
	}
	// Must contain the code string and the message.
	if !containsStr(s, "SVDB_BUSY") {
		t.Errorf("Error() = %q, want it to contain SVDB_BUSY", s)
	}
	if !containsStr(s, "database is busy") {
		t.Errorf("Error() = %q, want it to contain message", s)
	}
}

func TestErrorUnwrap(t *testing.T) {
	inner := errors.New("inner io error")
	e := &Error{Code: SVDB_IOERR, Message: "io failure", Err: inner}
	if e.Unwrap() != inner {
		t.Error("Unwrap() did not return inner error")
	}
}

func TestErrorUnwrapNil(t *testing.T) {
	e := NewError(SVDB_OK, "ok")
	if e.Unwrap() != nil {
		t.Error("Unwrap() should return nil when Err is not set")
	}
}

func TestErrorIsMatch(t *testing.T) {
	e := NewError(SVDB_CONSTRAINT_UNIQUE, "unique violation")
	target := NewError(SVDB_CONSTRAINT_UNIQUE, "")
	if !errors.Is(e, target) {
		t.Error("errors.Is should match equal codes")
	}
}

func TestErrorIsNoMatch(t *testing.T) {
	e := NewError(SVDB_CONSTRAINT_UNIQUE, "unique violation")
	target := NewError(SVDB_CONSTRAINT_NOTNULL, "")
	if errors.Is(e, target) {
		t.Error("errors.Is should not match different codes")
	}
}

func TestErrorIsNonSvdbTarget(t *testing.T) {
	e := NewError(SVDB_ERROR, "some error")
	target := errors.New("other")
	if errors.Is(e, target) {
		t.Error("errors.Is should return false for non-*Error target")
	}
}

// ---- ErrorCodeOf -------------------------------------------------------

func TestErrorCodeOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorCode
	}{
		{"nil", nil, SVDB_OK},
		{"svdb_error", NewError(SVDB_ERROR, "x"), SVDB_ERROR},
		{"svdb_busy", NewError(SVDB_BUSY, "x"), SVDB_BUSY},
		{"plain_error", errors.New("plain"), SVDB_ERROR},
		{"wrapped_svdb", fmt.Errorf("wrapped: %w", NewError(SVDB_NOTFOUND, "x")), SVDB_NOTFOUND},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ErrorCodeOf(tt.err); got != tt.want {
				t.Errorf("ErrorCodeOf() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---- IsErrorCode -------------------------------------------------------

func TestIsErrorCode(t *testing.T) {
	e := NewError(SVDB_SCHEMA, "schema changed")
	if !IsErrorCode(e, SVDB_SCHEMA) {
		t.Error("IsErrorCode should return true for matching code")
	}
	if IsErrorCode(e, SVDB_ERROR) {
		t.Error("IsErrorCode should return false for non-matching code")
	}
	if IsErrorCode(nil, SVDB_OK) != true {
		t.Error("IsErrorCode(nil, SVDB_OK) should be true")
	}
}

// ---- errors.Is integration --------------------------------------------

func TestErrorsIsIntegration(t *testing.T) {
	e := Errorf(SVDB_IOERR_WRITE, "write failed at offset 4096")
	target := NewError(SVDB_IOERR_WRITE, "")
	if !errors.Is(e, target) {
		t.Error("errors.Is integration failed for same code")
	}
}

func TestErrorsIsNegative(t *testing.T) {
	e := Errorf(SVDB_IOERR_READ, "read failed")
	target := NewError(SVDB_IOERR_WRITE, "")
	if errors.Is(e, target) {
		t.Error("errors.Is should not match different extended codes")
	}
}

// ---- ToError -----------------------------------------------------------

func TestToErrorNil(t *testing.T) {
	if ToError(nil) != nil {
		t.Error("ToError(nil) should return nil")
	}
}

func TestToErrorAlreadySvdb(t *testing.T) {
	orig := NewError(SVDB_BUSY, "busy")
	got := ToError(orig)
	if got != orig {
		t.Error("ToError should return the same *Error unchanged")
	}
}

func TestToErrorEOF(t *testing.T) {
	got := ToError(io.EOF)
	if got.Code != SVDB_DONE {
		t.Errorf("ToError(io.EOF).Code = %v, want SVDB_DONE", got.Code)
	}
}

func TestToErrorUnexpectedEOF(t *testing.T) {
	got := ToError(io.ErrUnexpectedEOF)
	if got.Code != SVDB_CORRUPT {
		t.Errorf("ToError(io.ErrUnexpectedEOF).Code = %v, want SVDB_CORRUPT", got.Code)
	}
}

func TestToErrorShortWrite(t *testing.T) {
	got := ToError(io.ErrShortWrite)
	if got.Code != SVDB_IOERR_WRITE {
		t.Errorf("ToError(io.ErrShortWrite).Code = %v, want SVDB_IOERR_WRITE", got.Code)
	}
}

func TestToErrorOsErrNotExist(t *testing.T) {
	got := ToError(os.ErrNotExist)
	if got.Code != SVDB_NOTFOUND {
		t.Errorf("ToError(os.ErrNotExist).Code = %v, want SVDB_NOTFOUND", got.Code)
	}
}

func TestToErrorOsErrPermission(t *testing.T) {
	got := ToError(os.ErrPermission)
	if got.Code != SVDB_PERM {
		t.Errorf("ToError(os.ErrPermission).Code = %v, want SVDB_PERM", got.Code)
	}
}

func TestToErrorContextDeadline(t *testing.T) {
	got := ToError(context.DeadlineExceeded)
	if got.Code != SVDB_BUSY_TIMEOUT {
		t.Errorf("ToError(context.DeadlineExceeded).Code = %v, want SVDB_BUSY_TIMEOUT", got.Code)
	}
}

func TestToErrorContextCanceled(t *testing.T) {
	got := ToError(context.Canceled)
	if got.Code != SVDB_INTERRUPT {
		t.Errorf("ToError(context.Canceled).Code = %v, want SVDB_INTERRUPT", got.Code)
	}
}

func TestToErrorPatternUnique(t *testing.T) {
	got := ToError(errors.New("UNIQUE constraint failed: users.email"))
	if got.Code != SVDB_CONSTRAINT_UNIQUE {
		t.Errorf("got %v, want SVDB_CONSTRAINT_UNIQUE", got.Code)
	}
}

func TestToErrorPatternNotNull(t *testing.T) {
	got := ToError(errors.New("NOT NULL constraint violated on column name"))
	if got.Code != SVDB_CONSTRAINT_NOTNULL {
		t.Errorf("got %v, want SVDB_CONSTRAINT_NOTNULL", got.Code)
	}
}

func TestToErrorPatternForeignKey(t *testing.T) {
	got := ToError(errors.New("foreign key constraint failed"))
	if got.Code != SVDB_CONSTRAINT_FOREIGNKEY {
		t.Errorf("got %v, want SVDB_CONSTRAINT_FOREIGNKEY", got.Code)
	}
}

func TestToErrorPatternConstraintGeneric(t *testing.T) {
	got := ToError(errors.New("constraint violation occurred"))
	if got.Code != SVDB_CONSTRAINT {
		t.Errorf("got %v, want SVDB_CONSTRAINT", got.Code)
	}
}

func TestToErrorPatternReadOnly(t *testing.T) {
	got := ToError(errors.New("database is read-only"))
	if got.Code != SVDB_READONLY {
		t.Errorf("got %v, want SVDB_READONLY", got.Code)
	}
}

func TestToErrorPatternCorrupt(t *testing.T) {
	got := ToError(errors.New("database is corrupt"))
	if got.Code != SVDB_CORRUPT {
		t.Errorf("got %v, want SVDB_CORRUPT", got.Code)
	}
}

func TestToErrorPatternLocked(t *testing.T) {
	got := ToError(errors.New("table is locked by another connection"))
	if got.Code != SVDB_LOCKED {
		t.Errorf("got %v, want SVDB_LOCKED", got.Code)
	}
}

func TestToErrorDefault(t *testing.T) {
	got := ToError(errors.New("some completely unknown error"))
	if got.Code != SVDB_ERROR {
		t.Errorf("got %v, want SVDB_ERROR", got.Code)
	}
}

func TestToErrorWrapsOriginal(t *testing.T) {
	inner := errors.New("wrapped")
	got := ToError(inner)
	if !errors.Is(got, inner) {
		t.Error("ToError should wrap original error so errors.Is works")
	}
}

// ---- ShardedMap --------------------------------------------------------

func TestShardedMapSetGet(t *testing.T) {
	sm := NewShardedMap()
	sm.Set("key1", 42)
	v, ok := sm.Get("key1")
	if !ok {
		t.Fatal("Get should find key1")
	}
	if v.(int) != 42 {
		t.Errorf("Get = %v, want 42", v)
	}
}

func TestShardedMapMiss(t *testing.T) {
	sm := NewShardedMap()
	_, ok := sm.Get("missing")
	if ok {
		t.Error("Get should return false for missing key")
	}
}

func TestShardedMapDelete(t *testing.T) {
	sm := NewShardedMap()
	sm.Set("k", "v")
	sm.Delete("k")
	_, ok := sm.Get("k")
	if ok {
		t.Error("key should be gone after Delete")
	}
}

func TestShardedMapKeys(t *testing.T) {
	sm := NewShardedMap()
	sm.Set("a", 1)
	sm.Set("b", 2)
	sm.Set("c", 3)
	keys := sm.Keys()
	if len(keys) != 3 {
		t.Errorf("Keys() len = %d, want 3", len(keys))
	}
}

func TestShardedMapOverwrite(t *testing.T) {
	sm := NewShardedMap()
	sm.Set("x", "first")
	sm.Set("x", "second")
	v, _ := sm.Get("x")
	if v.(string) != "second" {
		t.Errorf("expected overwritten value 'second', got %v", v)
	}
}

// ---- AtomicCounter -----------------------------------------------------

func TestAtomicCounterAdd(t *testing.T) {
	var c AtomicCounter
	c.Add(10)
	c.Add(5)
	if got := c.Get(); got != 15 {
		t.Errorf("Get() = %d, want 15", got)
	}
}

func TestAtomicCounterSet(t *testing.T) {
	var c AtomicCounter
	c.Set(100)
	if got := c.Get(); got != 100 {
		t.Errorf("Get() = %d, want 100", got)
	}
}

func TestAtomicCounterNegative(t *testing.T) {
	var c AtomicCounter
	c.Add(-5)
	if got := c.Get(); got != -5 {
		t.Errorf("Get() = %d, want -5", got)
	}
}

// ---- LockMetrics -------------------------------------------------------

func TestLockMetrics(t *testing.T) {
	var m LockMetrics
	m.RecordAcquisition()
	m.RecordAcquisition()
	m.RecordContention(100)
	if m.Acquisitions.Get() != 2 {
		t.Errorf("Acquisitions = %d, want 2", m.Acquisitions.Get())
	}
	if m.Contentions.Get() != 1 {
		t.Errorf("Contentions = %d, want 1", m.Contentions.Get())
	}
	if m.WaitNs.Get() != 100 {
		t.Errorf("WaitNs = %d, want 100", m.WaitNs.Get())
	}
}

// ---- AlignedCounter ----------------------------------------------------

func TestAlignedCounter(t *testing.T) {
	var c AlignedCounter
	c.Add(7)
	if got := c.Get(); got != 7 {
		t.Errorf("Get() = %d, want 7", got)
	}
}

// ---- ScanPrefetcher ----------------------------------------------------

func TestScanPrefetcherInBounds(t *testing.T) {
	rows := make([]map[string]interface{}, 10)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": i}
	}
	var p ScanPrefetcher
	// Should not panic.
	p.PrefetchRows(rows, 0, PrefetchDepth)
	p.PrefetchRows(rows, 5, PrefetchDepth)
}

func TestScanPrefetcherOutOfBounds(t *testing.T) {
	rows := make([]map[string]interface{}, 3)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": i}
	}
	var p ScanPrefetcher
	// Should not panic even when idx+depth >= len(rows).
	p.PrefetchRows(rows, 2, PrefetchDepth)
	p.PrefetchRows(rows, 100, PrefetchDepth)
}

// ---- helpers -----------------------------------------------------------

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || stringContains(s, sub))
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
