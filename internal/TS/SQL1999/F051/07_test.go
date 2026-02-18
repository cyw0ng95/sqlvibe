package F051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F507_F05107_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, d DATE, t TIME, ts TIMESTAMP)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, '2024-01-15', '10:30:00', '2024-01-15 10:30:00')"},
		{"InsertMore", "INSERT INTO t1 VALUES (2, '2023-12-31', '23:59:59', '2023-12-31 23:59:59')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	currentDatetimeTests := []struct {
		name string
		sql  string
	}{
		{"CurrentDate", "SELECT CURRENT_DATE"},
		{"CurrentTime", "SELECT CURRENT_TIME"},
		{"CurrentTimestamp", "SELECT CURRENT_TIMESTAMP"},
		{"Localtime", "SELECT LOCALTIME"},
		{"Localtimestamp", "SELECT LOCALTIMESTAMP"},
		{"DateNow", "SELECT DATE('now')"},
		{"TimeNow", "SELECT TIME('now')"},
		{"DatetimeNow", "SELECT DATETIME('now')"},
	}

	for _, tt := range currentDatetimeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F508_F05108_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, d1 DATE, d2 DATE)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, '2024-01-01', '2024-01-15')"},
		{"InsertMore", "INSERT INTO t1 VALUES (2, '2024-02-28', '2024-03-01')"},
		{"InsertNull", "INSERT INTO t1 VALUES (3, NULL, '2024-06-15')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dateComparisonTests := []struct {
		name string
		sql  string
	}{
		{"DateGreaterThan", "SELECT * FROM t1 WHERE d2 > d1"},
		{"DateLessThan", "SELECT * FROM t1 WHERE d1 < d2"},
		{"DateEqual", "SELECT * FROM t1 WHERE d1 = '2024-01-01'"},
		{"DateNotEqual", "SELECT * FROM t1 WHERE d1 <> '2024-01-01'"},
		{"DateBetween", "SELECT * FROM t1 WHERE d1 BETWEEN '2024-01-01' AND '2024-06-01'"},
		{"DateNullCheck", "SELECT * FROM t1 WHERE d1 IS NULL"},
		{"DateNotNullCheck", "SELECT * FROM t1 WHERE d1 IS NOT NULL"},
		{"DateOrderBy", "SELECT * FROM t1 ORDER BY d1 DESC"},
	}

	for _, tt := range dateComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F509_F05109_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, ts TIMESTAMP)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, '2024-01-15 10:30:00')"},
		{"InsertMore", "INSERT INTO t1 VALUES (2, '2023-12-31 23:59:59')"},
		{"InsertNull", "INSERT INTO t1 VALUES (3, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	castTests := []struct {
		name string
		sql  string
	}{
		{"CastToDate", "SELECT CAST(ts AS DATE) FROM t1 WHERE id = 1"},
		{"CastToTime", "SELECT CAST(ts AS TIME) FROM t1 WHERE id = 1"},
		{"CastToText", "SELECT CAST(ts AS TEXT) FROM t1 WHERE id = 1"},
		{"CastFromTextToDate", "SELECT CAST('2024-01-15' AS DATE)"},
		{"CastFromTextToTime", "SELECT CAST('10:30:00' AS TIME)"},
		{"CastFromTextToTimestamp", "SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)"},
		{"CastNull", "SELECT CAST(ts AS TEXT) FROM t1 WHERE id = 3"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F510_F05110_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, ts TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, '2024-01-15')"},
		{"InsertMore", "INSERT INTO t1 VALUES (2, '2024-12-31')"},
		{"InsertTime", "INSERT INTO t1 VALUES (3, '10:30:00')"},
		{"InsertTimestamp", "INSERT INTO t1 VALUES (4, '2024-01-15 10:30:00')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	datetimeExtractTests := []struct {
		name string
		sql  string
	}{
		{"StrftimeYear", "SELECT STRFTIME('%Y', ts) FROM t1 WHERE id = 1"},
		{"StrftimeMonth", "SELECT STRFTIME('%m', ts) FROM t1 WHERE id = 1"},
		{"StrftimeDay", "SELECT STRFTIME('%d', ts) FROM t1 WHERE id = 1"},
		{"StrftimeHour", "SELECT STRFTIME('%H', ts) FROM t1 WHERE id = 4"},
		{"StrftimeMinute", "SELECT STRFTIME('%M', ts) FROM t1 WHERE id = 4"},
		{"StrftimeSecond", "SELECT STRFTIME('%S', ts) FROM t1 WHERE id = 4"},
		{"DateFunction", "SELECT DATE(ts) FROM t1 WHERE id = 1"},
		{"TimeFunction", "SELECT TIME(ts) FROM t1 WHERE id = 4"},
		{"DatetimeFunction", "SELECT DATETIME(ts) FROM t1 WHERE id = 1"},
	}

	for _, tt := range datetimeExtractTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F511_F05111_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, ts TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, '2024-01-01')"},
		{"InsertMore", "INSERT INTO t1 VALUES (2, '2024-01-15')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	datetimeArithmeticTests := []struct {
		name string
		sql  string
	}{
		{"DatePlus1Day", "SELECT DATE(ts, '+1 day') FROM t1 WHERE id = 1"},
		{"DateMinus1Day", "SELECT DATE(ts, '-1 day') FROM t1 WHERE id = 2"},
		{"DatePlus1Month", "SELECT DATE(ts, '+1 month') FROM t1 WHERE id = 1"},
		{"DateMinus1Month", "SELECT DATE(ts, '-1 month') FROM t1 WHERE id = 2"},
		{"DatePlus1Year", "SELECT DATE(ts, '+1 year') FROM t1 WHERE id = 1"},
		{"DateMinus1Year", "SELECT DATE(ts, '-1 year') FROM t1 WHERE id = 2"},
		{"DatetimePlus1Hour", "SELECT DATETIME(ts, '+1 hour') FROM t1 WHERE id = 1"},
		{"DatetimePlus30Minutes", "SELECT DATETIME(ts, '+30 minutes') FROM t1 WHERE id = 1"},
		{"DateBetween", "SELECT * FROM t1 WHERE DATE(ts, '+7 days') > '2024-01-10'"},
	}

	for _, tt := range datetimeArithmeticTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
