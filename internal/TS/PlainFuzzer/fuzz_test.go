package PlainFuzzer

import (
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func FuzzSQL(f *testing.F) {
	f.Fuzz(func(t *testing.T, query string) {
		if len(query) == 0 || len(query) > 2000 {
			t.Skip()
		}

		db, err := sqlvibe.Open(":memory:")
		if err != nil {
			t.Fatalf("Failed to open sqlvibe: %v", err)
		}
		defer db.Close()

		query = strings.TrimSpace(query)
		if len(query) == 0 {
			t.Skip()
		}

		executeSQL(db, query)
	})
}

func executeSQL(db *sqlvibe.Database, query string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	if len(upperQuery) >= 6 {
		prefix := upperQuery[:6]
		if prefix == "SELECT" || prefix == "PRAGMA" || prefix == "INSERT" || prefix == "UPDATE" || prefix == "DELETE" || prefix == "CREATE" || prefix == "DROP" {
			db.Query(query)
			return nil
		}
	}
	db.Exec(query)
	return nil
}
