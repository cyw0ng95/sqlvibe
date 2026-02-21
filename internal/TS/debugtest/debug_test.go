package debugtest

import (
"fmt"
"testing"
"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestDebugSubqueryLimit(t *testing.T) {
sv, _ := sqlvibe.Open(":memory:")
defer sv.Close()

sv.Exec("CREATE TABLE t (id INTEGER, salary INTEGER)")
sv.Exec("INSERT INTO t VALUES (1, 85000), (2, 85000), (3, 85000), (4, 78870)")

r2, _ := sv.Query("SELECT id FROM t WHERE id IN (SELECT id FROM t ORDER BY salary DESC LIMIT 2)")
fmt.Printf("SELECT with IN subquery returns %d rows (expected 2)\n", len(r2.Data))
}
