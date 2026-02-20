package E111

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E11102_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE config (id INTEGER, key TEXT, value INTEGER, enabled INTEGER)"},
		{"InsertData", "INSERT INTO config VALUES (1, 'max_connections', 100, 1), (2, 'timeout', 30, 1), (3, 'cache_size', 256, 0), (4, 'max_requests', 50, 1)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	singleRowTests := []struct {
		name string
		sql  string
	}{
		{"SingleRowBasic", "SELECT value FROM config WHERE key = 'max_connections'"},
		{"SingleRowWithWhere", "SELECT value FROM config WHERE key = 'timeout' AND enabled = 1"},
		{"SingleRowNoResults", "SELECT value FROM config WHERE key = 'nonexistent'"},
		{"SingleRowMultipleColumns", "SELECT key, value, enabled FROM config WHERE key = 'cache_size'"},
		{"SingleRowAll", "SELECT * FROM config WHERE key = 'max_requests'"},
		{"SingleRowDistinct", "SELECT DISTINCT enabled FROM config"},
		{"SingleRowAggregates", "SELECT MAX(value), MIN(value), AVG(value) FROM config WHERE enabled = 1"},
		{"SingleRowCount", "SELECT COUNT(*) FROM config WHERE enabled = 1"},
		{"SingleRowSum", "SELECT SUM(value) FROM config WHERE enabled = 1"},
		{"SingleRowInExpression", "SELECT value + 10 FROM config WHERE key = 'max_connections'"},
		{"SingleRowWithFunction", "SELECT UPPER(key) FROM config WHERE id = 1"},
		{"SingleRowWithOrderBy", "SELECT * FROM config WHERE enabled = 1 ORDER BY value DESC LIMIT 1"},
		{"SingleRowWithLimit", "SELECT * FROM config ORDER BY id LIMIT 2"},
		{"SingleRowWithGroupBy", "SELECT key, COUNT(*) FROM config GROUP BY key"},
		{"SingleRowWithHaving", "SELECT key, COUNT(*) FROM config GROUP BY key HAVING COUNT(*) = 1"},
		{"SingleRowComplexWhere", "SELECT * FROM config WHERE id > 2 AND id < 4 AND enabled = 1"},
		{"SingleRowInSubquery", "SELECT * FROM config WHERE value > (SELECT AVG(value) FROM config WHERE enabled = 1)"},
		{"SingleRowWithExists", "SELECT * FROM config WHERE EXISTS (SELECT 1 FROM config c WHERE c.id = config.id - 1 AND c.key = 'max_connections')"},
		{"SingleRowWithNotExists", "SELECT * FROM config WHERE NOT EXISTS (SELECT 1 FROM config c WHERE c.key = 'nonexistent')"},
		{"SingleRowWithIn", "SELECT * FROM config WHERE key IN ('max_connections', 'timeout', 'max_requests')"},
		{"SingleRowWithNotIn", "SELECT * FROM config WHERE key NOT IN ('cache_size', 'nonexistent')"},
		{"SingleRowWithBetween", "SELECT * FROM config WHERE value BETWEEN 50 AND 150"},
		{"SingleRowWithLike", "SELECT * FROM config WHERE key LIKE '%connection%'"},
		{"SingleRowOrderByMultiple", "SELECT * FROM config ORDER BY key, value DESC LIMIT 1"},
		{"SingleRowWithCase", "SELECT *, CASE WHEN value > 50 THEN 'high' ELSE 'low' END AS priority FROM config"},
		{"SingleRowWithNull", "SELECT * FROM config WHERE enabled IS NULL"},
		{"SingleRowWithNotNull", "SELECT * FROM config WHERE enabled IS NOT NULL"},
		{"SingleRowWithAnd", "SELECT * FROM config WHERE enabled = 1 AND value < 100"},
		{"SingleRowWithOr", "SELECT * FROM config WHERE enabled = 0 OR value > 200"},
		{"SingleRowWithMath", "SELECT * FROM config WHERE value * 2 < 200"},
		{"SingleRowWithDivision", "SELECT * FROM config WHERE value / 10 > 5"},
		{"SingleRowWithModulo", "SELECT * FROM config WHERE value % 10 = 0"},
		{"SingleRowWithCast", "SELECT CAST(value AS TEXT) FROM config WHERE id = 1"},
		{"SingleRowCoalesce", "SELECT COALESCE(value, 0) FROM config WHERE id = 3"},
		{"SingleRowNullif", "SELECT NULLIF(value, 0) FROM config WHERE id = 1"},
		{"SingleRowComplexExpression", "SELECT id, value * enabled + CASE WHEN enabled = 1 THEN 10 ELSE 0 END FROM config"},
		{"SingleRowWithSubqueryCount", "SELECT * FROM config WHERE value IN (SELECT MAX(value) FROM config GROUP BY id)"},
		{"SingleRowWithUnion", "SELECT value FROM config WHERE key = 'timeout' UNION SELECT value FROM config WHERE key = 'max_connections'"},
		{"SingleRowWithLimitOffset", "SELECT * FROM config ORDER BY id LIMIT 1 OFFSET 2"},
		{"SingleRowWithDistinctLimit", "SELECT DISTINCT key FROM config LIMIT 2"},
		{"SingleRowCountWithGroupBy", "SELECT id, COUNT(*) OVER () AS total, COUNT(*) OVER (PARTITION BY key) AS key_count FROM config"},
		{"SingleRowLagLead", "SELECT id, value, LAG(value, 1) OVER (ORDER BY id) AS prev_value FROM config"},
		{"SingleRowLead", "SELECT id, value, LEAD(value, 1) OVER (ORDER BY id) AS next_value FROM config"},
		{"SingleRowFirstLast", "SELECT id, value, FIRST_VALUE(value) OVER (ORDER BY id) AS first_val, LAST_VALUE(value) OVER (ORDER BY id) AS last_val FROM config"},
	}

	for _, tt := range singleRowTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
