package W012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W012_RankDenseRank_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	for _, stmt := range []string{
		"CREATE TABLE scores (id INTEGER, name TEXT, dept TEXT, score INTEGER)",
		"INSERT INTO scores VALUES (1, 'Alice', 'Eng', 90)",
		"INSERT INTO scores VALUES (2, 'Bob', 'Eng', 90)",
		"INSERT INTO scores VALUES (3, 'Carol', 'HR', 80)",
		"INSERT INTO scores VALUES (4, 'Dave', 'HR', 80)",
		"INSERT INTO scores VALUES (5, 'Eve', 'Eng', 95)",
		"INSERT INTO scores VALUES (6, 'Frank', 'HR', 75)",
	} {
		sqlvibeDB.Exec(stmt)
		sqliteDB.Exec(stmt)
	}

	tests := []struct{ name, sql string }{
		{
			"RankWithTies",
			"SELECT id, name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores ORDER BY rnk, id",
		},
		{
			"DenseRankWithTies",
			"SELECT id, name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY drnk, id",
		},
		{
			"RankVsDenseRankComparison",
			"SELECT id, name, score, RANK() OVER (ORDER BY score DESC) AS rnk, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY score DESC, id",
		},
		{
			"RankPartitionByDept",
			"SELECT id, dept, score, RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM scores ORDER BY dept, rnk, id",
		},
		{
			"DenseRankPartitionByDept",
			"SELECT id, dept, score, DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS drnk FROM scores ORDER BY dept, drnk, id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
