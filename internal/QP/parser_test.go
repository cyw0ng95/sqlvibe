package QP

import (
	"testing"
)

// Helper function to parse SQL
func parseSQL(t *testing.T, sql string) ASTNode {
	t.Helper()
	tok := NewTokenizer(sql)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize %q: %v", sql, err)
	}
	parser := NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return ast
}

func TestParser_SelectSimple(t *testing.T) {
	ast := parseSQL(t, "SELECT 1")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Distinct {
		t.Error("SELECT should not be DISTINCT")
	}
}

func TestParser_SelectMultipleColumns(t *testing.T) {
	ast := parseSQL(t, "SELECT a, b, c FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(stmt.Columns))
	}
}

func TestParser_SelectStar(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Errorf("expected 1 column (star), got %d", len(stmt.Columns))
	}
	col, ok := stmt.Columns[0].(*ColumnRef)
	if !ok || col.Name != "*" {
		t.Error("expected star column")
	}
}

func TestParser_SelectDistinct(t *testing.T) {
	ast := parseSQL(t, "SELECT DISTINCT name FROM users")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if !stmt.Distinct {
		t.Error("expected DISTINCT")
	}
}

func TestParser_SelectWithWhere(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users WHERE id = 1")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	bin, ok := stmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Op != TokenEq {
		t.Errorf("expected = operator, got %v", bin.Op)
	}
}

func TestParser_SelectWithOrderBy(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users ORDER BY name")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY, got %d", len(stmt.OrderBy))
	}
	if stmt.OrderBy[0].Desc {
		t.Error("ORDER BY should not be DESC")
	}
}

func TestParser_SelectWithOrderByDesc(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users ORDER BY name DESC")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if !stmt.OrderBy[0].Desc {
		t.Error("ORDER BY should be DESC")
	}
}

func TestParser_SelectWithLimit(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users LIMIT 10")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Limit == nil {
		t.Fatal("expected LIMIT")
	}
}

func TestParser_SelectWithLimitOffset(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users LIMIT 10 OFFSET 5")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Limit == nil {
		t.Fatal("expected LIMIT")
	}
	if stmt.Offset == nil {
		t.Fatal("expected OFFSET")
	}
}

func TestParser_SelectWithGroupBy(t *testing.T) {
	ast := parseSQL(t, "SELECT dept, COUNT(*) FROM users GROUP BY dept")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.GroupBy) != 1 {
		t.Errorf("expected 1 GROUP BY, got %d", len(stmt.GroupBy))
	}
}

func TestParser_SelectWithHaving(t *testing.T) {
	ast := parseSQL(t, "SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 5")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Having == nil {
		t.Fatal("expected HAVING")
	}
}

func TestParser_InsertSimple(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	stmt, ok := ast.(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", ast)
	}
	if stmt.Table != "users" {
		t.Errorf("expected table 'users', got %s", stmt.Table)
	}
	if len(stmt.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(stmt.Columns))
	}
	if len(stmt.Values) != 1 {
		t.Errorf("expected 1 row, got %d", len(stmt.Values))
	}
}

func TestParser_InsertMultipleRows(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	stmt, ok := ast.(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", ast)
	}
	if len(stmt.Values) != 2 {
		t.Errorf("expected 2 rows, got %d", len(stmt.Values))
	}
}

func TestParser_InsertDefaults(t *testing.T) {
	ast := parseSQL(t, "INSERT INTO users DEFAULT VALUES")
	stmt, ok := ast.(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", ast)
	}
	if !stmt.UseDefaults {
		t.Error("expected DEFAULT VALUES")
	}
}

func TestParser_UpdateSimple(t *testing.T) {
	ast := parseSQL(t, "UPDATE users SET name = 'Bob' WHERE id = 1")
	stmt, ok := ast.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", ast)
	}
	if stmt.Table != "users" {
		t.Errorf("expected table 'users', got %s", stmt.Table)
	}
	if len(stmt.Set) != 1 {
		t.Errorf("expected 1 SET clause, got %d", len(stmt.Set))
	}
}

func TestParser_UpdateMultipleColumns(t *testing.T) {
	ast := parseSQL(t, "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1")
	stmt, ok := ast.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", ast)
	}
	if len(stmt.Set) != 2 {
		t.Errorf("expected 2 SET clauses, got %d", len(stmt.Set))
	}
}

func TestParser_DeleteSimple(t *testing.T) {
	ast := parseSQL(t, "DELETE FROM users WHERE id = 1")
	stmt, ok := ast.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", ast)
	}
	if stmt.Table != "users" {
		t.Errorf("expected table 'users', got %s", stmt.Table)
	}
}

func TestParser_DeleteAll(t *testing.T) {
	ast := parseSQL(t, "DELETE FROM users")
	stmt, ok := ast.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", ast)
	}
	if stmt.Where != nil {
		t.Error("expected no WHERE clause")
	}
}

func TestParser_Begin(t *testing.T) {
	ast := parseSQL(t, "BEGIN")
	_, ok := ast.(*BeginStmt)
	if !ok {
		t.Fatalf("expected BeginStmt, got %T", ast)
	}
}

func TestParser_Commit(t *testing.T) {
	ast := parseSQL(t, "COMMIT")
	_, ok := ast.(*CommitStmt)
	if !ok {
		t.Fatalf("expected CommitStmt, got %T", ast)
	}
}

func TestParser_Rollback(t *testing.T) {
	ast := parseSQL(t, "ROLLBACK")
	_, ok := ast.(*RollbackStmt)
	if !ok {
		t.Fatalf("expected RollbackStmt, got %T", ast)
	}
}

func TestParser_Vacuum(t *testing.T) {
	ast := parseSQL(t, "VACUUM")
	_, ok := ast.(*VacuumStmt)
	if !ok {
		t.Fatalf("expected VacuumStmt, got %T", ast)
	}
}

func TestParser_Pragma(t *testing.T) {
	ast := parseSQL(t, "PRAGMA journal_mode")
	_, ok := ast.(*PragmaStmt)
	if !ok {
		t.Fatalf("expected PragmaStmt, got %T", ast)
	}
}

func TestParser_Setops_Union(t *testing.T) {
	ast := parseSQL(t, "SELECT a FROM t1 UNION SELECT a FROM t2")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.SetOp != "UNION" {
		t.Errorf("expected UNION, got %s", stmt.SetOp)
	}
	if stmt.SetOpRight == nil {
		t.Fatal("expected right side of UNION")
	}
}

func TestParser_Setops_UnionAll(t *testing.T) {
	ast := parseSQL(t, "SELECT a FROM t1 UNION ALL SELECT a FROM t2")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.SetOp != "UNION" {
		t.Errorf("expected UNION, got %s", stmt.SetOp)
	}
	if !stmt.SetOpAll {
		t.Error("expected UNION ALL flag")
	}
}

func TestParser_Setops_Intersect(t *testing.T) {
	ast := parseSQL(t, "SELECT a FROM t1 INTERSECT SELECT a FROM t2")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.SetOp != "INTERSECT" {
		t.Errorf("expected INTERSECT, got %s", stmt.SetOp)
	}
}

func TestParser_Setops_Except(t *testing.T) {
	ast := parseSQL(t, "SELECT a FROM t1 EXCEPT SELECT a FROM t2")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.SetOp != "EXCEPT" {
		t.Errorf("expected EXCEPT, got %s", stmt.SetOp)
	}
}

func TestParser_AggregateFunctions(t *testing.T) {
	tests := []struct {
		sql  string
		name string
	}{
		{"SELECT COUNT(*) FROM t", "COUNT"},
		{"SELECT SUM(x) FROM t", "SUM"},
		{"SELECT AVG(x) FROM t", "AVG"},
		{"SELECT MIN(x) FROM t", "MIN"},
		{"SELECT MAX(x) FROM t", "MAX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := parseSQL(t, tt.sql)
			stmt, ok := ast.(*SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", ast)
			}
			if len(stmt.Columns) != 1 {
				t.Fatalf("expected 1 column")
			}
			fn, ok := stmt.Columns[0].(*FuncCall)
			if !ok {
				t.Fatalf("expected FuncCall, got %T", stmt.Columns[0])
			}
			if fn.Name != tt.name {
				t.Errorf("expected function %s, got %s", tt.name, fn.Name)
			}
		})
	}
}

func TestParser_ColumnAlias(t *testing.T) {
	ast := parseSQL(t, "SELECT x AS col1, y AS col2 FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 2 {
		t.Fatalf("expected 2 columns")
	}
	alias1, ok := stmt.Columns[0].(*AliasExpr)
	if !ok {
		t.Fatalf("expected AliasExpr, got %T", stmt.Columns[0])
	}
	if alias1.Alias != "col1" {
		t.Errorf("expected alias 'col1', got %s", alias1.Alias)
	}
}

func TestParser_TableAlias(t *testing.T) {
	ast := parseSQL(t, "SELECT u.name FROM users u")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil {
		t.Fatal("expected FROM")
	}
	if stmt.From.Alias != "u" {
		t.Errorf("expected alias 'u', got %s", stmt.From.Alias)
	}
}

func TestParser_QuotedIdentifiers(t *testing.T) {
	ast := parseSQL(t, `SELECT "column_name" FROM "table_name"`)
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(stmt.Columns))
	}
}

func TestParser_StringLiterals(t *testing.T) {
	ast := parseSQL(t, "SELECT 'hello', 'world' FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 2 {
		t.Fatalf("expected 2 columns")
	}
}

func TestParser_NumericLiterals(t *testing.T) {
	ast := parseSQL(t, "SELECT 42, 3.14 FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 2 {
		t.Fatalf("expected 2 columns")
	}
}

func TestParser_NullLiteral(t *testing.T) {
	ast := parseSQL(t, "SELECT NULL FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column")
	}
	lit, ok := stmt.Columns[0].(*Literal)
	if !ok {
		t.Fatalf("expected Literal, got %T", stmt.Columns[0])
	}
	if lit.Value != nil {
		t.Errorf("expected nil value, got %v", lit.Value)
	}
}

func TestParser_AndExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE x > 1 AND y < 10")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE")
	}
	bin, ok := stmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Op != TokenAnd {
		t.Errorf("expected AND, got %v", bin.Op)
	}
}

func TestParser_OrExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE x = 1 OR y = 2")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE")
	}
	bin, ok := stmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Op != TokenOr {
		t.Errorf("expected OR, got %v", bin.Op)
	}
}

func TestParser_NotExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM t WHERE NOT x")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE")
	}
	_, ok = stmt.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", stmt.Where)
	}
}

func TestParser_Join(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil || stmt.From.Join == nil {
		t.Fatal("expected JOIN")
	}
}

func TestParser_LeftJoin(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil || stmt.From.Join == nil {
		t.Fatal("expected LEFT JOIN")
	}
	join := stmt.From.Join
	if join.Type != "LEFT" {
		t.Errorf("expected LEFT, got %s", join.Type)
	}
}

func TestParser_Using(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users JOIN orders USING (id)")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil || stmt.From.Join == nil {
		t.Fatal("expected JOIN")
	}
	join := stmt.From.Join
	if len(join.UsingColumns) != 1 || join.UsingColumns[0] != "id" {
		t.Errorf("expected USING (id), got %v", join.UsingColumns)
	}
}

func TestParser_NaturalJoin(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM users NATURAL JOIN orders")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil || stmt.From.Join == nil {
		t.Fatal("expected NATURAL JOIN")
	}
	if !stmt.From.Join.Natural {
		t.Error("expected NATURAL JOIN")
	}
}

func TestParser_Subquery(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM (SELECT id FROM users) AS sub")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil || stmt.From.Subquery == nil {
		t.Fatal("expected subquery")
	}
	if stmt.From.Alias != "sub" {
		t.Errorf("expected alias 'sub', got %s", stmt.From.Alias)
	}
}

func TestParser_CTE(t *testing.T) {
	ast := parseSQL(t, "WITH cte AS (SELECT 1) SELECT * FROM cte")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.CTEs) != 1 {
		t.Errorf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if stmt.CTEs[0].Name != "cte" {
		t.Errorf("expected CTE name 'cte', got %s", stmt.CTEs[0].Name)
	}
}

func TestParser_CTE_Recursive(t *testing.T) {
	ast := parseSQL(t, "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT * FROM cte) SELECT * FROM cte")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.CTEs) != 1 {
		t.Errorf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if !stmt.CTEs[0].Recursive {
		t.Error("expected recursive CTE")
	}
}

func TestParser_CreateTable(t *testing.T) {
	ast := parseSQL(t, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	stmt, ok := ast.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", ast)
	}
	if stmt.Name != "users" {
		t.Errorf("expected table name 'users', got %s", stmt.Name)
	}
	if len(stmt.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(stmt.Columns))
	}
}

func TestParser_CreateTable_WithAutoIncrement(t *testing.T) {
	ast := parseSQL(t, "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	stmt, ok := ast.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", ast)
	}
	if len(stmt.Columns) > 0 {
		col := stmt.Columns[0]
		if !col.IsAutoincrement {
			t.Error("expected AUTOINCREMENT")
		}
	}
}

func TestParser_CreateTable_WithDefault(t *testing.T) {
	ast := parseSQL(t, "CREATE TABLE users (id INTEGER, name TEXT DEFAULT 'unknown')")
	stmt, ok := ast.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", ast)
	}
	if len(stmt.Columns) > 1 {
		col := stmt.Columns[1]
		if col.Default == nil {
			t.Error("expected DEFAULT value")
		}
	}
}

func TestParser_DropTable(t *testing.T) {
	ast := parseSQL(t, "DROP TABLE users")
	stmt, ok := ast.(*DropTableStmt)
	if !ok {
		t.Fatalf("expected DropTableStmt, got %T", ast)
	}
	if stmt.Name != "users" {
		t.Errorf("expected table name 'users', got %s", stmt.Name)
	}
}

func TestParser_CreateIndex(t *testing.T) {
	ast := parseSQL(t, "CREATE INDEX idx_name ON users (name)")
	stmt, ok := ast.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", ast)
	}
	if stmt.Name != "idx_name" {
		t.Errorf("expected index name 'idx_name', got %s", stmt.Name)
	}
	if stmt.Table != "users" {
		t.Errorf("expected table 'users', got %s", stmt.Table)
	}
}

func TestParser_CreateIndex_Unique(t *testing.T) {
	ast := parseSQL(t, "CREATE UNIQUE INDEX idx_name ON users (name)")
	stmt, ok := ast.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", ast)
	}
	if !stmt.Unique {
		t.Error("expected UNIQUE index")
	}
}

func TestParser_DropIndex(t *testing.T) {
	ast := parseSQL(t, "DROP INDEX idx_name")
	stmt, ok := ast.(*DropIndexStmt)
	if !ok {
		t.Fatalf("expected DropIndexStmt, got %T", ast)
	}
	if stmt.Name != "idx_name" {
		t.Errorf("expected index name 'idx_name', got %s", stmt.Name)
	}
}

func TestParser_Explain(t *testing.T) {
	ast := parseSQL(t, "EXPLAIN SELECT * FROM users")
	_, ok := ast.(*ExplainStmt)
	if !ok {
		t.Fatalf("expected ExplainStmt, got %T", ast)
	}
}

func TestParser_Analyze(t *testing.T) {
	ast := parseSQL(t, "ANALYZE")
	_, ok := ast.(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected AnalyzeStmt, got %T", ast)
	}
}

func TestParser_CaseExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column")
	}
	_, ok = stmt.Columns[0].(*CaseExpr)
	if !ok {
		t.Fatalf("expected CaseExpr, got %T", stmt.Columns[0])
	}
}

func TestParser_CastExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT CAST(x AS INTEGER) FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column")
	}
	_, ok = stmt.Columns[0].(*CastExpr)
	if !ok {
		t.Fatalf("expected CastExpr, got %T", stmt.Columns[0])
	}
}

func TestParser_ParenthesizedExpression(t *testing.T) {
	ast := parseSQL(t, "SELECT (1 + 2) * 3 FROM t")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column")
	}
}

func TestParser_SchemaQualifiedTable(t *testing.T) {
	ast := parseSQL(t, "SELECT * FROM main.users")
	stmt, ok := ast.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	if stmt.From == nil {
		t.Fatal("expected FROM")
	}
	if stmt.From.Schema != "main" {
		t.Errorf("expected schema 'main', got %s", stmt.From.Schema)
	}
	if stmt.From.Name != "users" {
		t.Errorf("expected table 'users', got %s", stmt.From.Name)
	}
}
