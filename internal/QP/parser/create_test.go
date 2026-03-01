package parser_test

import (
"testing"

"github.com/cyw0ng95/sqlvibe/internal/QP"
)

func parseSQL(t *testing.T, sql string) QP.ASTNode {
t.Helper()
tokenizer := QP.NewTokenizer(sql)
tokens, err := tokenizer.Tokenize()
if err != nil {
t.Fatalf("Tokenize(%q): %v", sql, err)
}
ast, err := QP.NewParser(tokens).Parse()
if err != nil {
t.Fatalf("Parse(%q): %v", sql, err)
}
return ast
}

func TestParse_CreateTable_Basic(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE t (id INTEGER, name TEXT)")
stmt, ok := ast.(*QP.CreateTableStmt)
if !ok {
t.Fatalf("expected *CreateTableStmt, got %T", ast)
}
if stmt.Name != "t" {
t.Errorf("table name = %q, want %q", stmt.Name, "t")
}
if len(stmt.Columns) != 2 {
t.Fatalf("columns = %d, want 2", len(stmt.Columns))
}
if stmt.Columns[0].Name != "id" || stmt.Columns[0].Type != "INTEGER" {
t.Errorf("col[0] = %q %q, want id INTEGER", stmt.Columns[0].Name, stmt.Columns[0].Type)
}
}

func TestParse_CreateTable_PrimaryKey(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
stmt := ast.(*QP.CreateTableStmt)
if !stmt.Columns[0].PrimaryKey {
t.Error("id should be PRIMARY KEY")
}
if !stmt.Columns[1].NotNull {
t.Error("name should be NOT NULL")
}
}

func TestParse_CreateTable_Unique(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE t (id INTEGER, email TEXT UNIQUE)")
stmt := ast.(*QP.CreateTableStmt)
if !stmt.Columns[1].Unique {
t.Error("email should be UNIQUE")
}
}

func TestParse_CreateTable_ForeignKey(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))")
stmt := ast.(*QP.CreateTableStmt)
if stmt.Columns[1].ForeignKey == nil {
t.Fatal("user_id should have a foreign key reference")
}
if stmt.Columns[1].ForeignKey.ParentTable != "users" {
t.Errorf("fk parent = %q, want %q", stmt.Columns[1].ForeignKey.ParentTable, "users")
}
}

func TestParse_CreateTable_IfNotExists(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE IF NOT EXISTS t (id INTEGER)")
stmt := ast.(*QP.CreateTableStmt)
if !stmt.IfNotExists {
t.Error("IfNotExists should be true")
}
}

func TestParse_CreateIndex(t *testing.T) {
ast := parseSQL(t, "CREATE INDEX idx_name ON users (name)")
stmt, ok := ast.(*QP.CreateIndexStmt)
if !ok {
t.Fatalf("expected *CreateIndexStmt, got %T", ast)
}
if stmt.Name != "idx_name" {
t.Errorf("index name = %q, want idx_name", stmt.Name)
}
if stmt.Table != "users" {
t.Errorf("table name = %q, want users", stmt.Table)
}
if len(stmt.Columns) == 0 || stmt.Columns[0] != "name" {
t.Errorf("index columns = %v, want [name]", stmt.Columns)
}
}

func TestParse_CreateView(t *testing.T) {
ast := parseSQL(t, "CREATE VIEW v AS SELECT id, name FROM users")
stmt, ok := ast.(*QP.CreateViewStmt)
if !ok {
t.Fatalf("expected *CreateViewStmt, got %T", ast)
}
if stmt.Name != "v" {
t.Errorf("view name = %q, want v", stmt.Name)
}
if stmt.Select == nil {
t.Fatal("view SELECT should not be nil")
}
}

func TestParse_CreateTable_CheckConstraint(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE t (id INTEGER, age INTEGER CHECK (age >= 0))")
stmt := ast.(*QP.CreateTableStmt)
if stmt.Columns[1].Check == nil {
t.Error("age should have a CHECK constraint")
}
}

func TestParse_CreateTable_TableLevelUniqueConstraint(t *testing.T) {
ast := parseSQL(t, "CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE(a, b))")
stmt := ast.(*QP.CreateTableStmt)
if len(stmt.UniqueKeys) == 0 {
t.Error("expected table-level UNIQUE constraint")
}
if len(stmt.UniqueKeys[0]) != 2 {
t.Errorf("expected 2 columns in UNIQUE, got %d", len(stmt.UniqueKeys[0]))
}
}
