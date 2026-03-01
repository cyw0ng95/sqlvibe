package parser_test

import (
"testing"

"github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestParse_AlterTable_AddColumn(t *testing.T) {
ast := parseSQL(t, "ALTER TABLE users ADD COLUMN phone TEXT")
stmt, ok := ast.(*QP.AlterTableStmt)
if !ok {
t.Fatalf("expected *AlterTableStmt, got %T", ast)
}
if stmt.Action != "ADD_COLUMN" {
t.Errorf("action = %q, want ADD_COLUMN", stmt.Action)
}
if stmt.Column == nil {
t.Fatal("Column should not be nil for ADD COLUMN")
}
if stmt.Column.Name != "phone" {
t.Errorf("new column name = %q, want phone", stmt.Column.Name)
}
}

func TestParse_AlterTable_DropColumn(t *testing.T) {
ast := parseSQL(t, "ALTER TABLE users DROP COLUMN phone")
stmt := ast.(*QP.AlterTableStmt)
if stmt.Action != "DROP_COLUMN" {
t.Errorf("action = %q, want DROP_COLUMN", stmt.Action)
}
if stmt.Column == nil || stmt.Column.Name != "phone" {
t.Errorf("dropped column = %v, want phone", stmt.Column)
}
}

func TestParse_AlterTable_RenameColumn(t *testing.T) {
ast := parseSQL(t, "ALTER TABLE users RENAME COLUMN old_name TO new_name")
stmt := ast.(*QP.AlterTableStmt)
if stmt.Action != "RENAME_COLUMN" {
t.Errorf("action = %q, want RENAME_COLUMN", stmt.Action)
}
if stmt.Column == nil || stmt.Column.Name != "old_name" {
t.Errorf("old column = %v, want old_name", stmt.Column)
}
if stmt.NewName != "new_name" {
t.Errorf("new name = %q, want new_name", stmt.NewName)
}
}

func TestParse_AlterTable_RenameTable(t *testing.T) {
ast := parseSQL(t, "ALTER TABLE old_name RENAME TO new_name")
stmt := ast.(*QP.AlterTableStmt)
if stmt.Action != "RENAME_TO" {
t.Errorf("action = %q, want RENAME_TO", stmt.Action)
}
if stmt.Table != "old_name" {
t.Errorf("table = %q, want old_name", stmt.Table)
}
if stmt.NewName != "new_name" {
t.Errorf("new name = %q, want new_name", stmt.NewName)
}
}

func TestParse_DropTable(t *testing.T) {
ast := parseSQL(t, "DROP TABLE users")
stmt, ok := ast.(*QP.DropTableStmt)
if !ok {
t.Fatalf("expected *DropTableStmt, got %T", ast)
}
if stmt.Name != "users" {
t.Errorf("table name = %q, want users", stmt.Name)
}
}

func TestParse_DropTable_IfExists(t *testing.T) {
ast := parseSQL(t, "DROP TABLE IF EXISTS users")
stmt := ast.(*QP.DropTableStmt)
if !stmt.IfExists {
t.Error("IfExists should be true")
}
}

func TestParse_DropIndex(t *testing.T) {
ast := parseSQL(t, "DROP INDEX idx_name")
stmt, ok := ast.(*QP.DropIndexStmt)
if !ok {
t.Fatalf("expected *DropIndexStmt, got %T", ast)
}
if stmt.Name != "idx_name" {
t.Errorf("index name = %q, want idx_name", stmt.Name)
}
}

func TestParse_DropView(t *testing.T) {
ast := parseSQL(t, "DROP VIEW v")
stmt, ok := ast.(*QP.DropViewStmt)
if !ok {
t.Fatalf("expected *DropViewStmt, got %T", ast)
}
if stmt.Name != "v" {
t.Errorf("view name = %q, want v", stmt.Name)
}
}
