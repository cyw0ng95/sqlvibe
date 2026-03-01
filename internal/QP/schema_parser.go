package QP

import (
"fmt"
"strings"
)

// ParsedColumnDef represents a parsed column definition.
type ParsedColumnDef struct {
Name       string
TypeName   string
NotNull    bool
PrimaryKey bool
Unique     bool
Default    string // raw default expression as string
Check      string // raw check expression as string
}

// ParsedConstraint represents a table-level constraint.
type ParsedConstraint struct {
Type     string   // "PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CHECK"
Name     string   // constraint name (may be empty)
Columns  []string // columns for PK/UNIQUE/FK
RefTable string   // referenced table for FK
RefCols  []string // referenced columns for FK
Expr     string   // raw expression for CHECK
}

// ParsedTableSchema represents the result of parsing a CREATE TABLE statement.
type ParsedTableSchema struct {
TableName   string
Columns     []ParsedColumnDef
Constraints []ParsedConstraint
IfNotExists bool
Temporary   bool
}

// ParsedViewSchema represents the result of parsing a CREATE VIEW statement.
type ParsedViewSchema struct {
ViewName    string
Columns     []string // optional column list (not yet in grammar)
Query       string   // raw SELECT query text (reconstructed)
IfNotExists bool
Temporary   bool
}

// ParseTableSchema parses a CREATE TABLE DDL string and returns its schema.
func ParseTableSchema(ddl string) (*ParsedTableSchema, error) {
tokenizer := NewTokenizer(ddl)
tokens, err := tokenizer.Tokenize()
if err != nil {
return nil, err
}
p := NewParser(tokens)
ast, err := p.Parse()
if err != nil {
return nil, err
}
stmt, ok := ast.(*CreateTableStmt)
if !ok {
return nil, fmt.Errorf("expected CREATE TABLE statement, got %T", ast)
}
result := &ParsedTableSchema{
TableName:   stmt.Name,
IfNotExists: stmt.IfNotExists,
Temporary:   stmt.Temporary,
}
for _, col := range stmt.Columns {
pcd := ParsedColumnDef{
Name:       col.Name,
TypeName:   col.Type,
NotNull:    col.NotNull,
PrimaryKey: col.PrimaryKey,
Unique:     col.Unique,
}
if col.Default != nil {
pcd.Default = ExprToString(col.Default)
}
if col.Check != nil {
pcd.Check = ExprToString(col.Check)
}
result.Columns = append(result.Columns, pcd)
}
// Table-level UNIQUE constraints
for _, cols := range stmt.UniqueKeys {
result.Constraints = append(result.Constraints, ParsedConstraint{
Type:    "UNIQUE",
Columns: cols,
})
}
// Table-level CHECK constraints
for _, chk := range stmt.TableChecks {
result.Constraints = append(result.Constraints, ParsedConstraint{
Type: "CHECK",
Expr: ExprToString(chk),
})
}
// Table-level FOREIGN KEY constraints
for _, fk := range stmt.ForeignKeys {
result.Constraints = append(result.Constraints, ParsedConstraint{
Type:     "FOREIGN KEY",
Columns:  fk.ChildColumns,
RefTable: fk.ParentTable,
RefCols:  fk.ParentColumns,
})
}
return result, nil
}

// ParseViewSchema parses a CREATE VIEW DDL string and returns its schema.
func ParseViewSchema(ddl string) (*ParsedViewSchema, error) {
tokenizer := NewTokenizer(ddl)
tokens, err := tokenizer.Tokenize()
if err != nil {
return nil, err
}
p := NewParser(tokens)
ast, err := p.Parse()
if err != nil {
return nil, err
}
stmt, ok := ast.(*CreateViewStmt)
if !ok {
return nil, fmt.Errorf("expected CREATE VIEW statement, got %T", ast)
}
result := &ParsedViewSchema{
ViewName:    stmt.Name,
IfNotExists: stmt.IfNotExists,
}
if stmt.Select != nil {
result.Query = selectToString(stmt.Select)
}
return result, nil
}

// ExprToString converts an AST expression to a SQL string representation.
func ExprToString(expr Expr) string {
if expr == nil {
return ""
}
switch e := expr.(type) {
case *Literal:
if e.Value == nil {
return "NULL"
}
switch v := e.Value.(type) {
case string:
return "'" + strings.ReplaceAll(v, "'", "''") + "'"
case int64:
return fmt.Sprintf("%d", v)
case float64:
return fmt.Sprintf("%g", v)
case bool:
if v {
return "1"
}
return "0"
default:
return fmt.Sprintf("%v", v)
}
case *ColumnRef:
if e.Table != "" {
return e.Table + "." + e.Name
}
return e.Name
case *BinaryExpr:
return "(" + ExprToString(e.Left) + " " + tokenTypeToStr(e.Op) + " " + ExprToString(e.Right) + ")"
case *UnaryExpr:
return tokenTypeToStr(e.Op) + " " + ExprToString(e.Expr)
case *FuncCall:
args := make([]string, len(e.Args))
for i, a := range e.Args {
args[i] = ExprToString(a)
}
return e.Name + "(" + strings.Join(args, ", ") + ")"
case *CaseExpr:
parts := []string{"CASE"}
if e.Operand != nil {
parts = append(parts, ExprToString(e.Operand))
}
for _, w := range e.Whens {
parts = append(parts, "WHEN", ExprToString(w.Condition), "THEN", ExprToString(w.Result))
}
if e.Else != nil {
parts = append(parts, "ELSE", ExprToString(e.Else))
}
parts = append(parts, "END")
return strings.Join(parts, " ")
case *CastExpr:
return "CAST(" + ExprToString(e.Expr) + " AS " + e.TypeSpec.Name + ")"
case *CollateExpr:
return ExprToString(e.Expr) + " COLLATE " + e.Collation
case *SubqueryExpr:
return "(SELECT ...)"
case *AliasExpr:
return ExprToString(e.Expr) + " AS " + e.Alias
case *PlaceholderExpr:
if e.Name != "" {
return ":" + e.Name
}
return "?"
default:
return fmt.Sprintf("%v", expr)
}
}

// tokenTypeToStr converts a TokenType operator to its string form.
func tokenTypeToStr(op TokenType) string {
switch op {
case TokenPlus:
return "+"
case TokenMinus:
return "-"
case TokenAsterisk:
return "*"
case TokenSlash:
return "/"
case TokenPercent:
return "%"
case TokenEq:
return "="
case TokenNe:
return "<>"
case TokenLt:
return "<"
case TokenLe:
return "<="
case TokenGt:
return ">"
case TokenGe:
return ">="
case TokenAnd:
return "AND"
case TokenOr:
return "OR"
case TokenNot:
return "NOT"
default:
return fmt.Sprintf("op(%d)", int(op))
}
}

// selectToString produces a rough SQL string from a SelectStmt (for schema display).
func selectToString(s *SelectStmt) string {
if s == nil {
return ""
}
cols := make([]string, len(s.Columns))
for i, c := range s.Columns {
cols[i] = ExprToString(c)
}
q := "SELECT " + strings.Join(cols, ", ")
if s.From != nil {
q += " FROM " + tableRefToString(s.From)
}
if s.Where != nil {
q += " WHERE " + ExprToString(s.Where)
}
return q
}

func tableRefToString(t *TableRef) string {
if t == nil {
return ""
}
name := t.Name
if t.Schema != "" {
name = t.Schema + "." + name
}
if t.Alias != "" {
name += " AS " + t.Alias
}
return name
}
