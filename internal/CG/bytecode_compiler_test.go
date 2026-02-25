package CG

import (
	"testing"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// parseSelect is a helper that parses a SELECT SQL string.
func parseSelect(t *testing.T, sql string) *QP.SelectStmt {
	t.Helper()
	tok := QP.NewTokenizer(sql)
	tokens, err := tok.Tokenize()
	if err != nil {
		t.Fatalf("tokenize %q: %v", sql, err)
	}
	parser := QP.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	stmt, ok := ast.(*QP.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", ast)
	}
	return stmt
}

func TestBytecodeCompiler_SelectLiteral(t *testing.T) {
	stmt := parseSelect(t, "SELECT 1+1")
	bc := NewBytecodeCompiler()
	prog, err := bc.CompileSelect(stmt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if prog == nil {
		t.Fatal("nil prog")
	}
	// Execute and verify result
	vm := VM.NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(2) {
		t.Errorf("SELECT 1+1 = %v, want 2", rows[0][0])
	}
}

func TestBytecodeCompiler_SelectMultipleLiterals(t *testing.T) {
	stmt := parseSelect(t, "SELECT 10, 'hello', NULL")
	bc := NewBytecodeCompiler()
	prog, err := bc.CompileSelect(stmt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	vm := VM.NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 || len(rows[0]) != 3 {
		t.Fatalf("unexpected rows: %v", rows)
	}
	if rows[0][0] != int64(10) {
		t.Errorf("col0 = %v, want 10", rows[0][0])
	}
	if rows[0][1] != "hello" {
		t.Errorf("col1 = %v, want hello", rows[0][1])
	}
	if rows[0][2] != nil {
		t.Errorf("col2 = %v, want nil", rows[0][2])
	}
}

func TestBytecodeCompiler_SelectFromTable(t *testing.T) {
	stmt := parseSelect(t, "SELECT n FROM nums")
	bc := NewBytecodeCompiler()
	bc.TableColOrder["nums"] = []string{"n"}
	bc.TableSchemas["nums"] = map[string]string{"n": "INTEGER"}

	prog, err := bc.CompileSelect(stmt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Supply data via a context
	ctx := &testBcCtx{
		rows: []map[string]interface{}{
			{"n": int64(1)},
			{"n": int64(2)},
			{"n": int64(3)},
		},
		cols: []string{"n"},
	}
	vm := VM.NewBytecodeVM(prog, ctx)
	if err := vm.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := vm.ResultRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestBytecodeCompiler_SelectWhereFilter(t *testing.T) {
	stmt := parseSelect(t, "SELECT n FROM nums WHERE n > 1")
	bc := NewBytecodeCompiler()
	bc.TableColOrder["nums"] = []string{"n"}
	bc.TableSchemas["nums"] = map[string]string{"n": "INTEGER"}

	prog, err := bc.CompileSelect(stmt)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx := &testBcCtx{
		rows: []map[string]interface{}{
			{"n": int64(1)},
			{"n": int64(2)},
			{"n": int64(3)},
		},
		cols: []string{"n"},
	}
	vm := VM.NewBytecodeVM(prog, ctx)
	if err := vm.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := vm.ResultRows()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (n>1), got %d", len(rows))
	}
}

// testBcCtx is a minimal BcVmContext for compiler tests.
type testBcCtx struct {
	rows []map[string]interface{}
	cols []string
}

func (c *testBcCtx) GetTableRows(table string) ([]map[string]interface{}, []string, error) {
	return c.rows, c.cols, nil
}
func (c *testBcCtx) GetTableSchema(table string) map[string]string { return nil }
