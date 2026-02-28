package CG

import (
	"testing"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

func TestNewCompiler(t *testing.T) {
	c := NewCompiler()
	if c == nil {
		t.Error("NewCompiler should not return nil")
	}
	if c.program == nil {
		t.Error("program should not be nil")
	}
	if c.ra == nil {
		t.Error("ra should not be nil")
	}
	if c.optimizer == nil {
		t.Error("optimizer should not be nil")
	}
}

func TestOptimizer_New(t *testing.T) {
	o := NewOptimizer()
	if o == nil {
		t.Error("NewOptimizer should not return nil")
	}
}

func TestOptimizer_Optimize(t *testing.T) {
	o := NewOptimizer()

	program := VM.NewProgram()
	program.Emit(VM.OpNoop)
	program.Emit(VM.OpHalt)

	result := o.Optimize(program)
	if result == nil {
		t.Error("Optimize should not return nil")
	}
}

func TestOptimizer_Optimize_Nil(t *testing.T) {
	o := NewOptimizer()

	result := o.Optimize(nil)
	if result != nil {
		t.Error("Optimize(nil) should return nil")
	}
}

func TestOptimizer_Optimize_Empty(t *testing.T) {
	o := NewOptimizer()

	program := VM.NewProgram()
	result := o.Optimize(program)
	if result == nil {
		t.Error("Optimize should not return nil for empty program")
	}
}

func TestDirectCompiler_New(t *testing.T) {
	dc := NewDirectCompiler(func(table string) ([]string, error) {
		return []string{"id", "name"}, nil
	}, nil)
	if dc == nil {
		t.Error("NewDirectCompiler should not return nil")
	}
}

func TestCanFastPath(t *testing.T) {
	tests := []struct {
		sql    string
		expect bool
	}{
		{"SELECT 1", true},
		{"SELECT * FROM t", true},
		{"SELECT id, name FROM t", true},
		{"SELECT id FROM t WHERE id = 1", true},
		{"", false},
		{"INSERT INTO t VALUES(1)", false},
		{"UPDATE t SET id = 1", false},
		{"DELETE FROM t", false},
		{"SELECT * FROM t1 JOIN t2 ON t1.id = t2.id", false},
		{"SELECT * FROM t UNION SELECT * FROM t2", false},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", false},
		// Note: OVER() without space is not currently detected
		{"SELECT row_number() FROM t", true},
	}

	for _, tt := range tests {
		result := canFastPath(tt.sql)
		if result != tt.expect {
			t.Errorf("canFastPath(%q) = %v, want %v", tt.sql, result, tt.expect)
		}
	}
}

func TestIsFastPath(t *testing.T) {
	if !IsFastPath("SELECT 1") {
		t.Error("IsFastPath should return true for SELECT 1")
	}
	if IsFastPath("SELECT * FROM t JOIN t2") {
		t.Error("IsFastPath should return false for JOIN")
	}
}

func TestCompiler_CompileSelect_Literal(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.Literal{Value: 1},
			&QP.Literal{Value: "hello"},
		},
	}

	prog := c.CompileSelect(stmt)
	if prog == nil {
		t.Error("CompileSelect should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}
}

func TestCompiler_CompileSelect_Empty(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{},
	}

	prog := c.CompileSelect(stmt)
	if prog == nil {
		t.Error("CompileSelect should not return nil")
	}
}

func TestHasAggregates(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.Literal{Value: 1}}},
		},
	}

	if !hasAggregates(stmt) {
		t.Error("Should detect aggregate function")
	}

	stmt2 := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.Literal{Value: 1},
		},
	}

	if hasAggregates(stmt2) {
		t.Error("Should not detect aggregate in literal")
	}
}

func TestCompiler_compileLiteral(t *testing.T) {
	c := NewCompiler()

	lit := &QP.Literal{Value: 42}
	reg := c.compileLiteral(lit)
	if reg < 0 {
		t.Errorf("compileLiteral should return valid register, got %d", reg)
	}

	lit2 := &QP.Literal{Value: "test"}
	reg2 := c.compileLiteral(lit2)
	if reg2 < 0 {
		t.Errorf("compileLiteral should return valid register, got %d", reg2)
	}
}

func TestCompiler_compileExpr_Nil(t *testing.T) {
	c := NewCompiler()

	reg := c.compileExpr(nil)
	if reg < 0 {
		t.Errorf("compileExpr(nil) should return valid register, got %d", reg)
	}
}

func TestCompiler_compileExpr_Default(t *testing.T) {
	c := NewCompiler()

	expr := &QP.Literal{Value: nil}
	reg := c.compileExpr(expr)
	if reg < 0 {
		t.Errorf("compileExpr should return valid register, got %d", reg)
	}
}
